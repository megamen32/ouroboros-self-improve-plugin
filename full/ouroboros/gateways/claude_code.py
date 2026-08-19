"""Claude Agent SDK transport for the read-only advisory path.

Callers own orchestration and validation. This layer keeps SDK hooks,
ANTHROPIC_API_KEY auth, bundled CLI resolution, stderr capture, and no
CLI fallback when the SDK is missing. The edit half (`run_edit` /
`claude_code_edit`) was retired by the owner-approved D10 migration
(phase 6.4); delegated coding rides the subagent lane now.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import os
import pathlib
import re
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ouroboros.runtime_mode_policy import (
    SAFETY_CRITICAL_PATHS,
)
from ouroboros.usage_accounting import (
    AttemptRequest,
    UsageAccountingError,
    UsageScope,
    current_usage_scope,
    mark_dispatched,
    mark_unresolved,
    reserve_attempt,
    settle_attempt,
    usage_scope,
)
from ouroboros.utils import resolve_path_allow_missing

log = logging.getLogger(__name__)

# Eager import preserves the no-CLI-fallback install hint path.
from claude_agent_sdk import (  # noqa: E402
    ClaudeAgentOptions, ClaudeSDKClient, HookMatcher,
    AssistantMessage, ResultMessage,
)

_STDERR_MAX_LINES = 200
_stderr_lock = threading.Lock()
_stderr_buffer: collections.deque[str] = collections.deque(maxlen=_STDERR_MAX_LINES)
DEFAULT_CLAUDE_CODE_MAX_TURNS = 50
_READONLY_CHILD_TIMEOUT_SEC = 900

# --- Tool surface (SSOT for both delegated paths) --------------------------
# The allowlist is the only place a tool is granted. Everything else — the CLI
# deny list, the base `tools` set, and the PreToolUse allowlist hook — is
# DERIVED from it, so a tool added by a future CLI/SDK release, or an `mcp__*`
# tool injected by a foreign config, arrives denied instead of enabled.
READONLY_TOOLS = ("Read", "Grep", "Glob")
# Named for the CLI's own deny list (defense in depth behind the closed base
# surface and the hook). `Agent`/`Task` spawn a subagent whose tool surface and
# hooks we do not control, so they are never delegated. Only names the CLI
# actually knows belong here — it warns "matches no known tool" on every run for
# anything else — and a name it does not know is denied structurally anyway, by
# the closed base surface and the allowlist hook.
_DENYABLE_TOOLS = (
    "Agent", "Task", "Bash", "BashOutput", "KillShell", "KillBash",
    "Edit", "Write", "NotebookEdit", "WebFetch", "WebSearch", "Skill",
)
# Path-carrying inputs of the read tools. The KNOWN names are kept only as
# documentation of what the CLI sends today; the guard does not trust the list.
_READ_PATH_FIELDS = ("file_path", "path", "notebook_path", "pattern", "glob")

# Child->parent accounting control lines (prefixed so they can never be mistaken
# for the child's final result JSON, which stays the last stdout line).
_CHILD_ATTEMPT_LINE = "@@ouroboros-attempt "
_CHILD_USAGE_LINE = "@@ouroboros-usage "


def _stderr_callback(line: str) -> None:
    """Store raw CLI stderr for failure diagnostics."""
    log.warning("claude-cli stderr: %s", line)
    with _stderr_lock:
        _stderr_buffer.append(line)


def get_last_stderr(max_chars: int = 4000) -> str:
    """Return recent CLI stderr."""
    with _stderr_lock:
        lines = list(_stderr_buffer)
    if not lines:
        return ""
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text


def clear_stderr_buffer() -> None:
    """Clear captured CLI stderr."""
    with _stderr_lock:
        _stderr_buffer.clear()


SAFETY_CRITICAL = SAFETY_CRITICAL_PATHS


@dataclass
class ClaudeCodeResult:
    """Structured SDK invocation result."""

    success: bool
    result_text: str = ""
    session_id: str = ""
    cost_usd: float = 0.0
    usage: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    stderr_tail: str = ""
    # Populated by callers after invocation.
    changed_files: List[str] = field(default_factory=list)
    diff_stat: str = ""
    validation_summary: str = ""

    def to_tool_output(self) -> str:
        """Return structured JSON for tool output."""
        out: Dict[str, Any] = {
            "success": self.success,
            "result": self.result_text,
        }
        if self.session_id:
            out["session_id"] = self.session_id
        if self.cost_usd:
            out["cost_usd"] = round(self.cost_usd, 6)
        if self.usage:
            out["usage"] = self.usage
        if self.changed_files:
            out["changed_files"] = self.changed_files
        if self.diff_stat:
            out["diff_stat"] = self.diff_stat
        if self.error:
            out["error"] = self.error
        if self.stderr_tail:
            out["stderr_tail"] = self.stderr_tail
        if self.validation_summary:
            out["validation"] = self.validation_summary
        return json.dumps(out, ensure_ascii=False, indent=2)


def _coerce_usage_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_sdk_usage(usage: Any) -> Dict[str, Any]:
    """Map Anthropic token usage names to Ouroboros budget/log keys."""
    if not isinstance(usage, dict):
        return {}
    normalized = dict(usage)
    normalized["prompt_tokens"] = _coerce_usage_int(
        usage.get("prompt_tokens", usage.get("input_tokens", 0))
    )
    normalized["completion_tokens"] = _coerce_usage_int(
        usage.get("completion_tokens", usage.get("output_tokens", 0))
    )
    normalized["cached_tokens"] = _coerce_usage_int(
        usage.get("cached_tokens", usage.get("cache_read_input_tokens", 0))
    )
    normalized["cache_write_tokens"] = _coerce_usage_int(
        usage.get("cache_write_tokens", usage.get("cache_creation_input_tokens", 0))
    )
    return normalized


def _reserve_sdk_attempt(
    prompt: str,
    model: str,
    *,
    max_budget_usd: Optional[float],
    source: str,
):
    reservation = reserve_attempt(AttemptRequest(
        model=str(model or "claude-code"),
        provider="anthropic",
        prompt_tokens_estimate=max(0, len(str(prompt or "")) // 4),
        max_budget_usd=max_budget_usd,
        force_unknown_reservation=max_budget_usd is None,
        source=source,
    ))
    mark_dispatched(reservation)
    return reservation


def _emit_child_control(prefix: str, payload: Dict[str, Any]) -> None:
    """Publish accounting state to the supervising parent (child process only).

    The child owns its reservation, so a kill it cannot catch (timeout, native
    abort) leaves that reservation non-terminal and holding its whole upper
    bound — for advisory that bound IS the remaining budget — against every
    later call. These lines are what the parent needs to close it instead."""
    if os.environ.get("OUROBOROS_CLAUDE_READONLY_CHILD") != "1":
        return
    try:
        sys.stdout.write(prefix + json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    except Exception:
        log.debug("Claude readonly child control line emit failed", exc_info=True)


def _accumulate_usage(totals: Dict[str, int], usage: Any) -> Dict[str, int]:
    """Sum per-turn SDK usage into a running session total."""
    normalized = _normalize_sdk_usage(usage)
    for key in ("prompt_tokens", "completion_tokens", "cached_tokens", "cache_write_tokens"):
        totals[key] = int(totals.get(key, 0)) + _coerce_usage_int(normalized.get(key))
    return totals


def _settle_sdk_attempt(reservation: Any, result: ClaudeCodeResult, reported_cost: Any) -> None:
    result.usage["ledger_attempt_ids"] = [reservation.attempt_id]
    try:
        cost = float(reported_cost) if reported_cost is not None else None
    except (TypeError, ValueError):
        cost = None
    try:
        settle_attempt(
            reservation,
            result.usage,
            cost_usd=cost,
            cost_final=cost is not None,
        )
    except Exception as exc:
        # Preserve an already-paid SDK result and best-effort close unresolved.
        log.exception("Failed to settle Claude SDK attempt %s", reservation.attempt_id)
        try:
            mark_unresolved(reservation, f"settlement_write_failed:{type(exc).__name__}")
        except Exception:
            log.exception("Failed to mark Claude SDK settlement unresolved")


def _claude_options_has_explicit_param(name: str) -> bool:
    import inspect

    try:
        sig = inspect.signature(ClaudeAgentOptions.__init__)
    except (TypeError, ValueError):
        return False
    return name in sig.parameters


def _deny(reason: str) -> dict:
    """Return a PreToolUse deny decision."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def _hook_tool_call(input_data: Any) -> tuple:
    """The ``(tool name, tool inputs)`` a PreToolUse payload carries, in any shape.

    The SDK hands the callback ``request_data.get("input")`` — the CLI's JSON, with no
    default and no type check — so both the payload and its ``tool_input`` are dicts by
    CONVENTION only. Every guard here called ``.get`` on them directly, and one that was
    not a dict raised `AttributeError` out of the callback; the SDK turns that into a
    control-protocol ERROR response, which is not a deny, so the fence delivers no
    decision and the tool runs unfenced. That is the same failure `_resolved()` exists to
    remove, one level further out, so it is answered once, here, for every guard.

    The inputs are returned AS THEY ARRIVED rather than coerced to ``{}``: the read fence
    path-checks whatever it is given, and flattening a bare string payload to an empty
    dict would drop it out of the fence entirely.
    """
    data = input_data if isinstance(input_data, dict) else {}
    return str(data.get("tool_name") or ""), data.get("tool_input")


#: Everything the CLI's `trim()` strips from a tool path before resolving it. `\s` under
#: a str pattern is already the Unicode whitespace set; `﻿` is the one character
#: JavaScript trims and `str.strip()` does not.
_CONSUMER_TRIMMED = re.compile(r"^[\s﻿]+|[\s﻿]+$")


def _resolved(value: Any, base: Optional[pathlib.Path] = None) -> Optional[pathlib.Path]:
    """Turn a tool-input VALUE into a resolved path, or None if it cannot become one.

    The predicate takes the WHOLE question. Handing it an already-built `Path` left the
    failure-prone half — constructing that Path from arbitrary JSON — at each call site,
    where a non-string `file_path` raised `TypeError` straight out of the PreToolUse
    callback. A guard that raises delivers no permission decision at all, which is the
    one outcome a fence must never produce.

    Construction is STRICT: a list, dict, int or bytes is not a path, and coercing it
    with `str()` would fail OPEN by turning `[]` or `42` into a relative name that lands
    inside the working directory and is therefore allowed. Unusable input is `None`, and
    `None` means deny.

    A relative value is joined onto `base` before resolving, so callers cannot get that
    half subtly different from one another either.

    NORMALISATION IS PART OF THE QUESTION, and the answer belongs to the CONSUMER. The
    CLI resolves every tool path through one helper that trims the value and then expands
    a leading `~`; a fence that skips either step judges a path the tool will never use,
    which is the one thing a fence must never do. Both omissions failed OPEN in the same
    direction, because `pathlib` calls a value that does not START with a separator
    relative and joins it onto the working directory:

    * ``" /etc/passwd"`` was confined to ``<cwd>/ /etc/passwd`` while the tool read
      ``/etc/passwd`` — reproduced end to end against the CLI through ``Grep(path=…)``,
      which unlike ``Read``/``Write`` hands the hook its path verbatim.
    * ``"~/.ssh/authorized_keys"`` was confined to ``<cwd>/~/.ssh/…`` while the tool
      targeted ``$HOME``. The read fence had bought this back one layer up, so only the
      WRITE twin was still exposed to it.

    Expansion mirrors the consumer exactly — only a bare ``~`` and a ``~/`` prefix, and
    joined by text rather than by ``/``, which discards the left operand when the tail is
    absolute. Trimming is a superset: it drops everything the CLI's ``trim()`` drops
    (``\\ufeff`` included, which `str.strip()` leaves behind), and the few control
    characters where it goes further only ever make a value MORE absolute, i.e. deny.
    """
    if not isinstance(value, (str, pathlib.PurePath)):
        return None
    try:
        if isinstance(value, str):
            value = _CONSUMER_TRIMMED.sub("", value)
            if value == "~":
                value = str(pathlib.Path.home())
            elif value.startswith("~/"):
                value = f"{pathlib.Path.home()}/{value[2:]}"
        target = pathlib.Path(value)
        if base is not None and not target.is_absolute():
            target = base / target
        return resolve_path_allow_missing(target)
    except (OSError, ValueError, RuntimeError, TypeError):
        return None




def make_tool_allowlist_guard(allowed_tools):
    """Deny every tool outside ``allowed_tools`` (default-deny, all tools matched).

    An enumerated deny list is structurally incomplete: each CLI release may add
    a tool and an MCP server contributes arbitrary ``mcp__*`` names, so anything
    unnamed would arrive ENABLED. Denying by absence keeps the delegated surface
    closed no matter what the runtime offers."""
    permitted = frozenset(str(name) for name in allowed_tools)
    listed = ", ".join(sorted(permitted))

    async def tool_allowlist_guard(input_data: Any, tool_use_id: str, context: Any) -> dict:
        tool_name, _ = _hook_tool_call(input_data)
        if tool_name in permitted:
            return {}
        return _deny(
            f"SAFETY: '{tool_name}' is not allowed in this delegated run. "
            f"Only {listed} are permitted."
        )

    return tool_allowlist_guard


def make_readonly_guard():
    """Deny every tool outside the read-only advisory surface."""
    return make_tool_allowlist_guard(READONLY_TOOLS)


def _path_like_values(tool_input: Any, tool_name: str = ""):
    """Every value in a tool call that could name a filesystem location.

    Confine by VALUE, not by field name: an enumerated field allowlist is the same
    structurally-incomplete shape this module rejects for tool names, and a call whose
    only path rides an unlisted field would simply never be checked.

    This answers WHICH values to check, not what they resolve to. It used to expand `~`
    on its way past, which is how the two fences ended up disagreeing about `~/.ssh/*`:
    the read side got the expansion for free and the write side, calling the shared
    predicate directly, never did. Turning a value into a path is `_resolved`'s whole
    question, so the value is yielded VERBATIM and `_resolved` answers all of it — for
    both fences, and in the words the caller actually sent, which is what a deny quotes.

    Three things this has to get right, each of which it got wrong first:

    * A `~` value has the SHAPE of a path even with no separator in it, so it is caught
      by name rather than by expanding it here.
    * Containers are walked recursively. A path one level down in a dict or a nested
      list is still a path.
    * ``pattern`` means a glob for ``Glob`` and a REGEX for ``Grep``. Path-checking a
      regex denies ``Grep(pattern="/api/v1/users")``, which is an ordinary search for a
      string literal, so the field is only path-like for the tool where it names a path.
    """
    regex_fields = {"pattern"} if str(tool_name) == "Grep" else set()

    def walk(name: str, value: Any):
        if isinstance(value, dict):
            for key, item in value.items():
                yield from walk(str(key), item)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                yield from walk(name, item)
            return
        if not isinstance(value, str) or not value.strip() or name in regex_fields:
            return
        # A KNOWN path field counts whatever it looks like — a bare relative name is
        # still a path. Any OTHER field counts when its value has the shape of one.
        if (
            name in _READ_PATH_FIELDS
            or "/" in value
            or "\\" in value
            or value.strip().startswith("~")
        ):
            yield value

    yield from walk("", tool_input)


def make_read_guard(cwd: str, extra_roots=()):
    """Confine read tools to the workspace (plus any explicit extra root).

    Tool-level reads are governed by permission rules, and an ``allowed_tools``
    entry pre-approves ``Read``/``Grep``/``Glob`` for the WHOLE filesystem — the
    delegated agent could read credentials, other worktrees, or user documents.
    The write guard already confines writes to ``cwd``; this is the same fence on
    the read side (symlink escapes included: both sides are resolved). Edit-mode
    delegation passes the enclosing repo root as an extra READ root: writes stay
    in the work dir while the surrounding project stays readable."""
    roots = [pathlib.Path(cwd).resolve()]
    roots += [pathlib.Path(root).resolve() for root in (extra_roots or ()) if root]

    def _inside(target: pathlib.Path) -> bool:
        for root in roots:
            try:
                target.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    async def read_guard(input_data: Any, tool_use_id: str, context: Any) -> dict:
        tool_name, tool_input = _hook_tool_call(input_data)
        for raw in _path_like_values(tool_input, tool_name):
            resolved = _resolved(raw, roots[0])
            allowed = resolved is not None and _inside(resolved)
            if not allowed:
                return _deny(
                    f"SAFETY: Read blocked — '{raw}' resolves outside the "
                    f"allowed read roots ({', '.join(str(root) for root in roots)})."
                )
        return {}

    return read_guard


def _tool_surface_kwargs(allowed_tools) -> Dict[str, Any]:
    """SDK options that pin the delegated run's ambient trust surface.

    ``setting_sources=[]``: the CLI otherwise loads user/project/local settings
    from the TARGET directory, so a foreign ``.claude/settings.json`` (hooks,
    env, permission rules) would execute inside our run — and ``--print`` skips
    the workspace-trust prompt that normally gates it. ``strict_mcp_config``
    does the same for a foreign ``.mcp.json``. ``tools`` closes the BASE tool
    set instead of only permission-listing it; when the installed SDK predates
    that option the allowlist hook remains the fail-closed enforcement.

    ``setting_sources`` and ``mcp_servers`` are passed unconditionally: both
    exist in every importable ``claude_agent_sdk`` (present at the 0.1.60
    requirements floor; the pre-rename package fails this module's eager import
    outright). ``strict_mcp_config`` is YOUNGER than the floor — 0.1.60 has no
    such field, so passing it unconditionally turned options construction into
    a ``TypeError`` that killed BOTH delegated paths on a supported install. It
    is probed like ``tools``/``effort``, and its omission stays fail-closed:
    ``setting_sources=[]`` already keeps the project ``.mcp.json`` out (MCP
    config loads through the ``project`` settings source), and the allowlist
    hook denies every ``mcp__*`` tool regardless."""
    permitted = list(allowed_tools)
    kwargs: Dict[str, Any] = {
        "allowed_tools": permitted,
        "disallowed_tools": [name for name in _DENYABLE_TOOLS if name not in permitted],
        "setting_sources": [],
        "mcp_servers": {},
    }
    if _claude_options_has_explicit_param("strict_mcp_config"):
        kwargs["strict_mcp_config"] = True
    if _claude_options_has_explicit_param("tools"):
        kwargs["tools"] = list(permitted)
    return kwargs


async def _run_readonly_async(
    prompt: str,
    cwd: str,
    model: str = "opus",
    max_turns: int = DEFAULT_CLAUDE_CODE_MAX_TURNS,
    effort: Optional[str] = "high",
    max_budget_usd: Optional[float] = None,
) -> ClaudeCodeResult:
    """Run read-only advisory SDK with the client lifecycle to avoid stream races."""
    clear_stderr_buffer()
    options_kwargs: Dict[str, Any] = dict(
        cwd=cwd,
        model=model,
        permission_mode="default",  # no auto-approve
        max_turns=max_turns,
        max_budget_usd=max_budget_usd,
        stderr=_stderr_callback,
        hooks={
            "PreToolUse": [
                HookMatcher(hooks=[make_readonly_guard()]),
                HookMatcher(matcher="Read|Grep|Glob", hooks=[make_read_guard(cwd)]),
            ],
        },
        **_tool_surface_kwargs(READONLY_TOOLS),
    )
    if effort is not None:
        # Older SDKs may lack effort; omit it rather than failing advisory.
        import inspect as _inspect
        try:
            _sig = _inspect.signature(ClaudeAgentOptions.__init__)
            if "effort" in _sig.parameters:
                options_kwargs["effort"] = effort
        except (ValueError, TypeError):
            options_kwargs["effort"] = effort

    try:
        options = ClaudeAgentOptions(**options_kwargs)
    except TypeError:
        options_kwargs.pop("effort", None)
        options = ClaudeAgentOptions(**options_kwargs)

    result = ClaudeCodeResult(success=True)
    text_parts: List[str] = []
    accounting = None
    accounting_dispatched = False
    running_usage: Dict[str, int] = {}

    try:
        async with ClaudeSDKClient(options=options) as client:
            accounting = _reserve_sdk_attempt(
                prompt,
                model,
                max_budget_usd=max_budget_usd,
                source="claude_code.readonly",
            )
            accounting_dispatched = True
            _emit_child_control(_CHILD_ATTEMPT_LINE, {
                "attempt_id": accounting.attempt_id,
                "drive_root": str(accounting.drive_root),
                "model": accounting.model,
                "provider": accounting.provider,
                "reservation_upper_bound_usd": accounting.reservation_upper_bound_usd,
            })
            await client.query(prompt)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if hasattr(block, "text") and block.text:
                            text_parts.append(block.text)
                    if any(_accumulate_usage(running_usage, getattr(message, "usage", None)).values()):
                        _emit_child_control(_CHILD_USAGE_LINE, dict(running_usage))
                elif isinstance(message, ResultMessage):
                    result.session_id = getattr(message, "session_id", "") or ""
                    reported_cost = getattr(message, "total_cost_usd", None)
                    result.cost_usd = float(reported_cost or 0)
                    usage = getattr(message, "usage", None)
                    result.usage = _normalize_sdk_usage(usage)
                    _settle_sdk_attempt(accounting, result, reported_cost)
                    accounting_dispatched = False
                    subtype = getattr(message, "subtype", "")
                    if subtype and subtype != "success":
                        result.success = False
                        result.error = f"Agent ended with subtype: {subtype}"
                    elif getattr(message, "is_error", False):
                        # The CLI reports hard API failures (auth/org rejection,
                        # connection death) as subtype="success" with
                        # is_error=True and the error text in `result`. Honor
                        # is_error so these surface as transport failures
                        # (ADVISORY_ERROR / status="error"), not parse_failure.
                        result.success = False
                        error_text = str(getattr(message, "result", "") or "").strip()
                        result.error = (
                            "CLI reported an error result (is_error=true): "
                            + (error_text[:500] or "(no error text)")
                        )
                    break
            if accounting_dispatched:
                mark_unresolved(accounting, "Claude SDK stream ended without ResultMessage")
                accounting_dispatched = False
    except UsageAccountingError:
        raise
    except Exception as e:
        if accounting is not None and accounting_dispatched:
            try:
                mark_unresolved(accounting, f"{type(e).__name__}: {e}")
            except Exception:
                log.exception("Failed to mark Claude SDK readonly attempt unresolved")
        result.success = False
        result.error = f"{type(e).__name__}: {e}"

    if not result.success:
        result.stderr_tail = get_last_stderr()
    result.result_text = "\n".join(text_parts) if text_parts else "(no output)"
    return result


def _run_async(coro):
    """Run async SDK code from synchronous tool handlers."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.run(coro)
    else:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()


def _result_from_dict(data: Dict[str, Any]) -> ClaudeCodeResult:
    """Rehydrate a child-process result without trusting the JSON shape."""
    result = ClaudeCodeResult(success=bool(data.get("success")))
    result.result_text = str(data.get("result_text") or data.get("result") or "")
    result.session_id = str(data.get("session_id") or "")
    result.cost_usd = float(data.get("cost_usd") or 0.0)
    usage = data.get("usage")
    result.usage = dict(usage) if isinstance(usage, dict) else {}
    result.error = str(data.get("error") or "")
    result.stderr_tail = str(data.get("stderr_tail") or "")
    result.changed_files = list(data.get("changed_files") or [])
    result.diff_stat = str(data.get("diff_stat") or "")
    result.validation_summary = str(data.get("validation_summary") or "")
    return result


def _parse_child_stdout(stdout: Any) -> tuple:
    """Split child stdout into (attempt record, last usage totals, plain output)."""
    attempt: Optional[Dict[str, Any]] = None
    usage: Dict[str, Any] = {}
    plain: List[str] = []
    for line in str(stdout or "").splitlines():
        for prefix, sink in ((_CHILD_ATTEMPT_LINE, "attempt"), (_CHILD_USAGE_LINE, "usage")):
            if line.startswith(prefix):
                try:
                    parsed = json.loads(line[len(prefix):])
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    if sink == "attempt":
                        attempt = parsed
                    else:
                        usage = parsed
                break
        else:
            plain.append(line)
    return attempt, usage, "\n".join(plain)


def _settle_abandoned_child(attempt: Any, usage: Dict[str, Any], reason: str) -> None:
    """Terminalize the reservation a killed advisory child could not close."""
    if not isinstance(attempt, dict) or not str(attempt.get("attempt_id") or ""):
        return
    try:
        from ouroboros.usage_accounting import (
            AttemptReservation,
            terminalize_abandoned_attempt,
        )

        terminalize_abandoned_attempt(
            AttemptReservation(
                attempt_id=str(attempt.get("attempt_id")),
                drive_root=pathlib.Path(str(attempt.get("drive_root") or ".")),
                model=str(attempt.get("model") or ""),
                provider=str(attempt.get("provider") or "anthropic"),
                reservation_upper_bound_usd=attempt.get("reservation_upper_bound_usd"),
            ),
            reason=reason,
            usage=usage,
        )
    except Exception:
        log.exception("Failed to terminalize abandoned Claude readonly attempt")


def _run_readonly_out_of_process(
    prompt: str,
    cwd: str,
    model: str,
    max_turns: int,
    effort: Optional[str],
    max_budget_usd: Optional[float] = None,
) -> ClaudeCodeResult:
    """Run advisory SDK in a child process so native aborts cannot kill workers."""
    payload = {
        "prompt": prompt,
        "cwd": cwd,
        "model": model,
        "max_turns": max_turns,
        "effort": effort,
        "max_budget_usd": max_budget_usd,
    }
    active_scope = current_usage_scope()
    if active_scope is not None:
        scope_payload = dict(vars(active_scope))
        if scope_payload.get("drive_root") is not None:
            scope_payload["drive_root"] = str(scope_payload["drive_root"])
        payload["usage_scope"] = scope_payload
    env = dict(os.environ)
    env["OUROBOROS_CLAUDE_READONLY_CHILD"] = "1"
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    pythonpath = env.get("PYTHONPATH", "")
    if str(repo_root) not in pythonpath.split(os.pathsep):
        env["PYTHONPATH"] = str(repo_root) + (os.pathsep + pythonpath if pythonpath else "")
    try:
        from ouroboros.platform_layer import subprocess_new_group_kwargs

        group_kwargs = subprocess_new_group_kwargs()
    except Exception:
        group_kwargs = {}
    cmd = [sys.executable, "-m", "ouroboros.gateways.claude_code", "--readonly-child"]
    try:
        from ouroboros.platform_layer import kill_process_tree

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
            env=env,
            text=True,
            **group_kwargs,
        )
        try:
            stdout, stderr = proc.communicate(
                input=json.dumps(payload, ensure_ascii=False),
                timeout=_READONLY_CHILD_TIMEOUT_SEC,
            )
        except subprocess.TimeoutExpired:
            kill_process_tree(proc)
            stdout, stderr = proc.communicate(timeout=10)
            timeout_error = f"Claude readonly child timed out after {_READONLY_CHILD_TIMEOUT_SEC}s"
            attempt, usage, plain = _parse_child_stdout(stdout)
            _settle_abandoned_child(attempt, usage, timeout_error)
            return ClaudeCodeResult(
                success=False,
                result_text="(no output)",
                error=timeout_error,
                stderr_tail=((plain or "") + (stderr or ""))[-4000:],
            )
    except subprocess.TimeoutExpired as exc:
        timeout_error = f"Claude readonly child timed out after {_READONLY_CHILD_TIMEOUT_SEC}s"
        attempt, usage, plain = _parse_child_stdout(exc.stdout)
        _settle_abandoned_child(attempt, usage, timeout_error)
        return ClaudeCodeResult(
            success=False,
            result_text="(no output)",
            error=timeout_error,
            stderr_tail=((plain or "") + (exc.stderr or ""))[-4000:],
        )
    except Exception as exc:
        return ClaudeCodeResult(
            success=False,
            result_text="(no output)",
            error=f"Claude readonly child failed to start: {type(exc).__name__}: {exc}",
        )

    attempt, usage, stdout = _parse_child_stdout(stdout)
    stdout = stdout.strip()
    stderr = (stderr or "").strip()
    if proc.returncode == 0 and stdout:
        try:
            return _result_from_dict(json.loads(stdout.splitlines()[-1]))
        except Exception as exc:
            error = f"Claude readonly child returned invalid JSON: {type(exc).__name__}: {exc}"
            _settle_abandoned_child(attempt, usage, error)
            return ClaudeCodeResult(
                success=False,
                result_text="(no output)",
                error=error,
                stderr_tail=stderr[-4000:],
            )

    sig = ""
    if int(proc.returncode or 0) < 0:
        try:
            sig = signal.Signals(-int(proc.returncode)).name
        except (ValueError, TypeError):
            sig = {6: "SIGABRT"}.get(-int(proc.returncode), f"signal {-int(proc.returncode)}")
    error = f"Claude readonly child exited with code {proc.returncode}"
    if sig:
        error = f"Claude readonly child terminated by {sig} (code {proc.returncode})"
    _settle_abandoned_child(attempt, usage, error)
    return ClaudeCodeResult(
        success=False,
        result_text=stdout or "(no output)",
        error=error,
        stderr_tail=stderr[-4000:],
    )


def resolve_claude_code_model(default: str = "opus[1m]") -> str:
    """Return the env/settings Claude Code model, aligned with config defaults."""
    return os.environ.get("CLAUDE_CODE_MODEL", default).strip() or default


def run_readonly(
    prompt: str,
    cwd: str,
    model: str = "opus[1m]",
    max_turns: int = DEFAULT_CLAUDE_CODE_MAX_TURNS,
    effort: Optional[str] = "high",
    max_budget_usd: Optional[float] = None,
) -> ClaudeCodeResult:
    """Synchronous read-only advisory entry point."""
    if os.environ.get("OUROBOROS_CLAUDE_READONLY_CHILD") == "1":
        return _run_async(_run_readonly_async(
            prompt=prompt,
            cwd=cwd,
            model=model,
            max_turns=max_turns,
            effort=effort,
            max_budget_usd=max_budget_usd,
        ))
    return _run_readonly_out_of_process(
        prompt=prompt,
        cwd=cwd,
        model=model,
        max_turns=max_turns,
        effort=effort,
        max_budget_usd=max_budget_usd,
    )


def _main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "--readonly-child":
        try:
            from ouroboros.process_custody import start_parent_lifeline

            start_parent_lifeline(label="claude-readonly-child")
        except Exception:
            pass
        try:
            payload = json.loads(sys.stdin.read() or "{}")
        except Exception as exc:
            print(json.dumps({
                "success": False,
                "result_text": "(no output)",
                "error": f"invalid child payload: {type(exc).__name__}: {exc}",
            }, ensure_ascii=False), flush=True)
            return 2
        data = payload if isinstance(payload, dict) else {}
        raw_scope = data.get("usage_scope")
        try:
            restored_scope = UsageScope(**raw_scope) if isinstance(raw_scope, dict) else None
            max_budget_usd = (
                float(data["max_budget_usd"])
                if data.get("max_budget_usd") is not None
                else None
            )
        except (TypeError, ValueError) as exc:
            print(json.dumps({
                "success": False,
                "result_text": "(no output)",
                "error": f"invalid child accounting payload: {type(exc).__name__}: {exc}",
            }, ensure_ascii=False), flush=True)
            return 2
        scope_context = (
            usage_scope(restored_scope)
            if restored_scope is not None
            else contextlib.nullcontext()
        )
        with scope_context:
            result = _run_async(_run_readonly_async(
                prompt=str(data.get("prompt") or ""),
                cwd=str(data.get("cwd") or "."),
                model=str(data.get("model") or "opus[1m]"),
                max_turns=int(data.get("max_turns") or DEFAULT_CLAUDE_CODE_MAX_TURNS),
                effort=data.get("effort"),
                max_budget_usd=max_budget_usd,
            ))
        print(json.dumps(result.__dict__, ensure_ascii=False), flush=True)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
