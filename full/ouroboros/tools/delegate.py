"""Nanny tools: run a subagent's cognition on an already-paid subscription session.

A delegated subagent is an ORDINARY Ouroboros subagent acting as a NANNY: it lives in
the task tree with its own deadline and authority, but instead of thinking on metered
API tokens it starts a Claudexor run, watches it, and brings the result home. Because
the nanny IS the host, verification receipts stay host-authored and the harness's
output is a claim, not proof.

Four verbs: ``delegate_start``, a time-bounded ``delegate_wait``,
``delegate_cancel``, and ``delegate_answer`` (a run's pending interactive question is
answered by its own nanny — owner decision 7=A, poltergeist phase B). There is still
no ``hurry`` — Claudexor's only control verb is ``cancel``, and cancelling a reviewer
destroys the verdict you wanted.

Read-only and mutating children share ONE nanny and ONE transport. The only difference
is the access profile the HOST derives from the calling task's authority (``readonly``
vs ``workspace_write``) and the run shape that follows from it; there is no second
pipeline and no second slot. The child gets a broker tool, never a shell, so it can ask
the host to run something but never choose with what powers.

Custody: the daemon token never leaves ``gateways.claudexor``; nothing here puts it in
a ToolContext, a child environment, or a harness sandbox. WHICH run belongs to WHICH
task is decided by ``ouroboros.delegate_custody`` against the durable event log, not by
a dict this process happens to still hold.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ouroboros import delegate_custody as custody
from ouroboros import delegate_progress as progress
from ouroboros.delegate_custody import RunCustody as _RunCustody
from ouroboros.tool_capabilities import tool_result_limit
from ouroboros.tools.registry import ToolContext, ToolEntry
from ouroboros.utils import truncate_within_limit
# The staged-output + read-receipt cluster lives in its own module (size gate);
# re-exported here because sibling code, the tests and the convergence census all
# name it on THIS surface, and `_READ_COVERAGE` must stay the same object.
from ouroboros.delegate_output import (  # noqa: F401
    _ARTIFACT_SUBDIR,
    _BULK_FIELDS,
    _PAYLOAD_ENVELOPE_HEADROOM,
    _PREVIEW_PREFIX_SLACK,
    _PREVIEW_STEPS,
    _READ_COVERAGE,
    _READ_COVERAGE_MAX_KEYS,
    _STRUCTURED_FIELDS,
    _covered_whole,
    _preview_payload,
    _resolve_full_primary_output,
    _safe_run_filename,
    _stage_full_output,
    acknowledge_staged_output_read,
)
# The interactive-question cluster (waiting_on_user + delegate_answer) lives in
# its own module too (size gate); re-exported here because the wait loop, the
# tests and sibling code name it on THIS surface, and `_REPORTED_INTERACTIONS`
# must stay the same object.
from ouroboros.delegate_interactions import (  # noqa: F401
    _ANSWER_NOTES,
    _REPORTED_INTERACTIONS,
    _answer_delivery_unknown,
    _bounded_interactions,
    _delegate_answer,
    _interactions_are_news,
    _normalized_answers,
    _waiting_on_user_payload,
)
# The refusal/emit/ownership helpers live in the neutral leaf
# `ouroboros/delegate_shared.py` (moved to break the facade back-edge:
# delegate_interactions needs them, and an extracted module never imports the
# facade back); re-exported here because sibling code, the tests and
# monkeypatch targets name them on THIS surface.
from ouroboros.delegate_shared import (  # noqa: F401
    _emit,
    _fail,
    _owned_run,
)
# The C1 integration seam (mutation authority, execution snapshots, retry binding,
# terminal patch capture) lives in its own module (size gate); re-exported here
# (same objects) because sibling code and the tests address it on THIS surface.
# `_fail` is NOT re-imported from it — the one shared refusal author is
# `delegate_shared._fail`, which delegate_integration itself imports.
from ouroboros.tools.delegate_integration import (  # noqa: F401
    _CAPTURE_DELEGATED_SNAPSHOT,
    _capture_block,
    _capture_terminal_patch,
    _mutation_authority,
    _provision_snapshot,
    _resolve_retry_invocation,
    _resolved,
    _retry_binding_refusal,
    _validated_invocation,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ouroboros.subagents import DelegatedRunShape

log = logging.getLogger(__name__)

_TERMINAL_STATES = custody.TERMINAL_STATES

# The containment verifiers moved to `ouroboros/delegate_containment.py` whole (the
# module-size gate); re-exported here because the nanny's seams and the existing
# tests address them through this module.
from ouroboros.delegate_containment import (  # noqa: E402
    _ACCESS_UNVERIFIED,  # noqa: F401  (re-export: tests address it through this module)
    _Breach,
    _home_isolation_breach,
    _widened_access,
    home_nested_under_operator_home,
)
_POLL_INTERVAL_SEC = 3.0
# Claudexor's own schema bound on maxSeconds (packages/schema/src/control.ts).
_CLAUDEXOR_MAX_SECONDS = 604_800

# The process-local memo of the durable custody rows (the authority lives in the module
# above); re-bound here because sibling code and tests name it on this surface.
_CUSTODY = custody._CUSTODY



# Layered onto every lane by Claudexor (native system-prompt channel per harness, so no
# dialect here). It states the SAME prohibitions an ordinary subagent carries — the
# delegated child is a worker inside the nanny's worktree, not a second committer. It is
# a statement, not the enforcement: the enforcement is the access profile plus the
# nanny's own workspace-patch capture, which invalidates itself if HEAD moved.
_HOST_INSTRUCTIONS = (
    "You are a delegated worker running inside another agent's working tree. Your "
    "authority is everything INSIDE this root and nothing outside it. Do not run git "
    "commit, tag, push, rebase, reset or any other history-moving command: your host "
    "takes the diff of this tree and integrates it itself, and a moved HEAD invalidates "
    "that diff and destroys your work. Do not review or accept your own change, do not "
    "touch the host's runtime controls, skills, or memory, and do not write outside "
    "this root. If your environment offers a way to ask your host a clarifying "
    "question, you may use it: your host may answer from its task context; a question "
    "that carries an engine expiry times out benignly if unanswered — continue with "
    "stated assumptions rather than blocking — while one without an expiry waits until "
    "answered. If your harness cannot ask mid-run, do NOT end the run to ask — "
    "state your assumption and continue."
)

# DESTINATION 2 of the disclosure (AGENTS.md "Disclose instead of forbid": the durable
# record, the CHILD'S PROMPT, and the parent's result). A mutating delegated child is the
# only lane that asks for an OS boundary, and the boundary is a REQUEST: the engine applies
# one where it has a mechanism for this host and applies none where it does not, and no
# version number distinguishes the two. The child therefore cannot be told at start that it
# IS confined — nothing at start knows — so it is told the only true thing, which is also
# the useful one: behave as though nothing is stopping you, and do not claim in your answer
# an isolation you cannot show. What was actually applied reaches the parent afterwards,
# from the run's own artifacts, through `_containment_evidence`.
_UNPROVEN_BOUNDARY_INSTRUCTION = (
    " An OS-enforced filesystem boundary was REQUESTED for this run but is NOT guaranteed: "
    "your engine applies one only where it has a mechanism for this host, and your host "
    "reads back from your own attempt records what was actually applied. Work as if there "
    "is no boundary — stay inside this root, do not read the operator's home directory, "
    "credential stores, or the harness runtime tree, and do NOT describe yourself in your "
    "answer as sandboxed or confined. If your own environment shows you whether a boundary "
    "was in force, say so plainly."
)


# Per-field bound on the contract text that rides the host instructions: the
# objective/expected_output of a task contract are prose, and an unbounded field
# would let one verbose contract dominate the run's system-prompt channel.
_ASSIGNMENT_FIELD_CHARS = 4_000


def _assignment_instructions(ctx: ToolContext) -> str:
    """The nanny's own objective/expected_output, riding STRUCTURALLY with the run.

    From the IMMUTABLE task contract (delegation-first economics, poltergeist phase
    B): the child's goal reaches the delegated run in the host-authored
    ``instructions``, so the nanny does not have to copy its contract into every
    ``prompt`` it writes — the prompt states the specific assignment, and this block
    states the task it serves. Host-authored and host-read: the model cannot widen
    or forge it, and a missing contract simply contributes nothing.
    """
    contract = getattr(ctx, "task_contract", None)
    if not isinstance(contract, dict) or not contract:
        meta = getattr(ctx, "task_metadata", {})
        meta = meta if isinstance(meta, dict) else {}
        raw = meta.get("task_contract")
        contract = raw if isinstance(raw, dict) else {}
    objective = str(contract.get("objective") or "").strip()
    expected = str(contract.get("expected_output") or "").strip()
    parts: List[str] = []
    # STRICT bound with the marker INSIDE the budget (F11/sol #9): the generic
    # preview helper's anti-waste floor let a 4050-char field through whole
    # against a 4000 budget, and its marker landed BEYOND the limit — right for
    # display previews, wrong for a bounded prompt-channel field.
    if objective:
        parts.append(
            "HOST TASK OBJECTIVE (the immutable contract of the task hosting this "
            "run — your prompt below is one assignment inside it): "
            + truncate_within_limit(objective, _ASSIGNMENT_FIELD_CHARS)
        )
    if expected:
        parts.append(
            "HOST EXPECTED OUTPUT: "
            + truncate_within_limit(expected, _ASSIGNMENT_FIELD_CHARS)
        )
    return "\n\n".join(parts)


def _host_instructions(authority: "DelegatedRunShape", assignment: str = "") -> str:
    """The system-prompt text this run's shape earns. One builder, no dialect.

    ``assignment`` is the host-authored contract block (``_assignment_instructions``);
    appended last so the prohibitions stay the opening statement.
    """
    text = _HOST_INSTRUCTIONS
    if authority.delegated:
        text += _UNPROVEN_BOUNDARY_INSTRUCTION
    if assignment:
        text += "\n\n" + assignment
    return text


def _derive_authority(ctx: ToolContext) -> "DelegatedRunShape":
    """Derive the run shape from the task's own authority — one question, asked here.

    Host-derived, never model-supplied: the child asks the host to run something, and
    the host decides with what powers. Ouroboros asks for an access PROFILE and lets
    Claudexor pick the mechanism (fs sandbox, tool allowlist, ...) — no harness branch.

    The SHAPE itself belongs to ``subagents.delegated_run_shape``, which the dispatcher
    also reads: this function only answers "does this task hold a mutating surface",
    which is the one part that needs the live ``ToolContext``. Two authorities qualify
    (B5, owner 2=A): an ACTING CHILD with a valid write surface, and the ROOT of an
    EXTERNAL-WORKSPACE task — the root already holds write+shell inside the project,
    so its delegated runs carry the same mutating shape, bounded by the same
    workspace; ``_mutation_authority`` (``tools.delegate_integration``) validates the
    concrete target either way.
    """
    from ouroboros.subagents import delegated_run_shape
    from ouroboros.tool_access import active_tool_profile

    profile = active_tool_profile(ctx)
    return delegated_run_shape(profile in ("acting_subagent", "external_workspace_task"))


def _containment_breach(detail: Dict[str, Any], authority: "DelegatedRunShape") -> Optional[_Breach]:
    """Everything the ENGINE enforced, checked against what the host asked for.

    ONE reader for both halves of containment — the access profile and the harness
    HOME — because they fail identically: the request is only a request, the engine
    derives the truth, and a verification written for one half leaves the other
    trusting an echo. The HOME half is asked only of a run that carried the marker;
    a read-only child is scoped by Claudexor's ordinary envelope and asks for nothing.
    """
    widened = _widened_access(detail, authority.access)
    if widened:
        return _Breach(
            "access_profile_widened",
            f"The delegated run was enforced at access profile {widened!r} while this "
            f"task is only entitled to {authority.access!r}.",
            {"entitled_access": authority.access, "effective_access": widened},
        )
    if authority.delegated:
        return _home_isolation_breach(detail)
    return None


_NESTED_HOME_NOTE = (
    "The scoped harness HOME for this run sits INSIDE the operator's own home, which is "
    "where the engine roots its scoped homes. That is allowed and the run's work is usable, "
    "but it is not isolation from the operator's home: everything there — credential stores "
    "and the Claudexor daemon token included — stays readable at its absolute path. Do NOT "
    "describe this run as running in an isolated home"
)

_NO_BOUNDARY_NOTE = (
    "NO OS-ENFORCED BOUNDARY was applied to this run. The engine reported no confinement "
    "mechanism for it, so the only containment it had is a scoped HOME — a redirect of "
    "`~`-relative lookups, which leaves the operator's home, credential stores and the "
    "Claudexor daemon token readable at their absolute paths. The run was allowed and its "
    "work is usable; do NOT describe it as sandboxed, confined or isolated, and weigh its "
    "output as coming from an unconfined shell in this worktree"
)


def _containment_evidence(detail: Dict[str, Any]) -> Dict[str, Any]:
    """What the ARTIFACTS prove about this run's containment — never what was asked.

    DESTINATION 3 of the disclosure: this is what the nanny hands its parent.

    BOTH halves, in one reader, because a report that states only the scoped HOME is the
    defect this function was rewritten to remove: a run with a kernel-enforced boundary
    and a run with none produced BYTE-IDENTICAL evidence here, both reading
    ``verified: true`` with a note about the HOME. Claudexor's own confinement document
    says the scoped home "is not a boundary and must never be reported as one".

    The predicate is what the engine says it APPLIED (``confinement_mechanism`` plus the
    denied path it proved), never which OS this host is. Ouroboros does not know what the
    engine did — only the artifact does — and a platform test would additionally freeze
    today's answer: the day a boundary ships for another OS, this reader is already right.

    Judged by the SAME predicate that halts a breached run, not by having been reached
    after it: a report whose honesty depends on its call site is one refactor away from
    claiming a containment nobody checked.

    This is also where a MISSING fact lands, because it is a reporting question and not an
    enforcement one: an attempt that disclosed nothing proves nothing, so ``verified``
    stays false and ``disclosed`` says how much of the run is actually covered. Silence
    read as success and silence enforced as a fault are the two ways to be wrong here,
    and stating the count avoids both.
    """
    from ouroboros.gateways.claudexor import attempt_containment

    attempts = attempt_containment(str(custody.summary_of(detail).get("runDir") or ""))
    disclosed = sum(1 for attempt in attempts if attempt.home_isolated is not None)
    # An engine that reported nothing is indistinguishable from one that applied nothing,
    # and the mechanisms the ATTEMPTS name are the vocabulary — Ouroboros keeps no list of
    # its own to fall out of date. "Every attempt" and not "any": one unconfined attempt
    # is an unconfined run.
    mechanisms = sorted({attempt.boundary_mechanism for attempt in attempts})
    boundary = mechanisms[0] if attempts and len(mechanisms) == 1 and mechanisms[0] else ""
    # A3: the engine's own typed reason for a missing boundary — an AMPLIFIER of
    # the unconfined disclosure (why there is no mechanism on this host), parsed
    # from the same attempt artifact. Telemetry only, never an admission token.
    unavailable_reasons = sorted({
        attempt.confinement_unavailable_reason
        for attempt in attempts if attempt.confinement_unavailable_reason
    })
    # A3: a scoped home NESTED under the operator's own is allowed (the engine's
    # own layout — disclosed, never refused), but it is NOT "outside the
    # operator's own": the daemon token stays reachable at its absolute path.
    # Recorded on the report and honoured by every branch below, so a run that
    # ALSO carries an OS boundary can no longer be promoted to verified with a
    # note that contradicts its own artifact — and so `_record_containment` keeps
    # emitting the durable unconfined row for it.
    nested = home_nested_under_operator_home(detail)
    report = {"verified": False, "attempts": len(attempts), "disclosed": disclosed,
              "os_boundary": boundary, "nested_under_operator_home": nested}
    if unavailable_reasons:
        report["confinement_unavailable_reason"] = "; ".join(unavailable_reasons)
    breach = _home_isolation_breach(detail)
    if breach is not None:
        return {**report, "note": breach.detail}
    if not disclosed:
        return {**report, "note":
                "this run recorded no harness-HOME fact, so its confinement is UNPROVEN "
                "— do not report it as isolated"}
    if disclosed < len(attempts):
        return {**report, "note":
                "not every attempt of this run recorded a harness-HOME fact, so its "
                "confinement is UNPROVEN — do not report it as isolated"}
    if nested:
        note = _NESTED_HOME_NOTE
        if boundary:
            note += (
                f" (an {boundary} boundary WAS applied — weigh it as the real containment, "
                "but the scoped HOME is not one)"
            )
        if unavailable_reasons:
            note += " (engine-declared reason: " + "; ".join(unavailable_reasons) + ")"
        return {**report, "note": note}
    if not boundary:
        note = _NO_BOUNDARY_NOTE
        if unavailable_reasons:
            note += (
                " (engine-declared reason: " + "; ".join(unavailable_reasons) + ")"
            )
        return {**report, "note": note}
    return {**report, "verified": True, "note":
            f"every attempt recorded a scoped harness HOME outside the operator's own AND "
            f"an applied {boundary} boundary, proven against a path it denies"}


def _terminal_payload(run_id: str, detail: Dict[str, Any],
                      authority: "DelegatedRunShape") -> Dict[str, Any]:
    summary = custody.summary_of(detail)
    payload = {
        "status": "terminal",
        "run_id": run_id,
        "state": str(summary.get("state") or ""),
        # The APPLIED model, from the engine's own summary — '' when the run
        # never disclosed one (live unpinned runs really do), shown as absence
        # rather than the requested model dressed up as the applied one.
        "model": str(summary.get("model") or ""),
        "outcome_banner": detail.get("outcomeBanner"),
        "outcome_facts": summary.get("outcomeFacts"),
        "output_conformance": summary.get("outputConformance"),
        "final_summary": detail.get("finalSummary"),
        "primary_output": detail.get("primaryOutput"),
        "failure": summary.get("failure"),
        "last_seq": int(detail.get("lastSeq") or 0),
        "cost": _reported_cost(summary),
        # The ACCESS half of the same honesty, on EVERY terminal payload — see
        # `_access_evidence`. Both lanes: `readonly` staying `readonly` is the profile
        # that matters most, while `containment` is asked only of marker-carrying runs.
        "access_evidence": _access_evidence(detail, authority.access),
    }
    if authority.delegated:
        payload["containment"] = _containment_evidence(detail)
    facts = payload.get("outcome_facts")
    if isinstance(facts, dict) and str(facts.get("reason") or "") == "input_required":
        # The codex-shaped question (B4): that lane has no mid-run channel, so a
        # question arrives as this TERMINAL. There is deliberately NO rerun verb
        # here — the engine's rerun_with_feedback would start a run outside this
        # task's custody trail — so the honest answer path is a plain new start.
        payload["input_required_note"] = (
            "This run ended NEEDING INPUT (outcome_facts.reason=input_required — "
            "see outcome_facts.work_state.required_inputs). Its harness has no "
            "mid-run question channel, so the question arrives as this terminal. Answer it by "
            "starting a plain NEW delegate_start whose prompt carries the original "
            "assignment plus the answers; custody of the new run stays with you. "
            "Do not look for a rerun/decision verb — none exists on this surface."
        )
    return payload


def _access_evidence(detail: Dict[str, Any], expected: str) -> Dict[str, Any]:
    """What the engine's own DERIVED profile proves about this finished run.

    ``effectiveAccess`` is the only witness: ``summary["access"]`` is computed as
    ``effectiveAccess ?? the client's own request``, so reading it compares the request
    against itself and always passes. A WIDER profile is already a breach before this
    runs; an ABSENT one cannot be enforced on a run that is over — cancelling a
    succeeded run to punish missing evidence would destroy the result the lane exists
    to fetch (the v6.87.37 lesson) — so it is named here instead.
    """
    summary = custody.summary_of(detail)
    effective = str(summary.get("effectiveAccess") or "")
    state = str(summary.get("state") or "")
    report = {"requested": expected, "effective": effective,
              "verified": bool(effective), "state": state}
    if effective:
        return report
    if state in custody.SUCCEEDED_STATES:
        return {**report, "note":
                "this run SUCCEEDED without ever disclosing an effective access "
                f"profile, so there is no evidence the engine enforced {expected!r} — "
                "do not report its containment as verified"}
    return {**report, "note":
            "no effective access profile was disclosed; a run that did not succeed may "
            "never have had one, so this is absence of evidence, not a breach"}


def _record_containment(ctx: ToolContext, entry: Optional[_RunCustody],
                        payload: Dict[str, Any]) -> None:
    """DESTINATION 1 of the disclosure: the durable record, written once per run.

    A missing boundary is not a fault and produces no refusal, which is exactly why it
    needs a durable line of its own — the run succeeds, its patch is integrated, and
    nothing else in the record would ever say the work came out of an unconfined shell.
    Emitted from what the PARENT was told, so the two cannot disagree.

    "Once per run" is now a DURABLE fact rather than a process-local one: the custody
    entry is replayed from the event log, so a restarted worker polling an already
    terminal run does not append a second identical finding.

    A NESTED scoped home is disclosed even when an OS boundary WAS recorded (A3):
    the boundary is real containment, the scoped home is not, and suppressing the
    row for that shape left the one durable line that says "this ran with the
    operator's home reachable" unwritten.
    """
    containment = payload.get("containment")
    if not isinstance(containment, dict):
        return
    if containment.get("os_boundary") and not containment.get("nested_under_operator_home"):
        return
    if entry is not None and entry.containment_disclosed:
        return
    _emit(ctx, custody.UNCONFINED, {
        "run_id": entry.run_id if entry is not None else "",
        "route": entry.route_id if entry is not None else "",
        "state": str(payload.get("state") or ""),
        "os_boundary": str(containment.get("os_boundary") or ""),
        "attempts": containment.get("attempts"),
        "home_disclosed": containment.get("disclosed"),
        "nested_under_operator_home": bool(containment.get("nested_under_operator_home")),
        "note": containment.get("note"),
        **({"confinement_unavailable_reason": containment["confinement_unavailable_reason"]}
           if containment.get("confinement_unavailable_reason") else {}),
    })
    if entry is not None:
        entry.containment_disclosed = True


def _reported_cost(summary: Dict[str, Any]) -> Dict[str, Any]:
    """What this run cost, as the AGENT will read it.

    This is the payload the nanny relays to its parent, so it must tell the same story
    the ledger does. It used to hardcode `$0.00 / final` — the exact shape the settlement
    fix was written to eliminate — so a run that really charged money settled honestly in
    the ledger and then told the reasoning path the work was free.
    """
    spend, estimated = custody.disclosed_spend(summary)
    if spend is None:
        return {
            "cost_usd": None,
            "cost_final": False,
            "note": "the harness disclosed no spend for this run; treat the cost as UNKNOWN, not zero",
        }
    if estimated:
        # The amount is the best fact anyone has, so it rides; the FINALITY does not. An
        # estimated zero is not a proven free session and an estimated charge is not a
        # closed book — both are `cost_final: False`, matching the ledger row exactly.
        return {
            "cost_usd": spend,
            "cost_final": False,
            "note": "the harness ESTIMATED this run's spend rather than settling it; treat "
                    "the amount as APPROXIMATE and the cost as NOT final",
        }
    if spend > 0:
        return {
            "cost_usd": spend,
            "cost_final": True,
            "note": "this run was BILLED — it did not ride the subscription",
        }
    return {
        "cost_usd": 0.0,
        "cost_final": True,
        "note": "subscription session — already paid; the nanny's own model calls are metered separately",
    }


# -- output delivery -----------------------------------------------------------


def _delivered_terminal_payload(ctx: ToolContext, run_id: str, detail: Dict[str, Any],
                                authority: "DelegatedRunShape",
                                entry: Optional[_RunCustody] = None,
                                gateway: Any = None) -> Dict[str, Any]:
    """The terminal payload, delivered whole or declared partial — never head-cut.

    ``final_summary``/``primary_output`` carry the run's real work product, and Claudexor
    returns a preview of up to 256 KiB. Outer truncation would head-cut that at the tool
    result limit and sever the JSON mid-string, which destroys the document rather than
    shortening it. So the payload bounds ITSELF against the same limit the truncator
    applies, and the remainder becomes a readable artifact — after the engine's bounded
    preview has been resolved to the verified full artifact, because a payload built on
    a truncated preview delivers 256 KiB wearing the whole result's name.
    """
    full = _terminal_payload(run_id, detail, authority)
    # Requested-vs-applied model, the review lane's own lexicon and rule
    # (AgentSessionReviewExecutor): compared only when BOTH are non-empty —
    # the engine writes aliases ('sonnet' beside 'claude-opus-5'), so a
    # mismatch is an advisory disclosure, never a failure of the run.
    requested_model = str(getattr(entry, "model", "") or "") if entry is not None else ""
    applied_model = str(full.get("model") or "")
    if requested_model and applied_model and requested_model != applied_model:
        full["capability_delta"] = [{
            "kind": "capability_delta",
            "requested": f"model {requested_model}",
            "effective": f"model {applied_model}",
            "reason": "session_route_resolves_its_own_model",
        }]
    primary, full_ok, full_note = _resolve_full_primary_output(
        gateway, run_id, full.get("primary_output"))
    full["primary_output"] = primary
    budget = tool_result_limit("delegate_wait")
    text = json.dumps(full, ensure_ascii=False, indent=2)
    if len(text) <= budget - _PAYLOAD_ENVELOPE_HEADROOM:
        full["output_delivery"] = {
            # An unresolved engine-side truncation makes even an inline-fitting payload
            # NOT the whole result: complete/consumed follow the verified fact.
            "complete": full_ok, "consumed": full_ok, "inline_is_preview": False,
            "total_chars": len(text), "artifact": None, "read_next": None,
            "note": ("The whole terminal payload is inline." if full_ok else
                     "INLINE BUT INCOMPLETE AT THE SOURCE: the engine reported its "
                     "primary output as a bounded preview and the full artifact could "
                     "not be matched to the size or the preview the run itself reported "
                     "(see primary_output_full). Treat this "
                     "as incomplete evidence, not as the verdict."),
        }
        if full_note is not None:
            full["output_delivery"]["primary_output_full"] = full_note
        return full
    artifact = _stage_full_output(ctx, run_id, text)
    _emit(ctx, custody.OUTPUT_SPILLED, {"run_id": run_id, "total_chars": len(text),
                                        "artifact": (artifact or {}).get("path", ""),
                                        "bytes": (artifact or {}).get("bytes"),
                                        "sha256": (artifact or {}).get("sha256", ""),
                                        "staged": artifact is not None,
                                        "full_content": bool(full_ok and artifact is not None)})
    if entry is not None and artifact is not None:
        if entry.output_consumed and entry.output_sha and artifact["sha256"] != entry.output_sha:
            # The ack named OTHER bytes: a re-stage of different content at the same
            # path owes a fresh acknowledgement — consumed never transfers by path.
            entry.output_consumed = False
        entry.output_sha = artifact["sha256"]
        entry.output_artifact = artifact["path"]
        entry.output_complete = bool(full_ok)
    return _preview_payload(full, text, artifact, budget,
                            consumed=bool(entry is not None and entry.output_consumed),
                            full_ok=full_ok, full_note=full_note)


# -- tools --------------------------------------------------------------------


def _start_request(ctx: ToolContext, route: Any, authority: "DelegatedRunShape",
                   root: str, text: str, seconds: int, instructions: str) -> Dict[str, Any]:
    """The POST body for one delegated run, built from the derived SHAPE.

    Extracted so the caller stays inside the method-size gate, and so the body has ONE
    author: the shape decides the mode and whether the delegated marker rides along,
    and nothing here re-derives either.

    ``seconds`` and ``instructions`` arrive PRE-BUILT rather than being derived here:
    a transport retry of a pending invocation must present a byte-identical body for
    the engine's replay match, and both the deadline-derived bound and the
    contract-derived instructions can change between calls, so the caller decides
    whether to recompute them or replay the recorded ones (the retry path never calls
    this function at all — it replays the stored canonical body verbatim).
    """
    request: Dict[str, Any] = {
        "prompt": text,
        # Built from the SHAPE plus the task contract, so a mutating delegated
        # child is told that its boundary is a request and not a fact — the same
        # disclosure the durable record and the parent's result carry, in the one
        # place the child can read — and the nanny's own objective rides along
        # structurally (`_assignment_instructions`).
        "instructions": instructions,
        # The engine's default authPreference is `auto` = subscription-first WITH
        # policy fallback to a paid API key. That fallback is invisible to us and
        # would be settled at a confident $0.00 — the one shape the ledger must
        # never produce. Ask for the substrate we are actually claiming.
        "authPreference": "subscription",
        # The run SHAPE comes from the derived authority, not from re-deriving it
        # here: one predicate decides what this child may do, and the mode follows it.
        "mode": authority.mode,
        "scope": {"kind": "project", "root": root},
        # PIN, not preference: `primaryHarness` only fronts the engine's
        # auto-pool, which still holds every other doctor-OK harness — the run
        # could fail over onto a route the owner never configured. The
        # explicit one-element `harnesses` pool is the engine's pinning
        # contract (its own MCP surface spells a forced route exactly this
        # way): the child rides THIS route or the start refuses typed.
        "harnesses": [route.route_id],
        "primaryHarness": route.route_id,
        "access": authority.access,
    }
    if authority.isolation:
        # `delegated` rides WITH the isolation, from the same record, because they
        # are the same decision: `live` is in-place, and in place is exactly where
        # Claudexor would otherwise hand the harness the operator's real `$HOME`
        # — daemon control token included. Sending one without the other is the
        # containment hole, so neither is assembled separately.
        request["execution"] = {
            "isolation": authority.isolation,
            "delegated": authority.delegated,
        }
    if route.model:
        request["model"] = route.model
    if route.effort:
        request["effort"] = route.effort
    if seconds:
        request["maxSeconds"] = seconds
    return request


def _delegate_start(ctx: ToolContext, prompt: str, max_seconds: Optional[int] = None,
                    retry_of: Optional[str] = None) -> str:
    from ouroboros.claudexor_daemon import ensure_owned_gateway
    from ouroboros.gateways.claudexor import ClaudexorUnavailable
    from ouroboros.subagents import (
        get_subagent_harness, resolve_subagent_executor, route_health,
    )

    text = str(prompt or "").strip()
    if not text:
        return _fail("delegate_start", "empty_prompt", "prompt is required")
    if _deadline_expired(ctx):
        # An EXPIRED nanny cannot honestly bound anything: the deadline-less fallback
        # would hand the run the absolute task ceiling, hours past the instant this
        # task's own deadline demanded it stop. Refused before the daemon is touched.
        return _fail(
            "delegate_start", "task_deadline_expired",
            "This task's deadline has already passed, so a delegated run started now "
            "would outlive it by design. Finalize with what you have — do not start "
            "new work a deadline has already closed.",
        )

    drive = custody.custody_root(ctx)
    owned_project_id = ""
    invocation_id = ""
    # C1 isolation binding facts — empty for read-only runs and until resolved.
    snapshot_id = ""
    baseline_sha = ""
    target_root = ""
    authority_source = ""
    # ONE logical invocation id per INTENDED invocation, and reuse ONLY by explicit
    # token — never by content-matching, because two identical intentions are still
    # two intentions (the owner's contract: an intended new start is a NEW id). An
    # ordinary call mints a fresh id and records the CANONICAL body beside it; a
    # start whose outcome was unknown hands its id back as a retry token, and only
    # a call that presents that token replays the invocation — the STORED body,
    # byte-identical by construction, under the SAME wire Idempotency-Key, so the
    # engine returns the run it already accepted instead of starting a second one
    # (a re-derived body would digest differently and 409 at the engine).
    retry_token = str(retry_of or "").strip()
    recovering = bool(retry_token)
    if recovering:
        # The stored invocation is the SINGLE SOURCE of EVERY fact about a retry
        # (route, shape, root, project, key, C1 binding — see
        # _resolve_retry_invocation). Validated BEFORE any daemon call, so a
        # refused token registers nothing.
        binding, refusal = _resolve_retry_invocation(ctx, drive, retry_token, text)
        if refusal:
            return refusal
        (request_body, route, authority, root, key, project_id, owned_project_id,
         seconds, snapshot_id, target_root, baseline_sha, authority_source) = binding
        invocation_id = retry_token
    else:
        route = get_subagent_harness()
        if route is None:
            resolution = resolve_subagent_executor("harness", route=None)
            return _fail(
                "delegate_start", resolution.reason,
                "No delegated route is configured (OUROBOROS_SUBAGENT_HARNESS is empty). "
                "Think natively instead — do not wait for a harness that does not exist.",
                executor=resolution.executor,
            )
        authority = _derive_authority(ctx)

    access = authority.access
    try:
        gateway = ensure_owned_gateway()
    except ClaudexorUnavailable as exc:
        resolution = resolve_subagent_executor("harness", route=route, unavailable_reason=exc.code)
        return _fail("delegate_start", exc.code, str(exc), executor=resolution.executor)

    try:
        # Health is asked about the whole SHAPE, so the same reader that refuses a route
        # which cannot write also refuses an ENGINE that cannot confine a delegated
        # harness's HOME. Both come back here as a typed blocker; neither can degrade
        # into starting the run anyway. On a retry the route and the shape are the
        # STORED invocation's, so the answer is about the run actually being replayed —
        # not about whatever the environment names today.
        unavailable, reset_at = route_health(
            gateway, route.route_id, authority, route_model=route.model,
        )
        resolution = resolve_subagent_executor(
            "harness", route=route, unavailable_reason=unavailable, reset_at=reset_at,
        )
        if resolution.blocked:
            return _fail(
                "delegate_start", resolution.reason,
                "The delegated route cannot run now. This is a typed blocker: do NOT "
                "silently fall back onto metered API spend — decide explicitly "
                "(wait for the reset, deliver partial work, or ask the parent).",
                executor="blocked", reset_at=resolution.reset_at, route=route.route_id,
            )

        if not recovering:
            record_auth, root_error = _mutation_authority(ctx, authority)
            if root_error:
                return root_error
            invocation_id = custody.new_invocation_id()
            root = record_auth["target_root"]
            if authority.access == "workspace_write":
                # C1: the run executes in a PRIVATE snapshot of the authority target,
                # never in the shared tree. Provisioned (and durably registered)
                # BEFORE the start intent below; scope.root becomes the snapshot.
                target_root = record_auth["target_root"]
                authority_source = record_auth["source"]
                snapshot, snap_error = _provision_snapshot(ctx, drive, target_root, invocation_id)
                if snap_error:
                    return snap_error
                snapshot_id = snapshot.snapshot_id
                baseline_sha = snapshot.baseline_sha
                root = snapshot.path
            existing_project = gateway.find_project_id(root)
            project_id = existing_project or gateway.register_project(root)
            owned_project_id = "" if existing_project else project_id
            # The canonical ASSIGNMENT — prompt plus host-authored instructions —
            # is digested together: two starts whose prompts agree but whose
            # contract blocks differ are two different logical starts. The digest
            # is the LOOKUP identity only; the wire key stays the invocation id,
            # and a retry replays the STORED body byte-identically regardless.
            instructions = _host_instructions(authority, _assignment_instructions(ctx))
            key = custody.idempotency_key(getattr(ctx, "task_id", ""), route.route_id, access,
                                          authority.mode, authority.isolation, root, text,
                                          instructions)
            seconds = _bounded_max_seconds(ctx, max_seconds)
            request_body = _start_request(ctx, route, authority, root, text, seconds, instructions)
        lineage = getattr(ctx, "task_metadata", {}) or {}
        lineage = lineage if isinstance(lineage, dict) else {}
        requested = custody.record_start_requested(
            drive, run_id="", task_id=str(getattr(ctx, "task_id", "") or ""),
            idempotency_key=key, invocation_id=invocation_id,
            max_seconds=seconds, request=request_body, project_id=project_id,
            project_owned=bool(owned_project_id), route=route.route_id,
            # Lineage rides the request row so a run RECOVERED from a pending
            # invocation (P34R.2: worker died between the accepted POST and
            # record_started) can still attribute its ledger row to the task tree.
            root_task_id=str(lineage.get("root_task_id") or ""),
            parent_task_id=str(lineage.get("parent_task_id") or ""),
            # The C1 isolation binding, durable BEFORE the POST: the canonical request
            # above carries scope.root = execution root, and these name the snapshot,
            # the baseline and the authority target so a retry reproduces the exact
            # binding and the startup GC can see a pending invocation's snapshot.
            snapshot_id=snapshot_id, execution_root=(root if snapshot_id else ""),
            baseline_sha=baseline_sha, target_root=target_root,
            authority_source=authority_source)
        if not requested:
            # The POST is CONDITIONAL on the durable request row: launching anyway
            # would start an overpowered run that nothing durable names, and a worker
            # death before record_started would leave it live and unfindable. Nothing
            # was sent on THIS attempt — so a fresh start's registration is
            # definitively retirable, but a RETRY's project belongs to the original
            # attempt, whose POST may have bound a live run: its fate stays unknown
            # and its invocation stays pending, or a local disk error would strand a
            # run the daemon may well be executing.
            return _fail(
                "delegate_start", "start_request_row_unwritable",
                "The durable start-request row could not be written, so the run was "
                "NOT started: a run launched without its custody trail would be "
                "unfindable if this worker died. Fix the drive/event log and retry.",
                **_retire_orphaned_registration(ctx, gateway, owned_project_id,
                                                definite_refusal=not recovering,
                                                reason="start_request_row_unwritable",
                                                invocation_id=invocation_id,
                                                snapshot_id=("" if recovering else snapshot_id)))
        handle = gateway.start_run(request_body, idempotency_key=invocation_id)
        # A 202 answers with `jobId` and no `runId` when the run has not bound a run dir
        # inside the daemon's start timeout. The run IS durably enqueued and will execute,
        # and `jobId` is a usable handle for GET and /control — discarding it left a live
        # run nobody could wait on, cancel or settle, and invited a duplicate start.
        run_id = str(handle.get("runId") or handle.get("jobId") or "")
        if not run_id:
            # The POST SUCCEEDED, so a run is more likely live here than on the refusal
            # branch beside it — the registration is retained and durably named through
            # the same path, instead of being silently abandoned as it was.
            return _fail("delegate_start", "queued_without_run_id",
                         f"Claudexor returned a queued handle without a run id: {handle!r}",
                         pending_invocation_id=invocation_id,
                         retry_hint="to retry THIS start call delegate_start with "
                                    "retry_of=pending_invocation_id; a plain call starts a NEW run",
                         **_retire_orphaned_registration(ctx, gateway, owned_project_id,
                                                         definite_refusal=False,
                                                         reason="queued_without_run_id",
                                                         invocation_id=invocation_id))
    except ClaudexorUnavailable as exc:
        # A registration we created BEFORE the start must not outlive a failed start.
        # It used to be left behind with nothing anywhere naming its id.
        status = int(getattr(exc, "status_code", 0) or 0)
        definite = 400 <= status < 500
        # An UNKNOWN outcome hands back the retry token: only the caller can say
        # whether the next call is a retry of this intention or a new intention, and
        # without the token every next call is a new one. A definite refusal retires
        # the id, so no token rides a refusal.
        pending = ({} if definite or not invocation_id else
                   {"pending_invocation_id": invocation_id,
                    "retry_hint": "to retry THIS start call delegate_start with "
                                  "retry_of=pending_invocation_id; a plain call starts a NEW run"})
        return _fail("delegate_start", exc.code, str(exc), executor="blocked",
                     reset_at=getattr(exc, "reset_at", ""), **pending,
                     **_retire_orphaned_registration(ctx, gateway, owned_project_id,
                                                     definite_refusal=definite,
                                                     reason=str(getattr(exc, "code", "")),
                                                     invocation_id=invocation_id,
                                                     snapshot_id=("" if recovering else snapshot_id)))
    except BaseException as exc:
        # EVERY pre-custody exit leaves a durable disposition, including the ones no
        # typed handler claims (a bug here, a timeout, a signal). NEVER retired: an
        # untyped exit says nothing about whether the POST reached the daemon, so a run
        # may be live against it. Named with a typed reason so the sweep's
        # pending-invocation recovery finds it, then re-raised — disclosure, not a swallow.
        _retire_orphaned_registration(ctx, gateway, owned_project_id,
                                      definite_refusal=False,
                                      reason=f"pre_custody_exit_{type(exc).__name__}",
                                      invocation_id=invocation_id)
        raise
    finally:
        gateway.close()

    metadata = getattr(ctx, "task_metadata", {}) or {}
    metadata = metadata if isinstance(metadata, dict) else {}
    # A start whose custody row did not land does not get to wear the plain name, for the
    # same reason a cut field is renamed `*_preview`: the run is live and only THIS process
    # knows it exists. A bare "started" there is the uncustodied-run leak this module was
    # written to close, one surface further out.
    durable = custody.record_started(drive, _RunCustody(
        run_id=run_id,
        task_id=str(getattr(ctx, "task_id", "") or ""),
        route_id=route.route_id,
        model=route.model,
        project_id=project_id,
        project_owned=bool(owned_project_id),
        root_task_id=str(metadata.get("root_task_id") or ""),
        parent_task_id=str(metadata.get("parent_task_id") or ""),
        # The CANONICAL (budget) root, the same one the custody rows themselves live
        # on — never ctx.drive_root, which on a split-root task is the disposable
        # child drive: a ledger row written there misses every budget fence and is
        # erased with the child drive's pruning (P34R.1).
        ledger_root=str(drive),
        idempotency_key=key,
        invocation_id=invocation_id,
        snapshot_id=snapshot_id,
        execution_root=(root if snapshot_id else ""),
        baseline_sha=baseline_sha,
        target_root=target_root,
        authority_source=authority_source,
    ), shape={
        # The shape rides on the SAME durable row as custody, so a forensic reader never
        # has to join two events to learn what authority a run was started with.
        # `max_seconds` rides too: delegate_wait reports elapsed-vs-cap from this row.
        "effort": route.effort, "access": access, "mode": authority.mode,
        "isolation": authority.isolation, "delegated": authority.delegated, "root": root,
        "max_seconds": seconds,
        "capture_mode": (_CAPTURE_DELEGATED_SNAPSHOT if snapshot_id else ""),
    })
    return _started_payload(handle, run_id, route, access, authority, root,
                            durable=durable, recovering=recovering,
                            snapshot_id=snapshot_id, target_root=target_root,
                            baseline_sha=baseline_sha)


def _started_payload(handle: Dict[str, Any], run_id: str, route: Any, access: str,
                     authority: "DelegatedRunShape", root: str, *, durable: bool,
                     recovering: bool, snapshot_id: str, target_root: str,
                     baseline_sha: str) -> str:
    """The one author of delegate_start's started result (note + payload).

    The AUTHORITY guidance and the CUSTODY warning are independent facts about the same
    start, so both are said. An undurable custody row is the louder one and goes first:
    a nanny that walks away from an uncustodied MUTATING run leaves a live shell in its
    own worktree that nothing outside this process can name.
    """
    note = "" if durable else (
        "CUSTODY IS NOT DURABLE: the run started, but its custody row could not be written, "
        "so nothing outside this worker can wait on, cancel or settle it. Do not walk away "
        "from it — finish it or delegate_cancel it in this session. ")
    note += (
        "You are the nanny and the host. Poll with delegate_wait; the run's own "
        "claims are evidence to check, not a verified result."
        + (
            " This run edits a PRIVATE SNAPSHOT of your write root, not the shared "
            "tree: at terminal its diff is captured for you, and NOTHING lands in "
            "the shared tree until you explicitly integrate_delegated_patch(run_id="
            "...) to apply or reject it — read the captured diff before you claim "
            "it, and never let the run commit. It was ASKED to run under a scoped "
            "HOME and an OS-enforced boundary; whether the engine applied either is "
            "a per-run fact that delegate_wait reads back from the run's own "
            "artifacts. A host with no boundary mechanism runs it anyway and says "
            "so there."
            if authority.isolation == "live" else
            " This run cannot write anything: it reads and answers."
        )
    )
    payload = {
        "status": "started" if durable else "started_uncustodied",
        "run_id": run_id,
        "run_dir": handle.get("runDir"),
        "route": route.route_id,
        "model": route.model,
        "effort": route.effort,
        "access": access,
        "mode": authority.mode,
        "isolation": authority.isolation or "envelope",
        "idempotent_recovery": recovering,
        # ASKED, not applied. The proof arrives with the run's own artifacts and is
        # relayed by delegate_wait; saying "isolated" here would be the exact claim
        # this whole verification exists to stop anyone from making.
        "scoped_home_requested": authority.delegated,
        "root": root,
        "custody_durable": durable,
        "note": note,
    }
    if snapshot_id:
        # The C1 binding, stated where the nanny can read it: the run's scope.root is
        # the EXECUTION snapshot; the authority target receives nothing until the
        # explicit apply.
        payload["execution_root"] = root
        payload["authority_target_root"] = target_root
        payload["baseline_id"] = baseline_sha
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _retire_orphaned_registration(ctx: ToolContext, gateway: Any, project_id: str, *,
                                  definite_refusal: bool, reason: str,
                                  invocation_id: str = "",
                                  snapshot_id: str = "") -> Dict[str, Any]:
    """Retire a registration this start created but never bound to a run.

    Only when the daemon gave a DEFINITE negative answer (a 4xx refusal): a transport
    error, a 5xx, or a 2xx handle with no run id all mean the POST's fate is unknown, and
    a run may well be live against this very registration. An unverified outcome is never
    grounds for destroying state — the durable row names the id either way, which is what
    the old code lacked. The caller supplies the verdict, so every failing start reaches
    this one path instead of one branch retiring and its twin abandoning.

    The row also settles the INVOCATION's fate: ``definite: true`` retires the logical
    invocation id (a definitely refused invocation must not be reused — the daemon may
    hold its key against a body a reconfigured route can no longer reproduce, which
    would 409 forever), while an unknown outcome leaves it pending so a transport retry
    presents the same key and lands on whatever the daemon really has. Written even
    with no registration to retire, because the invocation's fate is its own fact.
    """
    if snapshot_id and definite_refusal:
        # The C1 execution snapshot THIS attempt provisioned. Only a definite refusal
        # proves no run can be live against it; an unknown outcome keeps it — the
        # pending invocation names it durably, and the startup GC reconciles it.
        try:
            from ouroboros.subagent_worktrees import remove_execution_snapshot

            remove_execution_snapshot(snapshot_id)
        except Exception:
            log.warning("Failed to retire delegated execution snapshot %s", snapshot_id,
                        exc_info=True)
    retired = False
    if project_id and definite_refusal:
        try:
            gateway.remove_project(project_id)
            retired = True
        except Exception as exc:
            # A registration the daemon does not have is already retired: the same
            # absence-is-discharge fact `retire_project` settles on.
            retired = custody.daemon_says_absent(exc)
            if not retired:
                log.warning("Failed to retire orphaned delegated project %s", project_id, exc_info=True)
    if project_id or invocation_id:
        _emit(ctx, custody.START_FAILED, {"run_id": "", "project_id": project_id,
                                          "project_retired": retired, "reason": reason,
                                          "invocation_id": invocation_id,
                                          "definite": bool(definite_refusal)})
    if not project_id:
        return {"project_retired": False}
    if retired or definite_refusal:
        return {"project_retired": retired, "project_id": project_id}
    return {"project_retired": False, "project_id": project_id,
            "project_retention_reason": "start_outcome_unknown_run_may_exist"}


def _deadline_expired(ctx: ToolContext) -> bool:
    """True when the nanny HAS a deadline and it has already passed.

    The distinction the bound below could not make: ``deadline_remaining_sec`` answers
    0.0 both for "no deadline" and for "the deadline is behind us", and collapsing them
    let an EXPIRED nanny hand a fresh run the absolute task ceiling.
    """
    from ouroboros.deadline_utils import deadline_remaining_sec, parse_deadline_ts

    meta = getattr(ctx, "task_metadata", {})
    meta = meta if isinstance(meta, dict) else {}
    if parse_deadline_ts(meta.get("deadline_at")) is None:
        return False
    return deadline_remaining_sec(ctx) <= 0


def _bounded_max_seconds(ctx: ToolContext, requested: Optional[int]) -> int:
    """Narrow-only: the delegated run may never outlive the nanny's own deadline.

    A caller must ask ``_deadline_expired`` FIRST: an expired deadline cannot produce an
    honest bound at all, and this function's fallback is for a nanny that has NO
    deadline, never for one whose deadline is behind it.
    """
    from ouroboros.deadline_utils import deadline_remaining_sec

    remaining = int(max(0.0, deadline_remaining_sec(ctx)))
    try:
        asked = int(requested) if requested is not None else 0
    except (TypeError, ValueError):
        asked = 0
    candidates = [value for value in (asked, remaining) if value > 0]
    if candidates:
        # Clamp HERE too, not only on the fallback below: `max_seconds` is a model-supplied
        # tool argument with no maximum in its schema, so an explicit ask sailed past the
        # bound the fallback branch was careful about — the same defect, one branch over.
        return min(_CLAUDEXOR_MAX_SECONDS, min(candidates))
    # No positive bound is knowable: either the nanny has no deadline, or its deadline
    # has already passed. Omitting `maxSeconds` — the old behavior — handed the run
    # Claudexor's 7-day schema bound; the cap is damage limitation, and custody (the
    # durable start row plus reconciliation) is what actually stops an orphan.
    from ouroboros.config import get_task_abs_ceiling_sec

    # Claudexor bounds maxSeconds at 7 days (control.ts `.max(604_800)`), and the task
    # ceiling clamps only from BELOW — an owner who raises it past a week would make
    # every deadline-less start send an out-of-schema value.
    return min(_CLAUDEXOR_MAX_SECONDS, int(get_task_abs_ceiling_sec()))


def _halt_breached_run(ctx: ToolContext, gateway: Any, entry: _RunCustody,
                       breach: _Breach) -> str:
    """Stop a run the engine did not contain as asked, and say exactly what failed.

    The BREACH incident goes through ``custody.record_containment_fault``, the same
    writer an unverified cancel uses, so a breached run also surfaces as the CRITICAL
    health invariant that stays open until a terminal receipt resolves it. Emitting a
    look-alike event here instead left the breach out of the open-fault sweep.

    The stop itself goes through ``custody.cancel_and_verify`` — the ONE cancel path,
    with its four typed outcomes — and the sentence handed back to the agent is built
    from the outcome it returns. The ad-hoc cancel this replaced swallowed every
    exception into a log line and then said "The run was cancelled" unconditionally,
    which is precisely what ``record_containment_fault``'s own contract forbids: an
    incident must never surface "as a reassuring string in a tool result". An
    overpowered run that refused to stop was reported to the agent as stopped.
    """
    run_id = entry.run_id
    drive = custody.custody_root(ctx)
    try:
        cancelled = custody.cancel_and_verify(drive, gateway, entry, breach.code)
    except Exception:
        log.warning("Failed to cancel an uncontained delegated run %s", run_id, exc_info=True)
        cancelled = {"outcome": custody.CANCEL_CONTAINMENT_FAULT}
    custody.record_containment_fault(drive, entry, breach.code, breach.detail,
                                     fault=breach.code, **breach.facts)
    outcome = str(cancelled.get("outcome") or custody.CANCEL_CONTAINMENT_FAULT)
    return _fail(
        "delegate_wait", breach.code,
        f"{breach.detail} {_CANCEL_NOTES.get(outcome, '')} Do not retry it: this is a "
        "containment fault in the transport or the engine, not a task failure — report "
        "it and continue within your own authority.",
        run_id=run_id, cancel_outcome=outcome, **breach.facts,
    )


# The typed external-wait lease lives in `delegate_progress` (the wait-liveness
# module); re-bound here because the wait's own seams and the tests name it on
# this surface, exactly like the staged-output cluster above.
_external_wait_lease_until = progress.external_wait_lease_until
_emit_external_wait_lease = progress.emit_external_wait_lease


def _delegate_wait(ctx: ToolContext, run_id: str, wait_sec: Optional[int] = None,
                   since_seq: Optional[int] = None) -> str:
    """Time-bounded, progress-aware wait (docs/DEVELOPMENT.md "Timeout & Wait Control").

    HOLDS the window it was given. It returns early only on a terminal state or a
    containment fault; a journal-cursor advance past ``since_seq`` is RECORDED and
    streamed to the human live, and the model is woken once, at expiry, with the whole
    sequence in ``advances``. Returning on the first advance made the caller's window
    meaningless against a healthy run — the only path that ever consulted it was the
    SILENT one, so a streaming run cost a full-context round per event batch (measured:
    18 rounds, 861k prompt tokens, for a run that was doing fine). Progress is the
    JOURNAL cursor, so SSE ``: ping`` keepalives cannot masquerade as it.

    NARROW-ONLY, like ``_bounded_max_seconds``: the wait may not outlive the nanny's own
    deadline, minus the finalization grace it needs to answer at all. This tool is absent
    from ``_DEADLINE_CLAMPED_TOOLS`` (its ToolEntry value IS its outer bound), so nothing
    upstream cuts it — measured, a 2100s window against ten seconds of remaining deadline
    ran the full 2100s and slid the task past its deadline mid-tool, the defect that set
    built for ``web_search``. Clamping HERE keeps the graceful typed ``no_progress``
    return where the outer clamp delivers a thread-kill. Only "no deadline set" is left
    unclamped; a SPENT deadline clamps to the floor, the window is measured from before
    the connection, and every call is BOUNDED by what it has left (``progress.poll_bound``)
    so no read can outrun it as the 60s default could. Only the LAST poll of a spent
    window may go unanswered gracefully; a daemon that fails while the window still has
    time is the typed refusal it was, never a wait reported as quiet.
    """
    from ouroboros.config import get_delegate_wait_max_sec, get_delegate_wait_sec
    from ouroboros.gateways.claudexor import (
        ClaudexorGateway,
        ClaudexorUnavailable,
        pending_interactions as _cx_pending,
    )

    rid = str(run_id or "").strip()
    if not rid:
        return _fail("delegate_wait", "missing_run_id", "run_id is required")
    not_mine, entry = _owned_run(ctx, "delegate_wait", rid)
    if not_mine or entry is None:
        return not_mine or _fail("delegate_wait", "run_ownership_unknown", "custody unresolved", run_id=rid)
    ceiling = get_delegate_wait_max_sec()
    try:
        window = int(wait_sec) if wait_sec is not None else get_delegate_wait_sec()
    except (TypeError, ValueError):
        window = get_delegate_wait_sec()
    from ouroboros.deadline_utils import parse_deadline_ts, window_within_deadline

    window = window_within_deadline(ctx, max(1, min(window, ceiling)))

    # The clock starts HERE, before the connection: the window is a promise about how
    # long this CALL holds, and the opening handshake plus first poll are part of it.
    # Started after them, an unbounded connection could spend the whole deadline before
    # the window it was clamped into had begun.
    started = time.monotonic()
    deadline = started + window
    try:
        gateway = ClaudexorGateway()
        gateway.handshake(timeout_sec=progress.poll_bound(deadline - time.monotonic()))
    except ClaudexorUnavailable as exc:
        return _fail("delegate_wait", exc.code, str(exc), run_id=rid)

    # Re-derived from the LIVE context rather than read back from custody: the nanny's
    # authority is the authority, and a lost custody record must not become a wider run.
    authority = _derive_authority(ctx)
    # FACTS against premature cancels: how long the run has actually been going and
    # what its cap really is, from the durable start row. A nanny that cannot see
    # these confabulates "exceeded the cap" out of its own impatience. Absent facts
    # (an old row, an unknown run) stay null — never invented.
    _started_ts, _run_max_seconds = custody.run_timing(custody.custody_root(ctx), rid)
    _started_at = parse_deadline_ts(_started_ts)
    # The idle-rail lease for THIS hold: granted before the loop, released in the
    # finally below — the supervisor's idle enforcer spares a leased task while
    # every other rail (deadline, ceiling, budget, cancel) still cuts through.
    # The grant carries a unique lease_id (F5b) and the release names the SAME
    # id, so an abandoned, executor-killed wait thread's late release can never
    # blank a newer grant made by this task's next wait.
    _lease_id = uuid.uuid4().hex
    _emit_external_wait_lease(
        ctx, rid, _external_wait_lease_until(ctx, window, _started_at, _run_max_seconds),
        lease_id=_lease_id)
    try:
        detail = progress.bounded_poll(gateway, rid, deadline - time.monotonic())
        baseline = int(since_seq) if since_seq is not None else int(detail.get("lastSeq") or 0)
        seen = progress.WindowObservations()
        seen.observe_baseline(detail, baseline)
        while True:
            summary = custody.summary_of(detail)
            state = str(summary.get("state") or "")
            last_seq = int(detail.get("lastSeq") or 0)
            breach = _containment_breach(detail, authority)
            if breach:
                return _halt_breached_run(ctx, gateway, entry, breach)
            if state in _TERMINAL_STATES:
                was_settled = bool(entry.settled)
                settlement = custody.settle_run(custody.custody_root(ctx), gateway, entry, detail)
                payload = _delivered_terminal_payload(ctx, rid, detail, authority, entry, gateway)
                payload["settlement"] = settlement
                # C1: a mutating run's changes live in its private snapshot until the
                # nanny explicitly integrates them. Captured HERE, durably, on every
                # terminal observation (idempotent), cancelled runs included — a
                # cancelled run's partial work is salvage material, not garbage.
                capture = _capture_terminal_patch(ctx, entry)
                if capture is not None:
                    payload["workspace_capture"] = capture
                # The «last delegated run» settings receipt (Subagents section):
                # requested vs applied model, written ONLY when THIS call performed
                # a SUCCESSFUL settlement — a later wait re-reading an already-settled
                # run must not re-date it (or replace a newer run as "last"), and a
                # settlement whose durable obligations failed must not mint a receipt
                # it would re-mint on every retry. The delegated REVIEW sessions never
                # pass here — they have their own receipt store
                # (reviewer_slot_last_execution.json).
                if not was_settled and bool(settlement.get("settled")):
                    from ouroboros.subagents import record_last_delegation
                    record_last_delegation(
                        route=entry.route_id, requested_model=entry.model,
                        applied_model=str(payload.get("model") or ""), run_id=rid)
                # D7 made load-bearing: settlement is where "paid for and never read"
                # becomes permanent, so the parent is told in WORDS here — not left to
                # infer it from `output_delivery.consumed`. Re-settling an already
                # settled run reports the CURRENT durable fact, so the line disappears
                # once the read has happened rather than echoing a stale omission.
                if custody.record_settled_unread(custody.custody_root(ctx), entry):
                    payload["result_not_collected"] = (
                        "THIS RESULT IS NOT COLLECTED YET: the run is settled and its "
                        "full output is staged, but nothing has read it to EOF. Read the "
                        "artifact named in output_delivery with read_file "
                        "root='task_drive' until it is covered end to end — a result you "
                        "have not read is not a result you may report."
                    )
                # The containment disclosure is read off what the PARENT was told, so the
                # durable line and the relayed payload cannot disagree. It runs on the
                # PREVIEW path too: a payload big enough to spill is exactly the one whose
                # containment block a reader is least likely to reach.
                _record_containment(ctx, entry, payload)
                from ouroboros.tools.control import cache_horizon_note
                _horizon = cache_horizon_note(ctx, time.monotonic() - started)
                if _horizon:
                    payload["cache_horizon_note"] = _horizon
                return json.dumps(payload, ensure_ascii=False, indent=2)
            if last_seq > baseline:
                # The STREAM is not collapsed — the TIMER is. Every advance reaches the
                # live progress surface the instant this loop sees it, so the human's
                # view stays as rich; what stops is waking the MODEL per event batch.
                # The emit is also the frame the supervisor's idle enforcer reads, which
                # a silently blocking wait would starve.
                progress.emit(ctx, rid, seen.record(detail, last_seq, int(time.monotonic() - started)))
                baseline = last_seq          # so the NEXT advance is counted once
            pending = _cx_pending(detail)
            if pending and _interactions_are_news(rid, pending):
                # A NEW question returns IMMEDIATELY: the old wait kept only the
                # waitingOnUser boolean and showed it at window expiry, so a paused
                # run burned the rest of the window (up to the engine's whole
                # answer timeout) in dead metered polling. A question the model
                # ALREADY saw does not re-trigger — a nanny that escalated to its
                # human keeps holding windows instead of busy-looping, and the
                # engine timeout stays the backstop.
                return _waiting_on_user_payload(ctx, rid, state, last_seq, pending,
                                                seen=seen)
            def _expired() -> str:
                rendered = progress.rendered_window(
                    run_id=rid, state=state, last_seq=last_seq, window=window,
                    elapsed_seconds=(None if _started_at is None else max(0, int(
                        (_dt.datetime.now(tz=_dt.timezone.utc) - _started_at).total_seconds()))),
                    max_seconds=_run_max_seconds or None,
                    waiting_on_user=bool(summary.get("waitingOnUser")) or bool(pending),
                    pending_interactions=_bounded_interactions(pending) if pending else None,
                    detail=detail, seen=seen,
                    budget=tool_result_limit("delegate_wait"))
                from ouroboros.tools.control import cache_horizon_note
                _horizon = cache_horizon_note(ctx, time.monotonic() - started)
                return f"{rendered}\n\n{_horizon}" if _horizon else rendered

            if time.monotonic() >= deadline:
                return _expired()
            time.sleep(min(_POLL_INTERVAL_SEC, max(0.0, deadline - time.monotonic())))
            # BOUNDED whether or not the window is spent: a poll STARTED a moment before
            # expiry still carries the client's 60s read default, so the clamp bounded the
            # sleeping and not the waiting. What an UNANSWERED one MEANS is what differs.
            # The last poll of a spent window is bounded and never skipped — terminal
            # state and breach are judged on fresh data or not at all — and a daemon too
            # slow to answer THAT one is this window's expiry. Earlier, the window still
            # has time and there is no expiry to report: the typed refusal propagates to
            # the handler below, because a daemon that died mid-window relayed as a quiet
            # completed wait is a fabricated duration on top of a run nobody is watching.
            left = deadline - time.monotonic()
            fresh = (progress.expiring_poll(gateway, rid) if left <= 0
                     else progress.bounded_poll(gateway, rid, left))
            if fresh is None:
                return _expired()   # unanswered AT expiry: expire on what is already held
            detail = fresh
    except ClaudexorUnavailable as exc:
        return _fail("delegate_wait", exc.code, str(exc), run_id=rid)
    finally:
        _emit_external_wait_lease(ctx, rid, 0.0, lease_id=_lease_id)
        gateway.close()


_CANCEL_NOTES = {
    custody.CANCEL_CONFIRMED: "VERIFIED terminal: the run has stopped. Partial artifacts are "
                              "preserved by Claudexor; a cancelled run has no verdict.",
    custody.CANCEL_REQUESTED: "The daemon ACCEPTED the cancel but the run is not terminal yet. "
                              "It is still running. Call delegate_wait to confirm it stops.",
    custody.CANCEL_FAILED: "The daemon REFUSED the cancel and the run is still live and still "
                           "mutating. Escalate — this is not a stopped run.",
    custody.CANCEL_CONTAINMENT_FAULT: "CONTAINMENT FAULT: the cancel could not be verified, so an "
                                      "overpowered mutating run MAY STILL BE LIVE. A durable "
                                      "incident was recorded and is surfaced as a critical health "
                                      "invariant until a terminal receipt clears it.",
}


def _delegate_cancel(ctx: ToolContext, run_id: str, reason: str = "") -> str:
    """Stop a delegated run. Destructive by nature — a cancelled reviewer has no verdict.

    Reports only what a terminal receipt proves. Saying "cancelled" over an unverified
    control is worse than saying nothing: it retires the operator's attention from a run
    that is still writing to a workspace.
    """
    from ouroboros.gateways.claudexor import ClaudexorGateway, ClaudexorUnavailable

    rid = str(run_id or "").strip()
    if not rid:
        return _fail("delegate_cancel", "missing_run_id", "run_id is required")
    not_mine, entry = _owned_run(ctx, "delegate_cancel", rid)
    if not_mine or entry is None:
        return not_mine or _fail("delegate_cancel", "run_ownership_unknown", "custody unresolved", run_id=rid)
    try:
        gateway = ClaudexorGateway()
        gateway.handshake()
    except ClaudexorUnavailable as exc:
        return _fail("delegate_cancel", exc.code, str(exc), run_id=rid)
    try:
        result = custody.cancel_and_verify(custody.custody_root(ctx), gateway, entry, reason)
    finally:
        gateway.close()
    return json.dumps({
        "status": result["outcome"],
        "run_id": rid,
        "run_may_still_be_live": result["outcome"] != custody.CANCEL_CONFIRMED,
        "accepted": result["accepted"],
        "control_status": result["control_status"],
        "state": result["state"],
        "fault_reason": result["fault_reason"],
        "detail": result["detail"],
        "note": _CANCEL_NOTES.get(result["outcome"], ""),
    }, ensure_ascii=False, indent=2)


def get_tools() -> List[ToolEntry]:
    return [
        ToolEntry("delegate_start", {
            "name": "delegate_start",
            "description": (
                "Start a delegated run on the owner's configured subscription harness and "
                "become its NANNY. Subscription execution is REQUESTED, so the usual case "
                "is no metered API money — but the actual spend is a fact of the finished "
                "run, not a promise of this call: it may come back zero, billed, "
                "estimated, or undisclosed (an expired session, a route that bills by "
                "construction, or an auth fallback all charge real money). Read the "
                "terminal `cost` block from delegate_wait before you treat this as free; "
                "it also costs time, quota and a worker slot. Your working root, "
                "access profile and route come from YOUR task authority; you cannot widen "
                "them, and there is no argument here that would let you try. If you hold "
                "a MUTATING shape the run executes in a PRIVATE SNAPSHOT of your write "
                "root — it never edits the shared tree in place. Its diff is captured at "
                "terminal (delegate_wait's workspace_capture block) and reaches your tree "
                "ONLY when you explicitly call integrate_delegated_patch(run_id=..., "
                "decision='apply'|'reject'); read the captured diff before applying, and "
                "never let the run commit inside its snapshot. If you are read-only it "
                "can only read and answer. "
                "Returns a run_id: watch it with delegate_wait, stop it with "
                "delegate_cancel. The run's output is a CLAIM you must check — you are the "
                "host, so verification receipts are still yours to write. If no route is "
                "configured or it is unavailable you get a typed refusal: think natively "
                "instead of waiting."
            ),
            "parameters": {"type": "object", "required": ["prompt"], "properties": {
                "prompt": {"type": "string", "description": "The complete task for the delegated session."},
                "max_seconds": {"type": "integer", "description":
                    "Wall-clock cap for the run; narrowed to your own remaining deadline. "
                    "Harness runs routinely need 3-5+ minutes end to end, so do not set a "
                    "tight cap for what feels like a quick edit. While delegate_wait shows "
                    "an advancing cursor the run is WORKING, and it enforces this cap "
                    "itself — cancelling a progressing run discards the whole run's spend."},
                "retry_of": {"type": "string", "description":
                    "EXPLICIT retry token: the pending_invocation_id from a start whose "
                    "outcome was unknown (transport failure, lost response). Replays THAT "
                    "invocation byte-identically under its original key, so the engine "
                    "returns the run it already accepted instead of starting a second one. "
                    "Never set it for an intended new run — a plain call always starts a "
                    "NEW invocation, even with an identical prompt."},
            }},
        }, lambda ctx, prompt, max_seconds=None, retry_of=None: _delegate_start(ctx, prompt, max_seconds, retry_of), timeout_sec=120),
        ToolEntry("delegate_wait", {
            "name": "delegate_wait",
            "description": (
                "Wait for a delegated run, bounded in time. Returns IMMEDIATELY only when "
                "the run is TERMINAL or hit a containment fault; otherwise it HOLDS the "
                "window you asked for — the run's narration streams to your human live "
                "while you wait, so you are not the transport for it — and comes back at "
                "expiry with `advances`, every journal-cursor movement seen during the "
                "window, plus elapsed/max seconds and `quiet_for_sec`. A silent window "
                "returns a typed no-progress reason. Keepalives are not progress. Calling "
                "again with a tiny wait_sec to 'check' is a busy-poll: it costs a "
                "full-context round per call and buys nothing, because this call already "
                "waited. Pass since_seq=last_seq to keep following. A run that asks its "
                "user a question returns IMMEDIATELY as status='waiting_on_user' with "
                "the full question set (interaction/question ids ride WHOLE, never "
                "truncated): answer it with delegate_answer, or escalate to your "
                "human and keep waiting (a question with a timeout_at benign-declines "
                "at the engine timeout; timeout_at=null waits until answered). A "
                "large terminal result is delivered as a bounded preview plus an "
                "artifact: read output_delivery and finish reading the artifact before "
                "you rely on it."
            ),
            "parameters": {"type": "object", "required": ["run_id"], "properties": {
                "run_id": {"type": "string", "description": "Run id from delegate_start."},
                "wait_sec": {"type": "integer", "description":
                    "How long THIS call holds before handing control back to you (clamped "
                    "to the configured ceiling and to your own remaining deadline). It is a "
                    "WINDOW, not a quiet cutoff: a run that is streaming keeps streaming to "
                    "your human for the whole of it, and you get the whole batch at the end."},
                "since_seq": {"type": "integer", "description": "Event cursor: advances past it are recorded as progress."},
            }},
        }, lambda ctx, run_id, wait_sec=None, since_seq=None: _delegate_wait(ctx, run_id, wait_sec, since_seq), timeout_sec=2100),
        ToolEntry("delegate_cancel", {
            "name": "delegate_cancel",
            "description": (
                "Cancel a delegated run. Claudexor keeps partial artifacts, but a cancelled "
                "session has no verdict and no finished work product — cancel a stuck or "
                "misdirected run, never one you merely want to hurry. The result is typed: "
                "only `confirmed` means a verified terminal receipt; `requested`, `failed` "
                "and `containment_fault_run_may_still_be_live` all mean it may still be running."
            ),
            "parameters": {"type": "object", "required": ["run_id"], "properties": {
                "run_id": {"type": "string", "description": "Run id from delegate_start."},
                "reason": {"type": "string", "description": "Why you are stopping it."},
            }},
        }, lambda ctx, run_id, reason="": _delegate_cancel(ctx, run_id, reason), timeout_sec=120),
        ToolEntry("delegate_answer", {
            "name": "delegate_answer",
            "description": (
                "Answer a delegated run's pending interactive question — the "
                "status='waiting_on_user' payload from delegate_wait names the "
                "interaction_id and its questions. Only the task that started the run "
                "may answer. Policy: answer from the task context you already hold; a "
                "question ABOVE your authority (spending money, changing scope, "
                "external actions) is not yours to guess — surface it to your human "
                "via progress and keep waiting; an unanswered question with a "
                "timeout_at benign-declines at the engine timeout (the run continues "
                "on stated assumptions), while timeout_at=null waits until answered. "
                "Typed outcomes: delivered; already_resolved (the run moved on — do "
                "not re-post); not_found; rejected (a definite engine refusal of "
                "these rows — HTTP 400/409/413/422 only — fix them); "
                "subscription_window_exhausted (a distinct outcome carrying reset_at "
                "— the answer did NOT land; retry the SAME answers after reset_at); "
                "delivery_unknown "
                "(transport died mid-answer — re-check with delegate_wait and NEVER "
                "post a different answer for the same interaction). Codex-lane runs "
                "have no mid-run questions: a run that ENDS needing input "
                "(outcome_facts.reason=input_required) is answered with a plain NEW "
                "delegate_start whose prompt carries the assignment plus the answers "
                "— there is no rerun/decision verb, and custody stays with you."
            ),
            "parameters": {"type": "object",
                           "required": ["run_id", "interaction_id", "answers"],
                           "properties": {
                "run_id": {"type": "string", "description": "Run id from delegate_start."},
                "interaction_id": {"type": "string", "description":
                    "The interaction being answered, from the waiting_on_user payload."},
                "answers": {"type": "array", "items": {"type": "object", "properties": {
                    "question_id": {"type": "string", "description":
                        "The question's id from the waiting_on_user payload."},
                    "selected_labels": {"type": "array", "items": {"type": "string"},
                                        "description": "Labels of the chosen option(s)."},
                    "free_text": {"type": "string", "description":
                        "Free-text answer; omit when options were selected."},
                }, "required": ["question_id"]}, "description":
                    "One row per question you are answering."},
            }},
        }, lambda ctx, run_id, interaction_id, answers: _delegate_answer(
            ctx, run_id, interaction_id, answers), timeout_sec=120),
    ]


__all__ = ["get_tools"]
