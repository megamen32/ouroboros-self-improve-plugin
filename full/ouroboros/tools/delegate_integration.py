"""The C1 integration seam of the delegated-run tools (authority → snapshot → capture).

Extracted from ``ouroboros/tools/delegate.py`` when that module crossed its size gate:
this is one coherent concern — how a MUTATING delegated run is bound to the tree it may
change. The host derives the unified authority record (B5), provisions the private
execution snapshot the run edits instead of the shared tree (C1), validates the
byte-identical retry binding, and captures the terminal diff for explicit integration.
``tools.delegate`` re-exports every name here (same objects), so every existing
reference — sibling code, the tests, the convergence census — still finds them there.
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional, Tuple

from ouroboros import delegate_custody as custody
from ouroboros.delegate_custody import RunCustody as _RunCustody
# ONE refusal author for the whole delegate surface: the neutral leaf
# `delegate_shared` (phase B's facade split), never a local twin that could drift.
from ouroboros.delegate_shared import _fail
from ouroboros.tools.registry import ToolContext, active_repo_dir_for
from ouroboros.utils import resolve_path_allow_missing

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ouroboros.subagents import DelegatedRunShape

log = logging.getLogger(__name__)


def _resolved(path: Any) -> Optional[pathlib.Path]:
    """Resolve a path, or None when it cannot be resolved at all (null byte, symlink
    loop, unreadable parent). One predicate, so no call site re-enumerates the set."""
    try:
        return resolve_path_allow_missing(pathlib.Path(str(path)))
    except (OSError, ValueError, RuntimeError, TypeError):
        return None


# C1 capture mode: every mutating delegated run executes in a private snapshot and
# returns its diff for explicit integration. The value is recorded (not branched on)
# so the durable record names the regime a run was admitted under.
_CAPTURE_DELEGATED_SNAPSHOT = "delegated_snapshot"


def _mutation_authority(ctx: ToolContext, authority: "DelegatedRunShape") -> tuple[Dict[str, str], str]:
    """The UNIFIED host-derived authority record for one delegated run (B5).

    ``{"target_root", "source", "capture_mode"}`` — the tree the run's changes are
    destined for, WHERE that authority came from, and how the changes travel. Two
    sources exist, validated differently but recorded in one shape:

    - ``acting_constraint``: an acting child derives its target from its own
      ``task_constraint.write_root``, which must equal the genuinely ACTIVE workspace
      root (the original guard: `active_repo_dir_for` falls back to the LIVE Ouroboros
      repo whenever `is_workspace_mode()` is false, and a constraint naming that same
      directory would hand an external shell the live repository).
    - ``external_workspace_root``: the ROOT of an external-workspace task holds no
      acting constraint at all — the old seam answered ``write_root_missing`` here —
      so its authority derives from its own VALIDATED active workspace: workspace mode
      external, workspace root resolving to the active root. (Owner 2=A: the root
      already holds write+shell inside the project; the only prior gap was provenance,
      which the C1 snapshot + explicit apply now records per run.)

    Read-only runs return the ordinary active root with ``capture_mode: "none"``.
    Disagreement anywhere is a typed refusal, never a best-effort guess.
    """
    root = str(active_repo_dir_for(ctx))
    if authority.access != "workspace_write":
        return {"target_root": root, "source": "readonly", "capture_mode": "none"}, ""
    constraint = getattr(ctx, "task_constraint", None)
    mode = str(
        (constraint.get("mode") if isinstance(constraint, dict)
         else getattr(constraint, "mode", "")) or ""
    ).strip()
    granted = str(
        (constraint.get("write_root") if isinstance(constraint, dict)
         else getattr(constraint, "write_root", "")) or ""
    ).strip()
    # ONE predicate, the one the registry already owns. `workspace_mode_block_reason`
    # returns "" precisely WHEN `workspace_mode` is empty, so "no block reason" is
    # satisfied by the absence of a workspace — the condition was true in exactly the
    # case it was written to refuse. `is_workspace_mode()` is the question actually
    # being asked, and `active_repo_dir()` branches on that same call.
    workspace_active = bool(
        callable(getattr(ctx, "is_workspace_mode", None)) and ctx.is_workspace_mode())
    if mode == "acting_subagent" or granted:
        if not granted:
            return {}, _fail(
                "delegate_start", "write_root_missing",
                "This child is allowed to write, but its task constraint names no write_root, "
                "so there is no directory the host can honestly confine the run to.",
            )
        # AGREEMENT IS NOT ENOUGH: `active_repo_dir_for` falls back to `repo_dir` when
        # workspace mode is off, so a constraint whose write_root happens to name that
        # same directory made the comparison pass and handed a shell the live repository
        # — the very case this guard was written for. Require a genuinely ACTIVE
        # workspace.
        if not workspace_active:
            return {}, _fail(
                "delegate_start", "workspace_not_active",
                "A delegated run may only WRITE inside an ACTIVE workspace, and this task "
                "has none. Refusing rather than falling back to the repository root.",
            )
        # "Can this path be resolved at all" is ONE question, not an exception set to
        # re-enumerate: an embedded null raises ValueError and a symlink loop RuntimeError,
        # and either escaping here would abort delegate_start with a traceback instead of
        # the typed refusal this function exists to produce.
        resolved_root, resolved_grant = _resolved(root), _resolved(granted)
        if resolved_root is None or resolved_root != resolved_grant:
            return {}, _fail(
                "delegate_start", "write_root_mismatch",
                "The active root and the granted write_root disagree, so the run would write "
                "somewhere this task was never given. Refusing rather than guessing.",
                active_root=root, granted_write_root=granted,
            )
        return {"target_root": root, "source": "acting_constraint",
                "capture_mode": _CAPTURE_DELEGATED_SNAPSHOT}, ""
    # ROOT branch (B5): no acting constraint. The mutating shape can only have come
    # from the external-workspace-root profile, and the workspace contract is the
    # authority: workspace genuinely active, mode external, and the declared
    # workspace root must BE the active root.
    if not workspace_active:
        return {}, _fail(
            "delegate_start", "workspace_not_active",
            "A delegated run may only WRITE inside an ACTIVE workspace, and this task "
            "has none. Refusing rather than falling back to the repository root.",
        )
    ws_mode = str(getattr(ctx, "workspace_mode", "") or "").strip().lower()
    if ws_mode not in {"external", "external_workspace"}:
        return {}, _fail(
            "delegate_start", "write_root_missing",
            "This task holds a mutating shape but neither an acting write_root nor an "
            "external workspace contract names the tree it may write. Refusing rather "
            "than guessing a target.",
        )
    declared = _resolved(getattr(ctx, "workspace_root", None))
    resolved_root = _resolved(root)
    if resolved_root is None or declared is None or resolved_root != declared:
        return {}, _fail(
            "delegate_start", "write_root_mismatch",
            "The active root does not resolve to this task's declared external "
            "workspace, so the run would write somewhere this task was never given.",
            active_root=root, declared_workspace_root=str(getattr(ctx, "workspace_root", "") or ""),
        )
    return {"target_root": root, "source": "external_workspace_root",
            "capture_mode": _CAPTURE_DELEGATED_SNAPSHOT}, ""


def _retry_binding_refusal(record: Dict[str, Any], retry_token: str) -> str:
    """Refuse a MUTATING retry whose stored row carries no C1 isolation binding.

    A PRE-C1 row's recorded body scopes the run at the LIVE target tree, and the
    body is replayed byte-identically — so the binding cannot be minted
    retroactively and replaying would write straight into the shared tree, in the
    in-place regime C1 retired. Returns "" when the full binding is present.
    """
    snapshot_id = str(record.get("snapshot_id") or "")
    baseline_sha = str(record.get("baseline_sha") or "")
    target_root = str(record.get("target_root") or "")
    if snapshot_id and str(record.get("execution_root") or "") and baseline_sha and target_root:
        return ""
    return _fail(
        "delegate_start", "retry_binding_absent",
        "This retry replays a MUTATING invocation recorded BEFORE private "
        "execution snapshots (no snapshot/baseline binding), so replaying it "
        "would write directly into the shared tree. Start a new run with a "
        "plain delegate_start — it takes its own snapshot and returns its "
        "diff for explicit integration.",
        retry_of=retry_token, recorded_root=target_root,
        snapshot_id=snapshot_id, baseline_sha=baseline_sha)


def _validated_invocation(drive: Any, retry_token: str, task_id: str,
                          text: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """The stored invocation a retry may replay, or the typed refusal that stops it.

    Six ways a token is not replayable, each answered by name: no record, another
    task's, already bound, definitely refused (its id is retired — replaying wedges a
    permanent 409), no canonical body, prompt disagrees. One author for all six.
    """
    record = custody.invocation_record(drive, retry_token)
    if record is None:
        return None, _fail("delegate_start", "unknown_invocation",
                           "retry_of names an invocation with no durable record on this "
                           "drive. Start a new run with a plain delegate_start.",
                           retry_of=retry_token)
    if record["task_id"] != task_id:
        return None, _fail("delegate_start", "invocation_not_owned",
                           "retry_of names another task's invocation. A delegated start "
                           "may only be retried by the task that requested it.",
                           retry_of=retry_token)
    if record["state"] == "started":
        return None, _fail("delegate_start", "invocation_already_started",
                           "That invocation already bound a run — do not re-post it. "
                           "Wait on the existing run instead.",
                           retry_of=retry_token, run_id=record["run_id"])
    if record["state"] == "failed_definite":
        return None, _fail("delegate_start", "invocation_definitely_refused",
                           "That invocation was definitively refused by the daemon; its "
                           "id is retired. Start a new run with a plain delegate_start.",
                           retry_of=retry_token)
    body = record["request"]
    if not isinstance(body, dict) or not body:
        return None, _fail("delegate_start", "invocation_request_unrecorded",
                           "That invocation's durable row carries no canonical request "
                           "body, so it cannot be replayed byte-identically. Start a "
                           "new run with a plain delegate_start.",
                           retry_of=retry_token)
    if str(body.get("prompt") or "") != text:
        return None, _fail("delegate_start", "retry_prompt_mismatch",
                           "retry_of replays the RECORDED invocation, but the prompt "
                           "you passed differs from the one it sent. Pass the original "
                           "prompt to retry, or drop retry_of to start a new run.",
                           retry_of=retry_token)
    return record, ""


class _RetryBinding(NamedTuple):
    """Every fact of a replayed invocation, read off its ONE durable record."""

    request_body: Dict[str, Any]
    route: Any          # DelegationRoute
    authority: Any      # DelegatedRunShape
    root: str
    key: str
    project_id: str
    owned_project_id: str
    seconds: int
    snapshot_id: str
    target_root: str
    baseline_sha: str
    authority_source: str


def _resolve_retry_invocation(ctx: ToolContext, drive: pathlib.Path, retry_token: str,
                              text: str) -> Tuple[Optional[_RetryBinding], str]:
    """Rebuild a retried start from its stored invocation, or refuse it typed.

    The stored invocation is the SINGLE SOURCE of EVERY fact about a retry — the
    health-checked route, the shape, the root, the project, the lookup key — not
    only of the wire bytes: re-deriving any of them POSTed the recorded body while
    the record and the parent's result described a configuration the run never had.
    Validated BEFORE any daemon call, so a refused token registers nothing.

    A MUTATING replay additionally re-proves its C1 binding: the row must carry the
    snapshot/baseline binding at all (pre-C1 rows are refused), the task's PRESENT
    workspace must still resolve to the recorded authority target, and the recorded
    snapshot must still exist on disk — a GC-collected baseline cannot be re-minted.
    """
    from ouroboros.subagents import DelegatedRunShape, DelegationRoute

    record, refusal = _validated_invocation(
        drive, retry_token, str(getattr(ctx, "task_id", "") or ""), text)
    if refusal:
        return None, refusal
    request_body = record["request"]
    execution = (request_body.get("execution")
                 if isinstance(request_body.get("execution"), dict) else {})
    scope = request_body.get("scope") if isinstance(request_body.get("scope"), dict) else {}
    route = DelegationRoute(route_id=str(request_body.get("primaryHarness") or ""),
                            model=str(request_body.get("model") or ""),
                            effort=str(request_body.get("effort") or ""))
    authority = DelegatedRunShape(access=str(request_body.get("access") or ""),
                                  mode=str(request_body.get("mode") or ""),
                                  isolation=str(execution.get("isolation") or ""),
                                  delegated=bool(execution.get("delegated")))
    root = str(scope.get("root") or "")
    project_id = str(record.get("project_id") or "")
    # The C1 isolation binding recorded at the original attempt. Pre-C1 rows
    # carry none; their scope.root IS the authority target (in-place regime).
    snapshot_id = str(record.get("snapshot_id") or "")
    target_root = str(record.get("target_root") or "") or root
    if authority.access == "workspace_write":
        binding_refusal = _retry_binding_refusal(record, retry_token)
        if binding_refusal:
            return None, binding_refusal
        # The replay will WRITE for the recorded AUTHORITY TARGET, so containment
        # is re-asked against the task's PRESENT workspace — and the answer must
        # be the very target the invocation recorded. A workspace that moved
        # between the attempts makes the replay a write destined for a tree this
        # task no longer holds, which is a refusal, never a re-derivation.
        record_auth, root_error = _mutation_authority(ctx, authority)
        if root_error:
            return None, root_error
        resolved_current = _resolved(record_auth.get("target_root"))
        resolved_recorded = _resolved(target_root)
        if resolved_current is None or resolved_current != resolved_recorded:
            return None, _fail(
                "delegate_start", "retry_root_divergence",
                "This retry replays a MUTATING invocation recorded against a root "
                "this task no longer holds: the active write root has moved since "
                "the original attempt. Start a new run for the current root.",
                retry_of=retry_token, recorded_root=target_root,
                active_root=record_auth.get("target_root"))
        if snapshot_id:
            # The retry reproduces the EXACT binding — same execution root, same
            # baseline. A snapshot the GC already collected cannot be re-minted
            # (the baseline commit is gone with its ref): typed refusal.
            from ouroboros.subagent_worktrees import find_execution_snapshot
            snap_entry = find_execution_snapshot(snapshot_id)
            if snap_entry is None or not pathlib.Path(str(snap_entry.get("path") or "")).exists():
                return None, _fail(
                    "delegate_start", "execution_snapshot_missing",
                    "This retry replays a MUTATING invocation whose private "
                    "execution snapshot no longer exists on disk, so the recorded "
                    "binding cannot be reproduced. Start a new run with a plain "
                    "delegate_start (it will take a fresh snapshot).",
                    retry_of=retry_token, snapshot_id=snapshot_id)
    return _RetryBinding(
        request_body=request_body,
        route=route,
        authority=authority,
        root=root,
        key=str(record.get("idempotency_key") or ""),
        project_id=project_id,
        owned_project_id=(project_id if record.get("project_owned") else ""),
        seconds=int(request_body.get("maxSeconds") or 0),
        snapshot_id=snapshot_id,
        target_root=target_root,
        baseline_sha=str(record.get("baseline_sha") or ""),
        authority_source=str(record.get("authority_source") or ""),
    ), ""


def _provision_snapshot(ctx: ToolContext, drive: pathlib.Path, target_root: str,
                        invocation_id: str) -> Tuple[Optional[Any], str]:
    """Provision the C1 private execution snapshot for one mutating run.

    Registered durably (worktree registry) and described durably (baseline manifest
    artifact) BEFORE the caller records any start intent, so a worker death at any
    later point leaves a nameable, GC-reconcilable root — never an orphan directory.
    Returns ``(handle, "")`` or ``(None, typed_refusal)``.
    """
    from ouroboros.subagent_worktrees import provision_execution_snapshot

    task_id = str(getattr(ctx, "task_id", "") or "")
    try:
        handle = provision_execution_snapshot(
            target_root=target_root, task_id=task_id, snapshot_id=invocation_id)
    except Exception as exc:
        return None, _fail(
            "delegate_start", "execution_snapshot_failed",
            "A private execution snapshot of the write root could not be provisioned "
            f"({type(exc).__name__}: {exc}). The run was NOT started: a mutating "
            "delegated run executes only in its own snapshot, never in the shared tree.",
            target_root=target_root)
    # The forensic baseline record beside the capture-to-be. Fail-soft: the BINDING
    # itself is durable on the custody request row, and the baseline tree stays
    # reconstructable from the pinned ref for as long as the snapshot lives.
    try:
        from ouroboros.utils import atomic_write_json, utc_now_iso

        cap_dir = custody.delegated_capture_dir(drive, task_id, invocation_id)
        cap_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(cap_dir / "baseline_manifest.json", {
            "schema_version": 1,
            "created_at": utc_now_iso(),
            "snapshot_id": handle.snapshot_id,
            "baseline_id": handle.baseline_sha,
            "baseline_tree": handle.baseline_tree,
            "manifest_digest": handle.manifest_digest,
            "entry_count": handle.entry_count,
            "target_root": handle.target_root,
            "target_head": handle.target_head,
            "execution_root": handle.path,
            "excluded_untracked": list(handle.excluded_untracked),
        }, trailing_newline=True)
    except Exception:
        log.warning("Failed to write delegated baseline manifest for %s", invocation_id,
                    exc_info=True)
    return handle, ""


def _capture_block(entry: _RunCustody, cap_dir: pathlib.Path,
                   manifest: Dict[str, Any]) -> Dict[str, Any]:
    """The terminal-payload projection of one captured run patch (C1)."""
    from ouroboros.headless import ARTIFACT_STATUS_READY_WITH_CHANGES

    status = str(manifest.get("status") or "")
    patch_path = cap_dir / "workspace.patch"
    has_patch = status == ARTIFACT_STATUS_READY_WITH_CHANGES and patch_path.exists()
    # The tool-surface read handle (CR1-2): the capture lives on the CANONICAL
    # drive, which a split-drive child cannot reach through a raw absolute path —
    # read_file(root='artifact_store', path=...) resolves this prefix against the
    # canonical root for the owning task, so THIS pair is what the agent reads.
    read_prefix = f"delegated_runs/{cap_dir.name}"
    block: Dict[str, Any] = {
        "status": status or "missing",
        "baseline_id": entry.baseline_sha,
        "execution_root": entry.execution_root,
        "authority_target_root": entry.target_root,
        "patch_artifact": str(patch_path) if has_patch else None,
        "manifest_artifact": str(cap_dir / "workspace_patch.json"),
        "patch_read": ({"root": "artifact_store", "path": f"{read_prefix}/workspace.patch"}
                       if has_patch else None),
        "manifest_read": {"root": "artifact_store", "path": f"{read_prefix}/workspace_patch.json"},
        "sha256": str(manifest.get("sha256") or ""),
        "diffstat": str(manifest.get("diffstat") or ""),
        "note": (
            "NOT APPLIED: the run edited its private execution snapshot only. Nothing "
            "reaches the shared tree until you explicitly call "
            "integrate_delegated_patch(run_id=...) with decision='apply' or 'reject'. "
            "The snapshot and this patch persist until that disposition. Read the "
            "captured diff via read_file(root='artifact_store', path=patch_read.path)."
        ),
    }
    current_head = str(manifest.get("current_head") or "")
    if current_head and entry.baseline_sha and current_head != entry.baseline_sha:
        # Single-writer snapshot: a moved HEAD can only mean the run itself committed.
        # The diff is still measured from the baseline (committed work is captured),
        # but the violation of the no-commit instruction is disclosed, not hidden.
        block["head_moved"] = {
            "baseline": entry.baseline_sha, "current": current_head,
            "note": "the run COMMITTED inside its snapshot despite instructions; its "
                    "committed work is still in the captured diff",
        }
    return block


def _capture_terminal_patch(ctx: ToolContext, entry: Optional[_RunCustody]) -> Optional[Dict[str, Any]]:
    """Capture a terminal mutating run's diff from its execution snapshot, durably.

    Idempotent: the durable ``patch_captured`` flag (replayed) plus the manifest on
    disk mean a re-wait re-reads the existing capture instead of re-diffing. Runs
    only for C1-isolated runs (``execution_root`` recorded); read-only and legacy
    in-place runs return None. Capture failure is disclosed in the block, never
    raised — the settlement and delivery around it must not die on a diff error.
    """
    if entry is None or not entry.execution_root:
        return None
    return capture_terminal_patch_for_drive(custody.custody_root(ctx), entry)


def capture_terminal_patch_for_drive(drive: Any, entry: _RunCustody) -> Optional[Dict[str, Any]]:
    """The drive-rooted core of the terminal capture — same contract, no ToolContext.

    The RECONCILIATION path (owner task gone: orphan sweep, kill-path reconcile,
    pending-invocation recovery) settles mutating runs from a bare drive root, and
    a run settled there with no capture strands the child's work in the snapshot
    with no apply/reject material. One capture author for both paths, so the nanny
    flow and the sweep cannot drift in what a "captured patch" is. Capture only —
    the apply/reject DECISION stays with a live owner, never taken here.

    ``patch_captured`` MEANS "a usable patch artifact exists" (C1-R3): the custody
    row is minted only over a manifest whose own status is ready. A manifest that
    reports its own failure is returned as the failed block but leaves the row
    uncaptured, so every retry point (re-wait, sweep, disposition) stays open —
    and a durable row minted by pre-R3 code over a failed manifest is not trusted
    either: the replay falls through to a fresh capture instead of serving the
    failed manifest forever.
    """
    if entry is None or not entry.execution_root:
        return None
    from ouroboros.headless import (
        ARTIFACT_STATUS_READY_NO_CHANGES,
        ARTIFACT_STATUS_READY_WITH_CHANGES,
    )

    ready = {ARTIFACT_STATUS_READY_WITH_CHANGES, ARTIFACT_STATUS_READY_NO_CHANGES}
    cap_dir = custody.delegated_capture_dir(drive, entry.task_id, entry.snapshot_id or entry.run_id)
    manifest_path = cap_dir / "workspace_patch.json"
    if entry.patch_captured and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            manifest = {}
        manifest = manifest if isinstance(manifest, dict) else {}
        if str(manifest.get("status") or "") in ready:
            return _capture_block(entry, cap_dir, manifest)
    exec_root = pathlib.Path(entry.execution_root)
    if not exec_root.exists():
        return {
            "status": "missing",
            "baseline_id": entry.baseline_sha,
            "execution_root": entry.execution_root,
            "authority_target_root": entry.target_root,
            "patch_artifact": None,
            "manifest_artifact": None,
            "note": "the private execution snapshot no longer exists on disk, so the "
                    "run's changes cannot be captured. Nothing was applied anywhere.",
        }
    try:
        from ouroboros.headless import write_workspace_patch_artifacts

        cap_dir.mkdir(parents=True, exist_ok=True)
        # The preflight-head shape pins the capture BASE to the snapshot's baseline
        # commit (never a moved HEAD), reusing the one existing capture primitive —
        # sensitive veto, binary handling, sha256 manifest and all.
        _, manifest = write_workspace_patch_artifacts(
            exec_root, cap_dir,
            task={"metadata": {"workspace_preflight": {"git": {"head": entry.baseline_sha}}}},
        )
    except Exception as exc:
        log.warning("Delegated run patch capture failed for %s", entry.run_id, exc_info=True)
        return {
            "status": "failed",
            "baseline_id": entry.baseline_sha,
            "execution_root": entry.execution_root,
            "authority_target_root": entry.target_root,
            "patch_artifact": None,
            "manifest_artifact": None,
            "note": f"patch capture failed ({type(exc).__name__}: {exc}); the execution "
                    "snapshot is preserved — inspect it directly.",
        }
    if str(manifest.get("status") or "") in ready:
        custody.record_patch_captured(
            drive, entry,
            status=str(manifest.get("status") or ""),
            sha256=str(manifest.get("sha256") or ""),
            patch_size=manifest.get("patch_size"),
            capture_dir=str(cap_dir),
        )
    return _capture_block(entry, cap_dir, manifest)


__all__ = [
    "_CAPTURE_DELEGATED_SNAPSHOT",
    "_RetryBinding",
    "_capture_block",
    "_capture_terminal_patch",
    "_fail",
    "_mutation_authority",
    "_provision_snapshot",
    "_resolve_retry_invocation",
    "_resolved",
    "_retry_binding_refusal",
    "_validated_invocation",
    "capture_terminal_patch_for_drive",
]
