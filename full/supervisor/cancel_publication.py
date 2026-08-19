"""Cancellation settlement PUBLICATION: typed outcomes, result fields, delivery.

Split from ``supervisor.task_lifecycle`` by the module-size gate — a code
boundary only, exactly like ``supervisor.queue_transitions``: the queue module
object (``q``) is always passed in by the caller, so ``supervisor.queue``
remains the single state authority and every mutation still runs under its
existing process lock. ``task_lifecycle`` re-imports every name here, so
callers, tests, and ``supervisor.queue``'s public re-exports keep ONE import
surface.

Owns the pieces of a cancel that happen AFTER custody decided the outcome:
the typed outcome vocabulary, the artifact-honest cancelled result fields,
the cost reconstruction adapter, the salvage adapter, the owed-before-settle
outbox registration (GR2-4), the publication of the stored terminal truth,
and the miss-lane delivery adapter.
"""

from __future__ import annotations

import logging
import pathlib
from typing import Any, Dict, List, Optional

from ouroboros.utils import utc_now_iso

log = logging.getLogger(__name__)


# Typed per-id cancellation outcome (v6.82.0). A boolean cannot distinguish
# "cancelled", "it had already finished on its own" and "refused/failed" — and a
# cascade that OR-aggregates booleans reports success while a child is still live.
CANCEL_CANCELLED = "cancelled"
CANCEL_ALREADY_SETTLED = "already_settled"
CANCEL_NOT_FOUND = "not_found"
CANCEL_FAILED = "failed"
_CANCEL_TERMINALIZED = frozenset({CANCEL_CANCELLED, CANCEL_ALREADY_SETTLED, CANCEL_NOT_FOUND})


def _load_result_row(q: Any, task_id: str) -> Dict[str, Any]:
    """The durable result row, or ``{}`` — fail-soft."""
    try:
        from ouroboros.task_results import load_task_result

        return load_task_result(q.DRIVE_ROOT, task_id) or {}
    except Exception:
        log.debug("durable result read failed for %s", task_id, exc_info=True)
        return {}


def _is_workspace_task_record(record: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(record, dict):
        return False
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    return bool(str(record.get("workspace_root") or "").strip() or str(metadata.get("workspace_root") or "").strip())


def _cancel_result_fields(
    task: Optional[Dict[str, Any]],
    *,
    existing: Optional[Dict[str, Any]] = None,
    result: str,
    artifact_capture: str = "never_started",
    **fields: Any,
) -> Dict[str, Any]:
    """Terminal-cancel result fields, artifact-honest per A4 (owner batch-1 9=A).

    Workspace artifact truth on a cancel comes from the REAL tree: the running
    kill path runs ``finalize_task_artifacts`` first (``artifact_capture=
    "attempted"``) and any terminal artifact status it recorded is PRESERVED
    here, never overwritten with a blanket "missing". Only when no capture
    evidence exists does the stamp depend on how far the task got: a task that
    never started honestly has ``missing`` artifacts, while an attempted capture
    that left nothing terminal is a capture FAILURE. Cancel-path captures of a
    shared workspace tree carry ``artifact_attribution: "shared_unproven"`` —
    the tree was not quiesced through the normal patch-capture seam, so the diff
    cannot prove which actor authored it (until the phase-C run isolation).
    """
    payload: Dict[str, Any] = {**fields, "result": result}
    if not (_is_workspace_task_record(task) or _is_workspace_task_record(existing)):
        return payload
    try:
        from ouroboros.headless import ARTIFACT_STATUS_FAILED, ARTIFACT_STATUS_MISSING
        from ouroboros.outcomes import artifact_bundle_from_result, normalize_outcome_axes
        from ouroboros.task_status import ARTIFACT_TERMINAL_STATUSES

        existing_artifact_status = str(
            (existing or {}).get("artifact_status") or ""
        ).strip().lower()
        if existing_artifact_status in ARTIFACT_TERMINAL_STATUSES:
            # A4: a real capture already recorded its terminal truth (patch,
            # no-changes, or failed) — keep it; only annotate attribution.
            payload["artifact_attribution"] = "shared_unproven"
            return payload
        if str(artifact_capture or "") == "attempted":
            capture_status = ARTIFACT_STATUS_FAILED
            capture_error = "Workspace artifact capture did not complete during cancellation teardown."
        elif str(artifact_capture or "") == "owed_no_result":
            # A4: the kill caught a RUNNING workspace task before its durable row
            # existed, so the owed capture could not run safely — a capture
            # FAILURE (the tree may hold uncaptured work), never ``missing``.
            capture_status = ARTIFACT_STATUS_FAILED
            capture_error = (
                "Workspace artifact capture was owed but could not run: the task was "
                "killed before its durable result row existed."
            )
        else:
            capture_status = ARTIFACT_STATUS_MISSING
            capture_error = "Task cancelled before workspace patch finalization."
        base: Dict[str, Any] = {}
        if isinstance(existing, dict):
            base.update(existing)
        if isinstance(task, dict):
            base.update(task)
        payload["artifact_status"] = capture_status
        payload.setdefault("artifact_error", capture_error)
        payload["artifact_attribution"] = "shared_unproven"
        base.update(payload)
        base["status"] = "cancelled"
        base["artifact_status"] = capture_status
        base.pop("artifact_bundle", None)
        bundle = artifact_bundle_from_result(base)
        payload["artifact_bundle"] = bundle
        axes = normalize_outcome_axes(base)
        artifact_axis = dict(axes.get("artifacts") or {})
        artifact_axis["status"] = capture_status
        axes["artifacts"] = artifact_axis
        payload["outcome_axes"] = axes
    except Exception:
        log.debug("Failed to build cancelled artifact fields for task %s", (task or existing or {}).get("id") or (task or existing or {}).get("task_id"), exc_info=True)
    return payload


def _reconstructed_cost_fields(q: Any, task_id: str, task: Dict[str, Any]) -> Dict[str, Any]:
    """Cost authority for a supervisor-settled cancel: the physical-attempt
    ledger (a never-dispatched task reconstructs to a CONFIRMED zero), degrading
    to an honest nullable unknown — never a fabricated final $0 (owner 10=B)."""
    try:
        return q.reconstruct_task_cost(
            str(task_id), fields=True,
            drive_root=pathlib.Path((task or {}).get("budget_drive_root") or q.DRIVE_ROOT),
        )
    except Exception:
        log.warning("Cost reconstruction failed for cancelled %s", task_id, exc_info=True)
        return {"cost_accounting_status": "unavailable", "cost_final": False,
                "cost_accounting_error": "ledger_unavailable", "cost_usd": None}


def _salvage_cancelled_output(
    q: Any, task: Dict[str, Any], task_id: str,
) -> tuple[str, str, str]:
    """(result-suffix note, full salvaged text, preserved-copy path) — fail-soft.

    Thin adapter over the shared delivery-seam salvage (terminal_delivery owns
    the mechanics; the reaper shares the same underlying helpers)."""
    from supervisor.terminal_delivery import salvage_cancelled_output

    return salvage_cancelled_output(
        pathlib.Path(q.DRIVE_ROOT), q._task_drive_for_task(task, str(task_id)), str(task_id),
    )


def _register_owed_terminal_delivery(
    q: Any, task: Dict[str, Any], task_id: str, stored: Dict[str, Any],
    *, deliver: bool, salvage_text: str = "", salvage_path: str = "",
    unreconciled_runs: Optional[List[str]] = None,
) -> bool:
    """GR2-4: durably register the terminal answer as OWED before the settle.

    Builds the exact event the publish step will enqueue (the build is
    deterministic, so both carry the same content-hash ``delivery_id``) and
    registers it in the durable outbox. When a delivery is expected but cannot
    be routed (no lineage chat), a typed handoff row records that decision —
    the crash-order evidence must show the answer was consciously not owed,
    not silently dropped. Cascade-suppressed (``deliver=False``) and subagent
    outcomes are owed elsewhere (the root summary / the parent handoff).

    Returns whether the answer is DURABLY accounted for (GR3-4): owed in the
    outbox, already delivered/owed, legitimately not deliverable here
    (suppressed / subagent / non-delivering status), or consciously handed
    off. ``False`` means a real durability gap — the registration or the
    handoff record could not be written — and the caller must leave the
    cancel intent OPEN instead of settling over an unowed answer. Never
    raises and never gates the teardown itself.
    """
    try:
        from supervisor.terminal_delivery import (
            build_completed_result_event,
            build_unreviewed_salvage_event,
            register_pending_delivery,
        )

        from ouroboros.task_results import STATUS_CANCELLED, STATUS_COMPLETED

        if not deliver or str((task or {}).get("delegation_role") or "") == "subagent":
            return True
        stored_status = str((stored or {}).get("status") or "")
        event: Optional[Dict[str, Any]] = None
        if stored_status == STATUS_CANCELLED:
            event = build_unreviewed_salvage_event(
                pathlib.Path(q.DRIVE_ROOT), task, task_id,
                outcome="cancelled",
                salvaged_text=salvage_text, preserved_path=salvage_path,
                unreconciled_runs=list(unreconciled_runs or []),
                settled_status=stored_status,
            )
        elif stored_status == STATUS_COMPLETED:
            # GR6-5a: the disclosure rides the completed text too, and the
            # owed registration and the publish half must build the SAME text
            # (one delivery id) — both pass the identical list.
            event = build_completed_result_event(
                pathlib.Path(q.DRIVE_ROOT), task, task_id, stored,
                unreconciled_runs=list(unreconciled_runs or []),
            )
        else:
            # failed/rejected outcomes keep their own paths' delivery.
            return True
        if event is not None:
            return register_pending_delivery(pathlib.Path(q.DRIVE_ROOT), event)
        return bool(q.append_jsonl(
            pathlib.Path(q.DRIVE_ROOT) / "logs" / "supervisor.jsonl",
            {"ts": utc_now_iso(), "type": "terminal_delivery_handoff",
             "task_id": task_id, "settled_status": stored_status,
             "reason": "no_lineage_chat"},
        ))
    except Exception:
        log.warning("owed terminal-delivery registration failed for %s", task_id, exc_info=True)
        return False


def _settle_or_reopen_intent(
    q: Any, task_id: str, *, owed_ok: bool,
    intent: Optional[Dict[str, Any]], outcome: str, detail: str,
) -> None:
    """GR4-1, the ONE registration-failure rule for every settle site: an
    answer that could not be durably owed leaves the intent OPEN — the fenced
    claim is released for the watchdog, whose re-feed re-attempts the
    registration (loud via the typed ``terminal_delivery_unregistered`` event
    each tick) — instead of settling over an unowed answer. The teardown and
    publication themselves are never gated."""
    from supervisor.task_lifecycle import _release_intent_claim, _settle_intent

    if not owed_ok and intent and intent.get("request_id"):
        _release_intent_claim(
            q, task_id,
            error="owed terminal-delivery registration failed", intent=intent,
        )
    else:
        _settle_intent(q, task_id, outcome=outcome, detail=detail, intent=intent or None)


def _publish_cancelled_task(
    q: Any, task_id: str, task: Dict[str, Any], worker: Any,
    stored: Dict[str, Any], cost_fields: Dict[str, Any],
    *, deliver: bool = True, salvage_text: str = "", salvage_path: str = "",
    unreconciled_runs: Optional[List[str]] = None,
) -> str:
    """Publish the STORED terminal truth and reconcile the worker slot.

    The event carries whatever actually settled: if the worker wrote its own
    natural result before we killed it, the monotonic guard refused our
    cancellation — publishing nothing would leave that card unresolved until a
    reload, so the stored status is emitted instead.
    """
    from supervisor import workers

    from ouroboros.task_results import STATUS_CANCELLED

    settled_status = str((stored or {}).get("status") or STATUS_CANCELLED)
    # The row leaves RUNNING only NOW — death confirmed, terminal result durable.
    # A natural task_done that raced us may have consumed the row already; that
    # handler also emitted the terminal event, so whoever pops the row owns the
    # emit and the card resolves exactly once.
    with q._queue_lock:
        row_owned = q.RUNNING.pop(task_id, None) is not None
    try:
        from ouroboros.tools.services import archive_task_service_logs
        archive_task_service_logs(pathlib.Path(q.DRIVE_ROOT), str(task_id), task)
    except Exception:
        log.debug("Failed to archive service logs for cancelled task %s", task_id, exc_info=True)
    # A2 delivery: the salvaged answer reaches chat through the shared durable
    # outbox seam (loud UNREVIEWED banner) — the incident's finished report sat
    # on disk 30 seconds from the owner. Non-subagent tasks only (a child's
    # result flows to its parent through the ordinary handoff); a completed
    # natural result is delivered as itself. Fail-soft, idempotent (the owed
    # row was registered BEFORE the intent settled — GR2-4 — and the shared
    # delivery_id dedupes the enqueue here).
    if deliver and str(task.get("delegation_role") or "") != "subagent":
        try:
            from supervisor.terminal_delivery import deliver_unreviewed_salvage

            if settled_status == STATUS_CANCELLED:
                deliver_unreviewed_salvage(
                    pathlib.Path(q.DRIVE_ROOT), task, task_id,
                    outcome="cancelled",
                    salvaged_text=salvage_text, preserved_path=salvage_path,
                    unreconciled_runs=list(unreconciled_runs or []),
                    settled_status=settled_status,
                )
            elif settled_status == "completed":
                # Completion-wins: the worker died before its own final delivery
                # could be confirmed; ship the completed result through the same
                # deduped seam (a copy the worker already delivered is suppressed
                # by the shared delivery id).
                from supervisor.terminal_delivery import deliver_completed_result

                deliver_completed_result(
                    pathlib.Path(q.DRIVE_ROOT), task, task_id, stored,
                    unreconciled_runs=list(unreconciled_runs or []),
                )
        except Exception:
            log.debug("Terminal salvage delivery failed for %s", task_id, exc_info=True)
    if row_owned:
        try:
            q._emit_cancel_task_done(task, task_id, cost_fields=cost_fields, status=settled_status)
        except Exception:
            log.warning("Failed to publish terminal event for %s", task_id, exc_info=True)
    # Respawn recovery is the REAPER'S canonical step 5, not a private variant.
    # The helper serializes against shutdown with the lifecycle lock and starts
    # the child outside the queue lock; on failure the marker is cleared under
    # the lock so the crash detector can recover the slot on a later tick.
    try:
        workers.respawn_worker(worker.wid)
    except Exception:
        log.warning("Respawn after cancelling %s failed; clearing reaping for recovery", task_id, exc_info=True)
        try:
            with q._queue_lock:
                slot = workers.WORKERS.get(worker.wid)
                if slot is not None:
                    slot.reaping = False
        except Exception:
            log.debug("Could not clear the slot marker for %s", task_id, exc_info=True)
    if str(task.get("delegation_role") or "") == "subagent":
        try:
            from ouroboros.headless import remove_subagent_task_drive
            remove_subagent_task_drive(q.DRIVE_ROOT, str(task_id))
        except Exception:
            log.debug("Failed to remove cancelled subagent drive for %s", task_id, exc_info=True)
    try:
        q.persist_queue_snapshot(reason="cancel_running")
    except Exception:
        log.debug("Snapshot after cancelling %s failed", task_id, exc_info=True)
    return CANCEL_ALREADY_SETTLED if settled_status != STATUS_CANCELLED else CANCEL_CANCELLED


def _deliver_on_miss(
    q: Any, task_id: str, row: Dict[str, Any], status: str,
    *, unreconciled_runs: Optional[List[str]] = None,
) -> bool:
    """Owner 5=A on the miss lane — the routing logic lives in the shared
    delivery seam (``deliver_miss_lane_outcome``); this adapter only supplies
    the queue-owned drive facts. Fail-soft.

    ``unreconciled_runs`` (GR5-3) threads the delegated-custody audit result
    into the miss-lane message, so the owner disclosure matches the kill path.

    Returns the seam's durable-accounting verdict (GR4-1): ``False`` means the
    owed registration failed and the caller must leave the cancel intent OPEN
    (claim released) instead of settling over an unowed answer. A truthy
    return from a monkeypatched/legacy delivery counts as accounted."""
    row = row if isinstance(row, dict) else {}
    try:
        from supervisor.terminal_delivery import deliver_miss_lane_outcome

        return deliver_miss_lane_outcome(
            pathlib.Path(q.DRIVE_ROOT), q._task_drive_for_task(row, str(task_id)),
            row, task_id, status,
            unreconciled_runs=list(unreconciled_runs or []),
        ) is not False
    except Exception:
        log.warning("Miss-lane terminal delivery failed for %s", task_id, exc_info=True)
        return False


def _reconcile_delegated_runs_on_kill(q: Any, task_id: str) -> List[str]:
    """Settle this task's open DELEGATED runs after its worker is dead; disclose
    what stayed open.

    Task-scoped reconciliation (A1.9): the graceful ``release_task_runs`` lived
    inside the worker that is now dead (cancel kill, timeout reap, or an
    already-settled task whose custody is being audited — GR5-3), so its open
    Claudexor runs would keep mutating until the next 10-minute orphan sweep.
    Durable custody rows are the complete view; cheap when the task never
    delegated.

    The DISCLOSURE is unconditional on the audit, never on the outcome list
    (GR2-7): ``reconcile_task_runs`` returning rows proves an ATTEMPT was made
    per run, not that any run settled — ``unreadable``/``requested``/failed
    outcomes and transport exceptions all leave runs open. So after the
    reconcile (successful, empty, or raising) the durable custody rows are
    ALWAYS re-read, and every run still open — plus any invocation still
    pending for this task — is disclosed (typed forensic row + a field on the
    cancelled result + a delivery note), not silently logged at debug.

    An AUDIT FAILURE is typed UNKNOWN, never clean (GR3-7): when the custody
    re-read itself raises, this returns a ``delegated_run_state_unknown``
    marker instead of ``[]`` — the marker rides the same
    ``delegated_runs_unreconciled`` surface (result field, typed event,
    delivery note, ``audit_failed`` flavor), and periodic reconciliation
    remains the eventual closer. A pending-invocation audit failure surfaces
    the same way. GR6-4 closes the quiet corner of the same class: a custody
    log that EXISTS but cannot be OPENED used to replay as empty (audits as
    "cleanly reconciled") because ``_iter_rows`` swallows its own ``OSError``
    — the audit now probes readability first and reports the typed
    ``delegated_run_state_unknown:custody_log_unreadable`` marker; an ABSENT
    log stays a positively-established clean state.
    """
    from supervisor.terminal_delivery import RUN_STATE_UNKNOWN_PREFIX

    still_open: List[str] = []
    audit_failed = False
    try:
        from ouroboros.delegate_custody import (
            custody_log_unreadable, open_runs, pending_invocations, reconcile_task_runs,
        )

        try:
            reconcile_task_runs(pathlib.Path(q.DRIVE_ROOT), task_id)
        except Exception:
            log.warning(
                "Delegated-run reconciliation raised for cancelled %s; auditing custody rows",
                task_id, exc_info=True,
            )
        if custody_log_unreadable(pathlib.Path(q.DRIVE_ROOT)):
            # GR6-4: the custody log EXISTS but cannot be read — the fail-soft
            # replay would answer [] and the audit would report "cleanly
            # reconciled" over evidence it never saw. ABSENT stays a clean
            # empty state; UNREADABLE is typed UNKNOWN through the existing
            # GR3-7 marker vocabulary and rides the same disclosure surface.
            log.warning(
                "delegate custody log unreadable during the teardown audit of %s; "
                "run state is UNKNOWN", task_id,
            )
            audit_failed = True
            still_open = [f"{RUN_STATE_UNKNOWN_PREFIX}:custody_log_unreadable"]
        else:
            still_open = [
                str(getattr(record, "run_id", "") or "")
                for record in open_runs(pathlib.Path(q.DRIVE_ROOT))
                if str(getattr(record, "task_id", "") or "") == task_id
            ]
            try:
                still_open += [
                    f"invocation:{str(row.get('invocation_id') or '')}"
                    for row in pending_invocations(pathlib.Path(q.DRIVE_ROOT))
                    if isinstance(row, dict)
                    and str(row.get("task_id") or "") == task_id
                    and str(row.get("invocation_id") or "")
                ]
            except Exception:
                log.warning("pending-invocation audit failed for %s", task_id, exc_info=True)
                audit_failed = True
                still_open.append(f"{RUN_STATE_UNKNOWN_PREFIX}:pending_invocation_audit_failed")
        still_open = [rid for rid in still_open if rid]
    except Exception:
        log.warning(
            "Delegated-run open-run audit failed for cancelled %s; run state is UNKNOWN",
            task_id, exc_info=True,
        )
        audit_failed = True
        still_open = [f"{RUN_STATE_UNKNOWN_PREFIX}:audit_failed"]
    if not still_open:
        return []
    log.warning(
        "Cancelled task %s left %d delegated run(s) unreconciled: %s",
        task_id, len(still_open), still_open,
    )
    try:
        q.append_jsonl(
            pathlib.Path(q.DRIVE_ROOT) / "logs" / "events.jsonl",
            {
                "ts": utc_now_iso(),
                "type": "delegated_runs_unreconciled",
                "task_id": task_id,
                "run_ids": still_open,
                **({"flavor": "audit_failed"} if audit_failed else {}),
                "detail": (
                    "the teardown audit itself failed; whether this task holds live "
                    "delegated runs is UNKNOWN until periodic reconciliation settles them"
                    if audit_failed else
                    "cancellation custody could not settle these delegated runs "
                    "(engine unreachable); they may keep mutating until the orphan sweep"
                ),
            },
        )
    except Exception:
        log.debug("Unreconciled-run disclosure append failed for %s", task_id, exc_info=True)
    return still_open
