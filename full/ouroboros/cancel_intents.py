"""Durable cancel-intent projection — the ONE ingress for task cancellation.

The Poltergeist incident class (2026-08-11): cancel intent used to travel as a
``cancel_requested`` value of the canonical task ``status``, which made one field
carry both INTENT and OUTCOME. A lost control event left four children latched
forever, the post-kill re-check read the latch back as a terminal result, and a
nonterminal ``task_done`` wedged the parent. This module separates the two:

- ``state/cancel_intents.json`` (per canonical data root) is a COMPACT, locked
  projection of ACTIVE intents only — ``requested`` → ``claimed`` → settled rows
  LEAVE the projection — so consulting it on queue restore, worker assignment,
  and effective-status reads is one small locked read, never a scan of a growing
  ledger.
- Every state change also appends a typed ``cancel_intent`` row to
  ``logs/supervisor.jsonl`` as a forensic trail (reusing the existing supervisor
  ledger; the trail is never read back for state).

Ownership: every cancel ingress (agent ``cancel_task`` tool, HTTP single, HTTP
cascade, boot migration of legacy latch files) writes an intent through
``request_cancel``. The supervisor's ``cancel_task_custody`` is the ONE settle
owner: it CLAIMS the intent before teardown and SETTLES it with the terminal
outcome; the supervisor-tick watchdog only re-feeds unclaimed/stale intents back
into custody. The canonical ``status`` never carries intent again.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ouroboros.utils import append_jsonl, update_json_locked, utc_now_iso

log = logging.getLogger(__name__)


class CancelIntentProjectionCorrupt(RuntimeError):
    """The intent projection file exists but is malformed/non-object JSON.

    Raised (GR3-9) instead of silently collapsing the projection to ``{}`` and
    overwriting it — which would lose EVERY active intent in one write. The
    append-only forensic trail in ``logs/supervisor.jsonl`` keeps the evidence;
    the caller fails closed (the cancel ingress reports the intent write as
    failed rather than pretending the intent is durable).
    """


_SCHEMA_VERSION = 1
# A claim older than this is presumed abandoned (custody crashed mid-teardown)
# and the watchdog may re-feed the intent into custody.
CLAIM_STALE_SEC = 180.0

INTENT_REQUESTED = "requested"
INTENT_CLAIMED = "claimed"

# Cancel SCOPE: a cascade intent must be re-fed as a cascade, not as a single
# cancel — a watchdog replay of the root alone would leave descendants live.
SCOPE_SINGLE = "single"
SCOPE_CASCADE = "cascade"

# Settle outcomes (forensic vocabulary; the projection row is removed on settle).
SETTLED_CANCELLED = "cancelled"
SETTLED_ALREADY_SETTLED = "already_settled"
SETTLED_NOT_FOUND = "not_found"


def _intents_path(drive_root: Any) -> pathlib.Path:
    root = pathlib.Path(drive_root) / "state"
    root.mkdir(parents=True, exist_ok=True)
    return root / "cancel_intents.json"


def _forensic(drive_root: Any, row: Dict[str, Any]) -> None:
    """Append one typed forensic row; the trail is evidence, never state."""
    try:
        append_jsonl(
            pathlib.Path(drive_root) / "logs" / "supervisor.jsonl",
            {"ts": utc_now_iso(), "type": "cancel_intent", **row},
        )
    except Exception:
        log.debug("cancel-intent forensic append failed", exc_info=True)


def _valid_task_id(task_id: Any) -> str:
    from ouroboros.task_results import validate_task_id

    return validate_task_id(task_id)


def _load_intents(data: Dict[str, Any], *, strict: bool = False) -> Dict[str, Any]:
    """The active-intent rows; ``strict`` refuses a malformed nested value.

    GR5-6: the MINTING mutator (``request_cancel``) passes ``strict=True`` — a
    present-but-non-dict ``intents`` under a valid top-level dict used to be
    coerced to ``{}``, so the next mint rewrote the file and silently dropped
    every other active intent, the exact loss the top-level
    ``strict_existing_dict`` check refuses. The raise is the same typed
    ``ValueError``, so the caller's existing corrupt-projection handling
    (refuse + forensic row + ``CancelIntentProjectionCorrupt``) applies. The
    non-minting mutators find no row in ``{}`` and abort without overwriting;
    read paths disclose separately (``active_intents``)."""
    intents = data.get("intents")
    if isinstance(intents, dict):
        return intents
    if strict and intents is not None:
        raise ValueError(
            "cancel-intent projection 'intents' is malformed (not an object)"
        )
    return {}


def settled_status(drive_root: Any, task_id: str) -> str:
    """The task's own already-settled durable status, or "" — fail-soft."""
    try:
        from ouroboros.task_results import load_task_result
        from ouroboros.task_status import SETTLED_STATUSES

        status = str((load_task_result(drive_root, task_id) or {}).get("status") or "")
        return status if status in SETTLED_STATUSES else ""
    except Exception:
        log.debug("cancel-intent settled-status read failed for %s", task_id, exc_info=True)
        return ""


def request_cancel(
    drive_root: Any,
    task_id: str,
    *,
    reason: str = "",
    source: str = "",
    requested_by: str = "",
    scope: str = "",
    allow_settled_target: bool = False,
) -> Dict[str, Any]:
    """Record durable cancel intent for ``task_id`` — idempotent per task.

    Returns the ACTIVE intent row (existing or newly minted) plus
    ``already_requested``. Never touches the canonical task status: teardown and
    the terminal write belong to the supervisor's cancellation custody.

    An ALREADY-SETTLED task with NO live ownership mints nothing: an intent for
    a task that finished on its own would show a false "Cancelling…" badge on a
    settled card until the watchdog cleaned it up, and nothing is left to tear
    down. The caller gets ``already_settled`` plus the real ``status`` instead
    (completion wins).

    ``allow_settled_target`` is the LIVE-OWNERSHIP exception (GR6-1, widening
    the GR2-1b cascade case): the pipeline persists the durable terminal result
    BEFORE post-task cognition ends, so a settled STATUS alone does not prove a
    dead WORKER — ``already_settled`` is a terminal answer only when no live
    physical ownership remains (no RUNNING row / busy worker). This module
    stays pure (it never reads the queue): each INGRESS checks its own live
    ownership fact (`supervisor.queue.task_has_live_ownership` in-process, the
    queue-snapshot read worker-side) and passes ``allow_settled_target=True``
    when ownership is live, so custody can kill the still-spending worker while
    completion-wins preserves the stored result. The cascade-coordination
    ingress (GR2-1b) passes it for a settled root with live descendants — the
    intent is the watchdog's replay trigger and settles only at the cascade
    postcondition. The settled-card badge hazard does not apply: the
    effective-status read only projects ``cancel_state`` onto NON-settled
    results.

    ``scope`` is stored on the row so a watchdog replay re-runs the SAME shape:
    a ``cascade`` intent re-fed as a single cancel would settle the root while
    its descendants kept running.
    """
    tid = _valid_task_id(task_id)
    reason_text = " ".join(str(reason or "").split())[:500]
    settled = settled_status(drive_root, tid)
    if settled and not allow_settled_target:
        _forensic(drive_root, {
            "event": "already_settled", "task_id": tid, "status": settled,
            "source": str(source or ""), "reason": reason_text,
        })
        return {"task_id": tid, "already_requested": False, "already_settled": True,
                "status": settled}
    minted: Dict[str, Any] = {}
    scope_text = str(scope or "").strip()

    def _mutate(current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        intents = _load_intents(current, strict=True)
        existing = intents.get(tid)
        if existing is not None and not isinstance(existing, dict):
            # GR6-3 row strictness: a present-but-malformed ROW is corruption,
            # not an absent intent — silently overwriting it would destroy the
            # forensic bytes exactly like the {}-collapse the container check
            # refuses. Same typed ValueError → CancelIntentProjectionCorrupt.
            raise ValueError(
                f"cancel-intent row for {tid} is malformed (not an object)"
            )
        if isinstance(existing, dict) and existing.get("request_id"):
            minted.update(existing)
            minted["already_requested"] = True
            if scope_text == SCOPE_CASCADE and str(existing.get("scope") or "") != SCOPE_CASCADE:
                # A single-cancel intent later re-entered through the cascade
                # ingress must be replayed as a cascade. WIDEN-ONLY (GR2-1d):
                # cascade → single is never written back — narrowing a recorded
                # cascade would let a watchdog replay settle the root and leave
                # its descendants running.
                row = {**existing, "scope": scope_text}
                intents[tid] = row
                minted.update(row)
                return {"schema_version": _SCHEMA_VERSION, "intents": intents}
            return None
        row = {
            "request_id": f"ci_{uuid.uuid4().hex[:12]}",
            "task_id": tid,
            "state": INTENT_REQUESTED,
            "reason": reason_text,
            "source": str(source or ""),
            "requested_by": str(requested_by or ""),
            "requested_at": utc_now_iso(),
            "generation": 0,
            "scope": scope_text or SCOPE_SINGLE,
        }
        intents[tid] = row
        minted.update(row)
        minted["already_requested"] = False
        return {"schema_version": _SCHEMA_VERSION, "intents": intents}

    try:
        # GR3-9 strict read: a malformed projection file must REFUSE the mutation
        # loudly instead of collapsing to {} and being overwritten — that write
        # would silently drop every other active intent. Absent file (first
        # write) is unaffected.
        update_json_locked(_intents_path(drive_root), _mutate, strict_existing_dict=True)
    except ValueError as exc:
        _forensic(drive_root, {
            "event": "projection_corrupt_refused", "task_id": tid,
            "op": "request_cancel", "error": str(exc)[:200],
        })
        log.error(
            "cancel-intent projection is corrupt; refusing to record intent for %s", tid,
        )
        raise CancelIntentProjectionCorrupt(str(exc)) from exc
    if not minted.get("already_requested"):
        _forensic(drive_root, {
            "event": "requested", "task_id": tid,
            "request_id": minted.get("request_id"),
            "source": minted.get("source"), "requested_by": minted.get("requested_by"),
            "scope": minted.get("scope"), "reason": reason_text,
            **({"settled_target_status": settled} if settled else {}),
        })
    minted.setdefault("already_settled", False)
    return dict(minted)


def mark_intent_scope(drive_root: Any, task_id: str, scope: str) -> bool:
    """Widen an EXISTING intent's scope; never mints one. Returns whether it changed.

    The ingress owns minting (owner batch-4 1=A); the cascade only records the
    SHAPE it is running, so a watchdog replay re-runs a cascade as a cascade
    instead of a single cancel that would settle the root and leave descendants
    running.

    WIDEN-ONLY (GR2-1d): ``single`` → ``cascade`` is the one legal transition.
    ``cascade`` → ``single`` is refused as a no-op plus a forensic row — a
    narrowed record would make the watchdog replay the root alone while its
    descendants kept running, exactly the shape the scope exists to prevent.
    """
    try:
        tid = _valid_task_id(task_id)
    except ValueError:
        return False
    scope_text = str(scope or "").strip()
    if not scope_text:
        return False
    changed = {"value": False}
    narrowed: Dict[str, Any] = {}

    def _mutate(current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        narrowed.clear()
        intents = _load_intents(current)
        row = intents.get(tid)
        if not isinstance(row, dict) or str(row.get("scope") or "") == scope_text:
            return None
        if str(row.get("scope") or "") == SCOPE_CASCADE:
            narrowed.update(row)
            return None
        intents[tid] = {**row, "scope": scope_text}
        changed["value"] = True
        return {"schema_version": _SCHEMA_VERSION, "intents": intents}

    try:
        update_json_locked(_intents_path(drive_root), _mutate)
    except Exception:
        log.debug("cancel-intent scope update failed for %s", task_id, exc_info=True)
        return False
    if narrowed:
        _forensic(drive_root, {
            "event": "scope_narrow_refused", "task_id": tid,
            "request_id": narrowed.get("request_id"),
            "scope": SCOPE_CASCADE, "requested_scope": scope_text,
        })
        return False
    if changed["value"]:
        _forensic(drive_root, {"event": "scope_recorded", "task_id": tid, "scope": scope_text})
    return changed["value"]


def active_intents(
    drive_root: Any, *, disclose_corruption: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """All active intents keyed by task id (a private copy; read-only callers).

    GR5-6 read semantics: an ABSENT projection is an ordinary empty read; an
    UNREADABLE/MALFORMED one (file or nested ``intents``) is a real gap — the
    enforcement readers would otherwise collapse corruption to "no intent".
    It is disclosed with a typed ``log.error`` (and, for the supervisor
    watchdog's enforcement read via ``disclose_corruption=True``, the existing
    typed ``projection_corrupt_refused`` forensic row so the owner can see
    enforcement is degraded) before the read still returns ``{}`` — fail-soft
    read, fail-closed write.
    """
    path = _intents_path_read(drive_root)
    if not path.is_file():
        return {}
    try:
        from ouroboros.utils import read_json_dict

        data = read_json_dict(path)
        if data is None:
            raise ValueError("projection file is malformed or is not an object")
        intents = _load_intents(data, strict=True)
    except Exception as exc:
        log.error(
            "cancel-intent projection is unreadable/malformed (%s); enforcement "
            "reads see NO active intents until the file is repaired", exc,
        )
        if disclose_corruption:
            _forensic(drive_root, {
                "event": "projection_corrupt_refused",
                "op": "active_intents", "error": str(exc)[:200],
            })
        return {}
    malformed = [str(tid) for tid, row in intents.items() if not isinstance(row, dict)]
    if malformed:
        # GR6-3 row strictness on the enforcement read: a malformed per-task
        # row used to be silently filtered — the watchdog then saw "no intent"
        # over bytes that still claim one. Disclose loudly once per read and
        # QUARANTINE the row (skipped here, never dropped from the durable
        # file — only the strict mutators rewrite it, and they refuse).
        log.error(
            "cancel-intent projection holds %d malformed row(s) (%s); "
            "quarantined — skipped by reads, bytes kept on disk",
            len(malformed), ", ".join(malformed[:5]),
        )
        # GR7-5: the log.error above stays per sweep, but the typed EVENT is
        # emitted once per distinct row content (in-process memo keyed by
        # row hash) — the watchdog re-reads every ~20s, so a lingering
        # quarantined row used to append the same forensic row forever. A
        # restart re-announcing once is honest.
        fresh = [
            tid for tid in malformed
            if _malformed_row_memo_key(drive_root, tid, intents.get(tid))
            not in _DISCLOSED_MALFORMED_ROWS
        ]
        if disclose_corruption and fresh:
            for tid in fresh:
                if len(_DISCLOSED_MALFORMED_ROWS) < _DISCLOSED_MALFORMED_ROWS_CAP:
                    _DISCLOSED_MALFORMED_ROWS.add(
                        _malformed_row_memo_key(drive_root, tid, intents.get(tid)),
                    )
            _forensic(drive_root, {
                "event": "projection_corrupt_refused",
                "op": "active_intents_row",
                "error": f"malformed intent row(s): {', '.join(fresh[:5])}"[:200],
            })
    return {
        str(tid): dict(row)
        for tid, row in intents.items()
        if isinstance(row, dict)
    }


def _intents_path_read(drive_root: Any) -> pathlib.Path:
    # Read path without the mkdir side effect: a scan of a never-provisioned
    # root must not materialise its state directory.
    return pathlib.Path(drive_root) / "state" / "cancel_intents.json"


# GR7-5: in-process memo of already-disclosed quarantined rows (typed EVENT
# once per row content; the per-sweep log.error stays). Bounded; a restart
# re-announces each row once, which is honest.
_DISCLOSED_MALFORMED_ROWS: set[str] = set()
_DISCLOSED_MALFORMED_ROWS_CAP = 1024


def _malformed_row_memo_key(drive_root: Any, task_id: str, row: Any) -> str:
    # Keyed per data root too: one process can serve several roots (tests,
    # child drives) and a memo hit on one must not mute another's disclosure.
    return hashlib.sha256(
        f"{pathlib.Path(drive_root)}:{task_id}:{row!r}".encode("utf-8", "replace")
    ).hexdigest()[:16]


def active_intent(drive_root: Any, task_id: str) -> Optional[Dict[str, Any]]:
    try:
        tid = _valid_task_id(task_id)
    except ValueError:
        return None
    row = active_intents(drive_root).get(tid)
    return dict(row) if isinstance(row, dict) else None


def has_active_intent(drive_root: Any, task_id: str) -> bool:
    return active_intent(drive_root, task_id) is not None


def claim_intent(drive_root: Any, task_id: str, *, owner: str) -> Optional[Dict[str, Any]]:
    """Mark the intent claimed by one custody attempt; bumps the generation.

    EXCLUSIVE while the holder is alive: a LIVE claim is never stolen, because a
    second custody attempt that took the claim from a live one would let both
    write the terminal result and both emit ``task_done`` for the same task (the
    concurrent-ingress double-settle a reviewer probe reproduced on a pending
    task). A refused claim comes back as the existing row plus
    ``claim_refused: True`` so the caller can restore custody and let the real
    owner — or the watchdog, once the claim is ABANDONED — finish.

    An ABANDONED claim (its process is gone, or it aged past ``CLAIM_STALE_SEC``)
    is taken over and the generation is bumped, which is exactly what makes the
    old holder's late ``settle``/``release`` a no-op (see ``expected_generation``).
    Returns None when no active intent exists.
    """
    try:
        tid = _valid_task_id(task_id)
    except ValueError:
        return None
    claimed: Dict[str, Any] = {}
    refused: Dict[str, Any] = {}

    def _mutate(current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        intents = _load_intents(current)
        row = intents.get(tid)
        if not isinstance(row, dict):
            return None
        if row.get("state") == INTENT_CLAIMED and not claim_is_abandoned(row):
            refused.update(row)
            return None
        row = dict(row)
        row["state"] = INTENT_CLAIMED
        row["claim_owner"] = str(owner or "")
        row["claim_pid"] = int(os.getpid())
        row["claimed_at"] = utc_now_iso()
        row["generation"] = int(row.get("generation") or 0) + 1
        intents[tid] = row
        claimed.update(row)
        return {"schema_version": _SCHEMA_VERSION, "intents": intents}

    update_json_locked(_intents_path(drive_root), _mutate)
    if claimed:
        _forensic(drive_root, {
            "event": "claimed", "task_id": tid,
            "request_id": claimed.get("request_id"),
            "claim_owner": claimed.get("claim_owner"),
            "generation": claimed.get("generation"),
        })
        return dict(claimed)
    if refused:
        _forensic(drive_root, {
            "event": "claim_refused", "task_id": tid,
            "request_id": refused.get("request_id"),
            "claim_owner": refused.get("claim_owner"),
            "generation": refused.get("generation"), "owner": str(owner or ""),
        })
        return {**refused, "claim_refused": True}
    return None


def _generation_mismatch(
    row: Dict[str, Any], expected_generation: Optional[int], request_id: str,
) -> str:
    """"" when this row is the caller's own claim, else why it is not.

    ``generation`` used to be forensic decoration. It is a FENCE now: a custody
    attempt whose claim was taken over (crash, stale takeover) must not mutate
    the projection afterwards — its late ``release`` would revert a newer claim
    and its late ``settle`` would delete an intent the new owner is still
    working. A mismatch records a forensic row and changes nothing.
    """
    if request_id and str(row.get("request_id") or "") != str(request_id):
        return f"request_id {row.get('request_id')!r} != {request_id!r}"
    if expected_generation is None:
        return ""
    try:
        current = int(row.get("generation") or 0)
    except (TypeError, ValueError):
        current = 0
    return "" if current == int(expected_generation) else (
        f"generation {current} != {int(expected_generation)}"
    )


def release_claim(
    drive_root: Any, task_id: str, *, error: str = "",
    expected_generation: Optional[int] = None, request_id: str = "",
) -> None:
    """Return a claimed intent to ``requested`` after a failed custody attempt.

    Fenced by ``expected_generation``/``request_id``: a stale claimant's release
    must never revert the claim of the custody attempt that took over from it.
    """
    try:
        tid = _valid_task_id(task_id)
    except ValueError:
        return
    released: Dict[str, Any] = {}
    mismatch: Dict[str, Any] = {}

    def _mutate(current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        intents = _load_intents(current)
        row = intents.get(tid)
        if not isinstance(row, dict) or row.get("state") != INTENT_CLAIMED:
            return None
        reason = _generation_mismatch(row, expected_generation, request_id)
        if reason:
            mismatch.update({**row, "_reason": reason})
            return None
        row = dict(row)
        row["state"] = INTENT_REQUESTED
        row["last_error"] = str(error or "")[:500]
        row.pop("claim_owner", None)
        row.pop("claim_pid", None)
        row.pop("claimed_at", None)
        intents[tid] = row
        released.update(row)
        return {"schema_version": _SCHEMA_VERSION, "intents": intents}

    update_json_locked(_intents_path(drive_root), _mutate)
    if released:
        _forensic(drive_root, {
            "event": "claim_released", "task_id": tid,
            "request_id": released.get("request_id"), "error": str(error or "")[:500],
        })
    elif mismatch:
        _forensic(drive_root, {
            "event": "claim_release_refused", "task_id": tid,
            "request_id": mismatch.get("request_id"),
            "reason": mismatch.get("_reason"),
            "expected_generation": expected_generation,
        })


def settle_intent(
    drive_root: Any, task_id: str, *, outcome: str, detail: str = "",
    expected_generation: Optional[int] = None, request_id: str = "",
    allow_cascade_scope: bool = False,
) -> Optional[Dict[str, Any]]:
    """Remove the active intent with its terminal ``outcome`` (forensic row kept).

    Called only by the supervisor settle paths (custody, pending drop, budget
    drain) — the ONE settle ownership the redesign establishes. Fenced by
    ``expected_generation``/``request_id``: a settle from a claim that was taken
    over is a NO-OP plus a forensic row, never a mutation.

    CASCADE OWNERSHIP (GR3-1): a ``scope=cascade`` intent is settled EXCLUSIVELY
    by the cascade postcondition (``allow_cascade_scope=True``). Every other
    settle site is refused ATOMICALLY inside the locked mutate — the scope is
    re-read from the CURRENT durable row, so a claim snapshot that went stale
    when the intent was widened mid-flight cannot settle the tree's replay
    trigger. When the refused caller holds the matching fenced claim, that
    claim is RELEASED in the same write (state back to ``requested``) so the
    watchdog can re-feed the cascade instead of waiting out a dead claim.
    """
    try:
        tid = _valid_task_id(task_id)
    except ValueError:
        return None
    settled: Dict[str, Any] = {}
    mismatch: Dict[str, Any] = {}
    cascade_deferred: Dict[str, Any] = {}

    def _mutate(current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        settled.clear()
        mismatch.clear()
        cascade_deferred.clear()
        intents = _load_intents(current)
        row = intents.get(tid)
        if not isinstance(row, dict):
            return None
        if not allow_cascade_scope and str(row.get("scope") or "") == SCOPE_CASCADE:
            cascade_deferred.update(row)
            fence_ok = not _generation_mismatch(row, expected_generation, request_id)
            # GR4-7: the auto-release requires a GENERATION proof. ``request_id``
            # alone is durable on the intent row (identical for every claimant),
            # so a fence-less ``settle_intent(..., request_id=X)`` from a caller
            # that holds NO claim would otherwise release a DIFFERENT owner's
            # live claim. Only a caller that captured the claimed generation may
            # have its refused claim returned to ``requested``.
            if row.get("state") == INTENT_CLAIMED and fence_ok and (
                expected_generation is not None
            ):
                released = dict(row)
                released["state"] = INTENT_REQUESTED
                released["last_error"] = "cascade settle deferred to postcondition"
                released.pop("claim_owner", None)
                released.pop("claim_pid", None)
                released.pop("claimed_at", None)
                intents[tid] = released
                cascade_deferred["_claim_released"] = True
                return {"schema_version": _SCHEMA_VERSION, "intents": intents}
            return None
        reason = _generation_mismatch(row, expected_generation, request_id)
        if reason:
            mismatch.update({**row, "_reason": reason})
            return None
        intents.pop(tid, None)
        settled.update(row)
        return {"schema_version": _SCHEMA_VERSION, "intents": intents}

    update_json_locked(_intents_path(drive_root), _mutate)
    if settled:
        _forensic(drive_root, {
            "event": "settled", "task_id": tid,
            "request_id": settled.get("request_id"),
            "outcome": str(outcome or ""), "detail": str(detail or "")[:500],
            "generation": settled.get("generation"),
        })
        return dict(settled)
    if cascade_deferred:
        _forensic(drive_root, {
            "event": "cascade_settle_deferred", "task_id": tid,
            "request_id": cascade_deferred.get("request_id"),
            "outcome": str(outcome or ""), "detail": str(detail or "")[:500],
            "claim_released": bool(cascade_deferred.get("_claim_released")),
        })
        return None
    if mismatch:
        _forensic(drive_root, {
            "event": "settle_refused", "task_id": tid,
            "request_id": mismatch.get("request_id"),
            "outcome": str(outcome or ""), "reason": mismatch.get("_reason"),
            "expected_generation": expected_generation,
        })
    return None


def claim_is_stale(intent: Dict[str, Any], *, now: Optional[float] = None) -> bool:
    """Whether a claimed intent's custody attempt is presumed dead by AGE."""
    if not isinstance(intent, dict) or intent.get("state") != INTENT_CLAIMED:
        return False
    raw = str(intent.get("claimed_at") or "").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        claimed_ts = (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()
    except (TypeError, ValueError):
        return True  # unreadable claim provenance: let the watchdog retry
    current = now if now is not None else datetime.now(timezone.utc).timestamp()
    return (current - claimed_ts) > CLAIM_STALE_SEC


def claim_is_abandoned(intent: Dict[str, Any], *, now: Optional[float] = None) -> bool:
    """Whether a claimed intent has no live owner left.

    ABANDONED (GR3-2) means the claimant pid is provably DEAD, or the claim is
    age-stale while liveness is UNKNOWN (pid missing/unparseable, or the probe
    raised). A claimant the probe just proved ALIVE is NEVER abandoned — age
    alone must not let a second custody steal a live claim and double-settle;
    the live owner settles or releases, and a genuinely wedged live claimant is
    the fenced write's problem, not the watchdog's. A demonstrably dead
    claimant is taken over immediately (waiting out ``CLAIM_STALE_SEC`` would
    keep a worker slot marked ``reaping`` — skipped by assignment and by the
    crash detector — for three minutes).
    """
    if not isinstance(intent, dict) or intent.get("state") != INTENT_CLAIMED:
        return False
    try:
        pid = int(intent.get("claim_pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    if pid > 0:
        try:
            from ouroboros.platform_layer import pid_is_alive

            alive = bool(pid_is_alive(pid))
        except Exception:
            log.debug("claim owner liveness probe failed", exc_info=True)
        else:
            if not alive:
                return True
            return False  # probed alive: never abandoned, regardless of age
    return claim_is_stale(intent, now=now)


def claim_still_owned(drive_root: Any, task_id: str, claim: Dict[str, Any]) -> bool:
    """Whether OUR fenced claim (pid + request_id + generation) still owns the intent.

    The minimal write-fence (GR3-2): cancellation custody re-checks this
    immediately BEFORE the durable terminal write — after the kill/join window,
    where a stale-takeover could have re-claimed. A lost claim aborts the
    publication (the caller restores custody and returns ``failed``); this is
    deliberately NOT a renewable-lease subsystem. Fail-CLOSED: an unreadable
    projection cannot prove ownership. A claim-less custody (legacy/no-intent
    path) passes trivially — capture under the queue lock is its exclusion.
    """
    if not isinstance(claim, dict) or not claim.get("request_id"):
        return True
    try:
        row = active_intent(drive_root, task_id)
        if not isinstance(row, dict):
            return False
        return (
            row.get("state") == INTENT_CLAIMED
            and str(row.get("request_id") or "") == str(claim.get("request_id") or "")
            and int(row.get("generation") or 0) == int(claim.get("generation") or 0)
            and int(row.get("claim_pid") or 0) == int(os.getpid())
        )
    except Exception:
        log.debug("claim ownership re-check failed for %s", task_id, exc_info=True)
        return False


def migrate_legacy_cancel_latches(drive_root: Any) -> List[str]:
    """Boot migration: legacy ``cancel_requested`` status files → synthetic intents.

    Pre-redesign task results may still sit in the ``cancel_requested`` latch (the
    incident's wedged shape). Each becomes an ordinary active intent so the
    supervisor watchdog drives it through custody to a settled outcome. The file
    itself is left untouched here (legacy read-path; custody writes the terminal).
    """
    from ouroboros.task_results import STATUS_CANCEL_REQUESTED, list_task_results

    migrated: List[str] = []
    try:
        latched = list_task_results(
            pathlib.Path(drive_root), statuses=[STATUS_CANCEL_REQUESTED],
        )
    except Exception:
        log.debug("legacy cancel-latch scan failed", exc_info=True)
        return migrated
    for row in latched:
        tid = str(row.get("task_id") or row.get("id") or "")
        if not tid:
            continue
        try:
            intent = request_cancel(
                pathlib.Path(drive_root), tid,
                reason="legacy cancel_requested latch migrated at boot",
                source="boot_migration",
            )
        except Exception:
            log.debug("legacy cancel-latch migration failed for %s", tid, exc_info=True)
            continue
        if not intent.get("already_requested") and not intent.get("already_settled"):
            migrated.append(tid)
    return migrated


def cancel_state_fields(drive_root: Any, task_id: str) -> Dict[str, Any]:
    """Typed public projection: ``{"cancel_state": "pending", ...}`` or ``{}``."""
    intent = active_intent(drive_root, task_id)
    if intent is None:
        return {}
    fields: Dict[str, Any] = {"cancel_state": "pending"}
    if intent.get("reason"):
        fields["cancel_reason"] = str(intent.get("reason") or "")
    return fields


def cancel_pending(drive_root: Any, task_id: str) -> bool:
    """Both cancel-pending carriers in ONE predicate — fail-soft.

    The durable intent projection is the live authority; the legacy
    ``cancel_requested`` STATUS latch covers pre-redesign result files that boot
    migration has not converted yet. Steering-refusal call sites must consult
    both, or a task wedged in the old shape still accepts new owner messages
    while the supervisor is tearing it down.
    """
    try:
        if has_active_intent(drive_root, task_id):
            return True
    except Exception:
        log.debug("cancel-pending intent read failed for %s", task_id, exc_info=True)
    try:
        from ouroboros.task_results import STATUS_CANCEL_REQUESTED, load_task_result

        status = str((load_task_result(drive_root, task_id) or {}).get("status") or "")
        return status == STATUS_CANCEL_REQUESTED
    except Exception:
        log.debug("cancel-pending legacy latch read failed for %s", task_id, exc_info=True)
        return False
