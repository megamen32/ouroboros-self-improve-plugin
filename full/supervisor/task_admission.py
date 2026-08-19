"""Token-owned user-ingress reservations for the managed task queue."""

from __future__ import annotations

import pathlib
from typing import Any, Dict


def reserve_task_admission(
    task_id: str,
    admission_token: str,
    *,
    require_worker_pool: bool = False,
    drive_root: Any = None,
    worker_pool: Any = None,
) -> Dict[str, Any]:
    """Atomically reserve one fresh user-ingress id before side effects."""
    from supervisor import queue

    tid = str(task_id or "").strip()
    token = str(admission_token or "").strip()
    if not tid or not token:
        return {"status": "blocked", "reason": "invalid_admission_reservation"}
    with queue._queue_lock:
        reserved = queue.ADMISSION_RESERVATIONS.get(tid)
        if reserved:
            if reserved == token:
                return {"status": "already_reserved", "reason": ""}
            return {"status": "blocked", "reason": "duplicate_task_id"}
        if tid in queue.RUNNING or any(
            isinstance(row, dict) and str(row.get("id") or "") == tid
            for row in queue.PENDING
        ):
            return {"status": "blocked", "reason": "duplicate_task_id"}
        try:
            from ouroboros.task_results import load_task_result

            existing = load_task_result(
                pathlib.Path(drive_root or queue.DRIVE_ROOT), tid
            ) or {}
        except Exception:
            return {"status": "blocked", "reason": "task_id_lookup_failed"}
        if existing:
            admission = existing.get("promotion_admission")
            if (
                isinstance(admission, dict)
                and str(admission.get("routing_token") or "") == token
            ):
                return {
                    "status": "existing_same_token",
                    "reason": "",
                    "task_status": str(existing.get("status") or ""),
                    "promotion_admission": dict(admission),
                }
            return {"status": "blocked", "reason": "duplicate_task_id"}
        if require_worker_pool:
            try:
                from supervisor import workers

                disabled_reason = str(workers._WORKER_POOL_DISABLED_REASON or "")
                pool = workers.WORKERS if worker_pool is None else worker_pool
                worker_count = len(pool)
            except Exception:
                return {"status": "blocked", "reason": "worker_pool_state_unavailable"}
            if disabled_reason or worker_count <= 0:
                return {
                    "status": "blocked",
                    "reason": "worker_pool_unavailable",
                    "worker_pool_disabled_reason": disabled_reason or "no_workers",
                }
        queue.ADMISSION_RESERVATIONS[tid] = token
        return {"status": "reserved", "reason": ""}


def release_task_admission(task_id: str, admission_token: str) -> bool:
    """Release only the reservation owned by the supplied token."""
    from supervisor import queue

    tid = str(task_id or "").strip()
    token = str(admission_token or "").strip()
    with queue._queue_lock:
        if queue.ADMISSION_RESERVATIONS.get(tid) != token:
            return False
        queue.ADMISSION_RESERVATIONS.pop(tid, None)
        return True


__all__ = ["release_task_admission", "reserve_task_admission"]
