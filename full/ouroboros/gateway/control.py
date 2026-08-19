"""Control, update, and evolution HTTP endpoints."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict

from starlette.requests import Request
from starlette.responses import JSONResponse

from ouroboros import get_version
from ouroboros.gateway._helpers import json_error, json_exception, request_drive_root, request_json_or, request_repo_dir
from ouroboros.gateway.ws import broadcast_ws_sync
from ouroboros.outcomes import public_task_result
from ouroboros.utils import utc_now_iso

log = logging.getLogger(__name__)

_RECENT_VISIBLE_COMMANDS: Dict[str, float] = {}
_VISIBLE_COMMAND_DEDUPE_SEC = 5.0
_evo_cache: Dict[str, Any] = {}
_evo_task: asyncio.Task | None = None


def _request_restart(request: Request) -> bool:
    # Every caller here is an OWNER action through the control surface (Reset All
    # Data, Rollback, Apply Update) — never the agent's own restart tool, which
    # goes through the supervisor. Saying so lets the re-exec re-read the runtime
    # mode from settings instead of re-pinning the inherited boot baseline. The
    # bool tells the caller whether a restart callback existed to accept it.
    callback = getattr(getattr(request.app, "state", None), "request_restart", None)
    if callable(callback):
        callback(owner=True)
        return True
    return False


def _runtime_branch_defaults(request: Request) -> tuple[str, str]:
    callback = getattr(getattr(request.app, "state", None), "runtime_branch_defaults", None)
    if callable(callback):
        return callback()
    return "ouroboros", "ouroboros-stable"


def _managed_update_payload(*, fetch: bool, include_tags: bool) -> dict[str, Any]:
    from supervisor.git_ops import compute_managed_update_status, git_capture

    status = compute_managed_update_status(fetch=fetch)
    latest_version = ""
    latest_sha = status.get("latest_sha") or ""
    if latest_sha:
        rc, version_text, _ = git_capture(["git", "show", f"{latest_sha}:VERSION"])
        if rc == 0:
            latest_version = version_text.strip()
    official_tags = []
    if include_tags:
        from supervisor.git_ops import list_official_update_tags

        official_tags = list_official_update_tags()
    return {
        "current_version": get_version(),
        "latest_version": latest_version,
        "official_tags": official_tags,
        **status,
    }


def _acquire_repo_mutation_lock() -> tuple[Any, JSONResponse | None]:
    """Serialize owner-triggered repo/reset mutations with managed updates."""
    from supervisor.update_merge import (
        acquire_update_lock,
        active_update_tx,
        release_update_lock,
    )

    try:
        lock_fh = acquire_update_lock()
    except RuntimeError:
        return None, json_error(
            "Another update or recovery operation is already changing the checkout.",
            409,
        )
    if active_update_tx():
        release_update_lock(lock_fh)
        return None, json_error(
            "A managed update transaction is still active; finish or recover it first.",
            409,
        )
    return lock_fh, None


def _release_repo_mutation_lock(lock_fh: Any) -> None:
    from supervisor.update_merge import release_update_lock

    if lock_fh is not None:
        release_update_lock(lock_fh)


async def api_reset(request: Request) -> JSONResponse:
    """Reset all runtime data (state, memory, logs, settings) but keep repo."""
    import shutil

    data_dir = request_drive_root(request)
    lock_fh, lock_error = _acquire_repo_mutation_lock()
    if lock_error is not None:
        return lock_error
    try:
        deleted = []
        # Keep synchronization files until restart. Removing the directory that
        # contains the held managed-update lock would let a second updater enter.
        for subdir in ("state", "memory", "logs", "archive", "task_results", "uploads"):
            target = data_dir / subdir
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
                deleted.append(subdir)
        settings_file = data_dir / "settings.json"
        if settings_file.exists():
            settings_file.unlink()
            deleted.append("settings.json")
        _request_restart(request)
        return JSONResponse({"status": "ok", "deleted": deleted, "restarting": True})
    except Exception as exc:
        return json_exception(exc)
    finally:
        _release_repo_mutation_lock(lock_fh)


async def api_command(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        cmd = body.get("cmd", "")
        if cmd:
            from supervisor.message_bus import get_bridge, log_chat

            bridge = get_bridge()
            visible_text = str(body.get("visible_text") or "").strip()
            task_constraint = body.get("task_constraint") if isinstance(body.get("task_constraint"), dict) else None
            visible_task_id = str(body.get("visible_task_id") or "").strip()
            if visible_task_id:
                now = time.monotonic()
                expired = [
                    key for key, ts in _RECENT_VISIBLE_COMMANDS.items()
                    if now - ts > _VISIBLE_COMMAND_DEDUPE_SEC
                ]
                for key in expired:
                    _RECENT_VISIBLE_COMMANDS.pop(key, None)
                if visible_task_id in _RECENT_VISIBLE_COMMANDS:
                    return JSONResponse({"ok": True, "deduped": True, "task_id": visible_task_id})
            send_kwargs: dict[str, Any] = {"broadcast": False, "suppress_chat_log": bool(visible_text)}
            if task_constraint:
                send_kwargs["task_constraint"] = task_constraint
            bridge.ui_send(cmd, **send_kwargs)
            if visible_task_id:
                _RECENT_VISIBLE_COMMANDS[visible_task_id] = time.monotonic()
            if visible_text:
                # X3: no invented ids. `visible_task_id` is a caller-supplied
                # UI correlation key; when the caller has none there is no task
                # id yet (`ui_send` returns nothing — the router mints the id at
                # promotion), and the old bare "skill_repair" literal was a
                # fabricated id persisted into the durable chat log. The typed
                # truth is an empty id plus the pending marker.
                task_id = visible_task_id
                ts = utc_now_iso()
                payload = {
                    "type": "chat",
                    "role": "system",
                    "content": visible_text,
                    "ts": ts,
                    "source": "skill_repair",
                    "system_type": "skill_repair",
                    "task_id": task_id,
                }
                if not task_id:
                    payload["task_id_pending"] = True
                broadcast_ws_sync(payload)
                log_chat(
                    "system",
                    0,
                    0,
                    visible_text,
                    ts=ts,
                    source="skill_repair",
                    task_id=task_id,
                )
        return JSONResponse({"status": "ok"})
    except Exception as exc:
        return json_exception(exc, 400)


async def api_git_log(_request: Request) -> JSONResponse:
    """Return recent commits, tags, and current branch/sha."""
    try:
        from supervisor.git_ops import git_capture, list_commits, list_versions

        commits = list_commits(max_count=30)
        tags = list_versions(max_count=20)
        rc, branch, _ = git_capture(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        rc2, sha, _ = git_capture(["git", "rev-parse", "--short", "HEAD"])
        return JSONResponse({
            "commits": commits,
            "tags": tags,
            "branch": branch.strip() if rc == 0 else "unknown",
            "sha": sha.strip() if rc2 == 0 else "",
        })
    except Exception as exc:
        return json_exception(exc)


def _git_rollback_fenced(request: Request, target: str) -> JSONResponse:
    """Run the complete restore transaction off the gateway event loop."""
    try:
        from supervisor.git_ops import git_capture, rollback_to_version

        lock_fh, lock_error = _acquire_repo_mutation_lock()
        if lock_error is not None:
            return lock_error
        try:
            rc, target_sha, error = git_capture(
                ["git", "rev-parse", "--verify", f"{target}^{{commit}}"]
            )
            if rc != 0:
                return json_error(error or f"cannot resolve {target}", 400)
            blockers = _quiesce_repo_writers("manual_rollback")
            if blockers:
                return _fence_failure(blockers)
            ok, msg = rollback_to_version(target_sha, reason="ui_rollback")
            if not ok:
                return JSONResponse(
                    {"error": msg, "restart_required": True}, status_code=500
                )
            try:
                restarting = _request_restart(request)
            except Exception:
                log.warning("manual rollback landed but restart request failed", exc_info=True)
                restarting = False
            return JSONResponse({
                "status": "ok" if restarting else "restart_required",
                "message": msg,
                "restarting": restarting,
            })
        finally:
            _release_repo_mutation_lock(lock_fh)
    except Exception as exc:
        return json_exception(exc)


async def api_git_rollback(request: Request) -> JSONResponse:
    """Roll back to a specific commit or tag, then restart."""
    try:
        body = await request.json()
        target = body.get("target", "").strip()
    except Exception as exc:
        return json_exception(exc)
    if not target:
        return json_error("missing target", 400)
    return await asyncio.to_thread(_git_rollback_fenced, request, target)


async def api_git_promote(request: Request) -> JSONResponse:
    """Promote the current dev branch to the runtime's stable branch."""
    try:
        lock_fh, lock_error = _acquire_repo_mutation_lock()
        if lock_error is not None:
            return lock_error
        try:
            from supervisor.git_ops import promote_branch_exact

            branch_dev, branch_stable = _runtime_branch_defaults(request)
            ok, result = promote_branch_exact(
                branch_dev, branch_stable, push_remote=False
            )
            if not ok:
                return json_error(str(result.get("error") or "promotion failed"), 400)
            return JSONResponse({
                "status": "ok",
                "sha": result["sha"],
                "message": f"{branch_stable} updated to {result['sha'][:8]}",
            })
        finally:
            _release_repo_mutation_lock(lock_fh)
    except Exception as exc:
        return json_exception(exc)


async def api_update_status(_request: Request) -> JSONResponse:
    """Return passive managed-update status without fetching."""
    try:
        return JSONResponse(_managed_update_payload(fetch=False, include_tags=False))
    except Exception as exc:
        return json_exception(exc)


async def api_update_check(_request: Request) -> JSONResponse:
    """Fetch the managed remote and return fresh update status."""
    try:
        payload = await asyncio.to_thread(
            _managed_update_payload,
            fetch=True,
            include_tags=True,
        )
        return JSONResponse(payload)
    except Exception as exc:
        return json_exception(exc)


def _respawn_workers_after_failed_update() -> None:
    """Revive workers when an update aborts after they were stopped (no restart follows)."""
    try:
        from supervisor.workers import ensure_worker_pool_started, open_repo_writer_admission

        open_repo_writer_admission()
        ensure_worker_pool_started(allow_disabled_restart=True)
    except Exception:
        log.warning("update_apply: failed to respawn workers after aborted update", exc_info=True)


async def api_update_preflight(_request: Request) -> JSONResponse:
    """Plan the managed update as a REAL 3-way merge (P2). Does NOT touch the live
    worktree/branch/index (it fetches + merges in an isolated temp worktree), so the UI
    can present the right staged choice (auto / assisted / manual)."""
    try:
        from supervisor.update_merge import plan_managed_update_merge

        plan = await asyncio.to_thread(plan_managed_update_merge, fetch=True)
        return JSONResponse({"merge_plan": plan})
    except Exception as exc:
        return json_exception(exc)


_KNOWN_UPDATE_PLAN_KINDS = frozenset({"clean", "conflicting"})
_UPDATE_STRATEGIES = frozenset({"auto_merge", "assisted", "manual", "replace"})


def _plan_is_clean(plan: dict) -> bool:
    """True for a complete deterministic Git plan with no semantic conflict."""
    return (
        str(plan.get("kind") or "") == "clean"
        and type(plan.get("local_dirty_count")) is int
        and plan.get("local_dirty_count") >= 0
        and bool(str(plan.get("merge_commit") or ""))
        and not plan.get("code_conflict_paths")
        and not plan.get("doc_conflict_paths")
    )


def _pins_match(plan: dict, base_sha: str, target_sha: str) -> bool:
    return bool(
        base_sha
        and target_sha
        and str(plan.get("base_sha") or "") == base_sha
        and str(plan.get("target_sha") or "") == target_sha
    )


def _quiesce_repo_writers(reason: str) -> list[str]:
    """Close new writers, drain in-process turns, then prove the pool stopped."""
    from supervisor.git_ops import DRIVE_ROOT
    from supervisor.workers import (
        close_repo_writer_admission,
        drain_repo_writers,
        kill_workers_for_update,
        open_repo_writer_admission,
    )

    close_repo_writer_admission(f"managed_update:{reason}")
    blocked = drain_repo_writers()
    if blocked:
        open_repo_writer_admission()
        return [f"active:{label}" for label in blocked]
    survivors = kill_workers_for_update(
        result_reason="Task interrupted by an owner-requested managed update.",
        terminal_status="interrupted",
    )
    if survivors:
        return survivors
    try:
        from ouroboros.tools.services import kill_all_services

        stopped = kill_all_services(DRIVE_ROOT, wait=True, include_keep_alive=True)
    except Exception as exc:
        return [f"services:{type(exc).__name__}: {exc}"]
    failed = [
        str(item.get("service_id") or item.get("name") or "unknown")
        for item in stopped
        if isinstance(item, dict)
        and (item.get("stop_failed") or item.get("state") == "running" or item.get("lifecycle") == "running")
    ]
    try:
        from ouroboros.process_custody import quiesce_custodied_services

        _custody_ok, custody_blockers = quiesce_custodied_services(DRIVE_ROOT)
    except Exception as exc:
        custody_blockers = [f"custody_ledger:{type(exc).__name__}: {exc}"]
    return [f"service:{label}" for label in failed] + custody_blockers


def _fence_failure(blockers: list[str]) -> JSONResponse:
    restart_required = not all(str(item).startswith("active:") for item in blockers)
    return JSONResponse(
        {
            "error": (
                "Could not prove every repository writer stopped. The repository was not "
                "changed, but runtime shutdown may be incomplete."
            ),
            "reason": "update_writer_fence_blocked",
            "blockers": blockers,
            "restart_required": restart_required,
        },
        status_code=409,
    )


def _rollback_fenced_update(reason: str, error: str, **extra: Any) -> JSONResponse:
    from supervisor.update_merge import mark_update_tx_gate_blocked, rollback_managed_update

    ok, message = rollback_managed_update(reason)
    if ok:
        _respawn_workers_after_failed_update()
        return JSONResponse(
            {"error": error, "rolled_back": True, "rollback": message, **extra},
            status_code=409,
        )
    mark_update_tx_gate_blocked(reason, message)
    return JSONResponse(
        {
            "error": error,
            "rolled_back": False,
            "rollback": message,
            "restart_required": True,
            **extra,
        },
        status_code=500,
    )


def _restart_response(request: Request, *, strategy: str, plan: dict) -> JSONResponse:
    try:
        restarting = _request_restart(request)
    except Exception as exc:
        log.warning("managed update landed but restart request failed", exc_info=True)
        restarting = False
        restart_error = f"{type(exc).__name__}: {exc}"
    else:
        restart_error = "restart callback is unavailable" if not restarting else ""
    if not restarting:
        return JSONResponse(
            {
                "status": "restart_required",
                "error": restart_error,
                "strategy": strategy,
                "merge_plan": plan,
            }
        )
    return JSONResponse(
        {"status": "ok", "restarting": True, "strategy": strategy, "merge_plan": plan}
    )


def _start_assisted_merge_fenced(plan: dict) -> JSONResponse:
    """Stage the exact planned merge and enqueue its one reviewed resolver."""
    import uuid as _uuid

    from supervisor.git_ops import BRANCH_DEV, _create_rescue_snapshot, git_capture
    from supervisor.state import budget_remaining, load_state
    from supervisor.update_merge import (
        assisted_writer_gate_reason,
        create_rescue_local_ref,
        enqueue_assisted_resolution_task,
        ensure_assisted_resolver_ready,
        materialize_assisted_merge_live,
        write_update_tx,
    )
    from supervisor.workers import close_repo_writer_admission, kill_workers_for_update

    branch = BRANCH_DEV
    base_sha = str(plan.get("base_sha") or "")
    target_sha = str(plan.get("target_sha") or "")
    local_snapshot = str(plan.get("local_snapshot") or "")
    if not local_snapshot or not target_sha:
        _respawn_workers_after_failed_update()
        return JSONResponse({"error": "could not build local snapshot / target"}, status_code=409)
    try:
        remaining = budget_remaining(load_state() or {}, strict=True)
    except Exception:
        _respawn_workers_after_failed_update()
        return JSONResponse(
            {"error": "Assisted update cannot start because model budget authority is unavailable."},
            status_code=409,
        )
    if remaining <= 0:
        _respawn_workers_after_failed_update()
        return JSONResponse(
            {"error": "Assisted update needs model budget to review local changes; nothing was changed."},
            status_code=409,
        )

    rc_b, cur_branch, _be = git_capture(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    rc_s, status_txt, _se = git_capture(["git", "status", "--porcelain"])
    _create_rescue_snapshot(branch, "ui_update_assisted_merge", {
        "current_branch": cur_branch if rc_b == 0 else "",
        "dirty_lines": [ln for ln in status_txt.splitlines() if ln.strip()] if rc_s == 0 else [],
        "unpushed_lines": [], "warnings": [],
    })
    if not create_rescue_local_ref(local_snapshot):
        _respawn_workers_after_failed_update()
        return JSONResponse({"error": "could not preserve the local update snapshot"}, status_code=409)

    st = load_state() or {}
    try:
        owner_chat_id = int(st.get("owner_chat_id") or 0)
    except (TypeError, ValueError):
        owner_chat_id = 0
    task_id = "update_assisted_merge_" + _uuid.uuid4().hex[:8]
    tx = {
        "phase": "materializing_assisted",
        "pre_update_sha": base_sha,
        "pre_update_branch": branch,
        "base_sha": base_sha,
        "target_sha": target_sha,
        "target_ref": str(plan.get("target_ref") or ""),
        "update_channel": str(plan.get("update_channel") or ""),
        "local_snapshot": local_snapshot,
        "conflict_paths": (
            list(plan.get("code_conflict_paths") or [])
            + list(plan.get("doc_conflict_paths") or [])
        ),
        "task_id": task_id,
        "owner_chat_id": owner_chat_id,
        "resolution_attempts": 0,
        "requested_at": utc_now_iso(),
    }
    close_repo_writer_admission(assisted_writer_gate_reason(tx))
    if not ensure_assisted_resolver_ready(base_sha):
        blockers = kill_workers_for_update(
            result_reason="Assisted update resolver did not become ready.",
            terminal_status="interrupted",
        )
        if blockers:
            return _fence_failure([f"resolver:{item}" for item in blockers])
        _respawn_workers_after_failed_update()
        return JSONResponse(
            {"error": "Assisted update could not boot its resolver before staging conflicts."},
            status_code=409,
        )
    write_update_tx(tx)
    ok, msg = materialize_assisted_merge_live(branch, local_snapshot, target_sha, base_sha)
    if not ok:
        return _rollback_fenced_update(
            "assisted_materialize_failed", f"could not stage the merge: {msg}"
        )
    tx["phase"] = "assisted_resolution"
    write_update_tx(tx)
    if not enqueue_assisted_resolution_task(tx):
        return _rollback_fenced_update(
            "assisted_worker_start_failed",
            "the merge was staged but its resolver worker could not start",
        )
    return JSONResponse({"status": "assisted_started", "task_id": task_id, "merge_plan": plan})


def _apply_clean_merge_fenced(request: Request, plan: dict) -> JSONResponse:
    """Land one exact clean plan transactionally, then request restart."""
    import uuid

    from supervisor.git_ops import BRANCH_DEV, _create_rescue_snapshot, git_capture
    from supervisor.update_merge import (
        apply_managed_merge_update,
        update_restart_smoke,
        write_update_tx,
    )

    branch = BRANCH_DEV
    merge_commit = str(plan.get("merge_commit") or "")
    if not merge_commit:
        _respawn_workers_after_failed_update()
        return JSONResponse({"error": "clean update plan did not produce a target commit"}, status_code=409)
    rc_b, cur_branch, _be = git_capture(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    rc_s, status_txt, _se = git_capture(["git", "status", "--porcelain"])
    _create_rescue_snapshot(branch, "ui_update_apply_merge", {
        "current_branch": cur_branch if rc_b == 0 else "",
        "dirty_lines": [ln for ln in status_txt.splitlines() if ln.strip()] if rc_s == 0 else [],
        "unpushed_lines": [], "warnings": [],
    })
    attempt_id = uuid.uuid4().hex[:12]
    tx = {
        "pre_update_sha": str(plan.get("base_sha") or ""),
        "pre_update_branch": branch,
        "target_sha": str(plan.get("target_sha") or ""),
        "target_ref": str(plan.get("target_ref") or ""),
        "update_channel": str(plan.get("update_channel") or ""),
        "merge_commit": merge_commit,
        "phase": "stashing_local_work",
        "pre_restart_smoke": "pending",
        "rollback_attempted": False,
        "attempt_id": attempt_id,
        "stash_sha": "",
    }
    # Owner decision (Q1=C): dirty local work never enters committed history on a
    # clean auto-update. The decision to stash binds to the LIVE worktree at apply
    # time (not the plan's possibly-stale dirty count), the durable pre-stash tx
    # phase lands BEFORE the stash mutation (boot recovers a crash in between),
    # and an unexplained still-dirty tree fails closed instead of being cleaned.
    stash_sha = ""
    rc_ds, dirty_now, dirty_error = git_capture(["git", "status", "--porcelain"])
    if rc_ds != 0:
        _respawn_workers_after_failed_update()
        return JSONResponse(
            {"error": f"could not inspect local changes before the update: {dirty_error}"},
            status_code=409,
        )
    if dirty_now.strip():
        from supervisor.update_merge import (
            clear_update_tx,
            stash_local_changes_for_update,
            write_update_tx as _write_tx,
        )

        _write_tx(tx)
        stash_ok, stash_sha, stash_error = stash_local_changes_for_update(attempt_id)
        if stash_ok and not stash_sha:
            rc_rs, still_dirty, _rse = git_capture(["git", "status", "--porcelain"])
            if rc_rs != 0 or still_dirty.strip():
                stash_ok, stash_error = False, (
                    "the worktree still reports local changes after an empty stash"
                )
        if not stash_ok:
            clear_update_tx()
            _respawn_workers_after_failed_update()
            return JSONResponse(
                {"error": f"could not preserve local changes before the update: {stash_error}"},
                status_code=409,
            )
    tx["stash_sha"] = stash_sha
    tx["phase"] = "pending_boot_smoke"
    write_update_tx(tx)
    ok, msg = apply_managed_merge_update(branch, merge_commit)
    if not ok:
        return _rollback_fenced_update(
            "merge_apply_failed", f"merge apply failed: {msg}"
        )
    smoke = update_restart_smoke()
    if not smoke.get("ok"):
        return _rollback_fenced_update(
            "pre_restart_smoke_failed",
            "pre-restart smoke failed",
            smoke=smoke,
        )
    tx["pre_restart_smoke"] = "passed"
    write_update_tx(tx)
    return _restart_response(request, strategy="auto_merge", plan=plan)


def _apply_smart_update_fenced(
    request: Request,
    *,
    expected_base_sha: str,
    expected_target_sha: str,
) -> JSONResponse:
    from supervisor.update_merge import (
        acquire_update_lock,
        active_update_tx,
        plan_managed_update_merge,
        release_update_lock,
    )

    plan = plan_managed_update_merge(fetch=True, build=False)
    kind = str(plan.get("kind") or "")
    if not plan.get("available") or kind not in _KNOWN_UPDATE_PLAN_KINDS:
        return JSONResponse(
            {"error": plan.get("error") or "no actionable managed update", "merge_plan": plan},
            status_code=409,
        )
    if not _pins_match(plan, expected_base_sha, expected_target_sha):
        return JSONResponse(
            {"error": "the update changed after preflight; check again", "reason": "release_moved", "merge_plan": plan},
            status_code=409,
        )
    try:
        lock_fh = acquire_update_lock()
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    try:
        if active_update_tx():
            return JSONResponse({"error": "a managed update is already in progress"}, status_code=409)
        blockers = _quiesce_repo_writers("smart")
        if blockers:
            return _fence_failure(blockers)
        plan2 = plan_managed_update_merge(fetch=False, build=True)
        if (
            not plan2.get("available")
            or str(plan2.get("kind") or "") not in _KNOWN_UPDATE_PLAN_KINDS
            or not _pins_match(plan2, expected_base_sha, expected_target_sha)
            or str(plan2.get("target_ref") or "") != str(plan.get("target_ref") or "")
            or str(plan2.get("update_channel") or "") != str(plan.get("update_channel") or "")
        ):
            _respawn_workers_after_failed_update()
            return JSONResponse(
                {"error": "the update plan changed while writers were stopping; nothing was applied", "reason": "release_moved", "merge_plan": plan2},
                status_code=409,
            )
        if _plan_is_clean(plan2):
            return _apply_clean_merge_fenced(request, plan2)
        return _start_assisted_merge_fenced(plan2)
    except Exception as exc:
        log.warning("managed smart update failed after writer fence", exc_info=True)
        from supervisor.update_merge import active_update_tx as _active_tx

        if _active_tx():
            return _rollback_fenced_update(
                "smart_update_exception",
                f"managed update failed: {type(exc).__name__}: {exc}",
            )
        _respawn_workers_after_failed_update()
        return json_exception(exc)
    finally:
        release_update_lock(lock_fh)


async def _apply_smart_update(
    request: Request,
    *,
    expected_base_sha: str,
    expected_target_sha: str,
) -> JSONResponse:
    return await asyncio.to_thread(
        _apply_smart_update_fenced,
        request,
        expected_base_sha=expected_base_sha,
        expected_target_sha=expected_target_sha,
    )


def _apply_replace_recovery_fenced(
    request: Request,
    *,
    expected_base_sha: str,
    expected_target_sha: str,
) -> JSONResponse:
    import uuid

    from supervisor.git_ops import (
        BRANCH_DEV,
        _write_update_intent,
        checkout_and_reset,
        prepare_managed_update,
    )
    from supervisor.update_merge import (
        acquire_update_lock,
        active_update_tx,
        plan_managed_update_merge,
        release_update_lock,
        update_restart_smoke,
        write_update_tx,
    )

    plan = plan_managed_update_merge(fetch=True, build=False)
    if (
        str(plan.get("kind") or "") not in (_KNOWN_UPDATE_PLAN_KINDS | {"current"})
        or not _pins_match(plan, expected_base_sha, expected_target_sha)
    ):
        return JSONResponse(
            {"error": "the recovery target changed after preflight", "reason": "release_moved", "merge_plan": plan},
            status_code=409,
        )
    try:
        lock_fh = acquire_update_lock()
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    try:
        if active_update_tx():
            return JSONResponse({"error": "a managed update is already in progress"}, status_code=409)
        blockers = _quiesce_repo_writers("replace_recovery")
        if blockers:
            return _fence_failure(blockers)
        plan2 = plan_managed_update_merge(fetch=False, build=False)
        if (
            str(plan2.get("kind") or "") not in (_KNOWN_UPDATE_PLAN_KINDS | {"current"})
            or not _pins_match(plan2, expected_base_sha, expected_target_sha)
            or str(plan2.get("target_ref") or "") != str(plan.get("target_ref") or "")
            or str(plan2.get("update_channel") or "") != str(plan.get("update_channel") or "")
        ):
            _respawn_workers_after_failed_update()
            return JSONResponse(
                {"error": "the recovery target changed while writers were stopping", "reason": "release_moved", "merge_plan": plan2},
                status_code=409,
            )
        ok, payload = prepare_managed_update(
            "replace",
            expected_base_sha=expected_base_sha,
            expected_target_sha=expected_target_sha,
            arm_intent=False,
        )
        if not ok:
            _respawn_workers_after_failed_update()
            return JSONResponse(payload, status_code=409)
        tx = {
            "pre_update_sha": expected_base_sha,
            "pre_update_branch": BRANCH_DEV,
            "target_sha": expected_target_sha,
            "target_ref": str(plan2.get("target_ref") or ""),
            "update_channel": str(plan2.get("update_channel") or ""),
            "merge_commit": expected_target_sha,
            "phase": "applying_replace",
            "pre_restart_smoke": "pending",
            "pre_update_dirty_count": int(plan2.get("local_dirty_count") or 0),
            "attempt_id": uuid.uuid4().hex[:12],
            "strategy": "replace",
        }
        write_update_tx(tx)
        _write_update_intent(dict(payload["update_intent"]))
        try:
            checkout_ok, checkout_msg = checkout_and_reset(
                BRANCH_DEV,
                reason="ui_update_apply",
                unsynced_policy="rescue_and_reset",
            )
        except Exception as exc:
            return _rollback_fenced_update(
                "replace_checkout_exception", f"recovery checkout failed: {exc}", **payload
            )
        if not checkout_ok:
            return _rollback_fenced_update(
                "replace_checkout_failed", f"recovery checkout failed: {checkout_msg}", **payload
            )
        tx["phase"] = "pending_boot_smoke"
        write_update_tx(tx)
        smoke = update_restart_smoke()
        if not smoke.get("ok"):
            return _rollback_fenced_update(
                "replace_pre_restart_smoke_failed", "pre-restart smoke failed", smoke=smoke
            )
        tx["pre_restart_smoke"] = "passed"
        write_update_tx(tx)
        return _restart_response(request, strategy="replace", plan=plan2)
    except Exception as exc:
        log.warning("managed replace recovery failed after writer fence", exc_info=True)
        if active_update_tx():
            return _rollback_fenced_update(
                "replace_update_exception",
                f"managed recovery failed: {type(exc).__name__}: {exc}",
            )
        _respawn_workers_after_failed_update()
        return json_exception(exc)
    finally:
        release_update_lock(lock_fh)


async def _apply_replace_recovery(
    request: Request,
    *,
    expected_base_sha: str,
    expected_target_sha: str,
) -> JSONResponse:
    return await asyncio.to_thread(
        _apply_replace_recovery_fenced,
        request,
        expected_base_sha=expected_base_sha,
        expected_target_sha=expected_target_sha,
    )


async def api_update_apply(request: Request) -> JSONResponse:
    """Apply an exact managed plan; replacement is an explicit recovery only."""
    body = await request_json_or(request, {}, exceptions=(Exception,))
    if not isinstance(body, dict):
        return json_error("JSON body must be an object.", 400)
    strategy = str(body.get("strategy") or "auto_merge").strip().lower()
    if strategy not in _UPDATE_STRATEGIES:
        return json_error(f"unsupported update strategy: {strategy or 'missing'}", 400)
    expected_base_sha = str(body.get("expected_base_sha") or "").strip()
    expected_target_sha = str(body.get("expected_target_sha") or "").strip()
    if strategy == "manual":
        from supervisor.update_merge import plan_managed_update_merge

        plan = await asyncio.to_thread(plan_managed_update_merge, fetch=True)
        return JSONResponse({"status": "manual", "merge_plan": plan})
    if strategy != "manual":
        from supervisor.update_merge import active_update_tx

        if active_update_tx():
            return JSONResponse({"error": "a managed update is already in progress"}, status_code=409)
    if not expected_base_sha or not expected_target_sha:
        return json_error("fresh preflight base and target SHA are required", 400)
    if strategy == "replace":
        if body.get("confirm_recovery") is not True:
            return json_error("replace is a recovery action and requires confirm_recovery=true", 400)
        return await _apply_replace_recovery(
            request,
            expected_base_sha=expected_base_sha,
            expected_target_sha=expected_target_sha,
        )
    # auto_merge and assisted share one smart flow; the fresh plan, not the
    # caller's guess, decides whether the supervisor can fast-forward/merge or
    # Ouroboros must resolve it through reviewed assisted mode.
    return await _apply_smart_update(
        request,
        expected_base_sha=expected_base_sha,
        expected_target_sha=expected_target_sha,
    )


async def api_evolution_data(request: Request) -> JSONResponse:
    """Collect evolution metrics for each git tag."""
    from ouroboros.utils import collect_evolution_metrics

    global _evo_task
    now = time.time()
    force_refresh = str(request.query_params.get("force") or "").strip().lower() in {"1", "true", "yes"}
    if not force_refresh and _evo_cache.get("ts") and now - _evo_cache["ts"] < 60:
        return JSONResponse({
            "points": _evo_cache["points"],
            "checkpoints": _evo_cache.get("checkpoints", []),
            "generated_at": _evo_cache.get("generated_at", ""),
            "cached": True,
        })
    if _evo_task is None or _evo_task.done():
        _evo_task = asyncio.create_task(
            collect_evolution_metrics(
                str(request_repo_dir(request)),
                data_dir=str(request_drive_root(request)),
            )
        )
    data_points = await _evo_task
    try:
        from ouroboros.evolution_checkpoints import CHECKPOINTS_REL
        from ouroboros.utils import iter_jsonl_objects

        checkpoints = []
        rows = [
            row for row in iter_jsonl_objects(request_drive_root(request) / CHECKPOINTS_REL)
            # cycle_outcome rows are solve-capability digest fodder (different
            # schema: no git_sha/identity hashes); the Dashboard checkpoints
            # view renders absorb checkpoints only.
            if isinstance(row, dict) and row.get("kind") != "cycle_outcome"
        ]
        for row in rows[-100:]:
            checkpoints.append(public_task_result(row))
    except Exception:
        checkpoints = []
    _evo_cache["ts"] = time.time()
    _evo_cache["points"] = data_points
    _evo_cache["checkpoints"] = checkpoints
    _evo_cache["generated_at"] = utc_now_iso()
    return JSONResponse({
        "points": data_points,
        "checkpoints": checkpoints,
        "generated_at": _evo_cache["generated_at"],
        "cached": False,
    })


__all__ = [
    "api_command",
    "api_evolution_data",
    "api_git_log",
    "api_git_promote",
    "api_git_rollback",
    "api_reset",
    "api_update_apply",
    "api_update_check",
    "api_update_preflight",
    "api_update_status",
]
