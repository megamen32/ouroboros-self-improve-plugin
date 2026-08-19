"""Task-event SSE endpoint and follow state (split out of gateway/tasks.py).

Extracted verbatim from ``ouroboros/gateway/tasks.py`` when the v6.9x P2
slice/discovery work pushed that module past the 1600-line size gate
(tests/test_smoke.py::test_no_oversized_modules). ``gateway/tasks.py``
re-exports this module's surface, so route wiring, CLI imports, and
monkeypatch pins keep addressing ``ouroboros.gateway.tasks`` unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import time
from typing import Any, Dict, List, Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from ouroboros.gateway._helpers import coerce_int, request_drive_root
from ouroboros.headless import ARTIFACT_STATUS_FINALIZING, ARTIFACT_STATUS_PENDING
from ouroboros.outcomes import public_task_result
from ouroboros.task_results import load_task_result, task_results_dir, validate_task_id
from ouroboros.task_status import FINAL_STATUSES


def _tasks_namespace():
    """``ouroboros.gateway.tasks``, resolved at CALL time (late-bound seam).

    Long-standing monkeypatch pins patch this code's collaborators on the
    module it lived in before the size-gate split: test_perf_budgets patches
    ``gateway.tasks._read_live_jsonl_entries``; test_task_events_sse patches
    ``gateway.tasks.load_effective_task_result`` and
    ``gateway.tasks.read_json_dict``. ``tasks.py`` binds those names (its own
    imports plus re-exports from here), so resolving them through that
    namespace keeps every existing pin effective while unpatched runs reach
    the exact same functions. The import is deferred to call time because
    ``tasks.py`` imports this module for its re-exports (module-load cycle).
    """
    from ouroboros.gateway import tasks

    return tasks


_LOG_SOURCES = (
    ("progress", ("logs", "progress.jsonl")),
    ("chat", ("logs", "chat.jsonl")),
    ("events", ("logs", "events.jsonl")),
    ("tools", ("logs", "tools.jsonl")),
    ("supervisor", ("logs", "supervisor.jsonl")),
)


async def api_task_events(request: Request) -> StreamingResponse:
    try:
        task_id = validate_task_id(request.path_params.get("task_id"))
    except ValueError as exc:
        message = str(exc)
        async def _bad_id():
            yield _sse({"type": "error", "error": message, "seq": 1}, event_id=1)
        return StreamingResponse(_bad_id(), media_type="text/event-stream", status_code=400)
    cursor = max(0, coerce_int(request.query_params.get("cursor"), 0))
    wait_sec = max(0, min(coerce_int(request.query_params.get("wait"), 30), 120))
    drive_root = request_drive_root(request)
    if not load_task_result(drive_root, task_id):
        async def _missing():
            yield _sse({"type": "error", "error": "task not found", "task_id": task_id, "seq": 1}, event_id=1)
        return StreamingResponse(_missing(), media_type="text/event-stream", status_code=404)

    async def _stream():
        # Initial replay = one full archive-aware merge (identical to a fresh
        # iter_task_events call, so the client's cross-reconnect `cursor` keeps
        # addressing the same positions — the CLI contract, ouroboros/cli.py
        # _watch_task). The follow phase then reads only bytes APPENDED to each
        # discovered log per tick; new rows are emitted incrementally with
        # monotonic in-stream seq only while they all sort strictly after the
        # emitted tail, otherwise one full re-merge resumes emission from the
        # cursor (at-least-once across those boundaries — pre-existing property,
        # disclosed in ARCHITECTURE.md).
        nonlocal cursor
        deadline = time.time() + wait_sec
        follower = _TaskEventFollower(drive_root, task_id)
        emitted_final = False
        tail_key = None
        need_full = True
        while True:
            refreshed = False
            advanced = False
            if need_full:
                rows = await asyncio.to_thread(follower.full_merge)
                pending = [row for row in rows if int(row.get("seq") or 0) > cursor]
                if rows:
                    tail_key = _event_sort_key(rows[-1])
                need_full = False
                refreshed = True  # full_merge reloaded the result projection
            else:
                new_rows, advanced = await asyncio.to_thread(follower.poll)
                interleaved = bool(new_rows) and tail_key is not None and _event_sort_key(new_rows[0]) <= tail_key
                if interleaved or follower.filter_grew:
                    # New rows interleave with already-emitted history, or a new
                    # child id joined the lineage filter (rows matching only via
                    # subagent_task_id may sit in already-consumed bytes): ONE
                    # full re-merge, resume emission from the cursor.
                    rows = await asyncio.to_thread(follower.full_merge)
                    pending = [row for row in rows if int(row.get("seq") or 0) > cursor]
                    if rows:
                        tail_key = _event_sort_key(rows[-1])
                    refreshed = True
                else:
                    pending = []
                    for row in new_rows:
                        row["seq"] = cursor + len(pending) + 1
                        pending.append(row)
                    if pending:
                        tail_key = _event_sort_key(pending[-1])
            for event in pending:
                cursor = int(event.get("seq") or cursor)
                if str(event.get("type") or "") == "task_result":
                    data = event.get("data") if isinstance(event.get("data"), dict) else {}
                    if str(data.get("status") or "").lower() in FINAL_STATUSES:
                        if not emitted_final:
                            # ONE materializing read at terminal emission (P2
                            # review, fix 5): the merged rows are status/cost
                            # projections, but watching a task to completion
                            # must still deliver the artifact-bearing terminal
                            # payload (and run its read-repair rebase) exactly
                            # once per stream.
                            full = await asyncio.to_thread(
                                _tasks_namespace().load_effective_task_result, drive_root, task_id
                            )
                            if full:
                                event["data"] = public_task_result(full)
                        emitted_final = True
                yield _sse(event, event_id=cursor)
            # Recompute the terminal projection only when something moved: log
            # offsets advanced, new roots joined, or the queue snapshot changed.
            if not refreshed and (advanced or follower.queue_snapshot_changed()):
                suppress_before = follower.suppress_task_done
                await asyncio.to_thread(follower.refresh_result)
                if follower.suppress_task_done != suppress_before:
                    # The task_done suppression window opened/closed: which rows
                    # exist in the merge changed, so re-merge before continuing.
                    need_full = True
                    continue
            if follower.result_is_final():
                if not emitted_final:
                    result = public_task_result(
                        _tasks_namespace().load_effective_task_result(drive_root, task_id)
                    )
                    if result:
                        final_event = {
                            "source": "task_result",
                            "line": 0,
                            "ts": str(result.get("ts") or ""),
                            "type": "task_result",
                            "task_id": task_id,
                            "data": result,
                            "seq": cursor + 1,
                        }
                        cursor = int(final_event["seq"])
                        yield _sse(final_event, event_id=cursor)
                break
            if time.time() >= deadline:
                yield ": heartbeat\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(_stream(), media_type="text/event-stream")


# Live logs that the supervisor rotates into archive/<prefix>_<ts>.jsonl
# (supervisor/state.rotate_jsonl_log_if_needed); the other sources never rotate.
_ROTATED_LOG_PREFIXES = {"progress": "progress", "chat": "chat"}


def _event_sort_key(item: Dict[str, Any]) -> tuple:
    return (str(item.get("ts") or ""), str(item.get("source") or ""), int(item.get("line") or 0))


def _compact_ts_stamp(ts: str) -> str:
    """ISO-ish timestamp -> archive-stamp form (YYYYMMDDTHHMMSS), or "" if unusable."""
    stamp = ts.strip().replace("-", "").replace(":", "")
    return stamp[:15] if len(stamp) >= 15 and stamp[8:9] == "T" else ""


def _archive_stamp_predates(name: str, prefix: str, floor: str) -> bool:
    """True when ``<prefix>_<stamp>[_N].jsonl`` was rotated strictly before ``floor``."""
    stamp = name[len(prefix) + 1:].split(".", 1)[0].split("_", 1)[0]
    return len(stamp) == 15 and stamp < floor


def _read_live_jsonl_entries(path: pathlib.Path, offset: int) -> tuple[List[Dict[str, Any]], int, Optional[int]]:
    """Parse COMPLETE JSONL lines from byte ``offset``; returns (entries, new_offset, ino).

    A torn final line (a concurrent append caught mid-write) is left unconsumed so
    the next read starts exactly at its first byte — unlike a naive full read, no
    row is ever half-parsed and then skipped forever."""
    try:
        with path.open("rb") as handle:
            stat = os.fstat(handle.fileno())
            if offset:
                handle.seek(offset)
            data = handle.read()
    except OSError:
        return [], offset, None
    cut = data.rfind(b"\n")
    if cut < 0:
        return [], offset, stat.st_ino
    chunk = data[: cut + 1]
    entries: List[Dict[str, Any]] = []
    for raw in chunk.splitlines():
        line = raw.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            entries.append(entry)
    return entries, offset + len(chunk), stat.st_ino


class _TaskEventFollower:
    """Byte-offset follow state for one ``/api/tasks/{id}/events`` stream.

    ``full_merge`` performs the complete archive-aware scan (also serving as the
    public ``iter_task_events``) while rebuilding per-(root, source) chain state:
    consumed archive names, the live-file byte offset/inode, and the running
    parsed-line count that keeps (ts, source, line) ordering identical between
    incremental reads and a re-merge. ``poll`` then reads only appended bytes,
    re-discovers late-spawned child roots every tick (their logs join at offset
    0), and heals a mid-stream rotation by reading the newest archive's suffix
    beyond the old offset before continuing on the new live file at offset 0.
    All effective-result reads here are status/cost projections
    (``materialize_artifacts=False``) — the SSE loop must never copy artifacts
    or make disposition/sha claims on a 0.5s tick. The single sanctioned
    exception lives in the stream's emit loop, not here: the terminal
    ``task_result`` emission performs one materializing read (see
    ``api_task_events``)."""

    def __init__(self, drive_root: pathlib.Path, task_id: str) -> None:
        self.drive_root = pathlib.Path(drive_root)
        self.task_id = task_id
        self.task_filter_ids = {task_id}
        self.roots: List[pathlib.Path] = []
        self.logs: Dict[tuple, Dict[str, Any]] = {}
        self.result: Dict[str, Any] = {}
        self.suppress_task_done = False
        self.filter_grew = False
        self._queue_snapshot_mtime: Any = None
        # Child discovery state (v6.9x P2): result filenames already read (and
        # lineage-classified) by the scandir name-diff in _discover_roots.
        self._results_dir = task_results_dir(self.drive_root, create=False)
        self._seen_result_names: set = set()
        # Archive floor (P2 review, fix 4): the RAW result's ts is the first
        # write's timestamp (creation/admission — no production writer passes an
        # explicit ts), so an archive whose rotation stamp predates it cannot
        # contain this task's rows. Empty floor = no bound (fail open).
        raw = load_task_result(self.drive_root, task_id) or {}
        self._created_floor = _compact_ts_stamp(str(raw.get("created_at") or raw.get("ts") or ""))

    def refresh_result(self) -> None:
        self.result = _tasks_namespace().load_effective_task_result(
            self.drive_root, self.task_id, materialize_artifacts=False
        )
        self.suppress_task_done = _is_workspace_result(self.result) and str(
            self.result.get("artifact_status") or ""
        ).lower() in {ARTIFACT_STATUS_PENDING, ARTIFACT_STATUS_FINALIZING}

    def result_is_final(self) -> bool:
        return str(self.result.get("status") or "").lower() in FINAL_STATUSES

    def queue_snapshot_changed(self) -> bool:
        try:
            mtime = (self.drive_root / "state" / "queue_snapshot.json").stat().st_mtime_ns
        except OSError:
            mtime = None
        changed = mtime != self._queue_snapshot_mtime
        self._queue_snapshot_mtime = mtime
        return changed

    def _discover_roots(self) -> bool:
        """Refresh roots + lineage filter ids; True when something new appeared.

        Child discovery is a scandir NAME-DIFF over the main root's
        task_results/ (v6.9x P2). Invariant this relies on: schedule_subagent
        durably writes the child's ``task_results/<tid>.json`` — already
        carrying lineage and child_drive_root — into the MAIN data root BEFORE
        emitting any event and BEFORE the child is enqueued
        (ouroboros/tools/control.py, the STATUS_REQUESTED write; a failed write
        means the child was never scheduled). A new child is therefore always
        visible as a new filename no later than its first log row. Each tick
        reads ONLY names outside the seen-set; a name is committed to the
        seen-set ONLY after read_json_dict succeeds (a torn/mid-write file is
        retried next tick), and successfully-read NON-lineage names are
        committed too so a busy shared store is not re-read every tick. The
        lineage match reproduces find_child_tasks' subtree semantics exactly:
        direct parent OR root equals the watched id, delegation_role ==
        "subagent", child_drive_root collection, NO recursion (mid-stream
        grandchildren of a non-root watched task did not match before either).
        A child missed through a transient failure is recovered by the next
        tick or, at worst, a client reconnect's full re-merge (at-least-once —
        pre-existing property)."""
        changed = False
        candidates = [self.drive_root]
        child = str(
            self.result.get("child_drive_root")
            or self.result.get("headless_child_drive_root")
            or ""
        ).strip()
        if child:
            candidates.append(pathlib.Path(child))
        try:
            with os.scandir(self._results_dir) as entries:
                names = [entry.name for entry in entries if entry.name.endswith(".json")]
        except OSError:
            names = []
        for name in names:
            if name in self._seen_result_names:
                continue
            row = _tasks_namespace().read_json_dict(self._results_dir / name)
            if row is None:
                continue  # torn write: not committed, re-read next tick
            self._seen_result_names.add(name)
            if str(row.get("delegation_role") or "") != "subagent":
                continue
            child_id = str(row.get("task_id") or row.get("id") or "").strip()
            if not child_id:
                continue
            if not (
                str(row.get("parent_task_id") or "") == self.task_id
                or str(row.get("root_task_id") or "") == self.task_id
            ):
                continue
            if child_id not in self.task_filter_ids:
                self.task_filter_ids.add(child_id)
                changed = True
                # A new FILTER ID over already-consumed bytes is lossy: rows
                # matching only via subagent_task_id were filtered out when
                # those bytes were read, so only a full re-merge recovers them
                # (new ROOTS are fine — their logs join at offset 0). The
                # stream checks this flag after every poll; full_merge resets it.
                self.filter_grew = True
            child_root = str(
                row.get("child_drive_root")
                or row.get("headless_child_drive_root")
                or ""
            ).strip()
            if child_root:
                candidates.append(pathlib.Path(child_root))
        for path in candidates:
            if path not in self.roots:
                self.roots.append(path)
                changed = True
        return changed

    def _log_state(self, root: pathlib.Path, source: str) -> Dict[str, Any]:
        key = (str(root), source)
        state = self.logs.get(key)
        if state is None:
            state = {"archives": [], "offset": 0, "ino": None, "lines": 0}
            self.logs[key] = state
        return state

    def _read_chain_delta(self, root: pathlib.Path, source: str, parts: tuple) -> List[Dict[str, Any]]:
        """Entries appended to one (root, source) chain since the recorded state.

        Fresh state (a late-discovered log) naturally degenerates to reading the
        whole chain: every archive is "new" and the live offset is 0."""
        state = self._log_state(root, source)
        live = root.joinpath(*parts)
        prefix = _ROTATED_LOG_PREFIXES.get(source)
        entries: List[Dict[str, Any]] = []
        if prefix:
            try:
                archive_paths = sorted(
                    (root / "archive").glob(f"{prefix}_*.jsonl"), key=lambda p: p.name
                )
            except OSError:
                archive_paths = []
            if self._created_floor:
                # An archive rotated before the watched task existed cannot
                # contain its rows (an archive's rows predate its rotation
                # stamp), so skip it: bounds the per-tick/merge archive work to
                # the task's lifetime instead of O(system age). Removes no
                # matching rows and touches no cursor positions by construction.
                archive_paths = [
                    path for path in archive_paths
                    if not _archive_stamp_predates(path.name, prefix, self._created_floor)
                ]
            known = set(state["archives"])
            new_archives = [p for p in archive_paths if p.name not in known]
            if new_archives:
                # Rotation: the previous live content now lives in the newest
                # archive(s). Read the first new archive beyond the consumed live
                # offset (or the offset stashed when the inode flip was observed
                # before the archive became visible), the rest fully, then
                # continue on the new live file from 0.
                had_stash = "rotated_offset" in state
                start = state.pop("rotated_offset", state["offset"])
                for index, path in enumerate(new_archives):
                    got, _, _ = _tasks_namespace()._read_live_jsonl_entries(path, start if index == 0 else 0)
                    entries.extend(got)
                    state["archives"].append(path.name)
                if not (had_stash and len(new_archives) == 1):
                    # No stash: offset/ino still describe the OLD live file (now
                    # the archive), so restart on the new live file from 0.
                    # With a consumed stash and exactly one new archive, the
                    # recorded offset/ino already track the NEW live file the
                    # follower partially consumed on the stash tick — resetting
                    # to 0 would re-emit those rows (P2 review, fix 2).
                    state["offset"] = 0
                    state["ino"] = None
        try:
            live_stat = live.stat()
        except OSError:
            return entries
        if (state["ino"] is not None and live_stat.st_ino != state["ino"]) or (
            live_stat.st_size < state["offset"]
        ):
            # Live file replaced/shrank but its archive is not visible yet: stash
            # the consumed offset for the archive suffix and restart on the new
            # live file. (Any resulting duplicate rows sort at-or-before the
            # emitted tail, which forces a full re-merge — the honest fallback.)
            if prefix and "rotated_offset" not in state:
                state["rotated_offset"] = state["offset"]
            state["offset"] = 0
        got, new_offset, ino = _tasks_namespace()._read_live_jsonl_entries(live, state["offset"])
        state["offset"], state["ino"] = new_offset, ino
        entries.extend(got)
        return entries

    def _entries_to_rows(
        self, root: pathlib.Path, source: str, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        state = self._log_state(root, source)
        rows: List[Dict[str, Any]] = []
        for entry in entries:
            state["lines"] += 1
            entry_task = str(entry.get("task_id") or "")
            entry_subagent = str(entry.get("subagent_task_id") or "")
            entry_parent = str(entry.get("parent_task_id") or "")
            entry_root = str(entry.get("root_task_id") or "")
            if (
                entry_task not in self.task_filter_ids
                and entry_subagent not in self.task_filter_ids
                and entry_parent != self.task_id
                and entry_root != self.task_id
            ):
                continue
            event = _event_from_log_entry(source, state["lines"], entry, root)
            if self.suppress_task_done and event.get("type") == "task_done":
                continue
            rows.append(event)
        return rows

    def full_merge(self) -> List[Dict[str, Any]]:
        """Full archive-aware merge; rebuilds ALL follow state from scratch."""
        self.logs = {}
        self.roots = []
        self.task_filter_ids = {self.task_id}
        # Reset the discovery baseline too: the merge below re-reads every
        # consumed byte, so every result name must be re-read and re-classified.
        self._seen_result_names = set()
        self.refresh_result()
        self.queue_snapshot_changed()
        self._discover_roots()
        rows: List[Dict[str, Any]] = []
        for root in self.roots:
            for source, parts in _LOG_SOURCES:
                entries = self._read_chain_delta(root, source, parts)
                rows.extend(self._entries_to_rows(root, source, entries))
        if self.result:
            rows.append({
                "source": "task_result",
                "line": 0,
                "ts": str(self.result.get("ts") or ""),
                "type": "task_result",
                "task_id": self.task_id,
                "data": public_task_result(self.result),
            })
        rows.sort(key=_event_sort_key)
        for idx, row in enumerate(rows, 1):
            row["seq"] = idx
        self.filter_grew = False  # the merge above read every consumed byte anew
        return rows

    def poll(self) -> tuple[List[Dict[str, Any]], bool]:
        """One follow tick: (new rows sorted by (ts, source, line), advanced?)."""
        advanced = self._discover_roots()
        rows: List[Dict[str, Any]] = []
        for root in list(self.roots):
            for source, parts in _LOG_SOURCES:
                entries = self._read_chain_delta(root, source, parts)
                if entries:
                    advanced = True
                    rows.extend(self._entries_to_rows(root, source, entries))
        rows.sort(key=_event_sort_key)
        return rows, advanced


def iter_task_events(drive_root: pathlib.Path, task_id: str) -> List[Dict[str, Any]]:
    """Return synthesized replayable events for a task from existing logs.

    Archive-aware (v6.90.x P2): each rotated log's ``archive/<prefix>_*.jsonl``
    chain is read oldest-first before the live file, so a rotation never erases
    replay history. Also the SSE initial-replay/re-merge path."""
    return _TaskEventFollower(drive_root, task_id).full_merge()


def _event_from_log_entry(source: str, line_no: int, entry: Dict[str, Any], root: pathlib.Path) -> Dict[str, Any]:
    event_type = str(entry.get("type") or source)
    if source == "progress":
        event_type = "progress"
    elif source == "chat":
        event_type = "message"
    elif source == "tools":
        event_type = "tool_call"
    data = dict(entry)
    data = public_task_result(
        data,
        include_outcome_axes=any(key in data for key in ("status", "outcome_axes", "result_status", "loop_outcome")),
    )
    return {
        "source": source,
        "line": line_no,
        "ts": str(entry.get("ts") or ""),
        "type": event_type,
        "task_id": str(entry.get("task_id") or ""),
        "root": str(root),
        "data": data,
    }


def _is_workspace_result(result: Dict[str, Any]) -> bool:
    return bool(str(result.get("workspace_root") or "").strip() or str(result.get("workspace_mode") or "").strip())


def _sse(event: Dict[str, Any], *, event_id: int) -> str:
    payload = json.dumps(event, ensure_ascii=False)
    return f"id: {event_id}\nevent: task_event\ndata: {payload}\n\n"


__all__ = ["api_task_events", "iter_task_events"]
