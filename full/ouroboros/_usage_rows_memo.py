"""In-process validated-rows memo and render cache for ledger display reads.

Extracted from ``usage_accounting.py`` at the perf2 P1 render-cache round for
the same reason ``_usage_rows.py`` exists: the accounting module sits at the
hard module-size gate and this layer is a self-contained seam. It is the READ
acceleration for display projections only — write paths keep their own full
locked reads — and it deliberately lives beside, not inside,
``usage_ledger.py``: the substrate stays cache-ignorant.

``usage_accounting`` re-binds every name here, and the implementation resolves
the substrate (``_locked``, ``_read_records_locked``, ...) through the
``usage_accounting`` namespace at call time, so the historical monkeypatch
sites (``ua._locked``, ``ua._read_records_locked``) keep governing these reads
exactly as when the code was inline.
"""
from __future__ import annotations

import copy
import pathlib
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Tuple

from ouroboros.usage_ledger import QUARANTINE_REL, LedgerResumeState


def _ua():
    """The accounting namespace, resolved lazily (import cycle + test pins)."""
    from ouroboros import usage_accounting

    return usage_accounting


@dataclass
class _LedgerRowsMemo:
    """In-process cache of one drive root's per-attempt FINAL rows.

    Holds only the ``_final_rows`` dict (one row per attempt, first-occurrence
    order) plus the resume fingerprint — O(final rows), not O(ledger rows);
    superseded transition rows are not retained.

    ``renders`` is the fingerprint-keyed cache of finished display renders
    (``usage_projection``/``usage_breakdown`` bodies) computed over these rows:
    valid exactly while the rows are, so it is cleared on refold and on every
    non-empty advance, never by TTL. ``generation`` increments on those same
    two events and guards the clear-then-publish race (see ``_render_cached``).
    """

    resume: LedgerResumeState
    final_rows: Dict[str, Dict[str, Any]]
    generation: int = 0
    renders: Dict[Tuple[Any, ...], Dict[str, Any]] = field(default_factory=dict)


# Read-side memo per RESOLVED drive root. Populated and advanced only under the
# cross-process ledger lock; the module lock guards the dict itself. Write paths
# (reserve/_transition/settle/import) never touch it — their own full locked
# reads stay the monetary authority, and the stat + seq-continuity check on the
# next read is what makes a stale memo impossible to serve, so correctness never
# depends on any writer remembering to invalidate.
_ROWS_MEMO: Dict[str, _LedgerRowsMemo] = {}
_ROWS_MEMO_LOCK = threading.Lock()


def _memoized_final_rows(
    root: pathlib.Path,
) -> Tuple[list, bool, "_LedgerRowsMemo", int]:
    """Validated final rows for display projections, resumed incrementally.

    Cold (or whenever the resume fingerprint is rejected — file replacement,
    size shrink, same-size rewrite, seq discontinuity, a non-row-aligned tail,
    structural corruption) this is one full ``_read_records_locked`` replay,
    which owns quarantine. Warm, it parses only the bytes appended since the
    previous read. Locks and substrate reads resolve through the
    ``usage_accounting`` namespace at call time (tests monkeypatch those
    names). Returned row dicts are shared read-only snapshots; row ORDER
    matches a from-scratch ``_final_rows`` exactly (first-occurrence order,
    updates in place), so aggregation over them is bit-identical to a fresh
    replay.

    Returns ``(rows, cacheable, memo, generation)`` — the render-cache
    transport: ``cacheable`` is False for the deliberately NON-RESUMABLE
    crash-tail fingerprint (``st_ino == -2``), whose every read stays a full
    replay, so caching a render of it would hide exactly the reads that must
    keep re-checking the torn tail. ``memo``/``generation`` let
    ``_render_cached`` publish a render computed OUTSIDE the lock only if the
    rows have not moved since.
    """
    ua = _ua()
    key = str(pathlib.Path(root).resolve(strict=False))
    with ua._locked(root):
        with _ROWS_MEMO_LOCK:
            memo = _ROWS_MEMO.get(key)
        advanced = ua._read_new_records_locked(root, memo.resume) if memo is not None else None
        if advanced is None:
            records = ua._read_records_locked(root)
            memo = _LedgerRowsMemo(
                resume=ua._ledger_resume_state(root, records),
                final_rows=ua._final_rows(records),
                generation=(memo.generation + 1) if memo is not None else 0,
            )
        else:
            new_records, new_resume = advanced
            if new_records:
                for row in new_records:
                    memo.final_rows[str(row["attempt_id"])] = row
                # The generation bump and the renders clear MUST happen under
                # _ROWS_MEMO_LOCK: the publisher in _render_cached checks the
                # generation and writes under that lock only (it never holds
                # the ledger lock), so without it the check-then-publish pair
                # could interleave with this clear — a stale render published
                # right after the clear would then serve pre-append data to
                # every warm reader until the next append. Lock order stays
                # "ledger lock → memo lock" (same as above/below); the
                # publisher takes the memo lock alone, so no deadlock. The
                # refold branch needs no such section: it swaps in a NEW memo
                # object and the publisher's `is memo` identity check already
                # rejects publications against a replaced object.
                with _ROWS_MEMO_LOCK:
                    memo.generation += 1
                    memo.renders.clear()
            memo.resume = new_resume
        with _ROWS_MEMO_LOCK:
            _ROWS_MEMO[key] = memo
        cacheable = memo.resume.st_ino != -2
        return list(memo.final_rows.values()), cacheable, memo, memo.generation


def _render_cached(
    root: pathlib.Path,
    cache_key: Tuple[Any, ...],
    render: Callable[[list, bool], Dict[str, Any]],
) -> Dict[str, Any]:
    """Serve one display render through the memo's fingerprint-keyed cache.

    The cache lives INSIDE the memo, so its lifetime is exactly the rows':
    refold and non-empty advance both replace/clear ``renders`` under the
    ledger lock. The quarantine stat happens HERE — outside the memo but after
    the row read, because that read itself may quarantine a torn tail — and the
    resulting bool joins the cache key, so an integrity change alone can never
    serve a stale render. The render itself runs OUTSIDE any lock; publication
    happens under ``_ROWS_MEMO_LOCK`` and only when the memo object and its
    generation are unchanged since the rows were read — a concurrent append
    between read and publish means the render is returned to this caller but
    never cached. Both directions hand out deep copies: the cached object is
    shared between requests, and callers (``_with_limit``/``_with_integrity``,
    gateway handlers) mutate nested buckets in place."""
    rows, cacheable, memo, generation = _memoized_final_rows(root)
    integrity_degraded = (root / QUARANTINE_REL).is_file()
    full_key = (*cache_key, integrity_degraded)
    if cacheable:
        with _ROWS_MEMO_LOCK:
            if memo.generation == generation:
                cached = memo.renders.get(full_key)
                if cached is not None:
                    return copy.deepcopy(cached)
    result = render(rows, integrity_degraded)
    if cacheable:
        key = str(pathlib.Path(root).resolve(strict=False))
        with _ROWS_MEMO_LOCK:
            if _ROWS_MEMO.get(key) is memo and memo.generation == generation:
                memo.renders[full_key] = copy.deepcopy(result)
    return result
