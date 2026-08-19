"""The one SSOT projection of task cost for every producer surface (C2, owner 10=B).

``accounted_upper_bound_usd`` is the honest NAME for what ``cost_usd`` always was:
the settled + reserved + unresolved upper bound from the physical attempt ledger,
not a settled receipt. The semantics do not change (owner 10=B); the name does.
``cost_usd`` stays outbound as a DEPRECATED alias carrying the same value — it is
part of the frozen wire contract, and its removal would be a separate, explicitly
approved ABI break. The same pairing covers ``cost_usd_with_children``.

Null is null: a missing or unknown cost projects ``None`` on BOTH names and must
never render as ``$0.00``, and finality is never fabricated — ``cost_final`` is
True only when the SOURCE says so about a known amount.

Producers do not hand-assemble these fields. They pass their source mapping
through :func:`cost_projection` (read-side projections: peek/recent/handoff/wait)
or :func:`with_cost_aliases` (write-side field dicts that already carry the full
meta set) so every surface tells the same story with the same names.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

# (additive honest name, deprecated alias) — same value on both, forever.
COST_ALIAS_PAIRS = (
    ("accounted_upper_bound_usd", "cost_usd"),
    ("accounted_upper_bound_usd_with_children", "cost_usd_with_children"),
)

# The accounting OPENNESS / INTEGRITY markers that must ride BESIDE an amount
# whenever the source carries them: an upper bound without them reads like a
# receipt. ONE list, here — producers import it instead of hand-building a
# closed whitelist each (three of those had silently dropped `reserved_usd`,
# `unresolved_upper_bound_usd` and the ledger-integrity marker, so a frame said
# "not final" with nothing on it explaining what was still open). Adding a new
# marker means adding it here, once.
COST_OPENNESS_FIELDS = (
    "cost_accounting_status",
    "cost_accounting_error",
    "cost_final",
    "cost_with_children_partial",
    "unknown_unmetered",
    "non_final_rows",
    "reserved_usd",
    "unresolved_upper_bound_usd",
    "ledger_integrity_degraded",
)


def _as_amount(value: Any) -> Optional[float]:
    """A dollar amount, or None. Booleans and unparseable values are None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_cost_pair(source: Optional[Mapping[str, Any]], new: str, old: str) -> tuple[bool, Any]:
    """``(present, raw_value)`` for one alias pair under the ONE precedence rule.

    THE DEPRECATED NAME WINS when both are present. Every seam — read and write,
    Python and JS — asks this question here, because two seams answering it
    differently is how a diverged pair starts telling two stories about the same
    record: the write side re-converged on ``cost_usd`` while the read side
    displayed ``accounted_upper_bound_usd``. Deprecated-wins is the direction the
    write side must take anyway (legacy mutators between two seam crossings edit
    ``cost_usd``), so reading the same way keeps a record self-consistent no
    matter which producer last touched it.
    """
    src = source if isinstance(source, Mapping) else {}
    if old in src:
        return True, src[old]
    if new in src:
        return True, src[new]
    return False, None


def with_cost_aliases(fields: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """A COPY of ``fields`` with each additive/deprecated cost name mirrored.

    Write-side seam: a producer that already assembled the full cost-meta dict
    (``reconstruct_task_cost``, the terminal frame builder) passes through here
    so both spellings leave with the same value — and it must be the LAST step
    after any cost mutation, or a later edit to one spelling persists a diverged
    pair. A name that is absent on both sides STAYS absent — aliasing never
    invents a field, and an explicit None mirrors as None. Precedence is
    :func:`resolve_cost_pair` (deprecated wins), which makes this idempotent.
    """
    out = dict(fields or {})
    for new, old in COST_ALIAS_PAIRS:
        present, value = resolve_cost_pair(out, new, old)
        if present:
            out[new] = value
            out[old] = value
    return out


def carry_cost_meta(source: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Both alias pairs plus every openness/integrity marker the source carries.

    For a producer assembling a NEW frame out of an existing payload: it copies
    what accounting actually said instead of re-listing field names by hand.
    Absent stays absent; unknown extra keys are the caller's business (this
    returns only accounting fields, so a caller merging it never loses its own).
    """
    src = source if isinstance(source, Mapping) else {}
    out: Dict[str, Any] = {}
    for new, old in COST_ALIAS_PAIRS:
        present, value = resolve_cost_pair(src, new, old)
        if present:
            out[new] = value
            out[old] = value
    for key in COST_OPENNESS_FIELDS:
        if key in src:
            out[key] = src[key]
    return out


def cost_projection(source: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Read-side projection of a task-result-like mapping into the SSOT shape.

    Emits ``accounted_upper_bound_usd`` + the ``cost_usd`` alias (same value,
    None when unknown), ``cost_known`` (whether ANY amount is accounted),
    ``cost_final`` (source-claimed finality about a KNOWN amount, never
    fabricated), the with-children pair when the source carries it, and every
    openness flag the source has.
    """
    src = source if isinstance(source, Mapping) else {}
    amount = _as_amount(resolve_cost_pair(src, *COST_ALIAS_PAIRS[0])[1])
    out: Dict[str, Any] = {
        "accounted_upper_bound_usd": amount,
        "cost_usd": amount,
        "cost_known": amount is not None,
        "cost_final": bool(src.get("cost_final")) and amount is not None,
    }
    present, raw_with_children = resolve_cost_pair(src, *COST_ALIAS_PAIRS[1])
    if present:
        with_children = _as_amount(raw_with_children)
        out["accounted_upper_bound_usd_with_children"] = with_children
        out["cost_usd_with_children"] = with_children
    for key in COST_OPENNESS_FIELDS:
        if key in src and key not in out:
            out[key] = src[key]
    return out


def cost_display(source: Optional[Mapping[str, Any]], *, decimals: int = 2) -> str:
    """Human rendering of one task's cost. Null NEVER renders as ``$0.00``.

    ``$1.23`` for a final amount, ``$1.23 (upper bound, not final)`` for an open
    one, ``unknown (accounting unavailable or not settled)`` when nothing is
    accounted.
    """
    projection = cost_projection(source)
    amount = projection["accounted_upper_bound_usd"]
    if amount is None:
        return "unknown (accounting unavailable or not settled)"
    text = f"${amount:.{decimals}f}"
    if projection["cost_final"]:
        return text
    return f"{text} (upper bound, not final)"


__all__ = [
    "COST_ALIAS_PAIRS",
    "COST_OPENNESS_FIELDS",
    "carry_cost_meta",
    "cost_display",
    "cost_projection",
    "resolve_cost_pair",
    "with_cost_aliases",
]
