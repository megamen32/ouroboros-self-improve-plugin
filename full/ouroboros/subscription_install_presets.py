"""Install-time subscription preset compiler (owner matrix D-3/D-9).

When the owner connects at least one agent SUBSCRIPTION during
onboarding, every surface that CAN run on a subscription moves onto it:
the commit triad, the scope rows, the advisory pre-reviewer, and the
delegated subagent default. The API model slots (main/heavy/light/vision/
consciousness/fallback) are deliberately untouched — the root loop is an API
LLM client and a subscription cannot serve it (owner decision D-2).

This module is a PURE COMPILER. It performs no network I/O and reads no
settings: the caller hands it the harnesses whose accounts the Claudexor
daemon actually vouches for, plus that daemon's live per-harness model
discovery, and gets back

* the ``OUROBOROS_REVIEWER_SLOTS`` JSON value (triad rows, scope rows, advisory),
* the ``OUROBOROS_SUBAGENT_HARNESS`` value in ``harness=model[:effort]`` form,
* a receipt describing every seat it resolved, and
* a TYPED REFUSAL when a required seat cannot be resolved from discovery.

A refusal is never softened into a fallback. Writing a harness default (or a
plausible-looking shorthand like ``opus-4.6``) would persist a model id the
engine may not route, and the failure would only surface later, inside a
review the owner believed was configured. Absence is reported as absence.

EFFORT lives in two different places because the harnesses spell it
differently, and the receipt records which: claude/codex model ids are
effort-free and take the row's ``effort`` field, while cursor publishes
COMPOUND slugs whose tail IS the effort (``cursor-grok-4.5-high``) — bracket
overrides are not supported there. A cursor seat therefore carries the effort
in BOTH channels: the slug is what Cursor honours, and the field is what the
review/delegation surfaces materialize anyway (an empty field does not mean
"no effort" downstream — it means "the surface's global default", which would
silently disagree with the slug).

``profile_id`` is deliberately never pinned: the daemon rotates credential
profiles (D28), and pinning one row to one account at install time would
outlive the account.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# The marker written beside the applied preset. Its ABSENCE is not authority to
# apply anything (every pre-preset install lacks it too); the server-side
# fresh-install latch is. Bumping this string marks a NEW preset generation.
SUBSCRIPTION_PRESET_VERSION = "1"
PRESET_MARKER_KEY = "OUROBOROS_SUBSCRIPTION_PRESET_VERSION"

REVIEWER_SLOTS_KEY = "OUROBOROS_REVIEWER_SLOTS"
SUBAGENT_HARNESS_KEY = "OUROBOROS_SUBAGENT_HARNESS"

HARNESS_CLAUDE = "claude"
HARNESS_CODEX = "codex"
HARNESS_CURSOR = "cursor"
# The harnesses the ratified matrix covers. A connected harness outside this
# tuple (opencode, raw-api, …) contributes no seat — the matrix is the owner's
# decision, not an auto-derivation over whatever the engine can reach.
PRESET_HARNESSES: Tuple[str, ...] = (HARNESS_CLAUDE, HARNESS_CODEX, HARNESS_CURSOR)

SURFACE_SUBAGENT = "subagent"
SURFACE_ADVISORY = "advisory"
SURFACE_TRIAD = "triad"
SURFACE_SCOPE = "scope"

# Harnesses whose discovery ids ENCODE the reasoning effort in the id itself.
_EFFORT_IN_MODEL_ID = frozenset({HARNESS_CURSOR})

# Owner shorthand -> ORDERED candidate discovery ids. ``{effort}`` is filled
# from the seat for a harness that spells effort inside the id. The FIRST
# candidate present in live discovery wins; when none is present the seat
# refuses. This is an alias table for ONE model family, never a fallback to a
# different model or to the harness default.
_MODEL_ALIASES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    HARNESS_CLAUDE: {
        "opus-5": ("claude-opus-5",),
        "sonnet-5": ("claude-sonnet-5",),
        "opus-4.6": ("claude-opus-4-6",),
        "fable-5": ("claude-fable-5",),
    },
    HARNESS_CODEX: {
        "gpt-5.6-sol": ("gpt-5.6-sol",),
        "gpt-5.6-terra": ("gpt-5.6-terra",),
        "gpt-5.5": ("gpt-5.5",),
    },
    HARNESS_CURSOR: {
        "grok-4.5": ("cursor-grok-4.5-{effort}", "grok-4.5-{effort}"),
        "gemini-3.6-flash": ("gemini-3.6-flash-{effort}",),
        # Cursor publishes no bare gpt-5.6 slug; the owner's "gpt-5.6" seat is
        # the 5.6 FAMILY, resolved in flagship order.
        "gpt-5.6": (
            "gpt-5.6-{effort}",
            "gpt-5.6-sol-{effort}",
            "gpt-5.6-terra-{effort}",
            "gpt-5.6-luna-{effort}",
        ),
    },
}


@dataclass(frozen=True)
class HarnessDiscovery:
    """One connected harness and the model ids the daemon discovered for it."""

    harness_id: str
    model_ids: Tuple[str, ...] = ()

    @property
    def has_models(self) -> bool:
        return bool(self.model_ids)


@dataclass(frozen=True)
class PresetSeat:
    """One seat of the matrix: which harness, which model preference, how hard."""

    surface: str
    position: int  # 1-based within its surface
    harness: str
    preference: str
    effort: str


@dataclass(frozen=True)
class PresetRefusal:
    """Why the preset could not be compiled — named down to the seat."""

    code: str
    seat: Optional[PresetSeat]
    candidates: Tuple[str, ...]
    message: str

    def as_dict(self) -> Dict[str, Any]:
        seat = self.seat
        return {
            "code": self.code,
            "message": self.message,
            "candidates": list(self.candidates),
            "surface": seat.surface if seat else "",
            "position": seat.position if seat else 0,
            "harness": seat.harness if seat else "",
            "preference": seat.preference if seat else "",
            "effort": seat.effort if seat else "",
        }


@dataclass(frozen=True)
class SubscriptionInstallPreset:
    """The compiled preset, or a typed refusal. Never both, never partial."""

    connected: Tuple[str, ...] = ()
    reviewer_slots: str = ""
    subagent_harness: str = ""
    receipt: Dict[str, Any] = field(default_factory=dict)
    refusal: Optional[PresetRefusal] = None

    @property
    def ok(self) -> bool:
        return self.refusal is None and bool(self.reviewer_slots)

    def settings_keys(self) -> Dict[str, str]:
        """The EXACT settings keys an install-time save adds. Empty on refusal —
        a half-applied preset is worse than none."""
        if not self.ok:
            return {}
        return {
            REVIEWER_SLOTS_KEY: self.reviewer_slots,
            SUBAGENT_HARNESS_KEY: self.subagent_harness,
            PRESET_MARKER_KEY: SUBSCRIPTION_PRESET_VERSION,
        }


def _seat(surface: str, position: int, harness: str, preference: str, effort: str) -> PresetSeat:
    return PresetSeat(surface=surface, position=position, harness=harness,
                      preference=preference, effort=effort)


def _combo(subagent: Tuple[str, str, str], advisory: Tuple[str, str, str],
           triad: Sequence[Tuple[str, str, str]],
           scope: Sequence[Tuple[str, str, str]]) -> Dict[str, Tuple[PresetSeat, ...]]:
    """One matrix row, written as (harness, preference, effort) triples."""
    return {
        SURFACE_SUBAGENT: (_seat(SURFACE_SUBAGENT, 1, *subagent),),
        SURFACE_ADVISORY: (_seat(SURFACE_ADVISORY, 1, *advisory),),
        SURFACE_TRIAD: tuple(
            _seat(SURFACE_TRIAD, idx, *spec) for idx, spec in enumerate(triad, start=1)),
        SURFACE_SCOPE: tuple(
            _seat(SURFACE_SCOPE, idx, *spec) for idx, spec in enumerate(scope, start=1)),
    }


_CLAUDE = HARNESS_CLAUDE
_CODEX = HARNESS_CODEX
_CURSOR = HARNESS_CURSOR

# ---------------------------------------------------------------------------
# THE RATIFIED MATRIX (plan §D.1 = owner batch-1 answer 2.1 + D-9 completions).
#
# Written as a TABLE, not as an algorithm: the four single/double rows the owner
# dictated verbatim and the three D-9 completions are one ratified artifact, and
# a reviewer must be able to diff this against §D.1 line by line. The priority
# policy that PRODUCED the completions is documented, not re-derived here:
#   delegated subagent  Opus-5 -> Sol -> Grok
#   advisory            Sonnet-5(low) -> Terra(medium) -> Grok(medium)
#   scope               Sol -> Fable -> Grok(high)
#   triad               the strongest model of every connected harness, the
#                       remaining seats filled from the preferred harness.
# ---------------------------------------------------------------------------
_MATRIX: Dict[frozenset, Dict[str, Tuple[PresetSeat, ...]]] = {
    frozenset({_CLAUDE}): _combo(
        subagent=(_CLAUDE, "opus-5", "medium"),
        advisory=(_CLAUDE, "sonnet-5", "low"),
        triad=[(_CLAUDE, "sonnet-5", "medium"), (_CLAUDE, "opus-5", "medium"),
               (_CLAUDE, "opus-4.6", "medium")],
        scope=[(_CLAUDE, "fable-5", "medium")],
    ),
    frozenset({_CODEX}): _combo(
        subagent=(_CODEX, "gpt-5.6-sol", "medium"),
        advisory=(_CODEX, "gpt-5.6-terra", "medium"),
        triad=[(_CODEX, "gpt-5.6-sol", "medium"), (_CODEX, "gpt-5.6-terra", "medium"),
               (_CODEX, "gpt-5.5", "medium")],
        scope=[(_CODEX, "gpt-5.6-sol", "medium")],
    ),
    frozenset({_CURSOR}): _combo(
        subagent=(_CURSOR, "grok-4.5", "high"),
        advisory=(_CURSOR, "grok-4.5", "medium"),
        triad=[(_CURSOR, "grok-4.5", "medium"), (_CURSOR, "gemini-3.6-flash", "medium"),
               (_CURSOR, "gpt-5.6", "medium")],
        scope=[(_CURSOR, "grok-4.5", "high")],
    ),
    frozenset({_CLAUDE, _CODEX}): _combo(
        subagent=(_CLAUDE, "opus-5", "medium"),
        advisory=(_CLAUDE, "sonnet-5", "low"),
        triad=[(_CLAUDE, "opus-5", "medium"), (_CODEX, "gpt-5.6-sol", "medium"),
               (_CLAUDE, "sonnet-5", "medium")],
        scope=[(_CODEX, "gpt-5.6-sol", "medium")],
    ),
    frozenset({_CLAUDE, _CURSOR}): _combo(
        subagent=(_CLAUDE, "opus-5", "medium"),
        advisory=(_CLAUDE, "sonnet-5", "low"),
        triad=[(_CLAUDE, "opus-5", "medium"), (_CURSOR, "grok-4.5", "medium"),
               (_CLAUDE, "sonnet-5", "medium")],
        scope=[(_CLAUDE, "fable-5", "medium")],
    ),
    frozenset({_CODEX, _CURSOR}): _combo(
        subagent=(_CODEX, "gpt-5.6-sol", "medium"),
        advisory=(_CODEX, "gpt-5.6-terra", "medium"),
        triad=[(_CODEX, "gpt-5.6-sol", "medium"), (_CURSOR, "grok-4.5", "medium"),
               (_CODEX, "gpt-5.6-terra", "medium")],
        scope=[(_CODEX, "gpt-5.6-sol", "medium")],
    ),
    frozenset({_CLAUDE, _CODEX, _CURSOR}): _combo(
        subagent=(_CLAUDE, "opus-5", "medium"),
        advisory=(_CLAUDE, "sonnet-5", "low"),
        triad=[(_CLAUDE, "opus-5", "medium"), (_CODEX, "gpt-5.6-sol", "medium"),
               (_CURSOR, "grok-4.5", "medium")],
        scope=[(_CODEX, "gpt-5.6-sol", "medium")],
    ),
}


def matrix_combinations() -> Tuple[frozenset, ...]:
    """Every connected-harness combination the ratified matrix covers (7)."""
    return tuple(_MATRIX)


def _candidate_ids(seat: PresetSeat) -> Tuple[str, ...]:
    aliases = _MODEL_ALIASES.get(seat.harness, {}).get(seat.preference, ())
    return tuple(spelling.format(effort=seat.effort) for spelling in aliases)


def _resolve_seat(seat: PresetSeat,
                  discovery: Mapping[str, HarnessDiscovery]) -> Tuple[str, Optional[PresetRefusal]]:
    """The EXACT discovery id for a seat, or a typed refusal naming it."""
    candidates = _candidate_ids(seat)
    found = discovery.get(seat.harness)
    available = set(found.model_ids) if found else set()
    for candidate in candidates:
        if candidate in available:
            return candidate, None
    if not candidates:
        return "", PresetRefusal(
            code="unknown_model_preference", seat=seat, candidates=(),
            message=(f"No alias is registered for {seat.preference!r} on the "
                     f"{seat.harness} harness — the {seat.surface} seat cannot be resolved."),
        )
    return "", PresetRefusal(
        code="model_not_in_discovery", seat=seat, candidates=candidates,
        message=(f"The {seat.harness} harness does not expose "
                 f"{seat.preference!r} (tried {', '.join(candidates)}), so the "
                 f"{seat.surface} seat #{seat.position} cannot be filled from live "
                 "discovery."),
    )


def _session_target(harness: str, model_id: str) -> str:
    """The reviewer-row route identity: Claudexor's own ``harness=model``
    spelling (no ``::`` — that is API-route syntax)."""
    return f"{harness}={model_id}"


def _row_effort(seat: PresetSeat) -> str:
    """A row's ``effort`` field.

    Present on EVERY harness on purpose. On cursor the compound slug is what the
    vendor honours, but the review/delegation surfaces materialize an effort
    regardless — leaving the field empty would send the surface's global default
    alongside a slug that says something else."""
    return seat.effort


def _resolved_row(seat: PresetSeat, model_id: str) -> Dict[str, Any]:
    return {
        "surface": seat.surface,
        "position": seat.position,
        "harness": seat.harness,
        "preference": seat.preference,
        "model": model_id,
        "effort": seat.effort,
        "effort_in_model_id": seat.harness in _EFFORT_IN_MODEL_ID,
        "target_id": _session_target(seat.harness, model_id),
    }


def _slot_id(surface: str, position: int) -> str:
    """Row identity from the ONE mint (``review_substrate.slot_id_for_row``) so a
    preset row's receipts line up with a hand-authored row's."""
    from ouroboros.review_substrate import (
        SCOPE_SLOT_ID_PREFIX,
        SLOT_ID_PREFIX,
        slot_id_for_row,
    )

    prefix = SCOPE_SLOT_ID_PREFIX if surface == SURFACE_SCOPE else SLOT_ID_PREFIX
    return slot_id_for_row(position, prefix=prefix)


def _resolve_surface(seats: Sequence[PresetSeat],
                     discovery: Mapping[str, HarnessDiscovery],
                     ) -> Tuple[List[Dict[str, Any]], Optional[PresetRefusal]]:
    rows: List[Dict[str, Any]] = []
    for seat in seats:
        model_id, refusal = _resolve_seat(seat, discovery)
        if refusal is not None:
            return [], refusal
        rows.append(_resolved_row(seat, model_id))
    return rows, None


def _slot_rows(resolved: Sequence[Mapping[str, Any]], surface: str) -> List[Dict[str, Any]]:
    return [
        {
            "slot_id": _slot_id(surface, int(row["position"])),
            "route": {"kind": "agent_session", "target_id": str(row["target_id"])},
            "effort": str(row["effort"]),
        }
        for row in resolved
    ]


def _reviewer_slots_json(triad: Sequence[Mapping[str, Any]],
                         scope: Sequence[Mapping[str, Any]],
                         advisory: Mapping[str, Any]) -> str:
    payload = {
        "triad": _slot_rows(triad, SURFACE_TRIAD),
        "scope": _slot_rows(scope, SURFACE_SCOPE),
        "advisory": {
            "enabled": True,
            "route": {"kind": "agent_session", "target_id": str(advisory["target_id"])},
            "effort": str(advisory["effort"]),
        },
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=False)


def _subagent_value(row: Mapping[str, Any]) -> str:
    """``harness=model[:effort]`` — Claudexor's reviewer-panel spelling, the
    same grammar ``subagents.parse_subagent_harness`` reads."""
    effort = str(row["effort"] or "")
    tail = f":{effort}" if effort else ""
    return f"{row['target_id']}{tail}"


def _validate_against_parser(raw: str) -> Optional[PresetRefusal]:
    """Feed our own output through the reviewer-slot SSOT parser.

    The compiler does not maintain a second schema: if the ONE strict parser
    every consumer uses refuses this value, the preset is not applied."""
    from ouroboros.reviewer_slot_config import parse_reviewer_slots

    try:
        parse_reviewer_slots(raw)
    except ValueError as exc:
        return PresetRefusal(
            code="preset_failed_slot_validation", seat=None, candidates=(),
            message=f"The compiled reviewer-slot value did not validate: {exc}",
        )
    return None


def connected_preset_harnesses(discoveries: Sequence[HarnessDiscovery]) -> Tuple[str, ...]:
    """The matrix-covered harnesses among the ones the caller vouched for,
    in stable matrix order."""
    seen = {str(d.harness_id) for d in discoveries}
    return tuple(h for h in PRESET_HARNESSES if h in seen)


def compile_install_preset(
    discoveries: Sequence[HarnessDiscovery],
    *,
    capability: Optional[Mapping[str, Any]] = None,
) -> SubscriptionInstallPreset:
    """Compile the install-time preset for the connected harnesses.

    ``discoveries`` are the harnesses whose accounts the caller already
    VERIFIED as connected, each with the live model ids discovered for it.
    ``capability`` is optional per-harness evidence (access profiles, engine
    notes); it is recorded in the receipt for disclosure and never gates a
    seat — an engine that answers a discovery list is the authority on what it
    can route.
    """
    connected = connected_preset_harnesses(discoveries)
    if not connected:
        return SubscriptionInstallPreset(
            connected=(),
            refusal=PresetRefusal(
                code="no_preset_harness_connected", seat=None, candidates=(),
                message=("No agent subscription among "
                         f"{', '.join(PRESET_HARNESSES)} is connected, so there is "
                         "no preset to apply."),
            ),
        )
    discovery = {str(d.harness_id): d for d in discoveries}
    empty = [h for h in connected if not discovery[h].has_models]
    if empty:
        return SubscriptionInstallPreset(
            connected=connected,
            refusal=PresetRefusal(
                code="discovery_empty", seat=None, candidates=(),
                message=("Model discovery returned nothing for: "
                         f"{', '.join(empty)}. Preset models are written only from "
                         "live discovery, never guessed."),
            ),
        )
    seats = _MATRIX[frozenset(connected)]
    resolved: Dict[str, List[Dict[str, Any]]] = {}
    for surface in (SURFACE_SUBAGENT, SURFACE_ADVISORY, SURFACE_TRIAD, SURFACE_SCOPE):
        rows, refusal = _resolve_surface(seats[surface], discovery)
        if refusal is not None:
            return SubscriptionInstallPreset(connected=connected, refusal=refusal)
        resolved[surface] = rows
    reviewer_slots = _reviewer_slots_json(
        resolved[SURFACE_TRIAD], resolved[SURFACE_SCOPE], resolved[SURFACE_ADVISORY][0])
    invalid = _validate_against_parser(reviewer_slots)
    if invalid is not None:
        return SubscriptionInstallPreset(connected=connected, refusal=invalid)
    receipt = {
        "version": SUBSCRIPTION_PRESET_VERSION,
        "connected": list(connected),
        "surfaces": {
            SURFACE_SUBAGENT: resolved[SURFACE_SUBAGENT][0],
            SURFACE_ADVISORY: resolved[SURFACE_ADVISORY][0],
            SURFACE_TRIAD: resolved[SURFACE_TRIAD],
            SURFACE_SCOPE: resolved[SURFACE_SCOPE],
        },
        "discovery_counts": {h: len(discovery[h].model_ids) for h in connected},
        "profile_pinned": False,  # D28: the daemon rotates accounts.
    }
    if capability:
        receipt["capability"] = dict(capability)
    return SubscriptionInstallPreset(
        connected=connected,
        reviewer_slots=reviewer_slots,
        subagent_harness=_subagent_value(resolved[SURFACE_SUBAGENT][0]),
        receipt=receipt,
    )


__all__ = [
    "HARNESS_CLAUDE",
    "HARNESS_CODEX",
    "HARNESS_CURSOR",
    "PRESET_HARNESSES",
    "PRESET_MARKER_KEY",
    "REVIEWER_SLOTS_KEY",
    "SUBAGENT_HARNESS_KEY",
    "SUBSCRIPTION_PRESET_VERSION",
    "SURFACE_ADVISORY",
    "SURFACE_SCOPE",
    "SURFACE_SUBAGENT",
    "SURFACE_TRIAD",
    "HarnessDiscovery",
    "PresetRefusal",
    "PresetSeat",
    "SubscriptionInstallPreset",
    "compile_install_preset",
    "connected_preset_harnesses",
    "matrix_combinations",
]
