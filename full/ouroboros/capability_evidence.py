"""Capability Evidence — sourced, auditable knowledge of a route's context window.

Replaces the stale static per-model window table (deleted in v6.33.0). Every
window claim is EVIDENCE with a status and a source, scoped to a route
fingerprint (provider + base_url + model + headers/beta + relevant options):

  status:
    confirmed   — a trustworthy live/local source reported it
                  (source = provider_metadata | local_health)
    asserted    — the owner acknowledged it for an EXACT route fingerprint
                  (source = owner_ack); auditable, invalidated on ANY route change
    unprobeable — no metadata source and no owner-ack (e.g. OpenAI/Anthropic
                  direct, whose 1M is an undiscoverable per-request beta header)
    failed      — a probe was attempted and errored (transient; retried later)

``unknown`` (unprobeable | failed | no record) => FAIL-CLOSED for any >=1M gate.

Probes are opportunistic and cached (24h for confirmed, 10 min for failed). Gate
readers pass ``allow_fetch=False`` so the hot path never blocks on a network
call. A provider outage marks evidence stale; it never erases a prior confirmed/
asserted record. The owner-ack is route-fingerprinted and NEVER a repo-wide
"trust this model" flag.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pathlib
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from ouroboros.deadline_utils import parse_deadline_ts, utc_now
from ouroboros.utils import atomic_write_json, read_json_dict, utc_now_iso

log = logging.getLogger(__name__)

# Serialises the load->mutate->save of the two owner-only writers (probe cache +
# owner-ack) within the process so neither loses the other's update; atomic_write_json
# additionally prevents torn/corrupt files across processes (durable-state SSOT).
_STORE_LOCK = threading.RLock()

STATUS_CONFIRMED = "confirmed"
STATUS_ASSERTED = "asserted"
STATUS_UNPROBEABLE = "unprobeable"
STATUS_FAILED = "failed"

SOURCE_PROVIDER_METADATA = "provider_metadata"
SOURCE_LOCAL_HEALTH = "local_health"
SOURCE_OWNER_ACK = "owner_ack"
SOURCE_GENERATIVE_PROBE = "generative_probe"
SOURCE_NONE = "none"

# Context-overflow rejections carry the model's limit in the human-readable message
# (NOT the `code` field, which varies: context_length_exceeded / invalid_request_error /
# 400 / 1261). Parse the number from the text across the known provider phrasings.
_CTX_LIMIT_PATTERNS: Tuple[re.Pattern[str], ...] = (
    re.compile(r"maximum context length is\s*([0-9][0-9,]*)", re.I),
    re.compile(r"context length is\s*([0-9][0-9,]*)", re.I),
    re.compile(r"longer than the model's context length\s*\(?\s*([0-9][0-9,]*)", re.I),
    re.compile(r"maximum allowed length\s*\(?\s*([0-9][0-9,]*)", re.I),
    re.compile(r"context (?:window|length)\s*(?:of|is)?\s*([0-9][0-9,]*)\s*tokens", re.I),
    re.compile(r"maximum (?:input |prompt )?(?:length|tokens?)\s*(?:is|of)?\s*([0-9][0-9,]*)", re.I),
)


def _parse_ctx_limit_number(text: str) -> int:
    """Extract the model's context-token limit from an overflow error message, or 0."""
    for pat in _CTX_LIMIT_PATTERNS:
        m = pat.search(str(text or ""))
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except (ValueError, TypeError):
                continue
    return 0


def classify_generative_probe_response(
    status_code: Optional[int],
    body_text: str,
    *,
    canaries: Optional[List[str]] = None,
    echoed_text: str = "",
    usage_prompt_tokens: int = 0,
    sent_token_estimate: int = 0,
) -> Tuple[int, str, str]:
    """Pure (no-network) classifier for a generative context-window probe response.

    Free-only policy (owner Q1): confirm a window ONLY from a FREE pre-inference
    reject that states the limit; a genuine 200 (the model ACCEPTED — and would bill —
    the oversized input) never auto-confirms >=1M, it routes to owner-ack.
    Returns ``(window_tokens, status, detail)``.
    """
    # 4xx: pre-inference reject (free). Parse the limit NUMBER from the text.
    if isinstance(status_code, int) and 400 <= status_code < 500:
        n = _parse_ctx_limit_number(body_text)
        if n > 0:
            return n, STATUS_CONFIRMED, f"generative overflow reject: max {n} tokens"
        # e.g. Zhipu code 1261 (no number) or a 413 size reject -> cannot size it.
        return 0, STATUS_UNPROBEABLE, "overflow reject without a parseable limit; owner-ack required"
    # 200: the oversized input was ACCEPTED. Under free-only this is a possibly-PAID
    # accept and must NOT confirm >=1M (owner chose owner-ack). Truncation guard is
    # recorded for forensics but does not change the owner-ack outcome.
    if status_code == 200:
        cs = canaries or []
        echoed_ok = bool(cs) and all(c in (echoed_text or "") for c in cs)
        usage_ok = sent_token_estimate > 0 and usage_prompt_tokens >= int(0.95 * sent_token_estimate)
        detail = "oversized input accepted (200); free-only policy -> owner-ack"
        if not (echoed_ok and usage_ok):
            detail = "oversized input 200 but truncation suspected (canaries/usage); owner-ack"
        return 0, STATUS_UNPROBEABLE, detail
    # transport / 5xx / timeout / unknown -> transient failure (short TTL, retried).
    return 0, STATUS_FAILED, f"generative probe transport/server error (status={status_code})"

_KNOWN_STATUS = {STATUS_CONFIRMED, STATUS_ASSERTED}

_CONFIRMED_TTL_SEC = 24 * 3600
_FAILED_TTL_SEC = 10 * 60

ONE_MILLION = 1_000_000


@dataclass
class CapabilityEvidence:
    window_tokens: int
    status: str
    source: str
    route_fp: str
    model: str = ""
    provider: str = ""
    ts: str = ""
    detail: str = ""
    stale: bool = False

    def to_json(self) -> Dict[str, Any]:
        return {
            "window_tokens": int(self.window_tokens or 0),
            "status": self.status,
            "source": self.source,
            "route_fp": self.route_fp,
            "model": self.model,
            "provider": self.provider,
            "ts": self.ts,
            "detail": self.detail,
            "stale": bool(self.stale),
        }


def is_known(evidence: Any, *, require_fresh: bool = False) -> bool:
    """Whether ``evidence`` is a KNOWN (confirmed/asserted) sourced observation.

    The SSOT for "does this record count as evidence at all". ``require_fresh``
    additionally rejects a STALE record — one past its TTL that the probe could not
    re-verify (an expired cache read on the no-fetch hot path, or a prior record kept
    across a provider outage). ``probe`` already documents that contract ("a stale or
    absent record then reads as unknown"); stating it HERE is what keeps every caller
    from restating it, or forgetting to.

    Accepts any evidence-shaped record (``status`` / ``window_tokens`` / ``stale``),
    so a surface that carries the same fields — e.g. ``reviewer_window.ReviewerWindow``
    — reuses this predicate instead of re-deriving it."""
    if evidence is None:
        return False
    return (
        str(getattr(evidence, "status", "") or "") in _KNOWN_STATUS
        and int(getattr(evidence, "window_tokens", 0) or 0) > 0
        and not (require_fresh and bool(getattr(evidence, "stale", False)))
    )


def confirms_at_least(
    evidence: Any, threshold: int = ONE_MILLION, *, require_fresh: bool = False,
) -> bool:
    """True only when KNOWN evidence meets the threshold.

    unprobeable / failed / None / below-threshold all fail closed.

    ``require_fresh`` picks the caller's freshness policy EXPLICITLY, because the two
    directions carry opposite risk and the choice must be visible at the call site:

    * a gate that AUTHORIZES on the evidence (blocking scope-review authority, BIBLE
      P3) passes ``require_fresh=True`` — an expired or outage-carried window is a
      dated impression, not the sourced Capability Evidence the floor turns on;
    * a gate that would DOWNGRADE the owner's own cognitive horizon on a provider blip
      keeps the default ``False`` — this module's standing invariant is that an outage
      must never erase a prior confirmed record (P4/P1)."""
    return is_known(evidence, require_fresh=require_fresh) and (
        int(getattr(evidence, "window_tokens", 0) or 0) >= int(threshold)
    )


# --- Route fingerprint ---------------------------------------------------------

def _canonical_headers(headers: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
    if not isinstance(headers, dict):
        return ()
    credential_names = {
        "authorization", "proxy-authorization", "api-key", "x-api-key",
        "x-goog-api-key", "anthropic-api-key", "openai-api-key",
    }
    # Credentials are dispatch authentication, not route capability identity.
    # Omitting both value and presence keeps key rotation (or late key loading)
    # from invalidating otherwise identical route evidence.  Non-secret beta /
    # routing headers remain fingerprinted because they can change the window.
    return tuple(sorted(
        (str(k).lower(), str(v))
        for k, v in headers.items()
        if str(k).lower() not in credential_names
    ))


def _canonical_options(options: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
    if not isinstance(options, dict):
        return ()
    # Only options that can change the effective window/route are fingerprinted.
    relevant = ("beta", "anthropic_beta", "context_1m", "max_tokens", "tenant")
    return tuple(sorted((k, str(options[k])) for k in relevant if k in options))


def route_fingerprint(
    *,
    provider: str,
    base_url: str = "",
    model: str = "",
    headers: Optional[Dict[str, Any]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> str:
    """Stable, NON-generic fingerprint of an exact route. Any change to provider,
    base_url, model, beta/headers, or relevant options yields a new fingerprint —
    so an owner-ack can never silently outlive the configuration it approved."""
    payload = json.dumps({
        "provider": str(provider or "").strip().lower(),
        "base_url": str(base_url or "").strip().rstrip("/").lower(),
        "model": str(model or "").strip(),
        "headers": _canonical_headers(headers),
        "options": _canonical_options(options),
    }, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


# --- Persistence ---------------------------------------------------------------

def _store_path(drive_root: Any) -> pathlib.Path:
    return pathlib.Path(drive_root) / "state" / "capability_evidence.json"


def _load(drive_root: Any) -> Dict[str, Any]:
    data = read_json_dict(_store_path(drive_root))
    if isinstance(data, dict):
        data.setdefault("probes", {})
        data.setdefault("owner_acks", {})
        data.setdefault("effort_ceilings", {})
        data.setdefault("effort_floors", {})
        data.setdefault("rejected_params", {})
        data.setdefault("token_density", {})
        return data
    return {
        "probes": {}, "owner_acks": {}, "effort_ceilings": {},
        "effort_floors": {}, "rejected_params": {}, "token_density": {},
    }


def _save(drive_root: Any, data: Dict[str, Any]) -> None:
    path = _store_path(drive_root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(path, data)  # atomic rename — never a torn/partial file
    except OSError:
        pass


def _store_evidence(drive_root: Any, kind: str, fp: str, value: Dict[str, Any]) -> None:
    """Locked, atomic read-modify-write of one evidence entry (``probes`` or
    ``owner_acks``). The lock re-reads the CURRENT file inside the critical section
    so a concurrent owner-ack and probe never clobber each other; the network probe
    itself runs OUTSIDE this lock. Never raises."""
    try:
        with _STORE_LOCK:
            data = _load(drive_root)
            data.setdefault(kind, {})[fp] = value
            _save(drive_root, data)
    except Exception:
        log.debug("capability evidence store failed (%s)", kind, exc_info=True)


def _age_seconds(ts: str) -> float:
    parsed = parse_deadline_ts(ts)
    if parsed is None:
        return float("inf")
    return max(0.0, (utc_now() - parsed).total_seconds())


# --- Learned reasoning-effort ceilings (v6.57.0) -------------------------------
# A COMPLETELY SEPARATE namespace ("effort_ceilings") from the context-window
# evidence (probes/owner_acks) — it shares only the store file and the lock.
# It NEVER touches window records, so the BIBLE P3 ≥1M scope-review floor
# evidence path is untouched. Value shape:
#   {"ceiling": "<effort>", "observed_at": iso, "reason": "provider_rejected"}
# KEYING (deliberate, r4 disclosure): the key is the NORMALIZED MODEL IDENTITY
# (llm.normalize_model_identity — provider-scoped model id), NOT the full route
# fingerprint the window evidence uses. Effort-level support is treated as a
# MODEL property: a ceiling learned on one base_url applies to the model on all
# routes. Coarser than per-route, and self-healing — the floor in llm.py keeps
# a bad endpoint from poisoning below "low", and clamps are disclosed per call.
# The ceiling is the highest effort a route ACCEPTED after a provider rejected a
# higher one (learned by the reject-and-step-down walk in llm.py). Fail-open:
# any error → no ceiling (send the requested effort). Owner-configured efforts are
# still honored UP TO the learned real ceiling; clamping is disclosed in usage.

def record_effort_ceiling(drive_root: Any, fingerprint: str, ceiling: str) -> None:
    """Persist the learned reasoning-effort ceiling. The key is the normalized
    model identity (see the namespace note above), not a full route fingerprint.
    Best-effort, never raises; a lower ceiling always wins (a route never silently
    regains an effort a provider already rejected within the cache window)."""
    fp = str(fingerprint or "").strip()
    ceil = str(ceiling or "").strip().lower()
    if not fp or not ceil:
        return
    try:
        with _STORE_LOCK:
            data = _load(drive_root)
            entry = data.setdefault("effort_ceilings", {}).get(fp) or {}
            data["effort_ceilings"][fp] = {
                "ceiling": ceil,
                "observed_at": utc_now_iso(),
                "reason": "provider_rejected",
                "prev": entry.get("ceiling") or "",
            }
            _save(drive_root, data)
    except Exception:
        log.debug("record_effort_ceiling failed", exc_info=True)


def get_effort_ceiling(drive_root: Any, fingerprint: str) -> str:
    """Return the learned effort ceiling for a normalized model identity, or ""
    when none. Fail-open (any error → "")."""
    fp = str(fingerprint or "").strip()
    if not fp:
        return ""
    try:
        entry = _load(drive_root).get("effort_ceilings", {}).get(fp)
        return str((entry or {}).get("ceiling") or "").strip().lower()
    except Exception:
        return ""


# --- Learned reasoning-effort floors (v6.73.2) ----------------------------------
# The VALUE-TOO-LOW mirror of effort_ceilings: some endpoints make reasoning
# MANDATORY (e.g. Gemini's "Reasoning is mandatory for this endpoint and cannot
# be disabled" 400 on effort "none"). llm.py learns a floor of "low" from such a
# rejection and later calls clamp UP to it (disclosed per call as
# reasoning_effort_clamped reason="learned_floor"). Same namespace design and
# NORMALIZED-MODEL-IDENTITY keying as effort_ceilings/rejected_params.
# LIFECYCLE ASYMMETRY (deliberate): ceilings are sticky (a model's max supported
# effort is a stable model property), floors EXPIRE like rejected_params —
# whether reasoning can be disabled is provider POLICY that changes; if the
# provider later allows disabling it again, behavior self-heals after the TTL at
# the cost of one reactive 400. Fail-open everywhere.

_EFFORT_FLOORS_TTL_SEC = 14 * 24 * 3600.0


def record_effort_floor(drive_root: Any, fingerprint: str, floor: str) -> None:
    """Persist the learned reasoning-effort floor for a normalized model identity.

    Best-effort, never raises; a HIGHER floor always wins on merge (mirror of the
    ceiling's lower-wins rule — a provider-required minimum is never silently
    lowered within the cache window)."""
    from ouroboros.config import effort_rank
    fp = str(fingerprint or "").strip()
    value = str(floor or "").strip().lower()
    if not fp or not value:
        return
    try:
        with _STORE_LOCK:
            data = _load(drive_root)
            store = data.setdefault("effort_floors", {})
            entry = store.get(fp) or {}
            prev = str(entry.get("floor") or "").strip().lower()
            fresh = _age_seconds(str(entry.get("observed_at") or "")) < _EFFORT_FLOORS_TTL_SEC
            if fresh and prev and effort_rank(prev) >= effort_rank(value):
                value = prev
            store[fp] = {
                "floor": value,
                "observed_at": utc_now_iso(),
                "reason": "provider_required",
                "prev": prev,
            }
            _save(drive_root, data)
    except Exception:
        log.debug("record_effort_floor failed", exc_info=True)


def get_effort_floor(drive_root: Any, fingerprint: str) -> str:
    """Return the non-expired learned effort floor for a normalized model
    identity, or "" (fail-open: absence, expiry, or any error → "")."""
    fp = str(fingerprint or "").strip()
    if not fp:
        return ""
    try:
        entry = _load(drive_root).get("effort_floors", {}).get(fp) or {}
        if _age_seconds(str(entry.get("observed_at") or "")) >= _EFFORT_FLOORS_TTL_SEC:
            return ""
        return str(entry.get("floor") or "").strip().lower()
    except Exception:
        return ""


# --- Learned rejected request parameters (v6.69.0) ------------------------------
# Same design as effort_ceilings: a separate namespace ("rejected_params") keyed by
# the NORMALIZED MODEL IDENTITY, sharing only the store file and lock. A provider
# rejection of an optional request parameter (e.g. temperature on a reasoning
# model) is learned reactively in llm.py; persisting it here means a NEW process
# (worker restart, review subprocess) strips the parameter proactively instead of
# re-paying a 404 + retry on its first call. Entries EXPIRE (providers change
# supported_parameters independently of releases — the mutable-external-fact rule
# in DEVELOPMENT.md), after which the reactive retry re-learns if still true.
# Fail-open everywhere: any error → no durable knowledge → today's behavior.

_REJECTED_PARAMS_TTL_SEC = 14 * 24 * 3600.0


def record_rejected_params(drive_root: Any, fingerprint: str, params: Any) -> None:
    """Persist provider-rejected optional request parameters for a model identity.

    Merges with (non-expired) existing knowledge; best-effort, never raises."""
    fp = str(fingerprint or "").strip()
    values = sorted({str(p).strip() for p in (params or []) if str(p or "").strip()})
    if not fp or not values:
        return
    try:
        with _STORE_LOCK:
            data = _load(drive_root)
            store = data.setdefault("rejected_params", {})
            entry = store.get(fp) or {}
            existing = entry.get("params") if _age_seconds(str(entry.get("observed_at") or "")) < _REJECTED_PARAMS_TTL_SEC else []
            merged = sorted({*(existing or []), *values})
            store[fp] = {
                "params": merged,
                "observed_at": utc_now_iso(),
                "reason": "provider_rejected",
            }
            _save(drive_root, data)
    except Exception:
        log.debug("record_rejected_params failed", exc_info=True)


def get_rejected_params(drive_root: Any, fingerprint: str) -> Set[str]:
    """Return non-expired learned rejected parameters for a model identity.

    Empty set on absence, expiry, or any error (fail-open)."""
    fp = str(fingerprint or "").strip()
    if not fp:
        return set()
    try:
        entry = _load(drive_root).get("rejected_params", {}).get(fp) or {}
        if _age_seconds(str(entry.get("observed_at") or "")) >= _REJECTED_PARAMS_TTL_SEC:
            return set()
        return {str(p) for p in (entry.get("params") or []) if str(p or "").strip()}
    except Exception:
        return set()


# --- Measured tokenizer density (v6.80.0) ---------------------------------------
# Same design as effort_ceilings/rejected_params: a separate namespace
# ("token_density") keyed by the NORMALIZED MODEL IDENTITY, sharing only the store
# file and the lock. It NEVER touches window records. This replaces the former
# hand-set CLAUDE_REAL_TOKENS_PER_ESTIMATED constant and its substring family gate:
# a tokenizer multiplier table perpetually goes stale, so the ratio is MEASURED at
# the physical send boundary from (prompt_chars, real prompt_tokens) and is honestly
# "unknown" otherwise. Value shape:
#   {"density": float, "observed_at": iso, "source": str,
#    "pairs": [{"prompt_chars": int, "prompt_tokens": int, "observed_at": iso}]}
# ``density`` is REAL prompt tokens per ESTIMATED token (chars/4) — the number the
# review-pack sizing formula divides by. The MAXIMUM of the retained fresh pairs
# wins (conservative: a denser observation is never averaged away).
# WRITE THROTTLE (deliberate): this store is one file behind one lock shared with
# the scope-review hot path, and a lock-starvation incident on the usage ledger
# under benchmark load is on record, so an observation is persisted only when
# nothing fresh is known or the density drifted past the tolerance, retention is
# bounded to _TOKEN_DENSITY_MAX_PAIRS raw pairs per model, and a process-local
# memo short-circuits the common repeat case without touching disk at all.
# Fail-open everywhere: any error => no durable knowledge => cold-start behaviour.

_TOKEN_DENSITY_TTL_SEC = 14 * 24 * 3600.0
_TOKEN_DENSITY_FRESH_SEC = 6 * 3600.0
_TOKEN_DENSITY_MAX_PAIRS = 5
_TOKEN_DENSITY_DRIFT_TOLERANCE = 0.05
# Below this, fixed request scaffolding (roles, JSON keys, tool schemas) dominates
# the ratio and the measurement says nothing about pack-scale tokenization.
_TOKEN_DENSITY_MIN_CHARS = 20_000
_TOKEN_DENSITY_SANE_RANGE = (0.5, 4.0)

# Documented conservative cold-start density: the SAME 1.58x measured on a real
# code-heavy Claude scope pack, plus margin. It applies where NO observation exists for
# a model identity, so a fresh install (and every isolated benchmark server, which
# always starts with an empty store) sizes review packs SMALLER than needed and passes
# instead of drawing a deterministic provider 400. It is deliberately NOT a global
# floor on a MEASURED density: it is a Claude-derived number, and flooring every model
# with it permanently shrank the pack of any lighter tokenizer (an all-GPT scope + triad
# lost ~27% / ~36% of its pack) with no way for measurement to correct the direction —
# the hand-set multiplier table D4 forbids, relocated. What keeps a measured value from
# LOOSENING a cap is MODEL-SCOPED instead: record_token_density stores the running
# maximum for that identity, so a run of prose-dominated doc-only packs cannot pull a
# code-heavy model's stored density back down. It is also NOT applied to the main loop's
# context-fit projection (see context_fit.py).
COLD_START_TOKEN_DENSITY = 1.65
# Covers measurement noise plus the serialisation-basis gap: recorded prompt_chars
# come from the serialized dispatch payload, whose JSON escaping inflates the char
# count relative to estimate_tokens' raw text, which would otherwise UNDER-state
# the real density.
MEASURED_DENSITY_SAFETY_FACTOR = 1.05

_DENSITY_MEMO: Dict[str, Tuple[float, str]] = {}


def _density_of(prompt_chars: Any, prompt_tokens: Any) -> float:
    """Real tokens per chars/4 estimated token, or 0.0 when not measurable."""
    try:
        chars = int(prompt_chars or 0)
        tokens = int(prompt_tokens or 0)
    except (TypeError, ValueError):
        return 0.0
    if chars < _TOKEN_DENSITY_MIN_CHARS or tokens <= 0:
        return 0.0
    density = tokens / max(1.0, chars / 4.0)
    low, high = _TOKEN_DENSITY_SANE_RANGE
    return density if low <= density <= high else 0.0


def record_token_density(
    drive_root: Any,
    fingerprint: str,
    *,
    prompt_chars: Any,
    prompt_tokens: Any,
    source: str = "dispatch_usage",
) -> None:
    """Persist one measured tokenizer-density observation for a model identity.

    Best-effort and throttled (see the namespace note above); never raises."""
    fp = str(fingerprint or "").strip()
    density = _density_of(prompt_chars, prompt_tokens)
    if not fp or density <= 0:
        return
    memo = _DENSITY_MEMO.get(fp)
    if memo and abs(density - memo[0]) <= _TOKEN_DENSITY_DRIFT_TOLERANCE * memo[0]:
        return  # nothing new to learn; skip the shared store entirely
    try:
        with _STORE_LOCK:
            data = _load(drive_root)
            store = data.setdefault("token_density", {})
            entry = store.get(fp) or {}
            fresh = _age_seconds(str(entry.get("observed_at") or "")) < _TOKEN_DENSITY_FRESH_SEC
            known = float(entry.get("density") or 0.0)
            if fresh and known > 0 and abs(density - known) <= _TOKEN_DENSITY_DRIFT_TOLERANCE * known:
                _DENSITY_MEMO[fp] = (known, "measured")
                return
            pairs = [
                pair for pair in (entry.get("pairs") or [])
                if isinstance(pair, dict)
                and _age_seconds(str(pair.get("observed_at") or "")) < _TOKEN_DENSITY_TTL_SEC
            ]
            pairs.append({
                "prompt_chars": int(prompt_chars or 0),
                "prompt_tokens": int(prompt_tokens or 0),
                "observed_at": utc_now_iso(),
            })
            pairs = pairs[-_TOKEN_DENSITY_MAX_PAIRS:]
            merged = max(
                [_density_of(p.get("prompt_chars"), p.get("prompt_tokens")) for p in pairs] or [density]
            )
            # Per-MODEL ratchet, and the reason no global cold-start floor is needed on
            # the measured path: the stored value is the running maximum for this
            # identity while the entry is inside its TTL, so a run of prose-dominated
            # packs cannot pull a code-heavy model's density (and thus its pack cap)
            # back up. Raw-pair retention is bounded, so `merged` alone would decay.
            retained = (
                known
                if _age_seconds(str(entry.get("observed_at") or "")) < _TOKEN_DENSITY_TTL_SEC
                else 0.0
            )
            store[fp] = {
                # 6 decimals: 4 was coarse enough that a seeded observation could not
                # reproduce a given effective density exactly (regression-test seam).
                "density": round(max(merged, density, retained), 6),
                "observed_at": utc_now_iso(),
                "source": str(source or "dispatch_usage"),
                "pairs": pairs,
            }
            _DENSITY_MEMO[fp] = (float(store[fp]["density"]), "measured")
            _save(drive_root, data)
    except Exception:
        log.debug("record_token_density failed", exc_info=True)


def get_token_density(drive_root: Any, fingerprint: str) -> float:
    """Non-expired measured density for a model identity, else 0.0 (fail-open)."""
    fp = str(fingerprint or "").strip()
    if not fp:
        return 0.0
    try:
        entry = _load(drive_root).get("token_density", {}).get(fp) or {}
        if _age_seconds(str(entry.get("observed_at") or "")) >= _TOKEN_DENSITY_TTL_SEC:
            return 0.0
        return float(entry.get("density") or 0.0)
    except Exception:
        return 0.0


def resolve_token_density(drive_root: Any, model_id: str) -> Tuple[float, str]:
    """``(effective density, provenance)`` for one model, for review-pack sizing.

    Provenance is ``measured`` (an observation exists for this exact normalized
    model identity; the safety factor is applied) or ``cold_conservative`` (the
    maximum of every fresh observation and COLD_START_TOKEN_DENSITY). Fail-open:
    any error resolves to the cold-conservative constant.

    The cold-start constant is MODEL-SCOPED: it bounds the cold path only. Flooring the
    measured path with it made a Claude-derived number the permanent minimum for every
    model, so a genuinely lighter tokenizer could never recover its own pack size no
    matter how much it was measured. The "measurement can only tighten" property is
    supplied where it belongs — ``record_token_density`` stores the running MAXIMUM per
    model identity — and ``calibrated_input_token_limit`` still bounds every result by
    the historical absolute-margin form, so no cap can exceed the pre-measurement one."""
    try:
        from ouroboros.provider_models import normalize_model_identity
        fp = normalize_model_identity(str(model_id or ""))
    except Exception:
        fp = str(model_id or "").strip()
    measured = get_token_density(drive_root, fp)
    if measured > 0:
        return (measured * MEASURED_DENSITY_SAFETY_FACTOR, "measured")
    try:
        observed = [
            float((entry or {}).get("density") or 0.0)
            for entry in (_load(drive_root).get("token_density", {}) or {}).values()
            if _age_seconds(str((entry or {}).get("observed_at") or "")) < _TOKEN_DENSITY_TTL_SEC
        ]
    except Exception:
        observed = []
    return max([COLD_START_TOKEN_DENSITY, *observed]), "cold_conservative"


# --- Owner acknowledgement (asserted) -----------------------------------------

def record_owner_ack(
    drive_root: Any,
    *,
    provider: str,
    base_url: str = "",
    model: str = "",
    window_tokens: int,
    owner: str = "owner",
    headers: Optional[Dict[str, Any]] = None,
    options: Optional[Dict[str, Any]] = None,
    note: str = "",
) -> Dict[str, Any]:
    """Persist a route-fingerprinted owner acknowledgement of a context window."""
    fp = route_fingerprint(provider=provider, base_url=base_url, model=model, headers=headers, options=options)
    record = {
        "route_fp": fp,
        "window_tokens": int(window_tokens or 0),
        "owner": str(owner or "owner"),
        "ts": utc_now_iso(),
        "note": str(note or ""),
        "route": {
            "provider": str(provider or ""),
            "base_url": str(base_url or ""),
            "model": str(model or ""),
            "headers": list(_canonical_headers(headers)),
            "options": list(_canonical_options(options)),
        },
    }
    _store_evidence(drive_root, "owner_acks", fp, record)
    return record


def list_owner_acks(drive_root: Any) -> List[Dict[str, Any]]:
    return list(_load(drive_root).get("owner_acks", {}).values())


def revoke_owner_ack(drive_root: Any, route_fp: str) -> bool:
    with _STORE_LOCK:
        data = _load(drive_root)
        if route_fp in data.get("owner_acks", {}):
            del data["owner_acks"][route_fp]
            _save(drive_root, data)
            return True
    return False


# --- Probing (opportunistic, cached) ------------------------------------------

def _openai_compatible_metadata_window(
    model: str, base_url: str, allow_fetch: bool, api_key: Optional[str] = None
) -> int:
    """CW6 (v6.34.0): an OpenAI-compatible server (vLLM, Ollama, LM Studio, TGI, ...)
    commonly publishes the per-model window in GET {base_url}/models — under
    max_model_len / context_length / context_window. Best-effort, fail-closed to 0
    (network/auth/parse error, no base_url, or hot-path allow_fetch=False all => 0).

    ``api_key`` may be passed by callers that already hold the key in scope (e.g.
    the settings-save gate, which has the not-yet-persisted value in ``current``).
    When omitted the function falls back to the already-saved settings on disk."""
    if not allow_fetch or not str(base_url or "").strip() or not str(model or "").strip():
        return 0
    try:
        import httpx

        if api_key is None:
            from ouroboros.config import load_settings
            api_key = str((load_settings() or {}).get("OPENAI_COMPATIBLE_API_KEY") or "")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        resp = httpx.get(str(base_url).rstrip("/") + "/models", headers=headers, timeout=5.0)
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("data") if isinstance(payload, dict) else payload
        # The saved model is normally provider-prefixed (e.g. ``openai-compatible::llama-3``)
        # while /models lists the BARE id — match either spelling.
        wanted = {str(model), str(model).split("::", 1)[-1]}
        for item in (items or []):
            if not isinstance(item, dict) or str(item.get("id") or item.get("name") or "") not in wanted:
                continue
            sources = [item, item.get("meta") if isinstance(item.get("meta"), dict) else {}]
            for src in sources:
                for key in ("max_model_len", "context_length", "context_window", "max_context_length"):
                    val = src.get(key)
                    if isinstance(val, (int, float)) and int(val) > 0:
                        return int(val)
        return 0
    except Exception:
        return 0


def _provider_metadata_window(
    provider: str, model: str, base_url: str, allow_fetch: bool, api_key: Optional[str] = None
) -> int:
    """Best-effort live window from provider metadata. 0 = no metadata source."""
    p = str(provider or "").strip().lower()
    # OpenRouter publishes context_length in /models (one cached fetch).
    if "openrouter" in p or (not p and "/" in str(model or "")):
        try:
            from ouroboros.llm import LLMClient
            return int(LLMClient.openrouter_context_length(model, allow_fetch=allow_fetch) or 0)
        except Exception:
            return 0
    # CW6: OpenAI-compatible /models probe (vLLM/Ollama/...) before falling to unprobeable.
    if p in {"openai-compatible", "minimax"}:
        if p == "minimax" and api_key is None:
            try:
                from ouroboros.config import load_settings
                api_key = str((load_settings() or {}).get("MINIMAX_API_KEY") or "")
            except Exception:
                api_key = ""
        return _openai_compatible_metadata_window(model, base_url, allow_fetch, api_key=api_key)
    # GigaChat's /models (aget_models) lists model ids but does NOT publish a per-model
    # context window, so a gigachat route stays unprobeable (owner-ack path) — no probe.
    return 0


def _local_health_window(model: str) -> int:
    """Local lane window from the running local model (n_ctx). 0 if unavailable."""
    try:
        from ouroboros.local_model import get_manager
        return int(get_manager().get_context_length() or 0)
    except Exception:
        return 0


def _metadata_fetch_transport_failed(provider: str, model: str, use_local: bool) -> bool:
    """True only when a metadata fetch was ATTEMPTED and failed at transport level
    (provider unreachable) — distinct from a route that simply has no metadata source.
    Only the OpenRouter /models fetch reports transport failure; the CW6 OpenAI-compatible
    probe instead fails closed to a 0 window (-> unprobeable -> owner-ack), so a flaky
    OpenAI-compatible endpoint reads as 'unknown', not as a hard connectivity error."""
    if use_local:
        return False  # local health is in-process; its absence is not an outage
    p = str(provider or "").strip().lower()
    is_openrouter = "openrouter" in p or (not p and "/" in str(model or ""))
    if not is_openrouter:
        return False
    try:
        from ouroboros.llm import LLMClient
        return bool(LLMClient.metadata_fetch_attempted_and_failed())
    except Exception:
        return False


_GENERATIVE_PROBE_PROVIDERS = {"cloudru", "openai-compatible", "minimax", "openai", "openrouter"}
_PROBE_CANARIES = ["OBOCANARYBEGIN7Q", "OBOCANARYMID7Q", "OBOCANARYEND7Q"]


def _generative_probe_enabled() -> bool:
    return (os.environ.get("OUROBOROS_GENERATIVE_PROBE", "1") or "").strip().lower() not in {"", "0", "false", "no", "off"}


def _generative_probe_pad_chars() -> int:
    try:
        return max(200_000, int(os.environ.get("OUROBOROS_GENERATIVE_PROBE_CHARS", "5000000") or "5000000"))
    except (ValueError, TypeError):
        return 5_000_000


def _generative_probe_window(
    provider: str, model: str, base_url: str = "", api_key: Optional[str] = None,
) -> Tuple[int, str, str]:
    """Empirically size a route's window with ONE oversized request, free-only.

    Sends a deliberately over-window input on an OpenAI-compatible route; the
    provider rejects it PRE-inference (free) with the limit in the message. Never
    raises — any setup/transport error returns FAILED (-> fail-closed owner-ack).
    """
    if not _generative_probe_enabled() or provider not in _GENERATIVE_PROBE_PROVIDERS:
        return 0, STATUS_UNPROBEABLE, "generative probe not applicable/enabled for this route"
    pad = _generative_probe_pad_chars()
    chunk = "x " * (pad // 4)
    content = f"{_PROBE_CANARIES[0]} {chunk} {_PROBE_CANARIES[1]} {chunk} {_PROBE_CANARIES[2]} Echo the three OBOCANARY tokens verbatim."
    sent_estimate = max(1, len(content) // 4)
    # Transport lives in the shared LLMClient seam (DEVELOPMENT.md): it owns route
    # resolution, the per-provider token key, the hard timeout, and never-raises. This
    # module only CLASSIFIES the raw outcome into window evidence (fail-closed).
    try:
        from ouroboros.llm import LLMClient

        out = LLMClient().probe_oversized_context(model, content, base_url=base_url, api_key=api_key)
    except Exception as exc:  # pragma: no cover - defensive
        return 0, STATUS_FAILED, f"generative probe failed: {type(exc).__name__}"
    if out.get("ok"):
        return classify_generative_probe_response(
            200, "", canaries=_PROBE_CANARIES, echoed_text=str(out.get("echoed_text") or ""),
            usage_prompt_tokens=int(out.get("usage_prompt") or 0), sent_token_estimate=sent_estimate,
        )
    status = out.get("status_code")
    return classify_generative_probe_response(
        status if isinstance(status, int) else None, str(out.get("body") or ""),
    )


def probe(
    drive_root: Any,
    *,
    provider: str,
    model: str,
    base_url: str = "",
    headers: Optional[Dict[str, Any]] = None,
    options: Optional[Dict[str, Any]] = None,
    use_local: bool = False,
    allow_fetch: bool = True,
    allow_generative: bool = False,
    force: bool = False,
    api_key: Optional[str] = None,
) -> CapabilityEvidence:
    """Resolve Capability Evidence for a route, using the cache unless ``force``.

    Order: fresh cache -> owner-ack (asserted) -> provider metadata / local health
    (confirmed) -> unprobeable. Network probing is skipped when allow_fetch=False
    (hot-path callers) — a stale or absent record then reads as unknown."""
    fp = route_fingerprint(provider=provider, base_url=base_url, model=model, headers=headers, options=options)
    data = _load(drive_root)

    # Owner-ack always wins as ASSERTED evidence for its exact route.
    ack = data.get("owner_acks", {}).get(fp)
    if ack:
        return CapabilityEvidence(
            window_tokens=int(ack.get("window_tokens") or 0), status=STATUS_ASSERTED,
            source=SOURCE_OWNER_ACK, route_fp=fp, model=model, provider=provider,
            ts=str(ack.get("ts") or ""), detail=f"owner-ack by {ack.get('owner') or 'owner'}",
        )

    cached = data.get("probes", {}).get(fp)
    # An EXPLICIT generative probe (owner toggle/save, allow_generative=True) must run even
    # when a prior LAZY (allow_generative=False) call left a fresh UNPROBEABLE/FAILED record
    # — otherwise the owner's empirical probe is silently short-circuited and never fires.
    # Only a CONFIRMED cache is authoritative enough to skip the live probe on that path.
    _skip_cache_for_generative = allow_generative and str((cached or {}).get("status") or "") != STATUS_CONFIRMED
    if cached and not force and not _skip_cache_for_generative:
        age = _age_seconds(str(cached.get("ts") or ""))
        ttl = _CONFIRMED_TTL_SEC if cached.get("status") == STATUS_CONFIRMED else _FAILED_TTL_SEC
        if age <= ttl:
            ev = CapabilityEvidence(
                window_tokens=int(cached.get("window_tokens") or 0), status=str(cached.get("status") or STATUS_UNPROBEABLE),
                source=str(cached.get("source") or SOURCE_NONE), route_fp=fp, model=model,
                provider=provider, ts=str(cached.get("ts") or ""), detail=str(cached.get("detail") or ""),
            )
            return ev

    if not allow_fetch:
        # Hot path: never block on the network. Return the (possibly stale) cache
        # marked stale, else unprobeable — both read as unknown for >=1M gates.
        if cached:
            return CapabilityEvidence(
                window_tokens=int(cached.get("window_tokens") or 0), status=str(cached.get("status") or STATUS_UNPROBEABLE),
                source=str(cached.get("source") or SOURCE_NONE), route_fp=fp, model=model,
                provider=provider, ts=str(cached.get("ts") or ""), detail="stale (no fetch on hot path)", stale=True,
            )
        return CapabilityEvidence(0, STATUS_UNPROBEABLE, SOURCE_NONE, fp, model, provider, detail="not probed")

    # Live probe.
    window = 0
    source = SOURCE_NONE
    if use_local:
        window = _local_health_window(model)
        if window > 0:
            source = SOURCE_LOCAL_HEALTH
    if window <= 0:
        meta = _provider_metadata_window(provider, model, base_url, allow_fetch=allow_fetch, api_key=api_key)
        if meta > 0:
            window, source = meta, SOURCE_PROVIDER_METADATA

    # Generative probe: only when metadata gave nothing AND a toggle/save call-site
    # opted in (allow_generative) — never on the lazy per-task hot path. Confirms a
    # window empirically via a free over-window reject; a 200/numberless reject -> owner-ack.
    if window <= 0 and allow_generative and not use_local:
        gwin, gstatus, gdetail = _generative_probe_window(provider, model, base_url, api_key=api_key)
        if gwin > 0:
            window, source = gwin, SOURCE_GENERATIVE_PROBE
        elif gstatus == STATUS_FAILED:
            ev = CapabilityEvidence(0, STATUS_FAILED, SOURCE_NONE, fp, model, provider,
                                    ts=utc_now_iso(), detail=gdetail)
            _store_evidence(drive_root, "probes", fp, ev.to_json())
            return ev

    if window > 0:
        ev = CapabilityEvidence(window, STATUS_CONFIRMED, source, fp, model, provider, ts=utc_now_iso(), detail="live probe")
        _store_evidence(drive_root, "probes", fp, ev.to_json())
        return ev

    # window <= 0. A provider OUTAGE must NEVER erase a prior confirmed record
    # (the module invariant) — keep it, surfaced as stale, and do not overwrite the
    # cache. Otherwise distinguish a transient outage (STATUS_FAILED, so the owner
    # sees an error: "no connection") from a route that simply has no metadata
    # source (STATUS_UNPROBEABLE -> the owner-ack path).
    prior = cached if isinstance(cached, dict) else None
    prior_win = int((prior or {}).get("window_tokens") or 0)
    prior_status = str((prior or {}).get("status") or "")
    if prior is not None and prior_status in _KNOWN_STATUS and prior_win > 0:
        return CapabilityEvidence(
            prior_win, prior_status, str(prior.get("source") or SOURCE_NONE), fp, model, provider,
            ts=str(prior.get("ts") or ""), detail="kept prior evidence (probe blip)", stale=True,
        )
    if _metadata_fetch_transport_failed(provider, model, use_local):
        ev = CapabilityEvidence(0, STATUS_FAILED, SOURCE_NONE, fp, model, provider, ts=utc_now_iso(),
                                detail="provider unreachable during probe")
    else:
        ev = CapabilityEvidence(0, STATUS_UNPROBEABLE, SOURCE_NONE, fp, model, provider, ts=utc_now_iso(),
                                detail="no provider metadata; owner-ack required for a >=1M gate")
    _store_evidence(drive_root, "probes", fp, ev.to_json())
    return ev
