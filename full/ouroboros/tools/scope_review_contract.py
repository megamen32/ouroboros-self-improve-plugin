"""Pure output-contract helpers for the commit scope reviewer.

This module owns no reviewer routing, retry, authority, or persistence.  It
validates the one-pass actor's JSON checklist response, projects FAIL rows into
the two finding buckets consumed by ``scope_review``, and owns the ladder's
touched-context sentinel type plus the terminal cause/remedy wording — the
refusal contract the owner reads when the guaranteed-fit ladder exhausts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from ouroboros.tools.review_context_atlas import (
    ATLAS_MISSING_ARTIFACT_REMEDY,
    ATLAS_MIXED_ASSEMBLY_REMEDY,
)


@dataclass
class TouchedContextStatus:
    """Touched-context sentinel; ``None`` means context OK."""
    status: str  # "empty" | "omitted" | "budget_exceeded" | "fixed_overflow"
    omitted_paths: List[str] = field(default_factory=list)
    token_count: int = 0  # estimated full prompt tokens when budget is exceeded
    # Required artifacts the atlas could not assemble (BIBLE P3): non-empty means
    # the ladder terminated on a MISSING artifact. NOT exclusive with an overflow:
    # the same refusal can also be a hard-budget overflow (`atlas_overflowed`).
    unassembled_required: List[str] = field(default_factory=list)
    # True when the atlas refusal itself was a hard-budget overflow (its manifest
    # says `budget_exceeded`): even the content-free atlas did not fit beside the
    # fixed prompt. Can hold TOGETHER with `unassembled_required` — the mixed
    # failure reports both causes, or the remedy prescribed cannot resolve it.
    atlas_overflowed: bool = False


def compute_touched_context_status(
    current_files_section: str,
    deleted_paths: list,
    omitted: list,
    current_paths: list,
) -> Optional[TouchedContextStatus]:
    """Return the touched-context failure sentinel, or None when context is complete."""
    if not current_files_section.strip() and not deleted_paths:
        return TouchedContextStatus(status="empty")
    if omitted and current_paths:
        return TouchedContextStatus(status="omitted", omitted_paths=list(omitted))
    return None


def ladder_terminal_cause(
    context_status: TouchedContextStatus, input_limit: int, budget_phrase: str = "",
) -> tuple[str, str]:
    """``(cause, remedy)`` for a terminal ladder status — ONE derivation, both terminals.

    A missing REQUIRED artifact alone is NOT an overflow: quoting a token count
    against the budget there states a comparison that is false (the pack was
    small, not oversized), and splitting the diff cannot shrink an UNCHANGED
    artifact. But the two causes can COINCIDE — the refusal that dropped the
    artifact can itself be a hard-budget overflow — and then both are reported:
    suppressing the overflow behind the artifact would prescribe a remedy that
    cannot resolve it."""
    budget = budget_phrase or f"input budget ({input_limit})"
    missing = list(context_status.unassembled_required or [])
    if missing:
        named = ", ".join(missing[:5]) + (f" (+{len(missing) - 5} more)" if len(missing) > 5 else "")
        missing_cause = (
            f"the review pack could not assemble required artifact(s): {named}, and "
            "every ladder step was exhausted (touched files degraded to diff-only, "
            "atlas reduced to its manifest)"
        )
        if not context_status.atlas_overflowed:
            return missing_cause, ATLAS_MISSING_ARTIFACT_REMEDY
        return (
            missing_cause + "; AND even the content-free atlas manifest still "
            f"exceeds the scope reviewer {budget} beside the fixed prompt "
            f"(~{context_status.token_count} estimated tokens)",
            ATLAS_MIXED_ASSEMBLY_REMEDY,
        )
    return (
        "the irreducible scope prompt (checklist + canonical docs + staged diff) is "
        f"~{context_status.token_count} estimated tokens and exceeds the scope reviewer "
        f"{budget}, with every touched file "
        "already degraded to diff-only and the atlas to its manifest",
        "Split the commit into smaller staged diffs, or configure a larger-window reviewer.",
    )


SCOPE_REQUIRED_ITEMS = frozenset({
    "intent_alignment",
    "forgotten_touchpoints",
    "cross_surface_consistency",
    "regression_surface",
    "prompt_doc_sync",
    "architecture_fit",
    "cross_module_bugs",
    "implicit_contracts",
})
SCOPE_VALID_SEVERITIES = frozenset({"critical", "advisory"})


def normalize_scope_items(items: list) -> tuple[list[dict], str]:
    """Validate and normalize the scope-review checklist coverage contract."""
    if not isinstance(items, list):
        return [], "reviewer output is not a JSON array"

    normalized: list[dict] = []
    seen_pass: set[str] = set()
    seen_fail: set[str] = set()
    seen_items: set[str] = set()
    unexpected: list[str] = []
    invalid: list[str] = []

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            invalid.append(f"entry {index} is not an object")
            continue
        item_id = str(item.get("item", "") or "").strip()
        verdict = str(item.get("verdict", "") or "").strip().upper()
        if item_id not in SCOPE_REQUIRED_ITEMS:
            unexpected.append(item_id or f"<missing item at {index}>")
            continue
        if verdict not in {"PASS", "FAIL"}:
            invalid.append(f"{item_id}: invalid verdict {verdict!r}")
            continue
        severity = str(item.get("severity", "") or "").strip().lower()
        if verdict == "PASS" and not severity:
            # Severity only classifies FAIL blocking-ness. Reviewer models may
            # omit it on PASS, matching the triad parser's advisory default.
            severity = "advisory"
        if severity not in SCOPE_VALID_SEVERITIES:
            invalid.append(f"{item_id}: missing or invalid severity {severity!r}")
            continue
        reason = str(item.get("reason", "") or "").strip()
        if not reason:
            invalid.append(f"{item_id}: missing reason")
            continue
        if verdict == "PASS":
            reason_words = [
                word.strip(".,;:!?()[]{}\"'")
                for word in reason.split()
                if word.strip(".,;:!?()[]{}\"'")
            ]
            if (
                reason.lower().strip(".!?:;")
                in {"pass", "ok", "okay", "yes", "n/a", "na", "none"}
                or len(reason_words) < 4
            ):
                invalid.append(f"{item_id}: PASS reason is too terse")
                continue
            if item_id in seen_pass:
                invalid.append(f"{item_id}: duplicate PASS")
            seen_pass.add(item_id)
        else:
            seen_fail.add(item_id)
        seen_items.add(item_id)

        normalized_item = dict(item)
        normalized_item["item"] = item_id
        normalized_item["verdict"] = verdict
        normalized_item["severity"] = severity
        normalized_item["reason"] = reason
        normalized.append(normalized_item)

    pass_and_fail = sorted(seen_pass & seen_fail)
    if pass_and_fail:
        invalid.append("items with both PASS and FAIL: " + ", ".join(pass_and_fail))
    missing = sorted(SCOPE_REQUIRED_ITEMS - seen_items)
    errors: list[str] = []
    if missing:
        errors.append("missing required items: " + ", ".join(missing))
    if unexpected:
        errors.append("unexpected items: " + ", ".join(unexpected))
    if invalid:
        errors.append("invalid entries: " + "; ".join(invalid))
    return normalized, "; ".join(errors)


def classify_scope_findings(items: list) -> tuple[List[dict], List[dict]]:
    """Project normalized FAIL rows into critical and advisory findings."""
    critical_findings: List[dict] = []
    advisory_findings: List[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        verdict = str(item.get("verdict", "")).upper()
        severity = str(item.get("severity", "advisory")).lower()
        if verdict != "FAIL":
            continue
        finding = {
            "verdict": "FAIL",
            "severity": severity,
            "item": str(item.get("item", "scope_review")),
            "reason": str(item.get("reason", "")),
            "model": "scope_reviewer",
        }
        obligation_id = str(item.get("obligation_id", "") or "")
        if obligation_id:
            finding["obligation_id"] = obligation_id
        if severity == "critical":
            critical_findings.append(finding)
        else:
            advisory_findings.append(finding)
    return critical_findings, advisory_findings


def build_scope_block_message(
    critical_findings: List[dict], advisory_findings: List[dict]
) -> str:
    """Format critical plus advisory findings into the blocking message."""
    crit_lines = "\n".join(
        f"  CRITICAL: [scope:{finding['item']}] {finding['reason']}"
        for finding in critical_findings
    )
    advisory_section = ""
    if advisory_findings:
        advisory_lines = "\n".join(
            f"  WARN: [scope:{finding['item']}] {finding['reason']}"
            for finding in advisory_findings
        )
        advisory_section = f"\n\nAdvisory warnings:\n{advisory_lines}"
    return (
        "⚠️ SCOPE_REVIEW_BLOCKED: Scope reviewer found critical completeness issues.\n"
        "Commit has NOT been created. Fix the issues and try again.\n\n"
        + crit_lines
        + advisory_section
    )
