"""Official managed-update channel mapping and bounded fetch settings."""

from __future__ import annotations

import os
from typing import Any, Mapping


UPDATE_CHANNEL_BRANCHES = {
    "stable": "main",
    "qa": "ouroboros-stable",
    "development": "ouroboros",
}

UPDATE_SETTINGS_DEFAULTS = {
    "OUROBOROS_UPDATE_CHANNEL": "stable",
    "OUROBOROS_MANAGED_UPDATE_FETCH_TIMEOUT_SEC": 300,
}


def normalize_update_channel(value: Any) -> str:
    """Normalize the owner-facing channel; invalid values stay on Stable."""
    channel = str(value or "").strip().lower()
    return channel if channel in UPDATE_CHANNEL_BRANCHES else "stable"


def get_update_channel(settings: Mapping[str, Any] | None = None) -> str:
    """Return the runtime update channel, independent from launcher metadata."""
    if settings is not None:
        raw = settings.get("OUROBOROS_UPDATE_CHANNEL")
    else:
        raw = os.environ.get("OUROBOROS_UPDATE_CHANNEL")
        if raw is None:
            from ouroboros.config import load_settings

            raw = load_settings().get("OUROBOROS_UPDATE_CHANNEL")
    return normalize_update_channel(raw)


def get_update_branch(settings: Mapping[str, Any] | None = None) -> str:
    """Map the closed channel enum to its official branch."""
    return UPDATE_CHANNEL_BRANCHES[get_update_channel(settings)]


def get_managed_update_fetch_timeout_sec() -> int:
    """Wall-clock ceiling for official git network operations."""
    raw = os.environ.get(
        "OUROBOROS_MANAGED_UPDATE_FETCH_TIMEOUT_SEC",
        UPDATE_SETTINGS_DEFAULTS["OUROBOROS_MANAGED_UPDATE_FETCH_TIMEOUT_SEC"],
    )
    try:
        parsed = int(float(raw))
    except (TypeError, ValueError):
        parsed = int(UPDATE_SETTINGS_DEFAULTS["OUROBOROS_MANAGED_UPDATE_FETCH_TIMEOUT_SEC"])
    return max(30, min(parsed, 1800))
