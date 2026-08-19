"""Wire placeholders for Settings and MCP secrets.

Each reader recognizes only a shape emitted by its matching producer. Keeping
the small mechanical contract here prevents a display placeholder from being
persisted without treating arbitrary values ending in ``...`` as secrets to
erase.
"""

from __future__ import annotations

import re
from typing import Any, Collection, Dict

# Credentials the Settings API answers a GET with a PLACEHOLDER instead of the
# stored value. The same set gates the read-side repair in
# ``config.load_settings`` and the write-side merge in
# ``gateway.settings._merge_settings_payload``.
MASKED_SECRET_SETTING_KEYS = frozenset(
    {
        "OPENROUTER_API_KEY",
        "OPENAI_API_KEY",
        "OPENAI_COMPATIBLE_API_KEY",
        "CLOUDRU_FOUNDATION_MODELS_API_KEY",
        "GIGACHAT_CREDENTIALS",
        "GIGACHAT_PASSWORD",
        "ANTHROPIC_API_KEY",
        "MINIMAX_API_KEY",
        "GITHUB_TOKEN",
        "OUROBOROS_NETWORK_PASSWORD",
    }
)

CONFIGURED_SECRET_PLACEHOLDER = "***set***"

PASSWORD_SECRET_SETTING_KEYS = frozenset(
    {
        "OUROBOROS_NETWORK_PASSWORD",
        "GIGACHAT_PASSWORD",
        "GIGACHAT_CREDENTIALS",
    }
)

_CUSTOM_SECRET_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,}$")


def is_custom_secret_setting_key(key: Any, *, known_setting_keys: Collection[str]) -> bool:
    """Whether ``key`` is an owner-defined top-level Settings secret."""
    text = str(key or "").strip()
    return (
        bool(_CUSTOM_SECRET_KEY_RE.fullmatch(text))
        and text not in known_setting_keys
        and not text.startswith("OUROBOROS_")
    )


def mask_prefixed_secret(value: Any, *, visible_chars: int) -> str:
    """Mask a token, retaining the prefix used by its calling surface."""
    text = str(value or "")
    if not text:
        return ""
    return text[:visible_chars] + "..." if len(text) > visible_chars else "***"


def mask_settings_secret(key: Any, value: Any) -> str:
    """Return the Settings GET placeholder for one top-level secret."""
    if str(key or "") in PASSWORD_SECRET_SETTING_KEYS:
        return CONFIGURED_SECRET_PLACEHOLDER if str(value or "").strip() else ""
    return mask_prefixed_secret(value, visible_chars=8)


def _looks_prefixed_mask(value: Any, *, visible_chars: int) -> bool:
    text = str(value or "").strip()
    return len(text) == visible_chars + 3 and text.endswith("...")


def looks_masked_settings_secret(key: Any, value: Any) -> bool:
    """Recognize only the placeholder emitted for this Settings key class."""
    text = str(value or "").strip()
    if text == CONFIGURED_SECRET_PLACEHOLDER:
        return True
    if str(key or "") in PASSWORD_SECRET_SETTING_KEYS:
        return False
    return text == "***" or _looks_prefixed_mask(text, visible_chars=8)


def looks_masked_mcp_secret(value: Any) -> bool:
    """Recognize the short status and longer Settings MCP token masks."""
    text = str(value or "").strip()
    return text == "***" or _looks_prefixed_mask(text, visible_chars=4) or _looks_prefixed_mask(text, visible_chars=8)


def looks_masked_secret(value: Any) -> bool:
    """Compatibility union of the exact placeholder shapes this module emits."""
    text = str(value or "").strip()
    return text == CONFIGURED_SECRET_PLACEHOLDER or looks_masked_mcp_secret(text)


def strip_masked_secrets(settings: Dict[str, Any], *, known_setting_keys: Collection[str]) -> Dict[str, Any]:
    """Blank recognized top-level placeholders before read or persistence.

    Read-side repair for an install an older round-trip already poisoned: the
    placeholder is dropped on load, the Settings field reads as empty, and the
    owner re-enters the real key instead of an endpoint seeing ``Bearer ***``.
    Silent by design — ``load_settings`` runs on nearly every request, so a
    warning here would be per-request noise; the emptied field is the
    owner-facing signal.

    Mutates and returns ``settings`` so a caller can wrap an existing return.
    """
    for key, value in settings.items():
        if key in MASKED_SECRET_SETTING_KEYS:
            masked = looks_masked_settings_secret(key, value)
        elif is_custom_secret_setting_key(key, known_setting_keys=known_setting_keys):
            masked = looks_masked_settings_secret(key, value)
        else:
            masked = False
        if masked:
            settings[key] = ""
    return settings
