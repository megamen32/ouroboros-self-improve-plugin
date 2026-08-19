"""Private settings authority shared by the Telegram extension and companion."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:  # Package import from plugin.py.
    from .platform_support import acquire_file_lock, path_is_link_or_reparse, release_file_lock
except ImportError:  # Direct execution from scripts/companion.py.
    from platform_support import acquire_file_lock, path_is_link_or_reparse, release_file_lock


MINIAPP_MARKER_HEADER = "X-Ouroboros-Telegram-MiniApp"
_SETTINGS_NAME = "settings.json"
_LOCK_NAME = ".settings.lock"
_TRUE = frozenset({"on", "true", "1", "yes"})


class TelegramSettingsError(RuntimeError):
    pass


def _settings_path(state_dir: Path) -> Path:
    root = Path(state_dir).expanduser()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path_is_link_or_reparse(root) or not root.is_dir():
        raise TelegramSettingsError("Telegram state must be a real directory.")
    root = root.resolve(strict=True)
    path = root / _SETTINGS_NAME
    if path_is_link_or_reparse(path):
        raise TelegramSettingsError("Telegram settings are an unsafe link.")
    return path


def load_settings(state_dir: Path) -> dict[str, Any]:
    path = _settings_path(state_dir)
    try:
        path.stat()
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise TelegramSettingsError("Could not read Telegram settings.") from exc
    if not path.is_file():
        raise TelegramSettingsError("Telegram settings are invalid.")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise TelegramSettingsError("Could not read Telegram settings.") from exc
    except (TypeError, ValueError) as exc:
        raise TelegramSettingsError("Telegram settings are invalid.") from exc
    if not isinstance(value, dict):
        raise TelegramSettingsError("Telegram settings are invalid.")
    return value


def owner_chat_id(state_dir: Path) -> int:
    try:
        value = int(str(load_settings(state_dir).get("TELEGRAM_CHAT_ID") or "").strip())
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


def miniapp_enabled(state_dir: Path) -> bool:
    raw = str(load_settings(state_dir).get("TELEGRAM_MINIAPP_ENABLED") or "on").strip().lower()
    return raw in _TRUE


def merge_settings(state_dir: Path, patch: dict[str, Any]) -> dict[str, Any]:
    path = _settings_path(state_dir)
    lock_path = path.with_name(_LOCK_NAME)
    if path_is_link_or_reparse(lock_path):
        raise TelegramSettingsError("Telegram settings lock is unsafe.")
    try:
        fd = acquire_file_lock(lock_path, timeout_sec=2.0)
    except Exception as exc:
        raise TelegramSettingsError("Telegram settings are busy.") from exc
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        current = load_settings(path.parent)
        current.update(patch)
        encoded = json.dumps(current, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write_text(encoded, encoding="utf-8")
        try:
            tmp.chmod(0o600)
        except OSError:
            pass
        os.replace(tmp, path)
        return current
    except OSError as exc:
        raise TelegramSettingsError("Could not persist Telegram settings.") from exc
    finally:
        release_file_lock(fd)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def request_may_change_owner(request: Any) -> bool:
    headers = getattr(request, "headers", {})
    if str(headers.get(MINIAPP_MARKER_HEADER, "")).strip():
        return False
    client = getattr(request, "client", None)
    host = str(getattr(client, "host", "") or "").strip()
    return host in {"127.0.0.1", "::1"}


class TelegramSettingsObserver:
    """Read-only companion view of this skill's own settings."""

    def __init__(self, state_dir: Path, _core_port: int) -> None:
        self.state_dir = state_dir

    def owner_chat_id(self) -> int:
        return owner_chat_id(self.state_dir)

    def safe_for_exposure(self, owner: int) -> bool:
        settings = load_settings(self.state_dir)
        try:
            current_owner = int(str(settings.get("TELEGRAM_CHAT_ID") or "").strip())
        except (TypeError, ValueError):
            current_owner = 0
        enabled = str(settings.get("TELEGRAM_MINIAPP_ENABLED") or "on").strip().lower() in _TRUE
        return owner > 0 and current_owner == owner and enabled

__all__ = [
    "MINIAPP_MARKER_HEADER",
    "TelegramSettingsError",
    "TelegramSettingsObserver",
    "load_settings",
    "merge_settings",
    "miniapp_enabled",
    "owner_chat_id",
    "request_may_change_owner",
]
