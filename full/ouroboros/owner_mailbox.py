"""Per-task owner-message mailboxes for running worker tasks."""
import json
import logging
import pathlib
import uuid
from typing import List, Optional

from ouroboros.task_results import validate_task_id
from ouroboros.utils import append_jsonl, utc_now_iso

log = logging.getLogger(__name__)

_MAILBOX_DIR = "memory/owner_mailbox"

# Typed mailbox entry kinds. KIND_OWNER_TEXT entries are injected verbatim as
# owner dialogue; control kinds carry supervisor->worker protocol signals and
# are routed structurally (never shown as user prose).
KIND_OWNER_TEXT = "owner_text"
KIND_FINALIZE_NOW = "finalize_now"
# The mailbox is append-only, so a sender that changes its mind cannot delete the
# control it already wrote — it appends this retraction naming the target msg_id.
# Revocations are resolved by the READER over the whole mailbox, so a control that
# was revoked before anyone drained it is never delivered at all. Its payload IS
# the revoked msg_id (same convention as finalize_now, whose payload is a reason).
KIND_CONTROL_REVOKED = "control_revoked"


def _mailbox_path(drive_root: pathlib.Path, task_id: str) -> pathlib.Path:
    return pathlib.Path(drive_root) / _MAILBOX_DIR / f"{validate_task_id(task_id)}.jsonl"


def write_owner_message(
    drive_root: pathlib.Path,
    text: str,
    task_id: str,
    msg_id: Optional[str] = None,
    kind: str = KIND_OWNER_TEXT,
) -> bool:
    """Write an owner message or typed control entry to a task's mailbox."""
    path = _mailbox_path(drive_root, task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "msg_id": msg_id or uuid.uuid4().hex,
        "ts": utc_now_iso(),
        "text": text,
        "kind": str(kind or KIND_OWNER_TEXT),
    }
    try:
        if not append_jsonl(path, entry):
            log.warning("Failed to durably append owner message for task %s", task_id)
            return False
        return True
    except Exception:
        log.warning("Failed to write owner message for task %s", task_id, exc_info=True)
        return False


def revoke_owner_control(
    drive_root: pathlib.Path, task_id: str, control_msg_id: str,
) -> bool:
    """Retract an already-written control entry by its msg_id.

    The only way to un-send a mailbox control: the file is append-only, so this
    appends a revocation that ``drain_owner_entries`` resolves for every reader.
    Returns False when there is nothing to revoke or the append was not durable —
    the caller must then treat the control as STILL LIVE.
    """
    control_msg_id = str(control_msg_id or "").strip()
    if not control_msg_id:
        return False
    return write_owner_message(
        drive_root, control_msg_id, task_id, kind=KIND_CONTROL_REVOKED,
    )


def drain_owner_entries(
    drive_root: pathlib.Path,
    task_id: str,
    seen_ids: Optional[set] = None,
) -> List[dict]:
    """Read unseen mailbox entries without mutating the append-only mailbox.

    Revocations are resolved over the WHOLE mailbox before anything is yielded,
    so a control retracted by a later line is never delivered even if the reader
    had not drained it yet; the revocation lines themselves are protocol and are
    never returned as content.
    """
    path = _mailbox_path(drive_root, task_id)
    if not path.exists():
        return []
    if seen_ids is None:
        seen_ids = set()
    try:
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return []
        parsed: List[dict] = []
        revoked: set = set()
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                log.debug("Malformed mailbox line for task %s", task_id, exc_info=True)
                continue
            if not isinstance(entry, dict):
                continue
            if str(entry.get("kind") or KIND_OWNER_TEXT) == KIND_CONTROL_REVOKED:
                revoked.add(str(entry.get("text") or ""))
            parsed.append(entry)
        entries = []
        for entry in parsed:
            mid = entry.get("msg_id", "")
            if mid and mid in seen_ids:
                continue
            if mid:
                seen_ids.add(mid)
            kind = str(entry.get("kind") or KIND_OWNER_TEXT)
            if kind == KIND_CONTROL_REVOKED or (mid and mid in revoked):
                continue
            text = entry.get("text", "")
            if text:
                entries.append({"msg_id": mid, "text": text, "kind": kind})
        return entries
    except Exception:
        log.debug("Failed to read mailbox for task %s", task_id, exc_info=True)
        return []


def drain_owner_messages(
    drive_root: pathlib.Path,
    task_id: str,
    seen_ids: Optional[set] = None,
) -> List[str]:
    """Read unseen owner-dialogue message texts (legacy text-only view)."""
    return [
        entry["text"]
        for entry in drain_owner_entries(drive_root, task_id, seen_ids=seen_ids)
        if entry.get("kind", KIND_OWNER_TEXT) == KIND_OWNER_TEXT
    ]


def cleanup_task_mailbox(drive_root: pathlib.Path, task_id: str) -> None:
    """Remove a task's mailbox file after task completes."""
    path = _mailbox_path(drive_root, task_id)
    try:
        if path.exists():
            path.unlink()
    except Exception:
        log.debug("Failed to cleanup mailbox for task %s", task_id, exc_info=True)
