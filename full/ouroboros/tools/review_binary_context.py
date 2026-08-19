"""Exact, bounded prompt metadata for staged binary files."""

from __future__ import annotations

import os
import pathlib
import subprocess


class StagedDiffUnavailable(RuntimeError):
    """The canonical staged diff could not be captured.

    A RuntimeError so it lands in the prompt-assembly fail-closed path: a review
    whose only evidence of a degraded file is the staged diff must not run
    authoritatively on a placeholder string that says the capture failed.
    """


def capture_staged_diff(repo_dir: pathlib.Path, *, unified: int = 3) -> str:
    """The staged diff exactly as the reviewer must see it.

    ONE capture for both ladder rungs (full context and ``-U0``). The flag tail
    pins away operator config that rewrites diff output into something that no
    longer describes the staged bytes: external diff drivers, textconv filters,
    colour escapes and prefix rewrites; GIT_DIFF_OPTS leaves the environment
    because it overrides the context width from outside the argv. Output is
    taken as BYTES and decoded strictly; non-UTF-8 staged text is rendered with
    ``backslashreplace`` plus an explicit note, so it reaches the reviewer in a
    readable form instead of raising or being flattened into U+FFFD.
    """
    env = {k: v for k, v in os.environ.items() if k != "GIT_DIFF_OPTS"}
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--no-ext-diff", "--no-textconv", "--no-color",
             "--src-prefix=a/", "--dst-prefix=b/", f"--unified={int(unified)}"],
            cwd=repo_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=300, env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise StagedDiffUnavailable(f"staged diff capture failed: {exc!r}") from exc
    if result.returncode != 0:
        detail = (result.stderr or b"").decode("utf-8", "replace").strip()
        raise StagedDiffUnavailable(
            f"staged diff capture failed (rc {result.returncode}): {detail or 'no detail'}"
        )
    raw = result.stdout or b""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        rendered = raw.decode("utf-8", "backslashreplace")
        return (
            f"{rendered}\n\n*(staged diff contained non-UTF-8 bytes; they are "
            "rendered above as backslash escapes)*\n"
        )


def _git_bytes(repo_dir: pathlib.Path, args: list[str]) -> bytes:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return b""
    return result.stdout if result.returncode == 0 else b""


def _tree_entry(repo_dir: pathlib.Path, ref: str, rel: str) -> tuple[str, str]:
    record = _git_bytes(repo_dir, ["ls-tree", "-z", ref, "--", rel]).split(b"\0", 1)[0]
    fields = record.partition(b"\t")[0].split()
    if len(fields) < 3:
        return "", ""
    return fields[0].decode("ascii"), fields[2].decode("ascii")


def _object_size(repo_dir: pathlib.Path, blob_oid: str) -> str:
    value = _git_bytes(repo_dir, ["cat-file", "-s", blob_oid]).strip()
    return value.decode("ascii") if value.isdigit() else ""


def staged_path_is_binary(repo_dir: pathlib.Path, rel: str) -> bool:
    """Detect staged binary content from Git numstat, independent of filename."""
    expected_path = os.fsencode(rel)
    raw = _git_bytes(
        repo_dir, ["diff", "--cached", "--numstat", "-z", "--", rel]
    )
    for record in raw.split(b"\0"):
        fields = record.split(b"\t", 2)
        if len(fields) == 3 and fields[2] == expected_path:
            return fields[0] == b"-" and fields[1] == b"-"
    return False


def render_staged_binary_metadata(repo_dir: pathlib.Path, rel: str) -> str | None:
    """Render stage-0 identity, or fail closed when the index object is unbound."""
    raw = _git_bytes(repo_dir, ["ls-files", "--stage", "-z", "--", rel])
    expected_path = os.fsencode(rel)
    for record in raw.split(b"\0"):
        prefix, separator, path = record.partition(b"\t")
        fields = prefix.split()
        if separator and path == expected_path and len(fields) == 3 and fields[2] == b"0":
            mode = fields[0].decode("ascii")
            blob_oid = fields[1].decode("ascii")
            object_size = _object_size(repo_dir, blob_oid)
            if not object_size:
                return None
            _head_mode, head_blob = _tree_entry(repo_dir, "HEAD", rel)
            _merge_mode, merge_blob = _tree_entry(repo_dir, "MERGE_HEAD", rel)
            return (
                "*(binary bytes are represented by exact Git metadata for this review; "
                "they are not rendered as text)*\n\n"
                f"- staged blob: `{blob_oid}`\n"
                f"- staged mode: `{mode}`\n"
                f"- staged object size: `{object_size}` bytes\n"
                f"- pre-merge HEAD blob: `{head_blob or 'absent'}`\n"
                f"- official MERGE_HEAD blob: `{merge_blob or 'absent'}`\n"
            )
    deleted = _git_bytes(
        repo_dir, ["diff", "--cached", "--name-only", "--diff-filter=D", "-z", "--", rel]
    ).split(b"\0")
    if expected_path not in deleted:
        return None
    head_mode, head_blob = _tree_entry(repo_dir, "HEAD", rel)
    merge_mode, merge_blob = _tree_entry(repo_dir, "MERGE_HEAD", rel)
    parent_blob = head_blob or merge_blob
    object_size = _object_size(repo_dir, parent_blob) if parent_blob else ""
    if not parent_blob or not object_size:
        return None
    return (
        "*(binary deletion is represented by exact Git metadata for this review; "
        "the deleted bytes are not rendered as text)*\n\n"
        "- staged blob: `absent (deletion)`\n"
        "- staged mode: `absent`\n"
        f"- deleted object size: `{object_size}` bytes\n"
        f"- pre-merge HEAD: `{head_mode or 'absent'} {head_blob or 'absent'}`\n"
        f"- official MERGE_HEAD: `{merge_mode or 'absent'} {merge_blob or 'absent'}`\n"
    )
