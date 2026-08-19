"""Hardened staged-diff capture (``capture_staged_diff``) — the one shared
evidence source for the scope reviewer and the triad."""

import subprocess

import pytest

from ouroboros.tools.review_binary_context import (
    StagedDiffUnavailable,
    capture_staged_diff,
)


def _repo(tmp_path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=tmp_path, check=True)
    (tmp_path / "f.py").write_text("a\nb\nc\nd\ne\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=tmp_path, check=True,
                   capture_output=True)
    return tmp_path


def test_capture_returns_the_staged_diff(tmp_path):
    repo = _repo(tmp_path)
    (repo / "f.py").write_text("a\nb\nCHANGED\nd\ne\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)

    diff = capture_staged_diff(repo)

    assert "+CHANGED" in diff
    assert "a/f.py" in diff and "b/f.py" in diff  # pinned prefixes


def test_capture_unified_zero_drops_unchanged_context(tmp_path):
    repo = _repo(tmp_path)
    (repo / "f.py").write_text("a\nb\nCHANGED\nd\ne\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)

    compact = capture_staged_diff(repo, unified=0)

    assert "+CHANGED" in compact
    assert " a\n" not in compact  # zero context: no unchanged surrounding lines


def test_non_utf8_staged_text_is_escaped_not_flattened(tmp_path):
    """Git treats NUL-free non-UTF-8 content as TEXT, so the bytes ride ordinary
    diff lines. They must reach the reviewer readably escaped — never U+FFFD,
    never an exception, never a placeholder."""
    repo = _repo(tmp_path)
    (repo / "f.py").write_bytes(b"a\nb\ncaf\xe9\nd\ne\n")  # latin-1 0xE9
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)

    diff = capture_staged_diff(repo)

    assert "\\xe9" in diff
    assert "�" not in diff
    assert "staged diff contained non-UTF-8 bytes" in diff


def test_capture_failure_raises_typed_runtime_error(tmp_path):
    (tmp_path / "not_a_repo").mkdir()

    with pytest.raises(StagedDiffUnavailable):
        capture_staged_diff(tmp_path / "not_a_repo")

    # The type is a RuntimeError so existing fail-closed paths catch it.
    assert issubclass(StagedDiffUnavailable, RuntimeError)


def test_git_diff_opts_env_cannot_reshape_the_capture(tmp_path, monkeypatch):
    """GIT_DIFF_OPTS overrides the context width from outside the argv; the
    capture drops it so the requested width survives."""
    repo = _repo(tmp_path)
    (repo / "f.py").write_text("a\nb\nCHANGED\nd\ne\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    monkeypatch.setenv("GIT_DIFF_OPTS", "--unified=0")

    diff = capture_staged_diff(repo)  # default width, env override dropped

    assert "+CHANGED" in diff
    assert " a\n" in diff  # context lines survived the hostile env override
