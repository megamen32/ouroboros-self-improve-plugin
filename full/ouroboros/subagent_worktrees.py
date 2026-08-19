"""Lifecycle for acting-subagent ``self_worktree`` checkouts.

Acting (mutative) subagents that modify the Ouroboros body itself run inside an
isolated ``git worktree`` checked out from the parent's base commit, under a root
that lives OUTSIDE ``repo/`` and ``data/``. The child writes only there and
returns a ``workspace.patch``; the parent integrates and is the sole committer.

git has no automatic worktree garbage collection, so we keep a durable JSON
registry (``data/state/subagent_worktrees.json``) and prune orphans on startup.
All worktree mutations are serialized by a portable cross-process lock because
``git worktree add/remove/prune`` mutate shared ``.git/worktrees`` metadata and
the existing repo git lock is drive-root scoped, not ``.git`` scoped.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import stat
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ouroboros.platform_layer import acquire_exclusive_file_lock, release_exclusive_file_lock
from ouroboros.utils import atomic_write_json
from ouroboros.config import DATA_DIR, get_subagent_projects_root, get_subagent_worktree_root
from ouroboros.retention import age_cutoff, get_gc_retention_days

_REGISTRY_NAME = "subagent_worktrees.json"
_LOCK_NAME = ".worktree_ops.lock"
_LOCK_TIMEOUT_SEC = 120.0
_LOCK_STALE_SEC = 600.0
_BRANCH_PREFIX = "subagent/"
# Delegated-run execution snapshots (C1): registry `kind` and the protected
# baseline ref namespace. The ref pins the baseline commit against GC for as
# long as the snapshot lives; it is deleted with the snapshot.
_KIND_DELEGATED_EXEC = "delegated_exec"
_BASELINE_REF_PREFIX = "refs/ouroboros/delegated/"

# Serializes worktree mutations within this process; the on-disk lock serializes
# across processes (parent worker, supervisor startup prune, etc.).
_inproc_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# Paths and registry
# --------------------------------------------------------------------------- #
def _data_dir(data_dir: Optional[Any] = None) -> Path:
    if data_dir:
        return Path(data_dir)
    env = os.environ.get("OUROBOROS_DATA_DIR")
    if env:
        return Path(env)
    return Path(DATA_DIR)


def _registry_path(data_dir: Optional[Any] = None) -> Path:
    return _data_dir(data_dir) / "state" / _REGISTRY_NAME


def _resolve_root(worktree_root: Optional[Any] = None) -> Path:
    root = Path(worktree_root) if worktree_root else Path(get_subagent_worktree_root())
    return root.expanduser().resolve()


def _is_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except (ValueError, OSError):
        return False


def _assert_root_isolated(root: Path, repo_dir: Path, data_dir: Path) -> None:
    """Refuse a worktree root that overlaps the live repo or runtime data."""
    if _is_within(root, repo_dir) or _is_within(repo_dir, root):
        raise ValueError(f"subagent worktree root {root} overlaps the Ouroboros repo {repo_dir}")
    if _is_within(root, data_dir) or _is_within(data_dir, root):
        raise ValueError(f"subagent worktree root {root} overlaps runtime data {data_dir}")


def _safe_name(task_id: Any) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(task_id or "").strip())
    safe = safe or f"wt_{int(time.time())}"
    # Bound the path component so an arbitrary-length input (e.g. a project display name,
    # which is not length-validated upstream) never hits ENAMETOOLONG on mkdir. On
    # truncation keep a short hash of the full slug so two long names with the same prefix
    # do not silently collide.
    if len(safe) > 64:
        import hashlib
        digest = hashlib.sha256(safe.encode("utf-8")).hexdigest()[:8]
        safe = f"{safe[:55]}_{digest}"
    return safe


def _load_registry(data_dir: Optional[Any] = None) -> List[Dict[str, Any]]:
    path = _registry_path(data_dir)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return []
    entries = raw.get("worktrees") if isinstance(raw, dict) else raw
    if isinstance(entries, list):
        return [e for e in entries if isinstance(e, dict)]
    return []


def _save_registry(entries: List[Dict[str, Any]], data_dir: Optional[Any] = None) -> None:
    path = _registry_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, {"worktrees": entries}, trailing_newline=True)


# --------------------------------------------------------------------------- #
# Locking
# --------------------------------------------------------------------------- #
@contextlib.contextmanager
def _ops_lock(root: Path):
    """Serialize worktree mutations in-process (threading.Lock) and across
    processes via the shared portable file-lock SSOT (platform_layer)."""
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / _LOCK_NAME
    with _inproc_lock:
        fd = acquire_exclusive_file_lock(
            lock_path,
            timeout_sec=_LOCK_TIMEOUT_SEC,
            stale_sec=_LOCK_STALE_SEC,
            metadata=str(os.getpid()),
        )
        if fd is None:
            raise TimeoutError(f"subagent worktree ops lock timeout: {lock_path}")
        try:
            yield
        finally:
            release_exclusive_file_lock(lock_path, fd)


# --------------------------------------------------------------------------- #
# git helpers
# --------------------------------------------------------------------------- #
def _force_rmtree(path: Path) -> None:
    """Best-effort recursive delete that also removes read-only files.

    On Windows git pack/object files under ``.git`` are read-only, and
    ``shutil.rmtree(ignore_errors=True)`` silently FAILS to delete them, leaving
    the directory behind. The onerror hook clears the read-only bit and retries
    so genesis-project / worktree teardown actually removes the tree."""
    def _on_error(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass

    try:
        shutil.rmtree(path, onerror=_on_error)
    except Exception:
        pass


def _git(repo_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo_dir),
        capture_output=True,
        text=True,
        check=check,
    )


def _remove_paths(repo_dir: Path, wt_path: Path, branch: str, *, allowed_root: Optional[Any] = None) -> None:
    """Best-effort teardown: drop the worktree checkout, dir, and branch.

    When ``allowed_root`` is given, refuse to touch any path that is empty or not
    strictly inside it. The registry is durable runtime state; a corrupt/malformed
    entry must never cause deletion of an arbitrary filesystem path.
    """
    wt_path = Path(wt_path)
    wt_text = str(wt_path).strip()
    if allowed_root is not None and (
        not wt_text or wt_text in (".", "/", "//") or not _is_within(wt_path, Path(allowed_root))
    ):
        return
    try:
        _git(repo_dir, "worktree", "remove", "--force", str(wt_path), check=False)
    except Exception:
        pass
    if wt_path.exists():
        _force_rmtree(wt_path)
    try:
        _git(repo_dir, "worktree", "prune", check=False)
    except Exception:
        pass
    if branch:
        try:
            _git(repo_dir, "branch", "-D", branch, check=False)
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WorktreeHandle:
    task_id: str
    path: str
    branch: str
    base_sha: str
    repo_dir: str
    created_at: float
    parent_task_id: str = ""


def provision_worktree(
    *,
    repo_dir: Any,
    task_id: Any,
    base_sha: str = "",
    parent_task_id: str = "",
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
) -> WorktreeHandle:
    """Create an isolated worktree branched from ``base_sha`` (default HEAD).

    The returned branch is a delta base for the child; the child's patch is a
    diff against ``base_sha`` so the parent can integrate it deliberately.
    """
    repo_dir = Path(repo_dir).resolve()
    root = _resolve_root(worktree_root)
    _assert_root_isolated(root, repo_dir, _data_dir(data_dir))
    safe_task = _safe_name(task_id)
    with _ops_lock(root):
        if base_sha:
            _git(repo_dir, "rev-parse", "--verify", f"{base_sha}^{{commit}}")
            base_sha = _git(repo_dir, "rev-parse", base_sha).stdout.strip()
        else:
            base_sha = _git(repo_dir, "rev-parse", "HEAD").stdout.strip()
        wt_path = (root / safe_task).resolve()
        branch = f"{_BRANCH_PREFIX}{safe_task}"
        # Clear any stale checkout/branch left by a crashed run.
        _remove_paths(repo_dir, wt_path, branch, allowed_root=root)
        wt_path.parent.mkdir(parents=True, exist_ok=True)
        _git(repo_dir, "worktree", "add", "--force", "-b", branch, str(wt_path), base_sha)
        handle = WorktreeHandle(
            task_id=str(task_id),
            path=str(wt_path),
            branch=branch,
            base_sha=base_sha,
            repo_dir=str(repo_dir),
            created_at=time.time(),
            parent_task_id=str(parent_task_id or ""),
        )
        entries = [e for e in _load_registry(data_dir) if e.get("path") != str(wt_path)]
        entries.append(asdict(handle))
        _save_registry(entries, data_dir)
        return handle


def provision_genesis_project(
    *,
    repo_dir: Any,
    task_id: Any,
    parent_task_id: str = "",
    projects_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
    dir_name: str = "",
) -> WorktreeHandle:
    """Provision a durable, isolated, EMPTY git project for a genesis acting child.

    Unlike a worktree this is a standalone repo (not a checkout of the live body)
    under the durable projects root. It is the deliverable itself and is NEVER
    GC-pruned, so it is intentionally not added to the worktree registry. The
    child builds the whole project here and returns a ``workspace.patch`` that is
    a diff from the empty initial commit (``base_sha``).

    ``dir_name`` names the genesis directory meaningfully (e.g. the project name)
    instead of the raw task id, so sibling builders share a recognizable project
    root; the handle's binding identity stays ``task_id`` (I, v6.39).
    """
    repo_dir = Path(repo_dir).resolve()
    root = Path(projects_root) if projects_root else Path(get_subagent_projects_root())
    root = root.expanduser().resolve()
    _assert_root_isolated(root, repo_dir, _data_dir(data_dir))
    safe_task = _safe_name(dir_name or task_id)
    with _ops_lock(root):
        proj = (root / safe_task).resolve()
        # Genesis projects are durable: never clobber an existing one -> unique name. Since
        # dir_name can repeat across projects (a shared display name), count up under the
        # ops lock until a free path is found — a single timestamp suffix could still
        # collide on a same-name re-provision within the same second (FileExistsError).
        _suffix = 0
        while proj.exists():
            _suffix += 1
            proj = (root / f"{safe_task}_{_suffix}").resolve()
        proj.mkdir(parents=True, exist_ok=False)
        try:
            _git(proj, "init")
            # A fresh repo may have no commit identity; set a local one for the seed
            # commit only (does not touch the user's global git config).
            _git(
                proj,
                "-c", "user.email=ouroboros@localhost",
                "-c", "user.name=Ouroboros",
                "commit", "--allow-empty", "-m", "genesis: empty project",
            )
            base_sha = _git(proj, "rev-parse", "HEAD").stdout.strip()
        except Exception:
            # Do not leak a partial/uninitialized project dir on git failure.
            _force_rmtree(proj)
            raise
        return WorktreeHandle(
            task_id=str(task_id),
            path=str(proj),
            branch="",
            base_sha=base_sha,
            repo_dir=str(proj),
            created_at=time.time(),
            parent_task_id=str(parent_task_id or ""),
        )


def remove_genesis_project(path: str, *, projects_root: Optional[Any] = None) -> bool:
    """Best-effort removal of a provisioned-but-unused genesis project.

    Only removes a path strictly INSIDE the configured projects root (never an
    arbitrary caller path). Used to clean up a genesis project whose schedule was
    rejected before the child ran; genesis projects are otherwise durable.
    """
    if not str(path or "").strip():
        return False
    root = Path(projects_root) if projects_root else Path(get_subagent_projects_root())
    root = root.expanduser().resolve()
    target = Path(path).resolve()
    if target == root or not _is_within(target, root):
        return False
    if target.exists():
        _force_rmtree(target)
    return True


@dataclass(frozen=True)
class ExecutionSnapshotHandle:
    """A private, disposable execution root for ONE mutating delegated run.

    The snapshot is a detached ``git worktree`` of the AUTHORITY TARGET tree,
    checked out at a synthetic BASELINE commit that captures the target's real
    current state: tracked + staged changes plus eligible untracked files
    (sensitive/credential-shaped, junk and oversized-binary untracked files are
    vetoed by the same predicate the workspace-patch capture uses). The run
    writes ONLY here; its diff against ``baseline_sha`` is its whole
    contribution, and the nanny applies or rejects that diff into the target
    tree explicitly — never automatically.
    """

    snapshot_id: str
    task_id: str
    path: str
    target_root: str
    baseline_ref: str
    baseline_sha: str
    baseline_tree: str
    manifest_digest: str
    target_head: str
    created_at: float
    entry_count: int = 0
    excluded_untracked: tuple = ()


def _git_env_index(index_path: Path) -> Dict[str, str]:
    env = dict(os.environ)
    env["GIT_INDEX_FILE"] = str(index_path)
    return env


def _git_env(repo_dir: Path, *args: str, env: Dict[str, str],
             check: bool = True, input_bytes: bytes = b"") -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo_dir),
        capture_output=True,
        check=check,
        env=env,
        input=input_bytes if input_bytes else None,
    )


def provision_execution_snapshot(
    *,
    target_root: Any,
    task_id: Any,
    snapshot_id: str,
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
) -> ExecutionSnapshotHandle:
    """Snapshot ``target_root``'s REAL current tree into a private execution root.

    Baseline construction never touches the target's own index, HEAD or working
    files: a TEMPORARY index is seeded from HEAD, ``git add -A`` stages the
    real tree (tracked + staged + untracked, ``.gitignore`` respected), the
    vetoed untracked files are removed from that index, and the resulting tree
    is committed as a synthetic baseline pinned by a ref under
    ``refs/ouroboros/delegated/``. The execution root is a detached worktree at
    that commit, registered durably BEFORE the caller records any start intent,
    and removed only by an explicit disposition (``remove_execution_snapshot``)
    or by the startup GC after custody says the run is closed.
    """
    from ouroboros.headless import untracked_capture_veto_reason

    target = Path(target_root).resolve()
    if not (target / ".git").exists():
        raise ValueError(f"delegated execution snapshot target {target} is not a git working tree")
    root = _resolve_root(worktree_root)
    data_root = _data_dir(data_dir)
    if _is_within(root, target) or _is_within(target, root):
        raise ValueError(f"subagent worktree root {root} overlaps the snapshot target {target}")
    if _is_within(root, data_root) or _is_within(data_root, root):
        raise ValueError(f"subagent worktree root {root} overlaps runtime data {data_root}")
    snap = str(snapshot_id or "").strip()
    if not snap:
        raise ValueError("snapshot_id is required for a delegated execution snapshot")
    safe_snap = _safe_name(snap)
    safe_task = _safe_name(task_id)
    with _ops_lock(root):
        wt_path = (root / f"dlg_{safe_task}_{safe_snap[:16]}").resolve()
        baseline_ref = f"{_BASELINE_REF_PREFIX}{safe_snap}"
        # Clear any stale checkout/ref left by a crashed earlier attempt of the
        # SAME snapshot id (idempotent re-provision).
        _remove_paths(target, wt_path, "", allowed_root=root)
        _git(target, "update-ref", "-d", baseline_ref, check=False)
        head_proc = _git(target, "rev-parse", "--verify", "HEAD", check=False)
        target_head = head_proc.stdout.strip() if head_proc.returncode == 0 else ""
        index_path = root / f".baseline_index_{safe_snap[:16]}_{os.getpid()}"
        env = _git_env_index(index_path)
        try:
            if target_head:
                _git_env(target, "read-tree", target_head, env=env)
            else:
                _git_env(target, "read-tree", "--empty", env=env)
            # ELIGIBILITY IS DECIDED BEFORE ANYTHING IS HASHED. `git add -A` writes a
            # blob for EVERY untracked file into the target's object database —
            # including `.env` / `credentials.json` — and removing the index entry
            # afterwards does not unwrite the object: the execution worktree shares
            # that ODB, so a vetoed secret stayed readable there by hash. The staged
            # set is therefore computed first (tracked/staged paths from the REAL
            # index, plus only the ELIGIBLE untracked ones) and fed to plumbing that
            # touches nothing else. NUL-delimited stdin: byte-safe and immune to argv
            # limits.
            real_env = dict(os.environ)
            indexed = [
                p for p in _git_env(target, "ls-files", "-z", env=real_env)
                .stdout.decode("utf-8", errors="surrogateescape").split("\0") if p
            ]
            untracked_raw = _git_env(
                target, "ls-files", "-z", "--others", "--exclude-standard",
                env=real_env,
            ).stdout.decode("utf-8", errors="surrogateescape")
            excluded: List[Dict[str, str]] = []
            eligible: List[str] = []
            for rel in (p for p in untracked_raw.split("\0") if p):
                reason = untracked_capture_veto_reason(target, rel)
                if reason:
                    excluded.append({"path": rel, "reason": reason})
                else:
                    eligible.append(rel)
            staged_paths = indexed + eligible
            if staged_paths:
                # `--add --remove` stages each named path's CURRENT worktree content
                # (and drops the entry when the file is gone), which is exactly what
                # `git add -A` did for these paths — without visiting any other file.
                _git_env(
                    target, "update-index", "-z", "--add", "--remove", "--stdin", env=env,
                    input_bytes=b"\0".join(
                        p.encode("utf-8", errors="surrogateescape") for p in staged_paths) + b"\0")
            tree_sha = _git_env(target, "write-tree", env=env).stdout.decode("utf-8").strip()
            manifest_raw = _git_env(target, "ls-tree", "-r", "-z", tree_sha,
                                    env=dict(os.environ)).stdout
            import hashlib
            manifest_digest = hashlib.sha256(manifest_raw).hexdigest()
            entry_count = sum(1 for chunk in manifest_raw.split(b"\0") if chunk)
            commit_args = ["commit-tree", tree_sha, "-m",
                           f"ouroboros: delegated-run baseline {snap}"]
            if target_head:
                commit_args[2:2] = ["-p", target_head]
            baseline_sha = _git_env(
                target, *commit_args,
                env={**env,
                     "GIT_AUTHOR_NAME": "Ouroboros", "GIT_AUTHOR_EMAIL": "ouroboros@localhost",
                     "GIT_COMMITTER_NAME": "Ouroboros", "GIT_COMMITTER_EMAIL": "ouroboros@localhost"},
            ).stdout.decode("utf-8").strip()
        finally:
            try:
                index_path.unlink()
            except OSError:
                pass
        _git(target, "update-ref", baseline_ref, baseline_sha)
        try:
            wt_path.parent.mkdir(parents=True, exist_ok=True)
            _git(target, "worktree", "add", "--detach", str(wt_path), baseline_sha)
        except Exception:
            # Do not leak the pinned baseline ref when the checkout failed.
            _git(target, "update-ref", "-d", baseline_ref, check=False)
            raise
        handle = ExecutionSnapshotHandle(
            snapshot_id=snap,
            task_id=str(task_id or ""),
            path=str(wt_path),
            target_root=str(target),
            baseline_ref=baseline_ref,
            baseline_sha=baseline_sha,
            baseline_tree=tree_sha,
            manifest_digest=manifest_digest,
            target_head=target_head,
            created_at=time.time(),
            entry_count=entry_count,
            excluded_untracked=tuple(excluded),
        )
        entries = [e for e in _load_registry(data_dir) if e.get("path") != str(wt_path)]
        record = asdict(handle)
        record["kind"] = _KIND_DELEGATED_EXEC
        record["excluded_untracked"] = excluded
        entries.append(record)
        _save_registry(entries, data_dir)
        return handle


def find_execution_snapshot(snapshot_id: str, data_dir: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """The registry record for a delegated execution snapshot, or None."""
    snap = str(snapshot_id or "").strip()
    if not snap:
        return None
    for entry in _load_registry(data_dir):
        if entry.get("kind") == _KIND_DELEGATED_EXEC and entry.get("snapshot_id") == snap:
            return entry
    return None


def remove_execution_snapshot(
    snapshot_id: str,
    *,
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
) -> bool:
    """Tear down one delegated execution snapshot: worktree, baseline ref, registry row.

    Call ONLY after the run's disposition (patch applied, rejected, or the
    startup GC proved the run closed): the snapshot plus its captured patch are
    the conflict-resolution material and must survive until then.
    """
    entry = find_execution_snapshot(snapshot_id, data_dir)
    if entry is None:
        return False
    root = _resolve_root(worktree_root)
    with _ops_lock(root):
        target = Path(str(entry.get("target_root") or "."))
        _remove_paths(target, Path(str(entry.get("path") or "")), "", allowed_root=root)
        ref = str(entry.get("baseline_ref") or "")
        if ref.startswith(_BASELINE_REF_PREFIX):
            try:
                _git(target, "update-ref", "-d", ref, check=False)
            except Exception:
                pass
        survivors = [e for e in _load_registry(data_dir) if not (
            e.get("kind") == _KIND_DELEGATED_EXEC and e.get("snapshot_id") == entry.get("snapshot_id")
        )]
        _save_registry(survivors, data_dir)
    return True


def prune_execution_snapshots(
    open_snapshot_ids: set,
    *,
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
) -> Dict[str, Any]:
    """Startup GC for delegated execution snapshots, cross-checked with custody.

    ``open_snapshot_ids`` is the set of snapshot ids that custody still holds
    OPEN (an undisposed run or a pending invocation). Those are KEPT regardless
    of age — the snapshot and its patch persist until explicit disposition. A
    snapshot custody does not hold open is removed; a registry row whose
    checkout directory is already gone is unregistered (with its baseline ref
    deleted) either way.
    """
    open_ids = {str(s) for s in (open_snapshot_ids or set())}
    removed: List[str] = []
    kept: List[str] = []
    for entry in list(_load_registry(data_dir)):
        if entry.get("kind") != _KIND_DELEGATED_EXEC:
            continue
        snap = str(entry.get("snapshot_id") or "")
        path_exists = bool(entry.get("path")) and Path(str(entry.get("path"))).exists()
        if snap in open_ids and path_exists:
            kept.append(snap)
            continue
        if remove_execution_snapshot(snap, worktree_root=worktree_root, data_dir=data_dir):
            removed.append(snap)
    return {"removed": removed, "kept": kept}


def remove_worktree(
    *,
    task_id: str = "",
    path: str = "",
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
) -> bool:
    """Tear down a worktree by task_id or path; unregister it. Returns success."""
    want_path = str(Path(path).resolve()) if path else ""
    entries = _load_registry(data_dir)
    match: Optional[Dict[str, Any]] = None
    for entry in entries:
        if task_id and entry.get("task_id") == str(task_id):
            match = entry
            break
        if want_path and entry.get("path") == want_path:
            match = entry
            break
    root = _resolve_root(worktree_root)
    with _ops_lock(root):
        if match is not None:
            _remove_paths(Path(match.get("repo_dir") or "."), Path(match.get("path") or ""), match.get("branch") or "", allowed_root=root)
            survivors = [e for e in _load_registry(data_dir) if e.get("path") != match.get("path")]
            _save_registry(survivors, data_dir)
            return True
        # Unregistered path: best-effort directory removal, but ONLY inside the
        # configured worktree root (never an arbitrary path supplied by a caller).
        if want_path and Path(want_path).exists() and _is_within(Path(want_path), root):
            _force_rmtree(Path(want_path))
            return True
    return False


def prune_orphans(
    *,
    worktree_root: Optional[Any] = None,
    data_dir: Optional[Any] = None,
    retention_days: Optional[int] = None,
) -> Dict[str, Any]:
    """Startup reconciliation: drop worktrees past retention or with a missing
    checkout, then reconcile git's own worktree metadata. Patch artifacts live in
    the task drive, independent of the worktree, so removal never loses results.
    """
    retention = retention_days if retention_days is not None else get_gc_retention_days()
    cutoff = age_cutoff(retention)
    root = _resolve_root(worktree_root)
    removed: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    repos: set[str] = set()
    with _ops_lock(root):
        for entry in _load_registry(data_dir):
            if entry.get("kind") == _KIND_DELEGATED_EXEC:
                # Delegated execution snapshots have their OWN lifecycle: they persist
                # until the run's explicit patch disposition, and the startup GC
                # (`prune_execution_snapshots`) cross-checks custody for open runs and
                # pending invocations. Age/path heuristics here must not eat them —
                # and their baseline ref needs deleting, which this loop cannot do.
                kept.append(entry)
                continue
            repo_dir = str(entry.get("repo_dir") or "")
            wt_path = str(entry.get("path") or "")
            created = float(entry.get("created_at") or 0)
            if repo_dir:
                repos.add(repo_dir)
            path_exists = Path(wt_path).exists() if wt_path else False
            if created < cutoff or not path_exists:
                if repo_dir or wt_path:
                    _remove_paths(Path(repo_dir or "."), Path(wt_path), entry.get("branch") or "", allowed_root=root)
                removed.append(entry)
            else:
                kept.append(entry)
        _save_registry(kept, data_dir)
        for repo in repos:
            try:
                _git(Path(repo), "worktree", "prune", check=False)
            except Exception:
                pass
    return {"removed": len(removed), "kept": len(kept)}


def list_worktrees(data_dir: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Return registered worktree records (for UI / inspection)."""
    return _load_registry(data_dir)
