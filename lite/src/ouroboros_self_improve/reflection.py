from __future__ import annotations
import json
from collections import Counter
from typing import Any, Dict, List, Tuple
from .models import TaskResult, ReflectionEntry

NONTRIVIAL_ROUNDS_THRESHOLD = 15
NONTRIVIAL_COST_THRESHOLD = 5.0
ERROR_MARKERS = frozenset({
    "REVIEW_BLOCKED", "TESTS_FAILED", "COMMIT_BLOCKED", "REVIEW_MAX_ITERATIONS",
    "TOOL_ERROR", "TOOL_TIMEOUT", "SHELL_EXIT_ERROR", "SHELL_ERROR",
    "CLAUDE_CODE_ERROR", "CLAUDE_CODE_TIMEOUT", "CLAUDE_CODE_INSTALL_ERROR",
    "CLAUDE_CODE_UNAVAILABLE",
})

def _scan_view(text: str) -> str:
    return text if len(text) <= 700 else text[:350] + "\n…\n" + text[-350:]

def _markers(task: TaskResult) -> List[str]:
    found = set()
    for tc in task.tool_calls:
        view = _scan_view(str(tc.get("result", "")))
        found.update(m for m in ERROR_MARKERS if m in view)
    return sorted(found)

def _has_errors(task: TaskResult) -> bool:
    for tc in task.tool_calls:
        status = str(tc.get("status") or "").strip().lower()
        if tc.get("is_error") or status not in ("", "ok"):
            return True
        if any(m in _scan_view(str(tc.get("result", ""))) for m in ERROR_MARKERS):
            return True
    return False

def should_reflect(task: TaskResult) -> bool:
    if task.task_type in {"evolution", "deep_self_review"}:
        return True
    if task.rounds >= NONTRIVIAL_ROUNDS_THRESHOLD:
        return True
    if task.cost_usd is not None and task.cost_usd >= NONTRIVIAL_COST_THRESHOLD:
        return True
    return _has_errors(task)

def _tool_profile(task: TaskResult) -> str:
    counts = Counter(str(tc.get("tool") or "").strip() for tc in task.tool_calls if tc.get("tool"))
    return ", ".join(f"{name}×{count}" for name, count in counts.most_common(15)) or "(no tool calls recorded)"

def _error_details(task: TaskResult, cap: int = 3000) -> str:
    parts: List[str] = []
    total = 0
    for tc in task.tool_calls:
        result = str(tc.get("result", ""))
        status = str(tc.get("status") or "").strip().lower()
        relevant = tc.get("is_error") or status not in ("", "ok") or any(m in _scan_view(result) for m in ERROR_MARKERS)
        if not relevant:
            continue
        snippet = f"[{tc.get('tool', 'unknown')}]: {result}"
        if len(snippet) > 1000:
            snippet = snippet[:940] + f" …[+{len(snippet)-940} chars omitted]"
        if total + len(snippet) > cap:
            break
        parts.append(snippet)
        total += len(snippet)
    return "\n\n".join(parts) or "(no error details captured)"

_ERROR_HEAD = """You are performing a post-task experience review for a self-modifying AI agent. The task had errors or blocking events. Write a concise 150-250 word reflection covering: (1) goal, (2) concrete errors/blocks, (3) root cause, (4) what to do differently next time. Be concrete; no platitudes."""
_CLEAN_HEAD = """You are performing a post-task experience review for a self-modifying AI agent. The task was non-trivial but completed without hard errors. Write a concise 150-250 word reflection covering: (1) goal, (2) biggest source of rounds/cost, (3) weak assumptions/detours/tool choices, (4) what would make a similar task cheaper or faster. Be concrete; no platitudes."""
_TAIL = """

Then append exactly these two machine-readable lines:
MEMORY_ACTIONS_JSON: [...]  # 0-3 durable actions; type is scratchpad_append, knowledge_write, or identity_update_candidate
BACKLOG_CANDIDATES_JSON: [...]  # 0-3 concrete deferred improvements with summary/category/source/evidence; optional context/proposed_next_step/task_id/requires_plan_review/priority/kind

Task goal: {goal}
Execution trace: {trace}
Tool usage: {tools}
Error details: {errors}
Structured review evidence: {review}
"""

def build_reflection_prompt(task: TaskResult) -> str:
    head = _ERROR_HEAD if _has_errors(task) or _markers(task) else _CLEAN_HEAD
    return head + _TAIL.format(
        goal=task.goal[:200], trace=task.trace_summary[:2000], tools=_tool_profile(task),
        errors=_error_details(task), review=json.dumps(task.review_evidence, ensure_ascii=False)[:8000],
    )

def _extract_json_line(text: str, marker: str) -> Tuple[str, List[Dict[str, Any]]]:
    idx = text.rfind(marker)
    if idx < 0:
        return text, []
    tail = text[idx + len(marker):].lstrip()
    try:
        value, end = json.JSONDecoder().raw_decode(tail)
        items = value if isinstance(value, list) else []
        return (text[:idx] + tail[end:]).rstrip(), [x for x in items if isinstance(x, dict)]
    except Exception:
        return text[:idx].rstrip(), []

def parse_reflection_output(task: TaskResult, text: str) -> ReflectionEntry:
    body, backlog = _extract_json_line(text, "BACKLOG_CANDIDATES_JSON:")
    body, memory = _extract_json_line(body, "MEMORY_ACTIONS_JSON:")
    allowed_memory = []
    for item in memory[:3]:
        t = str(item.get("type") or "")
        content = str(item.get("content") or "").strip()
        if t not in {"scratchpad_append", "knowledge_write", "identity_update_candidate"} or not content:
            continue
        if t == "knowledge_write" and not str(item.get("topic") or "").strip():
            continue
        allowed_memory.append(item)
    valid_backlog = []
    for item in backlog[:3]:
        if str(item.get("summary") or "").strip() and str(item.get("evidence") or "").strip():
            x = dict(item)
            x.setdefault("category", "process")
            x.setdefault("source", "execution_reflection")
            x.setdefault("task_id", task.task_id)
            x.setdefault("requires_plan_review", True)
            x.setdefault("priority", "med")
            x.setdefault("kind", "improvement")
            valid_backlog.append(x)
    return ReflectionEntry(
        task_id=task.task_id, goal=task.goal[:200], reflection=body.strip(), rounds=task.rounds,
        cost_usd=task.cost_usd, error_count=sum(1 for tc in task.tool_calls if tc.get("is_error")),
        key_markers=_markers(task), backlog_candidates=valid_backlog, memory_actions=allowed_memory,
    )
