from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

@dataclass
class TaskResult:
    task_id: str
    goal: str
    task_type: str = ""
    rounds: int = 0
    cost_usd: Optional[float] = None
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    trace_summary: str = ""
    review_evidence: Dict[str, Any] = field(default_factory=dict)
    project_id: str = ""
    delegation_role: str = ""
    canonical_run: bool = True

@dataclass
class ReflectionEntry:
    task_id: str
    goal: str
    reflection: str
    rounds: int = 0
    cost_usd: Optional[float] = None
    error_count: int = 0
    key_markers: List[str] = field(default_factory=list)
    backlog_candidates: List[Dict[str, Any]] = field(default_factory=list)
    memory_actions: List[Dict[str, Any]] = field(default_factory=list)

    def asdict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class PromotionDecision:
    promote: bool
    objective: str = ""
    requires_plan_review: bool = True
    backlog_id: str = ""

@dataclass
class PromotionRequest:
    objective: str
    origin_task_id: str
    requires_plan_review: bool = True
    backlog_id: str = ""
    source: str = "post_task"
