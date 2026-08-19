from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any
from .models import TaskResult, ReflectionEntry, PromotionRequest
from .reflection import should_reflect, build_reflection_prompt, parse_reflection_output
from .backlog import BacklogStore
from .promotion import PromotionPolicy

@dataclass
class SelfImproveConfig:
    post_task_evolution_enabled: bool = False
    post_task_evolution_cadence: str = "llm"

class SelfImproveLoop:
    """Runtime-neutral Ouroboros extraction.

    The host supplies two model calls: a cheaper reflector and a promotion chooser.
    The loop itself only learns/persists/proposes. It never patches source or enables
    evolution directly; the durable PromotionRequest is intentionally handed back to
    a host-side, owner-gated supervisor.
    """
    def __init__(self, root: str, *, reflect: Callable[[str], str], choose: Callable[[str], str], config: SelfImproveConfig | None = None):
        self.root=root; self.reflect_llm=reflect; self.choose_llm=choose; self.config=config or SelfImproveConfig()
        self.backlog=BacklogStore(root)
        self.promotion=PromotionPolicy(root,cadence=self.config.post_task_evolution_cadence,enabled=self.config.post_task_evolution_enabled)

    def after_task(self, task: TaskResult) -> Dict[str, Any]:
        reflection: Optional[ReflectionEntry]=None
        if should_reflect(task):
            reflection=parse_reflection_output(task,self.reflect_llm(build_reflection_prompt(task)))
            self.backlog.append(reflection.backlog_candidates)
        decision=self.promotion.decide(task,reflection,self.choose_llm)
        request: Optional[PromotionRequest]=None
        if decision:
            request=self.promotion.write_request(task,decision)
        return {"reflection": reflection, "promotion_decision": decision, "promotion_request": request}
