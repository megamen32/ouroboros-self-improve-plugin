from __future__ import annotations
import json, os, pathlib, tempfile
from typing import Callable, Optional
from .models import TaskResult, ReflectionEntry, PromotionDecision, PromotionRequest
from .backlog import BacklogStore
from .checkpoints import CheckpointStore

DECISION_PROMPT = """You decide whether a self-modifying agent should run ONE reviewed self-improvement cycle now.

[JUST-FINISHED TASK REFLECTION]
{reflection}

[CURRENT IMPROVEMENT BACKLOG]
{backlog}

[SOLVE-CAPABILITY HISTORY]
{capability}

Return ONLY JSON: {{"promote": true|false, "objective": "<one concrete self-contained improvement>", "requires_plan_review": true|false, "backlog_id": "<id or empty>"}}
Promote only a concrete, high-value, self-contained code/process improvement. Prefer small targeted objectives and existing backlog items. If nothing is clearly worthwhile, promote=false.
"""

class PromotionPolicy:
    def __init__(self, root: str | pathlib.Path, *, cadence: str = "llm", enabled: bool = False):
        self.root=pathlib.Path(root); self.cadence=cadence; self.enabled=enabled
        self.counter_path=self.root/"state"/"post_task_evolution_counter.json"
        self.request_path=self.root/"state"/"post_task_evolution_request.json"

    def eligible(self, task: TaskResult) -> bool:
        return task.task_type not in {"evolution","deep_self_review"} and task.delegation_role != "subagent" and task.canonical_run and not task.project_id

    def due(self) -> bool:
        if self.cadence == "off": return False
        if not self.cadence.startswith("every_n"): return True
        try: k=max(1,int(self.cadence.split(":",1)[1]))
        except Exception: k=1
        try: n=int(json.loads(self.counter_path.read_text()).get("n") or 0)
        except Exception: n=0
        n+=1; self.counter_path.parent.mkdir(parents=True,exist_ok=True); self.counter_path.write_text(json.dumps({"n":n}))
        return n % k == 0

    def decide(self, task: TaskResult, reflection: Optional[ReflectionEntry], ask: Callable[[str], str]) -> Optional[PromotionDecision]:
        if not self.enabled or not self.eligible(task) or not self.due(): return None
        prompt=DECISION_PROMPT.format(
            reflection=(reflection.reflection if reflection else "(none)"),
            backlog=BacklogStore(self.root).digest() or "(empty)",
            capability=CheckpointStore(self.root).capability_digest() or "(no history)",
        )
        try:
            raw=ask(prompt); a,b=raw.find("{"),raw.rfind("}")
            obj=json.loads(raw[a:b+1])
            d=PromotionDecision(bool(obj.get("promote")),str(obj.get("objective") or "").strip(),bool(obj.get("requires_plan_review",True)),str(obj.get("backlog_id") or "").strip())
            return d if d.promote and d.objective else None
        except Exception: return None

    def write_request(self, task: TaskResult, decision: PromotionDecision) -> PromotionRequest:
        req=PromotionRequest(decision.objective, task.task_id, decision.requires_plan_review, decision.backlog_id)
        self.request_path.parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix=self.request_path.name+".",dir=self.request_path.parent)
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as f: json.dump(req.__dict__,f,ensure_ascii=False,indent=2)
            os.replace(tmp,self.request_path)
        finally:
            try: os.unlink(tmp)
            except FileNotFoundError: pass
        return req
