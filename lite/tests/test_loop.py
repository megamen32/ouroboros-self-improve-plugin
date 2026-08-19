from pathlib import Path
from ouroboros_self_improve import SelfImproveLoop, SelfImproveConfig, TaskResult

REFLECTION = '''Concrete reflection body.\nMEMORY_ACTIONS_JSON: [{"type":"scratchpad_append","content":"Prefer structural search first."}]\nBACKLOG_CANDIDATES_JSON: [{"summary":"Add structural query fallback","category":"tools","source":"execution_reflection","evidence":"grep required 8 retries","priority":"high"}]'''
DECISION = '{"promote":true,"objective":"Add structural query fallback before grep retries","requires_plan_review":true,"backlog_id":""}'

def test_nontrivial_task_reflects_backlogs_and_promotes(tmp_path: Path):
    loop=SelfImproveLoop(str(tmp_path),reflect=lambda p: REFLECTION,choose=lambda p: DECISION,config=SelfImproveConfig(True,"llm"))
    result=loop.after_task(TaskResult(task_id="t1",goal="do hard thing",rounds=20))
    assert result["reflection"].reflection == "Concrete reflection body."
    assert len(loop.backlog.load()) == 1
    assert result["promotion_request"].requires_plan_review is True
    assert (tmp_path/"state"/"post_task_evolution_request.json").exists()

def test_trivial_clean_task_skips_reflection(tmp_path: Path):
    loop=SelfImproveLoop(str(tmp_path),reflect=lambda p: (_ for _ in ()).throw(AssertionError()),choose=lambda p:'{"promote":false}',config=SelfImproveConfig(True,"llm"))
    result=loop.after_task(TaskResult(task_id="t2",goal="easy",rounds=1))
    assert result["reflection"] is None
