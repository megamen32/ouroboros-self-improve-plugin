from ouroboros_self_improve import TaskResult
from ouroboros_self_improve.promotion import PromotionPolicy

def test_promotion_loop_guards(tmp_path):
    p=PromotionPolicy(tmp_path,enabled=True)
    assert not p.eligible(TaskResult("1","x",task_type="evolution"))
    assert not p.eligible(TaskResult("2","x",task_type="deep_self_review"))
    assert not p.eligible(TaskResult("3","x",delegation_role="subagent"))
    assert not p.eligible(TaskResult("4","x",project_id="private-project"))
    assert not p.eligible(TaskResult("5","x",canonical_run=False))
    assert p.eligible(TaskResult("6","x"))

def test_every_n_cadence(tmp_path):
    p=PromotionPolicy(tmp_path,enabled=True,cadence="every_n:2")
    assert p.due() is False
    assert p.due() is True
