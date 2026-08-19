# ouroboros-self-improve

Runtime-neutral extraction of Ouroboros' **post-task self-improvement loop**.

Source checkout: `/home/roomhacker/excode/ouroboros` on `roomhacker-server-88`  
Source revision: `776523264900`

## Extracted loop

1. **Reflect selectively** after errors/blocks, evolution/deep-review tasks, or expensive/non-trivial tasks.
2. **Emit durable learning** as memory actions plus 0-3 evidence-backed backlog candidates.
3. **Persist/deduplicate backlog** with a stable fingerprint and recurrence count.
4. **Choose promotion with an LLM**, considering the just-finished reflection, current backlog, and prior cycle outcomes.
5. **Publish a durable promotion request** rather than editing code or enabling evolution directly.
6. **Leave execution to the host supervisor**, where owner/budget/review/restart gates belong.

That last boundary is important: Ouroboros' worker-side self-improvement code proposes; its supervisor-side evolution machinery authorizes and executes.

## Intentionally not extracted

General plan/skill review, reviewer-slot infrastructure, provider/model routing, project memory routing, semantic-dedup LLM plumbing, supervisor state, campaign execution, restart verification, git mutation, budget accounting, UI/server integration, and deep-self-review context packing. Those are either host concerns or adjacent review machinery rather than the minimal post-task improvement algorithm.

## Minimal integration

```python
from ouroboros_self_improve import SelfImproveLoop, SelfImproveConfig, TaskResult

loop = SelfImproveLoop(
    "/var/lib/my-agent",
    reflect=lambda prompt: cheap_model(prompt),
    choose=lambda prompt: main_model(prompt),
    config=SelfImproveConfig(
        post_task_evolution_enabled=True,
        post_task_evolution_cadence="llm",
    ),
)

result = loop.after_task(TaskResult(
    task_id="t-42",
    goal="Fix flaky deploy",
    rounds=18,
    tool_calls=trace["tool_calls"],
    trace_summary=trace_summary,
))

# If result["promotion_request"] is present, a separate owner-gated supervisor
# may start ONE reviewed self-improvement/evolution cycle.
```

## Source map

| Extract | Ouroboros source |
|---|---|
| `reflection.py` | `ouroboros/reflection.py` |
| `backlog.py` | `ouroboros/improvement_backlog.py` |
| `promotion.py` | `ouroboros/post_task_evolution.py` |
| `checkpoints.py` | `ouroboros/evolution_checkpoints.py` |
| `loop.py` | orchestration in `ouroboros/agent_task_pipeline.py` |

## Verify

```bash
python -m pytest -q
```
