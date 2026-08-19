# Integration guide

## Choosing a variant

Choose `lite/` when the target agent already has its own scheduler/supervisor and you only need Ouroboros' self-improvement decision loop. The host supplies model calls and decides what to do with a durable promotion request.

Choose `full/` when the target must inherit the whole reviewed self-modification lifecycle, including actual evolution execution and the gates around it.

## Lite lifecycle

1. Observe a completed task.
2. Reflect only when the task is error-bearing or sufficiently non-trivial.
3. Extract durable memory actions and evidence-backed improvement candidates.
4. Deduplicate/persist the improvement backlog.
5. Let an LLM choose whether one concrete improvement should be promoted.
6. Write a durable promotion request.
7. Let the host's separately authorized executor decide whether/how to mutate code.
8. Record evolution outcomes as checkpoints so future promotion decisions can learn from what actually landed.

See `../../../lite/README.md` and `../../../lite/src/ouroboros_self_improve/`.

## Full lifecycle

The full variant keeps the corresponding Ouroboros runtime path:

- `ouroboros/reflection.py`
- `ouroboros/improvement_backlog.py`
- `ouroboros/post_task_evolution.py`
- `ouroboros/deep_self_review.py`
- `ouroboros/evolution_checkpoints.py`
- `supervisor/evolution_lifecycle.py`
- supervisor state/events/restart verification
- plan/general/triad/skill review and safety substrate
- provider/model and budget accounting used by those paths

See `../../../full/EXTRACTION.md` for the extraction map.

## Porting rule

If you replace a full-mode host dependency, replace its contract rather than deleting the gate. In particular, keep explicit equivalents for owner authorization, budget availability, reviewed-plan boundaries, mutation safety, restart verification, and terminal absorbed/abandoned/no-op outcomes.
