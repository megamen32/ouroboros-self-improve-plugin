# Full extraction manifest

Source revision: `77652326490010b47e338196facafb1c35cba3d0`

This directory preserves original Ouroboros module paths. The core self-improvement chain is:

1. `ouroboros/reflection.py` — post-task learning, memory actions, backlog candidates.
2. `ouroboros/improvement_backlog.py` — durable improvement candidate store/dedup/grooming.
3. `ouroboros/post_task_evolution.py` — promotion decision + durable request + gated application.
4. `supervisor/evolution_lifecycle.py` and supervisor event/state machinery — campaign execution, owner/budget gates, restart/absorption lifecycle.
5. `ouroboros/deep_self_review.py` — deeper self-review context/analysis path.
6. `ouroboros/evolution_checkpoints.py` + `evolution_fingerprint.py` — outcome memory and objective identity.
7. Review/safety substrate under `ouroboros/review*.py`, `ouroboros/skill_review*.py`, and `ouroboros/tools/*review*.py`.

The whole `ouroboros/` and `supervisor/` packages are included because these paths are tightly coupled to
runtime authorization, state, budgeting, model/provider selection, review evidence, tool constraints, and
restart verification. Keeping those dependencies is the point of the `full` variant.
