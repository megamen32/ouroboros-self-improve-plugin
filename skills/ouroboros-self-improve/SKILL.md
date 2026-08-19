---
name: ouroboros-self-improve
description: Add or study Ouroboros-derived self-improvement using a portable lite loop or the full reviewed self-modification runtime.
---

# Ouroboros Self Improve

Use this plugin when the user wants an agent/runtime to learn from completed work, maintain an improvement backlog, select improvements for execution, or adopt Ouroboros-style reviewed self-modification.

The plugin ships two variants at the plugin root:

- `lite/` — runtime-neutral reflection -> durable learning/backlog -> promotion request -> checkpoint loop.
- `full/` — source-faithful Ouroboros runtime including mutation/evolution execution, campaign lifecycle, restart verification, owner/budget gates, provider/model routing, and review/safety machinery.

Read `references/integration.md` before integrating either variant.

## Procedure

1. Identify whether the user wants **portable learning/promotion** or **actual autonomous reviewed self-modification**.
2. Use `lite/` when the host already owns authorization, mutation, restart, budgeting, and review execution.
3. Use `full/` when the goal is to preserve Ouroboros' complete self-improvement behavior. Do not strip owner, budget, review, restart-verification, or rollback/absorption gates just to simplify the port.
4. Preserve source provenance from `SOURCE.md` when copying code into another project.
5. Run the variant's tests after integration. For `full/`, use an environment satisfying `full/pyproject.toml` / `full/requirements*.txt`.
6. When adapting host APIs, keep the boundary explicit: reflection/backlog/promotion are cognitive proposal paths; code mutation/evolution is a separately gated execution path.

## Lite Integration

The public entry point is the `ouroboros_self_improve` package under `lite/src/`. Follow `lite/README.md` for the minimal `SelfImproveLoop` integration.

Use this option when you want the behavior without importing Ouroboros' supervisor/runtime stack.

## Full Integration

Start from `full/EXTRACTION.md`. Preserve the original `ouroboros/` and `supervisor/` layout unless the user explicitly asks for a refactor. The full extraction intentionally carries adjacent dependencies because they enforce the safety and lifecycle semantics of self-modification.

Do not represent `full/` as runtime-neutral. It is a source-faithful extraction that expects Ouroboros-style runtime dependencies and configuration.

## Verification

Before finishing an integration, confirm:

- the intended variant was used;
- reflection/backlog data is durably persisted;
- recursive evolution/deep-review/subagent loops remain guarded;
- full-mode code mutation cannot bypass owner/review/budget gates;
- restart verification and absorbed/abandoned outcomes remain represented;
- tests for the touched variant pass.
