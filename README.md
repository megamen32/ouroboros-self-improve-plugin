# Ouroboros Self Improve Plugin

Agent Plugins 1.0 package containing two extractions of Ouroboros self-improvement.

Source revision: `77652326490010b47e338196facafb1c35cba3d0`.

## Variants

### `lite/`

Runtime-neutral post-task improvement loop:

`reflection -> durable memory/backlog -> LLM promotion -> durable request -> outcome checkpoints`

Use it when the host already owns authorization, mutation, restart, budgeting, provider routing, and review execution.

### `full/`

Source-faithful extraction of the complete Ouroboros self-improvement runtime. It includes the pieces intentionally abstracted out of lite:

- actual self-modification/evolution execution
- campaign lifecycle and promotion/application
- restart verification and absorption/rollback lifecycle
- owner-gated evolution controls
- budget/cost accounting gates
- provider/model plumbing used by review/evolution
- plan, triad/general, and skill review safety machinery
- reflection, memory actions, backlog, deep self-review, checkpoints

See `full/EXTRACTION.md` for the source map.

## Agent Plugin 1.0

The repo root is an Agent Plugins 1.0 package:

- `plugin.json` — Agent Plugins 1.0.0 manifest
- `skills/ouroboros-self-improve/SKILL.md` — integration/routing skill
- `lite/` — portable implementation
- `full/` — full source-faithful implementation
- `SOURCE.md` — extraction provenance

## Verification

Lite:

```bash
cd lite
python -m pytest -q
```

Full (with Ouroboros dependencies installed):

```bash
cd full
python -m pytest -q
```

## License

MIT. See `LICENSE`.
