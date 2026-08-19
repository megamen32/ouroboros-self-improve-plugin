"""Reviewer-slot identity: one row, one id, minted in exactly one place.

The scope surface was fixed in v6.87.22. Its twin — the commit triad, the gate
every commit passes through — still ran a row under one id and wrote a durable
actor record under another. These tests pin both halves of the contract:

  * the durable record CARRIES the id the substrate ran, it does not re-derive
    one from the record's position (position is not row whenever an oversized
    skill review merges several chunk passes into one results list);
  * every configured-reviewer surface takes that id from ``slot_id_for_row``,
    so the "minted in exactly one place" claim in ARCHITECTURE.md is testable
    rather than aspirational.
"""

import asyncio
import json
import pathlib
import re
from types import SimpleNamespace
from unittest.mock import MagicMock

REPO = pathlib.Path(__file__).resolve().parent.parent


def _fake_ctx(tmp_path):
    return SimpleNamespace(
        repo_dir=tmp_path, drive_root=tmp_path, task_id="slot-identity",
        pending_events=[], drive_logs=lambda: tmp_path,
    )


def _substrate_stub(recorder, *, raise_for=()):
    """Stand in for ``run_review_request``, recording the id each row ran under."""
    rows = [{"item": "x", "verdict": "PASS", "severity": "advisory", "reason": "checked"}]

    def _run(request, *, slots, drive_root, llm, usage_ctx=None):
        # The triad dispatches one slot per call; plan review dispatches the
        # whole row list in one. Record whatever this call was asked to run.
        recorder.extend(slot.slot_id for slot in slots)
        if any(slot.model in raise_for for slot in slots):
            raise RuntimeError("transport died")
        return SimpleNamespace(actors=[{
            "slot_id": slot.slot_id,
            "model": slot.model,
            "status": "ok",
            "raw_text": json.dumps(rows),
            "usage": {},
            "prompt_ref": {},
            "response_ref": {},
        } for slot in slots])

    return _run


def _merged_chunk_results(ctx, models, *, chunks, raise_for=()):
    """Drive the real triad fan-out once per chunk and union the results.

    Mirrors ``skill_review_passes`` over an oversized skill: C passes across M
    configured rows are merged into ONE ``{"results": [...]}`` envelope, so the
    Nth entry is row ``N % M``, not row ``N``.
    """
    from ouroboros.tools import review

    merged = []
    for part in range(chunks):
        out = json.loads(review._handle_multi_model_review(
            ctx, content=f"PART {part + 1} of {chunks}", prompt="p", models=list(models),
        ))
        merged.extend(out.get("results") or [])
    return merged


def test_durable_actor_record_carries_the_row_the_substrate_ran(tmp_path, monkeypatch):
    """One row, one durable id — even when position stops matching row.

    Before this fix the record re-derived ``slot_{position + 1}``: over 2 rows x 2
    chunk passes it stamped slot_1..slot_4, naming two rows that are not
    configured and giving the SAME configured row two different identities, while
    the prompt/response refs it points at said slot_1/slot_2.
    """
    from ouroboros import review_substrate
    from ouroboros.tools import review

    ran_as: list = []
    monkeypatch.setattr(
        review_substrate, "run_review_request",
        _substrate_stub(ran_as, raise_for={"m/two"}),
    )
    ctx = _fake_ctx(tmp_path)
    merged = _merged_chunk_results(ctx, ["m/one", "m/two"], chunks=2)

    review._collect_review_findings(ctx, merged)
    durable = [str(r.get("slot_id") or "") for r in (ctx._last_triad_raw_results or [])]

    # The substrate ran two rows, twice. The durable records must say the same.
    assert ran_as == ["slot_1", "slot_2", "slot_1", "slot_2"], ran_as
    assert durable == ran_as, durable
    # Nothing may name a row that was never configured.
    assert set(durable) == {"slot_1", "slot_2"}, durable
    # The failing row is still a row: its errored record carries its own id, not
    # a position. (m/two raised transport, so it is the error envelope.)
    errored = [r for r in ctx._last_triad_raw_results if r.get("status") == "error"]
    assert errored, "expected the raising row to produce an error actor record"
    assert {r["slot_id"] for r in errored} == {"slot_2"}, errored


def _repoint_the_mint(monkeypatch):
    """Repoint the ONE mint. Every surface that reads it must follow."""
    from ouroboros import review_substrate
    from ouroboros.tools import review

    def _fake(index, *, prefix=review_substrate.SLOT_ID_PREFIX):
        return f"{prefix}_row{int(index)}"

    monkeypatch.setattr(review_substrate, "slot_id_for_row", _fake)
    # review.py binds the mint by name at import time; repoint that binding too,
    # so a module that spelled its own literal instead still fails this test.
    monkeypatch.setattr(review, "slot_id_for_row", _fake)


def test_triad_row_ids_come_from_the_one_mint(tmp_path, monkeypatch):
    """The commit triad must not spell its own row id (it said multi_model_slot_N,
    disagreeing with the slot_N its own durable records carried)."""
    from ouroboros import review_substrate
    from ouroboros.tools import review

    ran_as: list = []
    monkeypatch.setattr(review_substrate, "run_review_request", _substrate_stub(ran_as))
    _repoint_the_mint(monkeypatch)

    ctx = _fake_ctx(tmp_path)
    merged = _merged_chunk_results(ctx, ["m/one", "m/two"], chunks=1)
    review._collect_review_findings(ctx, merged)
    durable = [str(r.get("slot_id") or "") for r in (ctx._last_triad_raw_results or [])]

    assert ran_as == ["slot_row1", "slot_row2"], ran_as
    assert durable == ran_as, durable


def test_plan_row_ids_come_from_the_one_mint(tmp_path, monkeypatch):
    """Plan review is the third configured-reviewer surface; it minted its own
    ``plan_slot_{idx+1}`` beside the SSOT that owns exactly that shape."""
    from ouroboros import review_substrate
    from ouroboros.tools import plan_review, plan_review_runtime

    ran_as: list = []
    monkeypatch.setattr(review_substrate, "run_review_request", _substrate_stub(ran_as))
    monkeypatch.setattr(plan_review_runtime, "LLMClient", lambda *a, **k: object())
    _repoint_the_mint(monkeypatch)

    ctx = _fake_ctx(tmp_path)
    asyncio.run(plan_review._run_plan_review_slots(
        ctx, ["m/one", "m/two"], "system prompt", "user content",
    ))
    assert ran_as == ["plan_slot_row1", "plan_slot_row2"], ran_as


def test_duplicate_model_plan_rows_stay_distinct_through_finalization(tmp_path, monkeypatch):
    """Two plan rows configured on the SAME model keep their own ids all the way
    into the rows finalization stores and hands to the formatter/summarizer.

    Before this fix ``_plan_raw_result_from_actor`` dropped the ``slot_id`` the
    substrate ran under, so ``ctx._last_plan_review_raw_results`` held rows whose
    only identity was the model string — duplicate-model rows (a supported
    configuration; the configured list preserves duplicates on purpose) became
    indistinguishable downstream, against the carried-not-re-derived contract.
    """
    from ouroboros import review_substrate
    from ouroboros.tools import plan_review, plan_review_runtime

    ran_as: list = []
    monkeypatch.setattr(review_substrate, "run_review_request", _substrate_stub(ran_as))
    monkeypatch.setattr(plan_review_runtime, "LLMClient", lambda *a, **k: object())

    ctx = _fake_ctx(tmp_path)
    raw = asyncio.run(plan_review._run_plan_review_slots(
        ctx, ["m/dup", "m/dup"], "system prompt", "user content",
    ))
    assert ran_as == ["plan_slot_1", "plan_slot_2"], ran_as
    # The converted rows carry the id each row RAN; the model cannot tell them apart.
    assert [r.get("model") for r in raw] == ["m/dup", "m/dup"], raw
    assert [r.get("slot_id") for r in raw] == ["plan_slot_1", "plan_slot_2"], raw

    out = plan_review._finalize_plan_review_output(ctx, plan_review._PlanReviewFinalization(
        request=plan_review._PlanReviewRequest(plan="p", goal="g", files_to_touch=[]),
        raw_results=raw,
        models=["m/dup", "m/dup"],
        estimated_tokens=1,
        subject_repo=tmp_path,
        governance_repo=tmp_path,
        planning_handoffs={},
        state_root=tmp_path,
        state_task_id="t",
        request_fingerprint="fp",
        degraded_scout_note="",
        reviewed_result_hashes={},
    ))
    assert isinstance(out, str) and out
    stored = [r.get("slot_id") for r in (ctx._last_plan_review_raw_results or [])]
    assert stored == ["plan_slot_1", "plan_slot_2"], stored


def _plan_review_text(signal, findings):
    return (
        "## PROPOSALS\n\nConcrete review.\n\nPLAN_FINDINGS_JSON:\n"
        + json.dumps(findings) + f"\nAGGREGATE: {signal}"
    )


def _per_model_substrate(recorder, texts):
    """Substrate stub answering each slot with ITS model's review text."""
    def _run(request, *, slots, drive_root, llm, usage_ctx=None):
        recorder.extend(slot.slot_id for slot in slots)
        return SimpleNamespace(actors=[{
            "slot_id": slot.slot_id, "model": slot.model, "status": "ok",
            "raw_text": texts[slot.model], "usage": {}, "prompt_ref": {}, "response_ref": {},
        } for slot in slots])
    return _run


def _drive_plan_review_async(tmp_path, monkeypatch, *, models, limits, substrate):
    """Run the REAL _run_plan_review_async with the world around it stubbed."""
    from ouroboros import review_substrate
    from ouroboros.tools import plan_review, plan_review_runtime

    monkeypatch.setattr(review_substrate, "run_review_request", substrate)
    monkeypatch.setattr(plan_review_runtime, "LLMClient", lambda *a, **k: object())
    monkeypatch.setattr(
        plan_review, "_start_planning_swarm",
        lambda *a, **k: {"started": True, "handoffs": {}},
    )
    monkeypatch.setattr(plan_review, "_load_plan_checklist", lambda: "checklist")
    monkeypatch.setattr(plan_review, "load_governance_doc", lambda *a, **k: "doc")
    monkeypatch.setattr(plan_review, "build_head_snapshot_section", lambda *a, **k: "")
    monkeypatch.setenv("OUROBOROS_REVIEW_MODELS", ",".join(models))
    monkeypatch.setattr(plan_review, "_get_review_models", lambda: list(models))
    monkeypatch.setattr(
        plan_review, "_per_slot_input_token_limits", lambda _m, **_k: dict(limits),
    )

    ctx = MagicMock()
    ctx.repo_dir = tmp_path
    ctx.drive_root = tmp_path
    ctx.budget_drive_root = str(tmp_path)
    ctx.task_id = "slot-identity-fit"
    ctx.task_metadata = {}
    ctx.task_contract = {}
    ctx.project_id = ""
    ctx.drive_logs.return_value = tmp_path / "logs"

    out = asyncio.run(plan_review._run_plan_review_async(
        ctx,
        plan_review._PlanReviewRequest(
            plan="P", goal="G", files_to_touch=[], context_level="minimal",
        ),
    ))
    return ctx, out


def test_fit_filtered_plan_rows_keep_their_configured_ids(tmp_path, monkeypatch):
    """Preflight fit filtration must not renumber plan_slot_N.

    Before this fix ``_run_plan_review_async`` replaced the configured model
    rows with the fit-only list from ``plan_slot_fit`` and the mint then
    enumerated the COMPACTED list: with configured row 1 preflight-oversize,
    configured plan_slot_2/plan_slot_3 ran (and wrote durable receipts) as
    plan_slot_1/plan_slot_2 — the oversize row's id was silently claimed by a
    different row — and the typed oversize record carried no id at all.
    """
    green = "## PROPOSALS\n\nLooks solid.\n\nPLAN_FINDINGS_JSON:\n[]\nAGGREGATE: GREEN"
    ran_as: list = []
    models = ["m/small", "m/big-a", "m/big-b"]
    # Row 1's calibrated cap cannot hold any real prompt; rows 2 and 3 fit.
    ctx, out = _drive_plan_review_async(
        tmp_path, monkeypatch, models=models,
        limits={"m/small": 1, "m/big-a": 900_000, "m/big-b": 900_000},
        substrate=_per_model_substrate(ran_as, {"m/big-a": green, "m/big-b": green}),
    )

    assert "PLAN_REVIEW_OUTCOME" in out, out
    # The surviving rows ran under the ids of their CONFIGURED positions.
    assert ran_as == ["plan_slot_2", "plan_slot_3"], ran_as
    stored = list(ctx._last_plan_review_raw_results or [])
    assert [r.get("slot_id") for r in stored] == [
        "plan_slot_1", "plan_slot_2", "plan_slot_3",
    ], stored
    by_id = {r["slot_id"]: r for r in stored}
    # The dropped row answers under its ORIGINAL id, as a typed oversize record.
    assert "preflight_oversize" in str(by_id["plan_slot_1"].get("error") or ""), stored
    assert by_id["plan_slot_1"].get("model") == "m/small", stored
    assert by_id["plan_slot_2"].get("model") == "m/big-a", stored
    assert by_id["plan_slot_3"].get("model") == "m/big-b", stored


def test_oversize_middle_row_keeps_its_id_through_the_carry(tmp_path, monkeypatch):
    """The carry re-reads the partition ``plan_slot_fit`` made — a middle row
    dropping out leaves plan_slot_1/plan_slot_3 untouched — and the slots
    surface honors CARRIED ids instead of re-minting from its argument list."""
    from ouroboros import review_substrate
    from ouroboros.tools import plan_review, plan_review_runtime
    from ouroboros.tools.review_synthesis import plan_slot_fit_with_identity

    models = ["m/a", "m/mid-small", "m/c"]
    limits = {"m/a": 900_000, "m/mid-small": 1, "m/c": 900_000}
    assert plan_review._minted_plan_slot_ids(models) == [
        "plan_slot_1", "plan_slot_2", "plan_slot_3",
    ]

    fit_models, callable_ids, stamped, error = plan_slot_fit_with_identity(
        models, limits, 5_000,
    )
    assert error == ""  # two of three IS the quorum
    assert fit_models == ["m/a", "m/c"], fit_models
    assert callable_ids == ["plan_slot_1", "plan_slot_3"], callable_ids
    assert [r.get("slot_id") for r in stamped] == ["plan_slot_2"], stamped
    assert "preflight_oversize" in stamped[0]["error"], stamped

    ran_as: list = []
    monkeypatch.setattr(review_substrate, "run_review_request", _substrate_stub(ran_as))
    monkeypatch.setattr(plan_review_runtime, "LLMClient", lambda *a, **k: object())
    ctx = _fake_ctx(tmp_path)
    raw = asyncio.run(plan_review._run_plan_review_slots(
        ctx, fit_models, "system prompt", "user content", slot_ids=callable_ids,
    ))
    assert ran_as == ["plan_slot_1", "plan_slot_3"], ran_as
    assert [r.get("slot_id") for r in raw] == ["plan_slot_1", "plan_slot_3"], raw


def test_middle_slot_oversize_binds_findings_to_their_configured_rows(tmp_path, monkeypatch):
    """XG-5R2.1 end-to-end: the carry alone was not enough — the CONSUMERS stayed
    positional. ``oversize + answered`` concatenation put the filtered MIDDLE row's
    record first, and summarize/addressable/format then derived identity from
    ``enumerate``: findings and disposition ids were stitched to the WRONG
    configured row. Every assertion here reads the final public artifacts.
    """
    ran_as: list = []
    models = ["m/big-a", "m/mid-small", "m/big-b"]
    texts = {
        "m/big-a": _plan_review_text("REVIEW_REQUIRED", [{
            "id": "a-risk", "level": "RISK",
            "summary": "A-side seam is unnamed.", "recommendation": "Name it.",
        }]),
        "m/big-b": _plan_review_text("REVIEW_REQUIRED", [{
            "id": "b-risk", "level": "RISK",
            "summary": "B-side boundary is absent.", "recommendation": "Add it.",
        }]),
    }
    ctx, out = _drive_plan_review_async(
        tmp_path, monkeypatch, models=models,
        limits={"m/big-a": 900_000, "m/mid-small": 1, "m/big-b": 900_000},
        substrate=_per_model_substrate(ran_as, texts),
    )

    # The two callable rows ran under their CONFIGURED ids (middle row dropped).
    assert ran_as == ["plan_slot_1", "plan_slot_3"], ran_as
    # Stored rows are in CONFIGURED order — not oversize-first arrival order.
    stored = list(ctx._last_plan_review_raw_results or [])
    assert [r.get("slot_id") for r in stored] == [
        "plan_slot_1", "plan_slot_2", "plan_slot_3",
    ], stored
    by_id = {r["slot_id"]: r for r in stored}
    assert by_id["plan_slot_1"].get("model") == "m/big-a", stored
    assert "preflight_oversize" in str(by_id["plan_slot_2"].get("error") or ""), stored
    assert by_id["plan_slot_3"].get("model") == "m/big-b", stored

    # The rendered rows are labeled by carried identity, and the header counts
    # CONFIGURED rows, not the filtered call list.
    assert "### Reviewer plan_slot_1: m/big-a" in out, out
    assert "### Reviewer plan_slot_2: m/mid-small" in out, out
    assert "### Reviewer plan_slot_3: m/big-b" in out, out
    assert "3 configured reviewer rows — 2 called, 1 preflight-oversize" in out, out

    # Finding ids and models are bound to the RIGHT configured rows.
    findings = json.loads(out.split("## Findings Requiring Disposition")[1]
                          .split("```json")[1].split("```")[0])
    bound = {f["finding_id"]: (f["reviewer_slot"], f["model"]) for f in findings}
    assert bound["plan_slot_1:a-risk"] == ("plan_slot_1", "m/big-a"), bound
    assert bound["plan_slot_3:b-risk"] == ("plan_slot_3", "m/big-b"), bound
    # The oversize row is preflight_excluded (swarm-plan-liveness): a slot that
    # was never called is an availability fact, not a dispositionable finding —
    # it must not appear in the disposition ledger under any id.
    assert set(bound) == {"plan_slot_1:a-risk", "plan_slot_3:b-risk"}, bound
    assert "PLAN_REVIEW_OUTCOME: REVIEW_REQUIRED" in out, out


def test_skill_review_dispatches_through_the_slot_id_stamping_entry():
    """The to_dict positional fallback in ReviewActorRecord is LEGACY-READ-ONLY:
    that holds only while every live producer stamps slot_id. skill_review's one
    dispatch is review._handle_multi_model_review — the stamping entry the chunk
    -merge identity tests drive — so pin the call site itself."""
    source = (REPO / "ouroboros" / "skill_review.py").read_text(encoding="utf-8")
    assert source.count("run_review=") == 1, "skill_review grew a second review dispatch"
    assert "run_review=_handle_multi_model_review" in source
    assert "from ouroboros.tools.review import _handle_multi_model_review" in source


# An interpolated slot id built anywhere but the mint is the defect class itself:
# it is a second identity for a row that already has one. Constant sentinels
# (``slot_id="scope_slot_error"``) name no row and are not matched.
_INTERPOLATED_SLOT_ID = re.compile(r"""slot_id["']?\s*[:=]\s*f["'][^"']*\{""")


def test_no_surface_derives_a_row_id_outside_the_one_mint():
    """ARCHITECTURE.md says row identity is minted in exactly one place. This is
    the check that keeps that true when the next reviewer surface is added."""
    offenders = []
    for path in sorted((REPO / "ouroboros").rglob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "slot_id_for_row" in line:
                continue
            if _INTERPOLATED_SLOT_ID.search(line):
                offenders.append(f"{path.relative_to(REPO).as_posix()}:{lineno}: {line.strip()}")
    assert not offenders, (
        "row identity must come from review_substrate.slot_id_for_row, not a local "
        "f-string:\n" + "\n".join(offenders)
    )
