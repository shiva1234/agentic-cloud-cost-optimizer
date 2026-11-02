import json

import pytest

from cco.actions.executor import ActionExecutor
from cco.approval.workflow import ApprovalError
from cco.config import load_config
from cco.llm.reasoner import LLMClient
from cco.models.canonical import ActionType as A
from cco.models.canonical import RecommendationStatus as S
from cco.pipeline import CostOptimizationPipeline

SAMPLE = {
    "storage": {"mode": "memory"},
    "llm": {"enabled": False},
    "connectors": {"sample": {"enabled": True}},
}


@pytest.fixture()
def pipe():
    return CostOptimizationPipeline(load_config(overrides=SAMPLE))


def find(recs, table, action):
    return next(r for r in recs if r.resource_id == table and r.action_type == action)


def test_end_to_end_flow(pipe):
    res = pipe.run()
    rep = res.report
    assert res.ingestion.errors == []
    assert res.facts_summary["table"] == 8
    assert rep.plan.agent_names == ["delta_table_optimization"]
    assert rep.recommendations and res.queued_for_approval > 0
    scores = [r.score for r in rep.recommendations]
    assert scores == sorted(scores, reverse=True)
    assert all(r.llm_summary for r in rep.recommendations)  # template summaries without LLM
    assert rep.summary["estimated_monthly_savings"] > 0
    assert "Cloud Cost Optimization Report" in res.markdown()


def test_conflict_resolution_suppresses_tuning_on_archived_table(pipe):
    rep = pipe.run().report
    legacy = {r.action_type for r in rep.recommendations if r.resource_id == "demo.archive.legacy_orders"}
    assert A.ARCHIVE_TABLE in legacy and A.ARCHIVE_PARTITIONS not in legacy
    assert any(s["resource_id"] == "demo.archive.legacy_orders" for s in rep.suppressed)
    vac = find(rep.recommendations, "demo.web.clickstream_events", A.VACUUM)
    ret = find(rep.recommendations, "demo.web.clickstream_events", A.SET_RETENTION)
    assert ret.id in vac.depends_on


def test_nothing_executes_without_approval(pipe):
    pipe.run()
    ran = []
    ex = ActionExecutor(pipe.repo, sql_runner=ran.append, dry_run=False)
    assert ex.execute_approved() == []
    assert ran == []


def test_approval_then_execution(pipe):
    rep = pipe.run().report
    opt = find(rep.recommendations, "demo.sales.fact_transactions", A.OPTIMIZE)
    assert opt.status == S.PENDING_APPROVAL
    pipe.workflow.approve(opt.id, "alice@corp.com", "ok")
    with pytest.raises(ApprovalError):
        pipe.workflow.approve(opt.id, "alice@corp.com")
    ran = []
    results = ActionExecutor(pipe.repo, sql_runner=ran.append, dry_run=False).execute_approved()
    assert [r.status for r in results] == ["executed"]
    assert ran == [opt.command]
    assert pipe.repo.get(opt.id).status == S.EXECUTED
    events = [e["event"] for e in pipe.repo.audit_log if e["rec_id"] == opt.id]
    assert events == ["queued_for_approval", "approved", "executed"]


def test_high_risk_needs_two_approvers_and_manual_actions_never_run(pipe):
    rep = pipe.run().report
    arch = find(rep.recommendations, "demo.archive.legacy_orders", A.ARCHIVE_TABLE)
    if arch.status != S.PENDING_APPROVAL:  # below queue threshold - force into queue for the test
        arch.status = S.PENDING_APPROVAL
    assert pipe.workflow.approve(arch.id, "alice").status == S.PENDING_APPROVAL
    assert pipe.workflow.approve(arch.id, "bob").status == S.APPROVED
    res = ActionExecutor(pipe.repo, sql_runner=lambda s: None, dry_run=False).execute(arch)
    assert res.status == "blocked"  # no command / not in allow-list -> manual


def test_dependency_blocks_vacuum_until_retention_executed(pipe):
    rep = pipe.run().report
    vac = find(rep.recommendations, "demo.web.clickstream_events", A.VACUUM)
    vac.status = S.PENDING_APPROVAL
    pipe.workflow.approve(vac.id, "alice")
    res = ActionExecutor(pipe.repo, sql_runner=lambda s: None, dry_run=False).execute(pipe.repo.get(vac.id))
    assert res.status == "blocked" and "dependency" in res.message


def test_rerun_preserves_human_decisions(pipe):
    rep = pipe.run().report
    opt = find(rep.recommendations, "demo.sales.fact_transactions", A.OPTIMIZE)
    pipe.workflow.approve(opt.id, "alice")
    rej = find(rep.recommendations, "demo.customer.customer_360_wide", A.SET_TARGET_FILE_SIZE)
    pipe.workflow.reject(rej.id, "bob", "not now")
    rep2 = pipe.run().report
    assert find(rep2.recommendations, "demo.sales.fact_transactions", A.OPTIMIZE).status == S.APPROVED
    assert find(rep2.recommendations, "demo.customer.customer_360_wide", A.SET_TARGET_FILE_SIZE).status == S.REJECTED


def test_dry_run_default(pipe):
    rep = pipe.run().report
    opt = find(rep.recommendations, "demo.sales.fact_transactions", A.OPTIMIZE)
    pipe.workflow.approve(opt.id, "alice")
    ran = []
    res = ActionExecutor(pipe.repo, sql_runner=ran.append).execute_approved()
    assert res[0].status == "dry_run" and ran == []


class FakeLLM(LLMClient):
    def __init__(self):
        self.calls = 0

    def complete(self, system, user):
        self.calls += 1
        items = json.loads(user)["items"]
        out = [{"id": i["id"], "summary": "LLM says ok", "risks": ["check owners"], "confidence_delta": 5.0,
                "needs_human_review": True} for i in items]
        out.append({"id": "invented", "summary": "DROP everything", "confidence_delta": 1})
        return "```json\n" + json.dumps({"executive_summary": "Exec summary", "items": out}) + "\n```"


def test_llm_layer_is_bounded():
    cfg = load_config(overrides={**SAMPLE, "llm": {"enabled": True, "max_confidence_delta": 0.2}})
    llm = FakeLLM()
    pipe = CostOptimizationPipeline(cfg, llm=llm)
    rep = pipe.run(request={"agents": ["delta_table_optimization"]}).report
    assert llm.calls == 1
    assert rep.executive_summary == "Exec summary"
    assert all(r.id != "invented" for r in rep.recommendations)
    touched = [r for r in rep.recommendations if r.llm_summary == "LLM says ok"]
    assert touched
    assert all(r.score_breakdown.get("llm_confidence_delta") == 0.2 for r in touched)
