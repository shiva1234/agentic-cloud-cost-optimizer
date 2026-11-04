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
    assert rep.plan.agent_names == ["delta_table_optimization", "compute_utilization", "job_redundancy", "pipeline_optimization"]
    assert rep.errors == [] and res.normalization["errors"] == []
    assert rep.queue and [q.rank for q in rep.queue] == list(range(1, len(rep.queue) + 1))
    assert any(q["run_id"] == res.run_id for q in pipe.repo.tables["queue"])
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
    events = [e["event"] for e in pipe.repo.audit_log if e["recommendation_id"] == opt.id]
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


# ------------------------------------------------------------------ reporting data model
def test_reporting_model_stores_each_attribute_once(pipe):
    res = pipe.run()
    repo, rid, t = pipe.repo, res.run_id, pipe.repo.tables
    assert repo.runs[rid]["status"] == "succeeded" and repo.runs[rid]["agents_run"] == res.report.plan.agent_names
    fr = [r for r in t["findings"] if r["run_id"] == rid]
    assert len(fr) == len(res.report.findings) and len({r["finding_id"] for r in fr}) == len(fr)
    # resource-level metrics are stored once per run x resource, not repeated on every finding
    snaps = [s for s in t["resource_snapshots"] if s["run_id"] == rid]
    assert len({(s["resource_type"], s["resource_id"]) for s in snaps}) == len(snaps)
    assert {(r["resource_type"], r["resource_id"]) for r in fr} == {(s["resource_type"], s["resource_id"]) for s in snaps}
    by_id = {f.id: f for f in res.report.findings}
    for r in fr:
        m = json.loads(r["metrics"] or "{}")
        assert not ({"monthly_cost", "duplicates", "keeper", "duplicate_names"} | set(by_id[r["finding_id"]].resource_metric_keys)) & set(m)
    assert any(by_id[r["finding_id"]].resource_metric_keys for r in fr)  # job / pipeline metrics were moved out
    # duplicate sets: one finding, copies in the bridge table
    dup = next(r for r in fr if r["check_name"] == "duplicate_pipelines")
    members = [(m["resource_type"], m["resource_id"]) for m in t["finding_resources"] if m["finding_id"] == dup["finding_id"]]
    assert dup["resource_id"] == "p-001" and members == [("pipeline", "p-002")]
    # every referenced resource / check is in its dimension, with descriptive attributes
    assert {(r["resource_type"], r["resource_id"]) for r in fr} | set(members) <= set(repo.resources)
    assert repo.resources[("job", "1001")]["resource_name"] == "sales_ingest"
    assert repo.checks["duplicate_pipelines"] == {"check_name": "duplicate_pipelines", "agent": "pipeline_optimization",
                                                  "domain": "pipelines"}
    # queue: group metrics only; titles / resources / actions come from member recommendations
    q = [x for x in t["queue"] if x["run_id"] == rid]
    assert q and not {"title", "recommendation_ids", "resources", "agents", "actions", "statuses"} & set(q[0])
    qm = [x for x in t["queue_members"] if x["run_id"] == rid]
    assert sorted(x["recommendation_id"] for x in qm) == sorted(r.id for r in res.report.recommendations)


def test_finding_ids_are_stable_across_runs(pipe):
    a = {(f.check, f.resource_id): f.id for f in pipe.run().report.findings}
    b = {(f.check, f.resource_id): f.id for f in pipe.run().report.findings}
    assert a == b


def test_recommendation_row_round_trip(pipe):
    from cco.storage.repository import rec_from_row, rec_row
    per_run = {"group_id": None, "queue_rank": None}  # stored in cco_queue / cco_queue_members, not on the recommendation
    for rec in pipe.run().report.recommendations:
        row = rec_row(rec)
        assert "payload" not in row
        assert rec_from_row(row).to_dict() | per_run == rec.to_dict() | per_run


def test_failed_run_is_recorded(pipe, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(pipe, "_run", boom)
    with pytest.raises(RuntimeError):
        pipe.run(run_id="r-fail")
    assert pipe.repo.runs["r-fail"]["status"] == "failed" and "boom" in pipe.repo.runs["r-fail"]["error_message"]


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
