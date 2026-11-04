"""Agents B (compute), C (jobs), D (pipelines), cross-agent supersession and the Optimization Queue."""
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from cco.agents import AgentContext, ComputeUtilizationAgent, JobRedundancyAgent, PipelineOptimizationAgent
from cco.agents.pipeline_agent import ingestion_root, source_evidence
from cco.agents.queue import build_optimization_queue, combined_savings
from cco.config import load_config
from cco.connectors.sample import SampleConnector
from cco.etl.facts import FactStore
from cco.etl.fingerprint import logical_name, logical_path
from cco.etl.normalizer import Normalizer
from cco.etl.workload_facts import PipelineFacts
from cco.ingestion.collector import MetadataCollector
from cco.models.canonical import ActionType as A
from cco.models.canonical import CostMetric, Provider, Recommendation, Severity
from cco.pipeline import CostOptimizationPipeline
from cco.storage.raw_store import InMemoryRawStore


@pytest.fixture(scope="module")
def facts():
    store = InMemoryRawStore()
    MetadataCollector([SampleConnector()], store).run("t")
    norm = Normalizer().normalize(store.read("t"))
    assert norm.errors == []
    return FactStore(norm.objects)


def run(agent, facts):
    res = agent.analyze(facts, AgentContext(run_id="t"))
    assert res.errors == []
    return res


def checks(res, rid):
    return {f.check for f in res.findings if f.resource_id == rid}


def actions(res, rid):
    return {r.action_type for r in res.recommendations if r.resource_id == rid}


# ------------------------------------------------------------------ facts
def test_workload_facts_join(facts):
    wl = facts.workloads
    assert facts.workloads is wl  # cached
    c = wl.clusters["0101-allpurpose"]
    assert c.metrics_available and c.idle_ratio == pytest.approx(0.62) and c.job_ids == ["1003"]
    j = wl.jobs["1002"]
    assert j.job_cluster_ids == ["job-1002-run"] and j.compute_metrics_available and j.availability == ["ON_DEMAND"]
    assert j.source_writes_per_day == pytest.approx(30 / 90)
    assert wl.jobs["1003"].all_purpose_ids == ["0101-allpurpose"]
    assert wl.pipelines["p-001"].triggering_job_ids == ["1010"]
    assert wl.pipelines["p-004"].consumer_reads == 0


def test_fingerprint_normalization():
    assert logical_name("[dev alice] customer_snapshot") == "customer_snapshot"
    a = logical_path("/Workspace/Users/alice@demo.com/.bundle/dtc_etl/dev/files/src/x")
    b = logical_path("/Workspace/Users/sp/.bundle/dtc_etl/prod/files/src/x")
    assert a == b == "bundle:dtc_etl/src/x"
    assert ingestion_root("path:/Volumes/c/s/v/landing/sales/2026/10/22/a.json") == "/Volumes/c/s/v/landing/sales"
    assert ingestion_root("demo.sales.t") is None
    assert ingestion_root("path:s3://b/x/dt=2026-10-22/part-0.parquet") == "s3://b/x"          # hive partition
    assert ingestion_root("path:s3://b/x/2026-10-22/a.csv") == "s3://b/x"                      # iso date segment
    assert ingestion_root("path:/V/cfg/a.yaml") == "/V/cfg/a.yaml"                             # plain file: itself
    assert ingestion_root("path:/V/landing/sales/01/a.json") == "/V/landing/sales/01/a.json"   # bare 01 is no evidence


# ------------------------------------------------------------------ B. compute
def test_compute_agent_cluster_checks(facts):
    res = run(ComputeUtilizationAgent(), facts)
    assert {"cluster_idle_ratio", "start_stop_window", "overprovisioned_all_purpose", "old_node_generation"} <= checks(res, "0101-allpurpose")
    assert {A.SET_AUTOTERMINATION, A.SCHEDULE_START_STOP, A.RESIZE_CLUSTER, A.CHANGE_NODE_TYPE} <= actions(res, "0101-allpurpose")
    assert A.ENABLE_AUTOSCALING in actions(res, "0102-ds-fixed")
    assert "overprovisioned_all_purpose" not in checks(res, "0102-ds-fixed")  # p95 CPU 55% is healthy
    assert A.TUNE_AUTOSCALING in actions(res, "0103-bi-adhoc")
    consolidate = [r for r in res.recommendations if r.action_type == A.CONSOLIDATE_CLUSTERS]
    assert len(consolidate) == 1 and {"0103-bi-adhoc", "0104-eng-sandbox"} <= set(consolidate[0].related_resources)
    assert res.stats["fleet_useful_work_ratio"] is not None


def test_compute_agent_job_checks(facts):
    res = run(ComputeUtilizationAgent(), facts)
    assert {"job_cluster_oversized", "job_runtime_vs_work", "on_demand_batch"} <= checks(res, "1002")
    assert {A.RESIZE_CLUSTER, A.USE_SPOT} <= actions(res, "1002")
    assert "on_demand_batch" not in checks(res, "1001")  # already spot
    assert A.REDUCE_STARTUP_OVERHEAD in actions(res, "1001")
    conv = next(r for r in res.recommendations if r.action_type == A.CONVERT_TO_JOB_CLUSTER)
    assert conv.resource_id == "1003" and conv.related_resources == ["0101-allpurpose"]
    assert "queue_time" in checks(res, "1009")


def test_compute_agent_pools(facts):
    res = run(ComputeUtilizationAgent(), facts)
    assert A.REDUCE_POOL_IDLE in actions(res, "pool-a") and A.REDUCE_POOL_IDLE not in actions(res, "pool-b")
    assert A.CONSOLIDATE_POOLS in actions(res, "pools:m6i.xlarge")


def test_compute_thresholds_configurable(facts):
    res = run(ComputeUtilizationAgent({"thresholds": {"idle_ratio": 0.9, "overprovisioned_p95_cpu": 1}}), facts)
    assert "cluster_idle_ratio" not in checks(res, "0101-allpurpose")
    assert not any(f.check == "overprovisioned_all_purpose" for f in res.findings)


# ------------------------------------------------------------------ C. jobs
def test_job_agent_duplicates_and_deployments(facts):
    res = run(JobRedundancyAgent(), facts)
    dup = [r for r in res.recommendations if r.action_type == A.CONSOLIDATE_DEPLOYMENTS]
    assert {r.resource_id for r in dup} == {"1004", "1005"}  # 1006 (most runs / cost) is the keeper
    assert all(r.metrics["keeper_job_id"] == "1006" for r in dup)
    merge = next(r for r in res.recommendations if r.action_type == A.MERGE_JOBS)
    assert merge.resource_id == "1008" and merge.related_resources == ["1001"]  # same lineage, fewer runs
    # one finding row per duplicate set, on the keeper - not one row per copy
    rows = [f for f in res.findings if f.check == "same_logic_multiple_deployments"]
    assert len(rows) == 1 and rows[0].resource_id == "1006" and rows[0].metrics["duplicates"] == ["1004", "1005"]
    assert {r.resource_id for r in res.recommendations if rows[0].id in r.finding_ids} == {"1004", "1005"}
    lineage = [f for f in res.findings if f.check == "same_lineage"]
    assert len(lineage) == 1 and lineage[0].resource_id == "1001" and lineage[0].metrics["duplicates"] == ["1008"]


def test_job_agent_schedule_retry_reprocess(facts):
    res = run(JobRedundancyAgent(), facts)
    chain = next(r for r in res.recommendations if r.action_type == A.CHAIN_JOBS)
    assert chain.resource_id == "1007" and "1001" in chain.related_resources
    assert A.REDUCE_RETRIES in actions(res, "1002") and A.TRIGGER_ON_CHANGE in actions(res, "1002")
    assert A.ADD_INCREMENTAL in actions(res, "1009")      # overwrites inventory_snapshot nightly
    assert A.SHIFT_SCHEDULE in actions(res, "1009")       # peak window + queueing
    assert {A.DECOMMISSION_JOB, A.FIX_FAILING_WORKLOAD} <= actions(res, "1003")
    assert "duration_variance" in checks(res, "1009")


# ------------------------------------------------------------------ D. pipelines
def test_pipeline_agent(facts):
    res = run(PipelineOptimizationAgent(), facts)
    assert A.DEDUPLICATE_PIPELINE in actions(res, "p-002")
    assert A.ARCHIVE_PIPELINE in actions(res, "p-003")
    assert {A.PAUSE_PIPELINE, A.REDUCE_FREQUENCY} <= actions(res, "p-004")
    assert {A.TRIGGER_ON_CHANGE, A.ADD_INCREMENTAL} <= actions(res, "p-001")
    assert A.DEDUPLICATE_PIPELINE in actions(res, "p-005")   # same raw landing path as sales_ingest
    dup = next(f for f in res.findings if f.check == "duplicate_ingestion" and "p-005" in f.metrics["duplicates"])
    assert dup.metrics["match_basis"] == "shared_partitioned_root"  # different files, same date-partitioned root
    assert A.PRIORITIZE_PIPELINE in actions(res, "p-006")
    assert not actions(res, "p-001") & {A.PAUSE_PIPELINE, A.ARCHIVE_PIPELINE}


# ------------------------------------------------------------------ coordinator + queue
@pytest.fixture(scope="module")
def report():
    cfg = load_config(overrides={"storage": {"mode": "memory"}, "llm": {"enabled": False}, "connectors": {"sample": {"enabled": True}}})
    return CostOptimizationPipeline(cfg).run().report


def test_supersession_across_agents(report):
    on_1003 = {r.action_type for r in report.recommendations if r.resource_id == "1003"}
    assert A.DECOMMISSION_JOB in on_1003
    assert not on_1003 & {A.CONVERT_TO_JOB_CLUSTER, A.FIX_FAILING_WORKLOAD}
    sup = {(s["resource_id"], s["action_type"]) for s in report.suppressed}
    assert ("1003", "convert_to_job_cluster") in sup and ("p-004", "reduce_frequency") in sup


def test_queue_groups_related_recommendations(report):
    by_res = {}
    for q in report.queue:
        for r in q.resources:
            by_res[r] = q
    assert by_res["1003"] is by_res["demo.ops.audit_write_only"]          # job + its unused output table
    assert by_res["1004"] is by_res["1005"]                              # copies of one deployment
    assert by_res["p-001"] is by_res["p-002"]                            # duplicate pipelines
    multi = [q for q in report.queue if len(q.agents) > 1]
    assert multi
    assert all(r.queue_rank and r.group_id for r in report.recommendations)
    prios = [q.priority for q in report.queue]
    assert prios == sorted(prios)
    for a, b in zip(report.queue, report.queue[1:]):
        if a.priority == b.priority:
            assert a.impact >= b.impact


def _job_fps(job_id, name, task, parameters=None):
    from cco.connectors.base import RawRecord
    from cco.etl.normalizer import _dbx_job

    s = {"name": name, "tasks": [task]}
    if parameters:
        s["parameters"] = parameters
    rec = RawRecord(provider="databricks", entity="jobs", source_id=str(job_id), payload={"job_id": job_id, "settings": s},
                    connector="test")
    return next(iter(_dbx_job(rec))).attributes["task_fingerprints"]


def test_job_parameter_driven_tasks_are_not_duplicates():
    task = {"task_key": "ingest", "spark_python_task": {"python_file": "/x/Bronze_Ingestion.py",
                                                        "parameters": ["workday", "{{job.parameters.source_location}}"]}}
    a = _job_fps(1, "workday_restricted_ingest", task)
    b = _job_fps(2, "workday_unrestricted_ingest", task)
    assert a != b                                                   # same generic code, different job params
    assert _job_fps(3, "[dev bob] workday_restricted_ingest", task) == a  # dev copy of the same job still matches
    p1 = _job_fps(4, "j", task, [{"name": "source_location", "default": "s3://a"}])
    p2 = _job_fps(5, "k", task, [{"name": "source_location", "default": "s3://b"}])
    assert p1 != p2


def test_shared_config_paths_are_not_duplicate_ingestion(facts):
    agent = PipelineOptimizationAgent()
    wl = facts.workloads
    saved = {pid: list(p.sources) for pid, p in wl.pipelines.items()}
    try:
        for pid in ("p-001", "p-006"):
            wl.pipelines[pid].sources.append("path:/Volumes/demo/bronze/ext/config/config.yml")
        res = run(agent, facts)
        assert not any(f.check == "duplicate_ingestion" and {f.resource_id, f.metrics.get("other")} == {"p-001", "p-006"}
                       for f in res.findings)
    finally:
        for pid, src in saved.items():
            wl.pipelines[pid].sources = src


# ------------------------------------------------------------------ D. duplicate ingestion: structural evidence
@contextmanager
def extra_pipelines(facts, *pipes):
    wl = facts.workloads
    for p in pipes:
        wl.pipelines[p.pipeline_id] = p
    try:
        yield wl
    finally:
        for p in pipes:
            wl.pipelines.pop(p.pipeline_id, None)


def pf(pid, sources, targets, cost=100.0, **kw):
    return PipelineFacts(pipeline_id=pid, name=pid, lineage_available=True, sources=list(sources), targets=list(targets),
                         monthly_cost=cost, **kw)


def ingest_pairs(res):
    """duplicate_ingestion findings keyed by their full member set (keeper + duplicates)."""
    return {frozenset([f.resource_id, *f.metrics.get("duplicates", [])]): f for f in res.findings if f.check == "duplicate_ingestion"}


def test_sibling_files_are_not_duplicate_ingestion(facts):
    a = pf("t-sib-a", ["path:s3://ext/landing/vendor/feed_a.csv"], ["c.bronze.feed_a"])
    b = pf("t-sib-b", ["path:s3://ext/landing/vendor/feed_b.csv"], ["c.bronze.feed_b"])
    with extra_pipelines(facts, a, b):
        res = run(PipelineOptimizationAgent(), facts)
    assert frozenset({"t-sib-a", "t-sib-b"}) not in ingest_pairs(res)       # same folder, different files
    c = pf("t-sib-c", ["path:s3://ext/landing/vendor/feed_a.csv"], ["c.bronze.feed_a_copy"], cost=50.0)
    with extra_pipelines(facts, a, c):
        res = run(PipelineOptimizationAgent(), facts)
    f = ingest_pairs(res)[frozenset({"t-sib-a", "t-sib-c"})]                 # the exact same file IS a match
    assert f.resource_id == "t-sib-a" and f.metrics["duplicates"] == ["t-sib-c"]  # finding on the keeper
    assert f.metrics["match_basis"] == "identical_sources"
    rec = next(r for r in res.recommendations if f.id in r.finding_ids)
    assert rec.resource_id == "t-sib-c" and rec.confidence == pytest.approx(0.75)  # cheaper copy is retired


def test_shared_partitioned_landing_root_is_duplicate_ingestion(facts):
    a = pf("t-part-a", ["path:s3://ext/landing/pos/2026/10/21/f1.json", "path:s3://ext/landing/pos/2026/10/22/f2.json"],
           ["c.bronze.pos"], cost=300.0)
    b = pf("t-part-b", ["path:s3://ext/landing/pos/dt=2026-10-23/part-0.parquet"], ["c.silver.pos_copy"], cost=100.0)
    with extra_pipelines(facts, a, b):
        res = run(PipelineOptimizationAgent(), facts)
    f = ingest_pairs(res)[frozenset({"t-part-a", "t-part-b"})]
    assert f.resource_id == "t-part-a" and f.metrics["duplicates"] == ["t-part-b"]  # finding on the keeper
    assert f.metrics["match_basis"] == "shared_partitioned_root" and f.metrics["shared_sources"] == ["s3://ext/landing/pos"]
    rec = next(r for r in res.recommendations if f.id in r.finding_ids)
    assert rec.resource_id == "t-part-b" and rec.confidence == pytest.approx(0.55)  # cheaper copy is retired


def test_multi_file_root_requires_several_files_from_one_workload():
    files = [f"path:/V/drop/vendor/file_{i}.csv" for i in range(3)]
    assert source_evidence(files, []).keys == {"/V/drop/vendor": "multi_file_root"}
    assert set(source_evidence(files[:2], []).keys) == {"/V/drop/vendor/file_0.csv", "/V/drop/vendor/file_1.csv"}


def test_self_referential_sources_are_ignored(facts):
    pid_a, pid_b = "aaaaaaaa-1111-2222-3333-444444444444", "bbbbbbbb-1111-2222-3333-444444444444"
    ev = source_evidence([f"main.ops.event_log_{pid_a.replace('-', '_')}", "c.bronze.x", "c.ops.custom_log",
                          "path:/Volumes/c/s/v/in/2026/10/01/a.json"], ["c.bronze.x"],
                         owner_ids=[pid_a], event_log_tables=["c.ops.custom_log"])
    assert ev.keys == {"/Volumes/c/s/v/in": "partitioned_root"}
    assert ev.dropped == {"own_event_log": 2, "own_target": 1}
    # with self-references kept the pair would only be 1/3 similar; dropped, both read exactly the same file
    a = pf(pid_a, ["path:/Volumes/c/s/v/in/feed.json", f"c.ops.event_log_{pid_a.replace('-', '_')}", "c.bronze.a"],
           ["c.bronze.a"], cost=200.0)
    b = pf(pid_b, ["path:/Volumes/c/s/v/in/feed.json", "c.ops.b_events"], ["c.bronze.b"], cost=100.0,
           event_log_table="c.ops.b_events")
    with extra_pipelines(facts, a, b):
        res = run(PipelineOptimizationAgent(), facts)
    f = ingest_pairs(res)[frozenset({pid_a, pid_b})]
    assert f.metrics["match_basis"] == "identical_sources"
    rec = next(r for r in res.recommendations if f.id in r.finding_ids)
    assert rec.resource_id == pid_b and rec.metrics["dropped_sources"] == {"own_event_log": 1}


def test_pipeline_event_log_setting_is_normalized():
    from cco.connectors.base import RawRecord
    from cco.etl.normalizer import _dbx_pipeline

    rec = RawRecord(provider="databricks", entity="pipelines", source_id="x", connector="test",
                    payload={"pipeline_id": "x", "name": "n", "spec": {"catalog": "c", "schema": "s", "event_log": {"name": "ev"}}})
    assert next(iter(_dbx_pipeline(rec))).attributes["event_log_table"] == "c.s.ev"


def test_incomplete_lineage_gives_no_duplicate_ingestion(facts):
    # verified false positive: generic ingestion code, one YAML config per pipeline, Auto Loader paths not in lineage
    cfg = "path:/Volumes/dt_dev_bronze/visualcrossing/external_volume/config"
    a = pf("t-cfg-a", [f"{cfg}/visual_crossing_next15days_forecast_bronze.yaml", "c.vc.raw_forecast"], ["c.vc.raw_forecast"])
    b = pf("t-cfg-b", [f"{cfg}/visual_crossing_last30days_history_bronze.yaml"], ["c.vc.raw_history"])
    c = pf("t-cfg-c", [f"{cfg}/shared.yaml"], ["c.vc.raw_shared_1"])      # same config FILE, still not data
    d = pf("t-cfg-d", [f"{cfg}/shared.yaml"], ["c.vc.raw_shared_2"])
    ids = {"t-cfg-a", "t-cfg-b", "t-cfg-c", "t-cfg-d"}
    with extra_pipelines(facts, a, b, c, d):
        res = run(PipelineOptimizationAgent(), facts)
    assert not any(f.check == "duplicate_ingestion" and f.resource_id in ids for f in res.findings)
    gaps = {f.resource_id: f for f in res.findings if f.check == "lineage_incomplete"}
    assert ids <= set(gaps) and all(gaps[i].severity == Severity.INFO for i in ids)
    assert gaps["t-cfg-a"].metrics["dropped_sources"] == {"non_data_file": 1, "own_target": 1}
    assert not any(r.resource_id in ids and r.action_type == A.DEDUPLICATE_PIPELINE for r in res.recommendations)


def test_user_exclusion_patterns_are_optional(facts):
    a = pf("t-ex-a", ["path:s3://ref/lookup/stores.csv"], ["c.b.a"])
    b = pf("t-ex-b", ["path:s3://ref/lookup/stores.csv"], ["c.b.b"])
    with extra_pipelines(facts, a, b):
        assert frozenset({"t-ex-a", "t-ex-b"}) in ingest_pairs(run(PipelineOptimizationAgent(), facts))
        agent = PipelineOptimizationAgent({"thresholds": {"ingestion_exclude_patterns": ["s3://ref/*"]}})
        assert frozenset({"t-ex-a", "t-ex-b"}) not in ingest_pairs(run(agent, facts))


# ------------------------------------------------------------------ duplicate sets -> one finding row
def _rows_for(res, check, ids):
    return [f for f in res.findings if f.check == check and {f.resource_id, *f.metrics.get("duplicates", [])} & ids]


def test_duplicate_pipeline_set_is_one_finding(facts):
    # 4 pipelines from the same bundle code (e.g. deployed by different users) -> ONE row, not one per copy / pair
    lib = ["bundle:etl/pipelines/orders_dup_set"]
    group = [pf(f"t-dup-{i}", [], [f"c.s.orders_{i}"], cost=100.0 * i, libraries=lib, runs=10 * i) for i in range(1, 5)]
    ids = {p.pipeline_id for p in group}
    with extra_pipelines(facts, *group):
        res = run(PipelineOptimizationAgent(), facts)
    rows = _rows_for(res, "duplicate_pipelines", ids)
    assert len(rows) == 1
    f = rows[0]
    assert f.resource_id == "t-dup-4"                                        # keeper: most runs
    assert f.metrics["duplicates"] == ["t-dup-1", "t-dup-2", "t-dup-3"] and f.metrics["group_size"] == 4
    assert f.metrics["duplicates_monthly_cost"] == pytest.approx(600.0)
    recs = [r for r in res.recommendations if f.id in r.finding_ids]       # one retire action per copy, same finding
    assert sorted(r.resource_id for r in recs) == ["t-dup-1", "t-dup-2", "t-dup-3"]
    assert all(r.action_type == A.DEDUPLICATE_PIPELINE and r.metrics["keeper"] == "t-dup-4" for r in recs)
    assert not any(r.resource_id == "t-dup-4" and r.action_type == A.DEDUPLICATE_PIPELINE for r in res.recommendations)


def test_code_and_lineage_duplicates_merge_into_one_set(facts):
    # a~b by code, b~c by lineage -> one set {a, b, c}
    a = pf("t-mix-a", ["c.s.src"], ["c.s.out_a"], libraries=["bundle:etl/x_mix"], runs=50)
    b = pf("t-mix-b", ["c.s.src"], ["c.s.out_bc"], libraries=["bundle:etl/x_mix"], runs=5)
    c = pf("t-mix-c", ["c.s.src"], ["c.s.out_bc"], libraries=["bundle:etl/y_mix"], runs=1)
    ids = {"t-mix-a", "t-mix-b", "t-mix-c"}
    with extra_pipelines(facts, a, b, c):
        res = run(PipelineOptimizationAgent(), facts)
    rows = _rows_for(res, "duplicate_pipelines", ids)
    assert len(rows) == 1 and rows[0].resource_id == "t-mix-a" and rows[0].metrics["duplicates"] == ["t-mix-b", "t-mix-c"]
    assert "same source code" in rows[0].detail and "same source-to-target lineage" in rows[0].detail


def test_duplicate_ingestion_set_is_one_finding(facts):
    ws = [pf(f"t-ing-{i}", [f"path:s3://ext/landing/pos_set/2026/10/2{i}/f.json"], [f"c.bronze.pos_set_{i}"], cost=100.0 * i)
          for i in range(1, 4)]
    ids = {w.pipeline_id for w in ws}
    with extra_pipelines(facts, *ws):
        res = run(PipelineOptimizationAgent(), facts)
    rows = _rows_for(res, "duplicate_ingestion", ids)
    assert len(rows) == 1
    f = rows[0]
    assert f.resource_id == "t-ing-3" and f.metrics["duplicates"] == ["t-ing-1", "t-ing-2"] and f.metrics["group_size"] == 3
    assert sorted(r.resource_id for r in res.recommendations if f.id in r.finding_ids) == ["t-ing-1", "t-ing-2"]


# ------------------------------------------------------------------ robust monthly cost
def _day(rid, d, amount, incident=0.0):
    dt = datetime(2026, 10, d, tzinfo=timezone.utc)
    return CostMetric(id=f"{rid}:{d}", provider=Provider.DATABRICKS, resource_id=rid, resource_type="pipeline", amount=amount,
                      period_start=dt, period_end=dt, attributes={"granularity": "day", "incident_amount": incident})


def test_monthly_cost_is_robust_to_runaway_days():
    # verified billing of pipeline 69522fcb...: ~$0.10/day, then $1,059 on 2026-10-16 ($1,034 from canceled updates)
    now = datetime(2026, 10, 23, tzinfo=timezone.utc)
    rows = ([_day("pl", 7, 0.44)] + [_day("pl", d, 0.10) for d in range(8, 14)]
            + [_day("pl", 14, 0.33), _day("pl", 15, 0.72, 0.51), _day("pl", 16, 1059.42, 1034.38)])
    cd = FactStore(rows, now=now).cost_details("pl")
    assert cd["raw_monthly_cost"] == pytest.approx(3538.37, rel=1e-3)     # old estimator: total / 9 days x 30
    assert cd["incident_cost"] == pytest.approx(1034.89)
    assert cd["monthly_cost"] < 10 and cd["basis"] == "winsorized_daily" and cd["observed_days"] == 17
    # steady days: 0.44, 6 x 0.10, 0.33, 0.21, 25.04 -> median 0.10, cap 0.50 -> kept 2.08, trimmed 24.54
    assert cd["monthly_cost"] == pytest.approx(2.08 / 17 * 30, abs=0.01)
    assert cd["trimmed_cost"] == pytest.approx(24.54, abs=0.01)
    raw = FactStore(rows, now=now, cost_estimation={"outlier_cap_x_median": 0}).cost_details("pl")
    assert raw["monthly_cost"] == pytest.approx((1061.51 - 1034.89) / 17 * 30, rel=1e-3)  # incidents still excluded
    short = FactStore([_day("new", 22, 10.0), _day("new", 23, 10.0)], now=now).cost_details("new")
    assert short["monthly_cost"] == pytest.approx(20.0 / 14 * 30)          # min observation window, not 20/1 x 30


def test_daily_billing_rows_are_normalized_and_period_rows_unchanged():
    from cco.connectors.base import RawRecord
    from cco.etl.normalizer import _dbx_cost

    rec = RawRecord(provider="databricks", entity="costs", source_id="w:p:sku:2026-10-16", connector="test",
                    payload={"workspace_id": "w", "resource_id": "p", "resource_type": "pipeline", "sku_name": "sku",
                             "usage_date": "2026-10-16", "list_cost": 1059.42, "incident_cost": 1034.38})
    c = next(iter(_dbx_cost(rec)))
    assert c.attributes["granularity"] == "day" and c.attributes["incident_amount"] == pytest.approx(1034.38)
    p = CostMetric(id="x", provider=Provider.DATABRICKS, resource_id="q", amount=300.0,
                   period_start=datetime(2026, 9, 23, tzinfo=timezone.utc), period_end=datetime(2026, 10, 23, tzinfo=timezone.utc))
    assert FactStore([p]).monthly_cost("q") == pytest.approx(300.0)


def test_failed_update_spend_is_reported_as_incident(facts):
    p = pf("t-inc", ["c.s.src"], ["c.s.out"], cost=3.7, incident_cost=1034.89, cost_basis="winsorized_daily")
    with extra_pipelines(facts, p):
        res = run(PipelineOptimizationAgent(), facts)
    f = next(f for f in res.findings if f.resource_id == "t-inc" and f.check == "failed_update_spend")
    assert f.severity == Severity.HIGH and f.metrics["incident_cost"] == pytest.approx(1034.89)
    assert f.metrics["monthly_cost"] == pytest.approx(3.7)                  # steady state stays small


def test_combined_savings_not_double_counted():
    def rec(action, s):
        return Recommendation(resource_id="c1", resource_type="cluster", action_type=action, title="", rationale="", agent="x",
                              estimated_monthly_savings=s, metrics={"resource_monthly_cost": 1000})
    recs = [rec(A.RESIZE_CLUSTER, 500), rec(A.SET_AUTOTERMINATION, 500)]
    assert combined_savings(recs) == pytest.approx(750)  # 1000 x (1 - 0.5 x 0.5), not 1000
    q = build_optimization_queue(recs)
    assert len(q) == 1 and q.items[0].combined_monthly_savings == pytest.approx(750)
