import pytest

from cco.actions.executor import COMMAND_PATTERNS
from cco.agents import AgentContext, DeltaTableOptimizationAgent
from cco.connectors.sample import SampleConnector
from cco.etl.facts import FactStore
from cco.etl.normalizer import Normalizer
from cco.models.canonical import ActionType as A


@pytest.fixture(scope="module")
def facts():
    conn = SampleConnector()
    raw = [r for e in sorted(conn.supported_entities()) for r in conn.collect(e)]
    res = Normalizer().normalize(raw)
    assert not res.errors, res.errors
    return FactStore(res.objects, now=conn.now)


@pytest.fixture(scope="module")
def result(facts):
    return DeltaTableOptimizationAgent().analyze(facts, AgentContext(run_id="test"))


def checks(result, table):
    return {f.check for f in result.findings if f.resource_id == table}


def actions(result, table):
    return {r.action_type for r in result.recommendations if r.resource_id == table}


def test_agent_runs_without_errors(result):
    assert result.errors == []
    assert result.stats["tables_analyzed"] == 8


def test_small_files_and_compaction(result):
    t = "demo.sales.fact_transactions"
    assert {"too_many_small_files", "low_average_file_size", "compaction_needed", "low_quality_write_pattern"} <= checks(result, t)
    assert {A.OPTIMIZE, A.ENABLE_AUTO_OPTIMIZE, A.REVIEW_UPSTREAM_JOB} <= actions(result, t)


def test_cold_table_archive_stale_and_retention(result):
    t = "demo.archive.legacy_orders"
    assert {"table_inactive", "stale_partitions", "data_beyond_retention"} <= checks(result, t)
    assert {A.ARCHIVE_TABLE, A.ARCHIVE_PARTITIONS, A.PURGE_EXPIRED_DATA} <= actions(result, t)


def test_merge_heavy_table(result):
    t = "demo.web.clickstream_events"
    assert {"vacuum_opportunity", "retention_too_long", "high_write_amplification", "unpruned_scans"} <= checks(result, t)
    assert {A.VACUUM, A.SET_RETENTION, A.ENABLE_DELETION_VECTORS} <= actions(result, t)


def test_partition_skew(result):
    assert "partition_skew" in checks(result, "demo.store.inventory_snapshot")


def test_wide_table(result):
    t = "demo.customer.customer_360_wide"
    assert {"wide_small_file_table", "too_many_small_files"} <= checks(result, t)
    assert A.SET_TARGET_FILE_SIZE in actions(result, t)
    assert A.ENABLE_AUTO_OPTIMIZE not in actions(result, t)  # already enabled


def test_write_only_table(result):
    t = "demo.ops.audit_write_only"
    assert "write_only_table" in checks(result, t)
    rec = next(r for r in result.recommendations if r.resource_id == t and r.action_type == A.REVIEW_UPSTREAM_JOB)
    assert rec.estimated_monthly_savings >= 800  # writer job cost is attributed


def test_short_retention(result):
    assert "retention_too_short" in checks(result, "demo.tmp.scratch_short_retention")


def test_healthy_table_has_no_findings(result):
    assert checks(result, "demo.product.dim_product") == set()


def test_generated_commands_pass_executor_safety_regex(result):
    cmds = [r for r in result.recommendations if r.command]
    assert cmds
    for r in cmds:
        assert COMMAND_PATTERNS[r.action_type].match(r.command), r.command


def test_no_inactivity_without_access_logs():
    conn = SampleConnector()
    raw = [r for e in sorted(conn.supported_entities() - {"access_logs"}) for r in conn.collect(e)]
    facts = FactStore(Normalizer().normalize(raw).objects, now=conn.now)
    res = DeltaTableOptimizationAgent().analyze(facts, AgentContext(run_id="t"))
    assert not any(f.check in ("table_inactive", "write_only_table") for f in res.findings)
