# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Cloud Cost Optimizer - runner
# MAGIC metadata collection -> rule-based scoring -> AI reasoning -> human approval -> (optional) actions.
# MAGIC
# MAGIC Set `mode = sample` to run on synthetic data, or `live` to scan this workspace using `config/settings.yaml`.

# COMMAND ----------

# DBTITLE 1,Parameters and imports
import os, sys
sys.dont_write_bytecode = True
dbutils.widgets.dropdown("mode", "live", ["sample", "live"])
dbutils.widgets.text("catalogs", "")
dbutils.widgets.dropdown("execute_actions", "false", ["false", "true"])

ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
for _m in [m for m in sys.modules if m == "cco" or m.startswith("cco.")]:  # pick up code edits on re-run
    del sys.modules[_m]

from cco.config import load_config
from cco.pipeline import CostOptimizationPipeline
from cco.interfaces.report import to_spark_dataframes

# COMMAND ----------

# DBTITLE 1,Build config
mode = dbutils.widgets.get("mode")
overrides = {}
if mode == "sample":
    overrides = {"storage": {"mode": "memory"}, "llm": {"enabled": False},
                 "connectors": {k: {"enabled": False} for k in ("databricks", "aws", "azure", "gcp", "snowflake")} | {"sample": {"enabled": True}}}
elif dbutils.widgets.get("catalogs"):
    overrides = {"connectors": {"databricks": {"catalogs": [c.strip() for c in dbutils.widgets.get("catalogs").split(",") if c.strip()]}}}
cfg = load_config(os.path.join(ROOT, "config", "settings.yaml"), overrides)

# COMMAND ----------

# DBTITLE 1,Run pipeline
pipe = CostOptimizationPipeline(cfg, spark=spark)
res = pipe.run(execute_actions=dbutils.widgets.get("execute_actions") == "true")
print("ingestion:", res.ingestion.counts, res.ingestion.errors)
print("facts:", res.facts_summary)
displayHTML("<pre>" + res.markdown() + "</pre>")

# COMMAND ----------

# DBTITLE 1,Optimization queue, recommendations and findings
dfs = to_spark_dataframes(res.report, spark)
print("agent stats:", res.report.agent_stats)
if dfs["queue"] is not None:  # one prioritized list across all agents (overlap-adjusted savings)
    display(dfs["queue"])
if dfs["recommendations"] is not None:
    display(dfs["recommendations"])
if dfs["findings"] is not None:
    display(dfs["findings"])

# COMMAND ----------

# DBTITLE 1,Human approval (example)
# Review the queue, then approve / reject explicitly. Nothing runs without this step.
pending = pipe.workflow.pending()
for r in pending[:10]:
    print(r.id, r.priority, r.title, "|", r.command)

# pipe.workflow.approve("<rec_id>", approver=spark.sql("select current_user()").first()[0], comment="ok")
# pipe.workflow.reject("<rec_id>", approver="...", reason="...")

# COMMAND ----------

# DBTITLE 1,Execute approved actions (dry run by default)
from cco.actions.executor import ActionExecutor
for r in ActionExecutor.from_config(pipe.repo, cfg.get("actions"), spark).execute_approved():
    print(r)

# COMMAND ----------

# DBTITLE 1,Findings and savings by agent and check
from pyspark.sql import functions as F
findings_df, recs_df = dfs["findings"], dfs["recommendations"]
display(findings_df.groupBy("agent", "check", "severity").count()
        .join(recs_df.groupBy("agent", "resource_id").agg(F.sum("est_monthly_savings").alias("s"))
              .join(findings_df.select("agent", "check", "resource_id").distinct(), ["agent", "resource_id"])
              .groupBy("agent", "check").agg(F.round(F.sum("s"), 2).alias("est_monthly_savings_on_flagged")), ["agent", "check"], "left")
        .orderBy("agent", F.desc("count")))

# COMMAND ----------

# DBTITLE 1,Scratch: inspect duplicate job groups
from collections import Counter
print(Counter(f.check for f in res.report.findings if f.agent != "delta_table_optimization"))
for f in [f for f in res.report.findings if f.check in ("duplicate_jobs", "same_logic_multiple_deployments", "duplicate_ingestion", "overlapping_schedules")][:12]:
    print(f.check, "|", f.title[:150])
for q in res.report.queue[:10]:
    print(q.rank, q.priority, round(q.combined_monthly_savings, 2), q.agents, q.title[:110])

# COMMAND ----------

# DBTITLE 1,Pipeline costs vs lineage linkage (system tables)
# MAGIC %sql
# MAGIC -- Pipeline costs from billing joined with lineage targets.
# MAGIC -- Shows which pipelines have $$ but their writer links are missing from the
# MAGIC -- optimizer's cost-attribution chain (collect_access_logs only captures
# MAGIC -- entity_type = 'JOB', not 'PIPELINE').
# MAGIC WITH pipeline_costs AS (
# MAGIC   SELECT
# MAGIC     u.usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC     sum(u.usage_quantity * p.pricing.effective_list.default) AS monthly_cost
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices p
# MAGIC     ON u.sku_name = p.sku_name
# MAGIC    AND u.cloud = p.cloud
# MAGIC    AND u.usage_end_time >= p.price_start_time
# MAGIC    AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
# MAGIC   WHERE u.usage_date >= date_sub(current_date(), 30)
# MAGIC     AND u.usage_metadata.dlt_pipeline_id IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC pipeline_lineage AS (
# MAGIC   SELECT
# MAGIC     entity_id AS pipeline_id,
# MAGIC     collect_set(coalesce(target_table_full_name, concat('path:', target_path))) AS target_tables,
# MAGIC     count(*) AS lineage_events
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE event_date >= date_sub(current_date(), 90)
# MAGIC     AND entity_type = 'PIPELINE'
# MAGIC     AND entity_id IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   pc.pipeline_id,
# MAGIC   round(pc.monthly_cost, 2) AS monthly_cost_usd,
# MAGIC   coalesce(pl.lineage_events, 0) AS lineage_events,
# MAGIC   size(coalesce(pl.target_tables, array())) AS num_target_tables,
# MAGIC   slice(coalesce(pl.target_tables, array()), 1, 10) AS sample_target_tables,
# MAGIC   exists(coalesce(pl.target_tables, array()), t -> t LIKE 'dt_dev_platinum%') AS writes_to_platinum,
# MAGIC   CASE WHEN pl.pipeline_id IS NULL THEN 'NO LINEAGE - cost fully unlinked'
# MAGIC        WHEN size(coalesce(pl.target_tables, array())) = 0 THEN 'lineage exists but no table targets'
# MAGIC        ELSE 'lineage exists' END AS linkage_status
# MAGIC FROM pipeline_costs pc
# MAGIC LEFT JOIN pipeline_lineage pl ON pc.pipeline_id = pl.pipeline_id
# MAGIC WHERE pc.monthly_cost > 10
# MAGIC ORDER BY pc.monthly_cost DESC
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,Platinum table writer cost attribution gaps
# MAGIC %sql
# MAGIC -- For dt_dev_platinum tables specifically: which writer jobs have cost vs $0,
# MAGIC -- which tables are written by pipelines (missed by collect_access_logs),
# MAGIC -- and which tables have no lineage at all.
# MAGIC WITH platinum_writers AS (
# MAGIC   SELECT
# MAGIC     t.target_table_full_name AS table_id,
# MAGIC     collect_set(t.entity_id) FILTER (WHERE t.entity_type = 'JOB') AS writer_job_ids,
# MAGIC     collect_set(t.entity_id) FILTER (WHERE t.entity_type = 'PIPELINE') AS writer_pipeline_ids
# MAGIC   FROM system.access.table_lineage t
# MAGIC   WHERE t.event_date >= date_sub(current_date(), 90)
# MAGIC     AND t.target_table_full_name LIKE 'dt_dev_platinum.%'
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC job_costs AS (
# MAGIC   SELECT
# MAGIC     u.usage_metadata.job_id AS job_id,
# MAGIC     sum(u.usage_quantity * p.pricing.effective_list.default) AS monthly_cost
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices p
# MAGIC     ON u.sku_name = p.sku_name AND u.cloud = p.cloud
# MAGIC      AND u.usage_end_time >= p.price_start_time
# MAGIC      AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
# MAGIC   WHERE u.usage_date >= date_sub(current_date(), 30)
# MAGIC     AND u.usage_metadata.job_id IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC pipeline_costs AS (
# MAGIC   SELECT
# MAGIC     u.usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC     sum(u.usage_quantity * p.pricing.effective_list.default) AS monthly_cost
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices p
# MAGIC     ON u.sku_name = p.sku_name AND u.cloud = p.cloud
# MAGIC      AND u.usage_end_time >= p.price_start_time
# MAGIC      AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
# MAGIC   WHERE u.usage_date >= date_sub(current_date(), 30)
# MAGIC     AND u.usage_metadata.dlt_pipeline_id IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   pw.table_id,
# MAGIC   size(pw.writer_job_ids) AS num_writer_jobs,
# MAGIC   size(pw.writer_pipeline_ids) AS num_writer_pipelines,
# MAGIC   round(coalesce(sum(jc.monthly_cost), 0), 2) AS writer_job_cost,
# MAGIC   round(coalesce(sum(pc.monthly_cost), 0), 2) AS writer_pipeline_cost,
# MAGIC   CASE WHEN size(pw.writer_job_ids) = 0 AND size(pw.writer_pipeline_ids) = 0 THEN 'NO LINEAGE'
# MAGIC        WHEN coalesce(sum(jc.monthly_cost), 0) = 0 AND coalesce(sum(pc.monthly_cost), 0) = 0 THEN 'WRITERS EXIST BUT $0 COST'
# MAGIC        WHEN size(pw.writer_pipeline_ids) > 0 AND size(pw.writer_job_ids) = 0 THEN 'PIPELINE-WRITTEN (missed by access_logs)'
# MAGIC        ELSE 'HAS ATTRIBUTABLE COST' END AS attribution_status
# MAGIC FROM platinum_writers pw
# MAGIC LEFT JOIN job_costs jc ON array_contains(pw.writer_job_ids, jc.job_id)
# MAGIC LEFT JOIN pipeline_costs pc ON array_contains(pw.writer_pipeline_ids, pc.pipeline_id)
# MAGIC GROUP BY pw.table_id, pw.writer_job_ids, pw.writer_pipeline_ids
# MAGIC ORDER BY writer_job_cost ASC, writer_pipeline_cost ASC, pw.table_id

# COMMAND ----------

# DBTITLE 1,Validate cco_* findings against source system tables
# Independent accuracy checks of the persisted findings (latest run) vs. system tables.
S = "dt_dev_platinum.sales."
run = spark.sql(f"select run_id, started_at from {S}cco_runs order by started_at desc limit 1").first()
ts = run.started_at.strftime("%Y-%m-%d %H:%M:%S")

# 1. Referential integrity (all should be 0)
integrity = {
    "dup finding_id": f"select count(*)-count(distinct finding_id) from {S}cco_findings",
    "findings w/o resource": f"select count(*) from {S}cco_findings f left anti join {S}cco_resources r using(resource_type, resource_id)",
    "recs -> missing finding": f"select count(*) from (select explode(finding_ids) fid from {S}cco_recommendations) x left anti join {S}cco_findings f on x.fid=f.finding_id",
    "queue member -> missing rec": f"select count(*) from {S}cco_queue_members m left anti join {S}cco_recommendations r using(recommendation_id)",
}
for k, q in integrity.items():
    print(f"{k:30s}", spark.sql(q).first()[0])

# 2. failing_job: claimed success % vs job_run_timeline
display(spark.sql(f"""
WITH runs AS (SELECT job_id, run_id, max_by(result_state, period_end_time) rs FROM system.lakeflow.job_run_timeline
              WHERE period_end_time > timestamp'{ts}' - INTERVAL 30 DAYS AND period_end_time < timestamp'{ts}' GROUP BY ALL)
SELECT f.resource_id job_id, regexp_extract(f.title,'succeeds only (\\d+)%',1) claimed_pct,
       round(100*count_if(rs='SUCCEEDED')/count(*)) actual_pct, count(*) runs
FROM {S}cco_findings f JOIN runs r ON r.job_id=f.resource_id WHERE f.check_name='failing_job' GROUP BY ALL ORDER BY actual_pct DESC"""))

# 3. no_downstream_consumers: final targets that DO have reads by other entities (false positives)
display(spark.sql(f"""
WITH tgt AS (SELECT resource_id pid, explode(from_json(get_json_object(metrics,'$.final_targets'),'array<string>')) t
             FROM {S}cco_findings WHERE check_name='no_downstream_consumers'),
rd AS (SELECT tgt.pid, count(DISTINCT tgt.t) tables_read, count(*) reads, collect_set(l.entity_type) consumer_types
       FROM tgt JOIN system.access.table_lineage l ON l.source_table_full_name=tgt.t
       WHERE l.event_date >= date_sub(date(timestamp'{ts}'), 90) AND l.event_time < timestamp'{ts}'
         AND NOT (l.entity_type='PIPELINE' AND l.entity_id=tgt.pid) GROUP BY 1)
SELECT (rd.pid IS NOT NULL) false_positive, count(*) findings FROM (SELECT DISTINCT pid FROM tgt) p LEFT JOIN rd USING(pid) GROUP BY 1"""))