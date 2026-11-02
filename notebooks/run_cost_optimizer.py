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
dbutils.widgets.dropdown("mode", "sample", ["sample", "live"])
dbutils.widgets.text("catalogs", "")
dbutils.widgets.dropdown("execute_actions", "false", ["false", "true"])

ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))

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

# DBTITLE 1,Recommendations and findings
dfs = to_spark_dataframes(res.report, spark)
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

