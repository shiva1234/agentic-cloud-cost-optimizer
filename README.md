# Cloud Cost Optimizer (`cco`)

Modular, cloud-agnostic cost optimization framework for Databricks-centred platforms.

**Process:** metadata collection -> rule-based scoring -> AI reasoning -> human approval -> (optional) actions

```
Workspace / cloud APIs
   -> Connector layer          cco/connectors/      databricks | aws | azure | gcp | snowflake | sample
   -> Ingestion adapters       cco/ingestion/       per-(connector, entity) isolation, batching
   -> Raw metadata store       cco/storage/raw_store.py      <catalog>.<schema>.raw_metadata
   -> ETL / normalization      cco/etl/normalizer.py         -> canonical objects (cco/models)
   -> Causal+usage+cost facts  cco/etl/facts.py              table <- writer jobs -> $ ; reads/writes/scans
   -> Workload facts           cco/etl/workload_facts.py     jobs / pipelines / clusters: runs, lineage, cost
   -> Multi-agent analysis     cco/agents/          planner -> specialist agents -> coordinator -> queue
   -> Scoring + recommendation cco/scoring/         explainable 0-100 points, P1..P4
   -> AI reasoning layer       cco/llm/             summaries + bounded confidence nudges (no new actions)
   -> Human approval           cco/approval/        queue, dual approval for high risk, audit log
   -> UI / API / alerts        cco/interfaces/      markdown/DataFrames, FastAPI, Slack
   -> (optional) automation    cco/actions/         approved + allow-listed + regex-validated + dry-run default
```

## Canonical objects
`Workspace, Account (account/project/subscription), Cluster, Job, Task, Pipeline, Table (+PartitionStats, TableOperation), StorageObject, UsageMetric, CostMetric, Finding, Recommendation`

## Metadata entities
`workspaces, accounts, clusters, jobs, tasks, pipelines, schemas, tables, storage_stats, compute_metrics, access_logs, costs`

Databricks sources: SDK (clusters/jobs/pipelines/UC), `DESCRIBE DETAIL` / `DESCRIBE HISTORY`, optional `_metadata` file stats,
`system.compute.node_timeline`, `system.access.table_lineage`, `system.query.history`, `system.billing.usage` + `list_prices`.

## Agents
All four agents are implemented and run by default (`agents.<name>.enabled`). Thresholds live in `config/settings.yaml`.

| Agent | Module | Checks |
| --- | --- | --- |
| A. `delta_table_optimization` | `delta_table_agent.py` | too_many_small_files, low_average_file_size, compaction_needed, low_quality_write_pattern, high_write_amplification, stale_partitions, data_beyond_retention, unpruned_scans, vacuum_opportunity, retention_misconfigured, partition_skew, table_inactive, write_only_table, wide_small_file_table, fast_growth |
| B. `compute_utilization` | `compute_utilization_agent.py` | cluster_idle_ratio, start_stop_window, autoscaling_disabled, autoscaling_ineffective, overprovisioned_all_purpose, low_concurrency, old_node_generation, job_cluster_oversized, job_runtime_vs_work, jobs_on_all_purpose, startup_shutdown_waste, on_demand_batch, queue_time, pool_fragmentation, consolidation |
| C. `job_redundancy` | `job_redundancy_agent.py` | duplicate_jobs, same_logic_multiple_deployments, same_lineage, overlapping_schedules, peak_window_contention, unnecessary_retries, reprocess_unchanged_inputs, full_reprocessing, low_value_output, failing_job, duration_variance |
| D. `pipeline_optimization` | `pipeline_agent.py` | pipeline_inactive, no_downstream_consumers, scheduled_too_frequently, processing_same_data, duplicate_pipelines, duplicate_ingestion, lineage_incomplete (info), failed_update_spend |

Design rules:
* **Universal** - no project-specific naming heuristics. Duplicate detection is structural: job fingerprints (task code + parameters,
  `{{...}}` templates kept as markers, task multiplicity preserved); `duplicate_ingestion` needs exact file/table matches, date/hive
  partition evidence or >= `multi_file_root_min_files` files, and drops a workload's own targets, event log and non-data files.
  Optional org-specific exclusions: `ingestion_exclude_patterns` (globs, empty by default).
* **One finding per duplicate set** - mutually duplicate jobs/pipelines are grouped (union-find); the keeper (most runs, then cost)
  carries ONE finding listing every copy; each copy gets its own decommission recommendation.
* **Coordinator** - cross-agent supersession (retiring a resource suppresses tuning recs on it) and one prioritized optimization
  queue across agents with overlap-adjusted savings (`cco/agents/queue.py`).

## Cost estimation
`collect_costs` reads daily `system.billing.usage` x `list_prices` per job / pipeline / cluster and splits spend from canceled or
failed runs/updates into `incident_cost` (via `system.lakeflow.*_timeline`). `FactStore.cost_details` then computes a steady-state
monthly cost: each day is winsorized at `cost_estimation.outlier_cap_x_median` x the median active day, averaged over
max(observed window, `min_observation_days`) and scaled to 30 days. Incident spend above `incident_min_cost` is reported by
`failed_update_spend` instead of inflating the monthly estimate.

## Safety model
1. Agents are deterministic; the LLM only summarises and nudges confidence by at most `llm.max_confidence_delta`.
2. Re-runs never overwrite human decisions (pending/approved/rejected are preserved; ids are stable per resource+action).
3. Executor runs only `APPROVED` items whose action is allow-listed and whose SQL matches a strict regex, after dependencies ran.
4. `actions.dry_run: true` by default. Archive/purge/redesign actions are always manual.

## Quick start
```bash
pip install -e '.[dev]'
python -m cco.pipeline --sample            # synthetic data, in-memory, no credentials
python -m pytest -q
```
On Databricks: open `notebooks/run_cost_optimizer`, or add `src/` to `sys.path` and call
`CostOptimizationPipeline(load_config('config/settings.yaml'), spark=spark).run()`.

Runner notebook widgets: `mode` (`sample` | `live`), `catalogs` (comma list, overrides `connectors.databricks.catalogs`),
`execute_actions` (`false` by default). The last cell validates the persisted findings against system tables.

Tests: `PYTHONDONTWRITEBYTECODE=1 python -m pytest -p no:cacheprovider` (the workspace file system rejects `__pycache__`).

## Reporting data model
With `storage.mode: delta`, everything is written to `<storage.catalog>.<storage.schema>` (currently `dt_dev_platinum.sales`) as a
normalized, dashboard-ready model. Finding ids are deterministic (agent + check + resource), so re-runs update instead of duplicating.

| Table / view | Grain |
| --- | --- |
| `cco_runs` | one row per optimizer run (status, agents run, error) |
| `cco_checks` | check catalog (check, agent, domain) |
| `cco_resources` | one row per flagged resource (name, workspace, owner, run_as, first/last seen run) |
| `cco_resource_snapshots` | run x resource: monthly_cost + resource metrics |
| `cco_findings` | run x finding: check, severity, title, detail, metrics JSON |
| `cco_finding_resources` | members of multi-resource findings (keeper / duplicate roles) |
| `cco_recommendations` | one row per recommendation: action, command, risk, confidence, savings, score, priority, status |
| `cco_queue`, `cco_queue_members` | prioritized cross-agent queue groups and their recommendations |
| `cco_approval_audit` | approval / rejection / execution events |
| `raw_metadata` | raw collected payloads per connector + entity |
| `v_findings`, `v_latest_findings`, `v_recommendations`, `v_queue`, `v_run_summary` | denormalized views for dashboards |

The legacy `findings` / `recommendations` / `optimization_queue` / `approval_audit` tables were migrated into `cco_*` and dropped.

## Known limitations (validated 2026-11-03 against run `20261028T204701-69d0b7`)
Checked against `system.lakeflow.*`, `system.access.table_lineage`, `system.billing.usage`, `system.compute.node_timeline`,
`DESCRIBE DETAIL` / `DESCRIBE HISTORY` and the Jobs API.

Accurate: failing_job (24/24), pipeline_inactive (151/151), processing_same_data (7/7), table_inactive + write_only_table (30/30),
file-size checks (exact match), cluster node-hours, duplicate job groups (54/56 identical code; 1 copy deleted after the run).

Open issues:
* **`no_downstream_consumers` false positives (43/47).** Access logs are filtered to `connectors.databricks.catalogs`, and
  `WorkloadFacts.table_reads()` returns 0.0 for tables outside that scope (e.g. bronze/silver pipeline outputs), so read tables look unused.
* **`unnecessary_retries` false positives (3/3).** For-each task iterations are counted as retry attempts (`max_retries=0`).
* **`duration_variance` misleading (5/5).** CV mixes fast failed runs with successful runs; CV on successful runs alone is ~0.08.
* **Failing-job savings understated.** `fix_failing_workload` savings use `monthly_cost`, which excludes failed-run `incident_cost`,
  so a 0%-success job shows ~$0 savings.
* **Duplicate keeper choice** - on ties (e.g. both 0 runs) the keeper can be a subset of a copy with more tasks.
* **Self-noise** - the optimizer flags its own `cco_*` / `raw_metadata` tables and pipeline `event_log_*` tables.
* **Coverage** - jobs come from `w.jobs.list()`, so only jobs visible to the running identity are analyzed (run it as an admin /
  service principal for full coverage). Jobs paused at run time and jobs with < `failing_min_runs` (10) runs are skipped by design.
* `storage.catalog` is still a dev placeholder (`dt_dev_platinum`); point it at a dedicated ops / FinOps catalog.

## Extending
* New source: subclass `BaseConnector` (`collect_<entity>` yields `(id, payload)`), register in `connectors/registry.py`, add `@normalizes(provider, entity)` functions.
* New check: add a `check_*` method to an agent and list it in `checks()`.
* New agent: add a module under `cco/agents/`, subclass `BaseAgent` (or `WorkloadAgent` for jobs/pipelines/clusters),
  decorate with `@register_agent`, and import it in `cco/agents/__init__.py`.
