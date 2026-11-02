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
   -> Multi-agent analysis     cco/agents/          planner -> specialist agents -> coordinator
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
| Agent | Status | Purpose |
| --- | --- | --- |
| A. `delta_table_optimization` | implemented | small files, low avg file size, compaction, write patterns, write amplification, stale partitions, data past retention, unpruned scans, VACUUM, retention, partition skew, inactivity / write-only tables, wide small-file tables, growth |
| B, C, D | planned | add a module under `cco/agents/`, subclass `BaseAgent`, decorate with `@register_agent`, import it in `cco/agents/__init__.py` |

Recommendations produced by Agent A: `OPTIMIZE` (full or hot partitions), `VACUUM`, set/extend retention, enable auto-optimize,
larger `targetFileSize` compaction windows, deletion vectors, partition-pruning improvements / liquid clustering,
archive cold partitions / tables, purge data past retention, review upstream writer jobs.

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

Delta outputs (`storage.mode: delta`): `raw_metadata`, `findings`, `recommendations`, `approval_audit` in `<storage.catalog>.<storage.schema>`.

## Extending
* New source: subclass `BaseConnector` (`collect_<entity>` yields `(id, payload)`), register in `connectors/registry.py`, add `@normalizes(provider, entity)` functions.
* New check: add a `check_*` method to an agent and list it in `checks()`.
* New agent: see table above.
