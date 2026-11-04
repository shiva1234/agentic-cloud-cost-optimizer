"""Databricks connector.

Sources:
  * Workspace REST APIs via databricks-sdk  -> clusters, jobs, tasks, pipelines, schemas, tables, external locations
  * Delta metadata via Spark SQL             -> DESCRIBE DETAIL / DESCRIBE HISTORY / optional file-level stats
  * System tables via Spark SQL              -> compute metrics, lineage (access logs), query pruning, billing
"""
from __future__ import annotations

import fnmatch
import logging
from typing import Any

from cco.connectors.base import BaseConnector, Collected
from cco.models.canonical import Provider
from cco.utils import quote_fqn, sql_literal

log = logging.getLogger(__name__)

EXCLUDED_CATALOGS = {"system", "samples", "__databricks_internal", "hive_metastore"}
HISTORY_KEYS = ("version", "timestamp", "operation", "operationParameters", "operationMetrics", "userName", "job", "clusterId")


class DatabricksConnector(BaseConnector):
    name = "databricks"
    provider = Provider.DATABRICKS

    def __init__(self, config: dict | None = None, workspace_client: Any = None, spark: Any = None) -> None:
        super().__init__(config)
        self._w = workspace_client
        self._spark = spark
        self._jobs_cache: list[dict] | None = None
        self._tables_seen: list[str] = []
        self._ws_id: str | None = None

    # ------------------------------------------------------------------ clients
    @property
    def w(self) -> Any:
        if self._w is None:
            from databricks.sdk import WorkspaceClient

            self._w = WorkspaceClient(**(self.config.get("auth") or {}))
        return self._w

    @property
    def spark(self) -> Any:
        if self._spark is None:
            from pyspark.sql import SparkSession

            self._spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return self._spark

    def _sql(self, query: str) -> list[dict[str, Any]]:
        return [r.asDict(recursive=True) for r in self.spark.sql(query).collect()]

    @property
    def lookback_days(self) -> int:
        return int(self.config.get("lookback_days", 30))

    @property
    def access_lookback_days(self) -> int:
        return int(self.config.get("access_lookback_days", 90))

    @property
    def workspace_id(self) -> str:
        if self._ws_id is None:
            self._ws_id = str(self.w.get_workspace_id())
        return self._ws_id

    def _ws(self, col: str = "workspace_id") -> str:
        """System tables are account-wide; scope workload data to this workspace unless all_workspaces=true."""
        if self.config.get("all_workspaces", False):
            return ""
        return f" AND {col} = {sql_literal(self.workspace_id)}"

    def supported_entities(self) -> set[str]:
        return {
            "workspaces", "clusters", "jobs", "tasks", "pipelines", "schemas", "tables",
            "storage_stats", "compute_metrics", "access_logs", "costs",
            "pools", "job_run_stats", "pipeline_run_stats", "lineage",
        }

    def test_connection(self) -> bool:
        self.w.current_user.me()
        return True

    # ------------------------------------------------------------------ workspace objects
    def collect_workspaces(self) -> Collected:
        ws_id = self.workspace_id
        yield ws_id, {"workspace_id": ws_id, "host": self.w.config.host}

    def collect_clusters(self) -> Collected:
        """Default: latest definition (SCD2) of every cluster that had nodes running in the lookback window,
        from system.compute.clusters - includes job clusters, which the clusters API no longer lists."""
        if self.config.get("cluster_source", "system") == "api":
            skip_job = self.config.get("skip_job_clusters", True)
            for c in self.w.clusters.list():
                d = c.as_dict()
                if skip_job and d.get("cluster_source") == "JOB":
                    continue
                d["_source"] = "api"
                yield d["cluster_id"], d
            return
        d = self.lookback_days
        rows = self._sql(
            f"""
            WITH active AS (
              SELECT DISTINCT workspace_id, cluster_id FROM system.compute.node_timeline
              WHERE end_time >= current_timestamp() - INTERVAL {d} DAYS {self._ws()}),
            latest AS (
              SELECT * FROM system.compute.clusters WHERE 1 = 1 {self._ws()}
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1),
            nt AS (SELECT node_type, max(core_count) AS core_count, max(memory_mb) AS memory_mb
                   FROM system.compute.node_types GROUP BY node_type)
            SELECT l.workspace_id, l.cluster_id, l.cluster_name, l.owned_by, l.driver_node_type, l.worker_node_type,
                   l.worker_count, l.min_autoscale_workers, l.max_autoscale_workers, l.auto_termination_minutes,
                   l.cluster_source, l.tags, l.aws_attributes, l.azure_attributes, l.gcp_attributes,
                   l.driver_instance_pool_id, l.worker_instance_pool_id, l.dbr_version, l.policy_id,
                   l.data_security_mode, l.delete_time, nt.core_count AS worker_cores, nt.memory_mb AS worker_memory_mb
            FROM latest l JOIN active a ON l.workspace_id = a.workspace_id AND l.cluster_id = a.cluster_id
            LEFT JOIN nt ON nt.node_type = l.worker_node_type"""
        )
        for r in rows:
            r["_source"] = "system"
            yield r["cluster_id"], r

    def collect_pools(self) -> Collected:
        for p in self.w.instance_pools.list():
            d = p.as_dict()
            yield d["instance_pool_id"], d

    def _jobs(self) -> list[dict]:
        if self._jobs_cache is None:
            limit = int(self.config.get("max_jobs", 2000))
            out: list[dict] = []
            for i, j in enumerate(self.w.jobs.list(expand_tasks=True)):
                if i >= limit:
                    log.warning("max_jobs=%s reached; remaining jobs skipped", limit)
                    break
                out.append(j.as_dict())
            self._jobs_cache = out
        return self._jobs_cache

    def collect_jobs(self) -> Collected:
        for j in self._jobs():
            yield j["job_id"], j

    def collect_tasks(self) -> Collected:
        for j in self._jobs():
            for t in (j.get("settings") or {}).get("tasks") or []:
                yield f"{j['job_id']}:{t.get('task_key')}", {**t, "job_id": j["job_id"]}

    def _pipeline_settings(self) -> dict[str, dict]:
        """Latest pipeline settings (continuous/development/serverless/edition) from system.lakeflow.pipelines."""
        try:
            rows = self._sql(
                f"""SELECT pipeline_id, pipeline_type, settings, tags FROM system.lakeflow.pipelines WHERE 1 = 1 {self._ws()}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1"""
            )
            return {r["pipeline_id"]: r for r in rows}
        except Exception as e:  # noqa: BLE001
            self.warn(f"system.lakeflow.pipelines unavailable: {str(e)[:200]}")
            return {}

    def collect_pipelines(self) -> Collected:
        details = self.config.get("pipeline_details", False)
        sys_settings = self._pipeline_settings()
        for p in self.w.pipelines.list_pipelines():
            d = p.as_dict()
            sysrow = sys_settings.get(d["pipeline_id"]) or {}
            d["sys_settings"] = sysrow.get("settings") or {}
            d["pipeline_type"] = sysrow.get("pipeline_type")
            d["sys_tags"] = sysrow.get("tags") or {}
            if details:
                try:
                    d["spec"] = self.w.pipelines.get(d["pipeline_id"]).spec.as_dict()
                except Exception as e:  # noqa: BLE001
                    d["spec_error"] = str(e)[:300]
            yield d["pipeline_id"], d

    # ------------------------------------------------------------------ Unity Catalog
    def _catalogs(self) -> list[str]:
        cats = self.config.get("catalogs")
        if cats:
            return list(cats)
        exc = set(self.config.get("catalog_exclude") or []) | EXCLUDED_CATALOGS
        return [c.name for c in self.w.catalogs.list() if c.name and c.name not in exc]

    def _schemas(self, catalog: str) -> list[Any]:
        """Schemas of one catalog. Permission errors skip the catalog (with a warning) instead of failing the entity."""
        inc = self.config.get("schema_include") or ["*"]
        exc = self.config.get("schema_exclude") or ["information_schema"]
        try:
            return [s for s in self.w.schemas.list(catalog_name=catalog)
                    if any(fnmatch.fnmatch(s.name, p) for p in inc) and not any(fnmatch.fnmatch(s.name, p) for p in exc)]
        except Exception as e:  # noqa: BLE001
            self.warn(f"skipped catalog {catalog}: {str(e)[:200]}")
            return []

    def collect_schemas(self) -> Collected:
        for cat in self._catalogs():
            for s in self._schemas(cat):
                d = s.as_dict()
                yield d.get("full_name") or f"{cat}.{s.name}", d

    def _list_tables(self, catalog: str, schema: str) -> list[Any]:
        try:
            return list(self.w.tables.list(catalog_name=catalog, schema_name=schema))
        except Exception as e:  # noqa: BLE001
            self.warn(f"skipped schema {catalog}.{schema}: {str(e)[:200]}")
            return []

    def collect_tables(self) -> Collected:
        max_tables = int(self.config.get("max_tables", 500))
        n = 0
        for cat in self._catalogs():
            for s in self._schemas(cat):
                for t in self._list_tables(cat, s.name):
                    if n >= max_tables:
                        self.warn(f"max_tables={max_tables} reached; remaining tables skipped")
                        return
                    d = t.as_dict()
                    fmt = (d.get("data_source_format") or "").upper()
                    if fmt == "DELTA" and d.get("table_type") in ("MANAGED", "EXTERNAL"):
                        d["delta"] = self._delta_details(d["full_name"])
                    self._tables_seen.append(d["full_name"])
                    n += 1
                    yield d["full_name"], d

    def _delta_details(self, full_name: str) -> dict[str, Any]:
        q = quote_fqn(full_name)
        out: dict[str, Any] = {}
        try:
            rows = self._sql(f"DESCRIBE DETAIL {q}")
            out["detail"] = rows[0] if rows else {}
        except Exception as e:  # noqa: BLE001
            out["detail_error"] = str(e)[:500]
            return out
        try:
            limit = int(self.config.get("history_limit", 200))
            hist = self._sql(f"DESCRIBE HISTORY {q} LIMIT {limit}")
            out["history"] = [{k: h.get(k) for k in HISTORY_KEYS} for h in hist]
        except Exception as e:  # noqa: BLE001
            out["history_error"] = str(e)[:500]
        if self.config.get("collect_file_stats", False):
            size = out["detail"].get("sizeInBytes") or 0
            if size <= float(self.config.get("file_stats_max_bytes", 5e12)):
                try:
                    out["file_stats"] = self._file_stats(q, out["detail"].get("partitionColumns") or [])
                except Exception as e:  # noqa: BLE001
                    out["file_stats_error"] = str(e)[:500]
        return out

    def _file_stats(self, q: str, part_cols: list[str]) -> dict[str, Any]:
        """Exact file counts / small-file counts per partition using the _metadata column."""
        small = int(self.config.get("small_file_bytes", 32 * 1024 * 1024))
        cols = ", ".join(f"`{c}`" for c in part_cols)
        sel = f"{cols}, " if cols else ""
        grp = f"GROUP BY {cols}" if cols else ""
        rows = self._sql(
            f"""
            SELECT {sel}count(*) AS num_files, sum(file_size) AS size_bytes,
                   sum(CASE WHEN file_size < {small} THEN 1 ELSE 0 END) AS small_files,
                   max(mtime) AS last_modified
            FROM (SELECT DISTINCT {sel}_metadata.file_path AS file_path, _metadata.file_size AS file_size,
                         _metadata.file_modification_time AS mtime
                  FROM {q})
            {grp}"""
        )
        parts = [
            {
                "partition": {c: (None if r.get(c) is None else str(r.get(c))) for c in part_cols},
                "num_files": r["num_files"],
                "size_bytes": r["size_bytes"],
                "small_files": r["small_files"],
                "last_modified": r["last_modified"],
            }
            for r in rows
        ]
        return {
            "total_files": sum(p["num_files"] or 0 for p in parts),
            "small_files": sum(p["small_files"] or 0 for p in parts),
            "size_bytes": sum(p["size_bytes"] or 0 for p in parts),
            "partitions": parts if part_cols else [],
        }

    def collect_storage_stats(self) -> Collected:
        for loc in self.w.external_locations.list():
            d = loc.as_dict()
            yield d.get("name"), d

    # ------------------------------------------------------------------ system tables
    def _catalog_filter(self, col: str) -> str:
        cats = self.config.get("catalogs")
        if not cats:
            return ""
        return f" AND split_part({col}, '.', 1) IN ({', '.join(sql_literal(c) for c in cats)})"

    def collect_compute_metrics(self) -> Collected:
        """Per-cluster utilization profile from per-minute node metrics. Worker (w_) and driver (d_) stats are
        aggregated separately in one pass; the normalizer uses worker stats when the cluster has workers."""
        d = self.lookback_days
        idle = float(self.config.get("idle_cpu_pct", 5))
        cpu = "(cpu_user_percent + cpu_system_percent)"
        off = "(hour(start_time) < 7 OR hour(start_time) >= 19 OR dayofweek(start_time) IN (1, 7))"

        def side(prefix: str, cond: str) -> str:
            return f"""
                   sum(CASE WHEN {cond} THEN timestampdiff(SECOND, start_time, end_time) END) / 3600.0 AS {prefix}node_hours,
                   avg(CASE WHEN {cond} THEN {cpu} END)                                     AS {prefix}avg_cpu_pct,
                   percentile_approx(CASE WHEN {cond} THEN {cpu} END, 0.95)                AS {prefix}p95_cpu_pct,
                   avg(CASE WHEN {cond} THEN mem_used_percent END)                          AS {prefix}avg_mem_pct,
                   percentile_approx(CASE WHEN {cond} THEN mem_used_percent END, 0.95)     AS {prefix}p95_mem_pct,
                   avg(CASE WHEN {cond} THEN cpu_wait_percent END)                          AS {prefix}avg_cpu_wait_pct,
                   avg(CASE WHEN {cond} THEN CASE WHEN {cpu} < {idle} THEN 1.0 ELSE 0.0 END END) AS {prefix}idle_ratio,
                   avg(CASE WHEN {cond} AND {off} THEN CASE WHEN {cpu} < {idle} THEN 1.0 ELSE 0.0 END END) AS {prefix}offhours_idle_ratio,
                   avg(CASE WHEN {cond} THEN CASE WHEN {off} THEN 1.0 ELSE 0.0 END END)   AS {prefix}offhours_share,"""

        rows = self._sql(
            f"""
            SELECT workspace_id, cluster_id,
                   count(DISTINCT instance_id) AS instances,
                   count(DISTINCT CASE WHEN NOT driver THEN instance_id END) AS worker_instances,
                   {side('w_', 'NOT driver')}
                   {side('d_', 'driver')}
                   count(DISTINCT date_trunc('MINUTE', start_time)) / 60.0 AS active_hours,
                   min(start_time) AS period_start, max(end_time) AS period_end
            FROM system.compute.node_timeline
            WHERE end_time >= current_timestamp() - INTERVAL {d} DAYS {self._ws()}
            GROUP BY workspace_id, cluster_id"""
        )
        for r in rows:
            r["window_days"] = d
            yield r["cluster_id"], r

    # ------------------------------------------------------------------ run history + lineage
    def collect_job_run_stats(self) -> Collected:
        d = self.lookback_days
        win = f"period_end_time > current_timestamp() - INTERVAL {d} DAYS {self._ws()}"
        rows = self._sql(
            f"""
            WITH runs AS (
              SELECT workspace_id, job_id, run_id,
                     min(period_start_time) AS start_time, max(period_end_time) AS end_time,
                     max_by(result_state, period_end_time) AS result_state,
                     max_by(trigger_type, period_end_time) AS trigger_type,
                     max(run_duration_seconds) AS run_s, max(execution_duration_seconds) AS exec_s,
                     max(queue_duration_seconds) AS queue_s, max(setup_duration_seconds) AS setup_s,
                     CAST(sum(timestampdiff(SECOND, period_start_time, period_end_time)) AS DOUBLE) AS wall_s
              FROM system.lakeflow.job_run_timeline WHERE {win}
              GROUP BY ALL),
            task_attempts AS (
              SELECT workspace_id, job_id, job_run_id, task_key, count(DISTINCT run_id) AS attempts,
                     max_by(result_state, period_end_time) AS final_state
              FROM system.lakeflow.job_task_run_timeline WHERE {win}
              GROUP BY ALL),
            task_agg AS (
              SELECT workspace_id, job_id, count(*) AS task_runs, sum(attempts - 1) AS retries,
                     sum(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS retried_task_runs,
                     sum(CASE WHEN attempts > 1 AND final_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS retried_then_succeeded
              FROM task_attempts GROUP BY ALL),
            compute AS (
              SELECT workspace_id, job_id, slice(collect_set(cid), 1, 300) AS compute_ids
              FROM (SELECT workspace_id, job_id, explode(compute_ids) AS cid
                    FROM system.lakeflow.job_task_run_timeline WHERE {win})
              GROUP BY ALL),
            hours AS (
              SELECT workspace_id, job_id, map_from_entries(collect_list(struct(h, n))) AS start_hour_histogram
              FROM (SELECT workspace_id, job_id, hour(start_time) AS h, count(*) AS n FROM runs GROUP BY ALL)
              GROUP BY ALL),
            triggers AS (
              SELECT workspace_id, job_id, map_from_entries(collect_list(struct(coalesce(trigger_type, 'UNKNOWN'), n))) AS trigger_types
              FROM (SELECT workspace_id, job_id, trigger_type, count(*) AS n FROM runs GROUP BY ALL)
              GROUP BY ALL),
            agg AS (
              SELECT workspace_id, job_id, count(*) AS runs,
                     avg(CASE WHEN result_state IS NULL THEN NULL WHEN result_state = 'SUCCEEDED' THEN 1.0 ELSE 0.0 END) AS success_rate,
                     avg(CASE WHEN run_s > 0 THEN run_s ELSE wall_s END) AS avg_run_s,
                     stddev(CASE WHEN run_s > 0 THEN run_s ELSE wall_s END) AS std_run_s,
                     avg(CASE WHEN exec_s > 0 THEN exec_s END) AS avg_exec_s,
                     avg(CASE WHEN run_s > 0 THEN queue_s END) AS avg_queue_s,
                     avg(CASE WHEN run_s > 0 THEN setup_s END) AS avg_setup_s,
                     sum(CASE WHEN run_s > 0 THEN run_s ELSE wall_s END) / 3600.0 AS total_hours,
                     min(start_time) AS first_run, max(end_time) AS last_run
              FROM runs GROUP BY ALL)
            SELECT a.*, t.task_runs, t.retries, t.retried_task_runs, t.retried_then_succeeded,
                   c.compute_ids, h.start_hour_histogram, g.trigger_types
            FROM agg a
            LEFT JOIN task_agg t ON a.workspace_id = t.workspace_id AND a.job_id = t.job_id
            LEFT JOIN compute c ON a.workspace_id = c.workspace_id AND a.job_id = c.job_id
            LEFT JOIN hours h ON a.workspace_id = h.workspace_id AND a.job_id = h.job_id
            LEFT JOIN triggers g ON a.workspace_id = g.workspace_id AND a.job_id = g.job_id"""
        )
        for r in rows:
            r["window_days"] = d
            yield f"{r['workspace_id']}:{r['job_id']}", r

    def collect_pipeline_run_stats(self) -> Collected:
        d = int(self.config.get("pipeline_lookback_days", 90))
        rows = self._sql(
            f"""
            WITH u AS (
              SELECT workspace_id, pipeline_id, update_id,
                     min(period_start_time) AS start_time, max(period_end_time) AS end_time,
                     max_by(result_state, period_end_time) AS result_state,
                     max_by(update_type, period_end_time) AS update_type,
                     max_by(trigger_type, period_end_time) AS trigger_type,
                     max(CASE WHEN size(full_refresh_selection) > 0 THEN 1 ELSE 0 END) AS partial_full_refresh,
                     CAST(sum(timestampdiff(SECOND, period_start_time, period_end_time)) AS DOUBLE) AS wall_s
              FROM system.lakeflow.pipeline_update_timeline
              WHERE period_end_time > current_timestamp() - INTERVAL {d} DAYS {self._ws()}
              GROUP BY ALL),
            u2 AS (SELECT * FROM u WHERE coalesce(update_type, '') <> 'VALIDATE'),
            triggers AS (
              SELECT workspace_id, pipeline_id, map_from_entries(collect_list(struct(coalesce(trigger_type, 'UNKNOWN'), n))) AS trigger_types
              FROM (SELECT workspace_id, pipeline_id, trigger_type, count(*) AS n FROM u2 GROUP BY ALL) GROUP BY ALL),
            hours AS (
              SELECT workspace_id, pipeline_id, map_from_entries(collect_list(struct(h, n))) AS start_hour_histogram
              FROM (SELECT workspace_id, pipeline_id, hour(start_time) AS h, count(*) AS n FROM u2 GROUP BY ALL) GROUP BY ALL)
            SELECT a.*, t.trigger_types, h.start_hour_histogram FROM (
              SELECT workspace_id, pipeline_id, count(*) AS runs,
                     avg(CASE WHEN result_state IS NULL THEN NULL WHEN result_state = 'COMPLETED' THEN 1.0 ELSE 0.0 END) AS success_rate,
                     sum(CASE WHEN update_type = 'FULL_REFRESH' OR partial_full_refresh = 1 THEN 1 ELSE 0 END) AS full_refreshes,
                     avg(wall_s) AS avg_run_s, stddev(wall_s) AS std_run_s, sum(wall_s) / 3600.0 AS total_hours,
                     min(start_time) AS first_run, max(end_time) AS last_run
              FROM u2 GROUP BY ALL) a
            LEFT JOIN triggers t ON a.workspace_id = t.workspace_id AND a.pipeline_id = t.pipeline_id
            LEFT JOIN hours h ON a.workspace_id = h.workspace_id AND a.pipeline_id = h.pipeline_id"""
        )
        for r in rows:
            r["window_days"] = d
            yield f"{r['workspace_id']}:{r['pipeline_id']}", r

    def collect_lineage(self) -> Collected:
        """Observed sources/targets per job and pipeline (tables preferred, else storage paths)."""
        d = self.access_lookback_days
        rows = self._sql(
            f"""
            SELECT workspace_id, entity_type, entity_id,
                   slice(collect_set(coalesce(source_table_full_name, concat('path:', source_path))), 1, 500) AS sources,
                   slice(collect_set(coalesce(target_table_full_name, concat('path:', target_path))), 1, 500) AS targets,
                   max(event_time) AS last_event
            FROM system.access.table_lineage
            WHERE event_date >= date_sub(current_date(), {d}) AND entity_type IN ('JOB', 'PIPELINE')
              AND entity_id IS NOT NULL {self._ws()}
            GROUP BY ALL"""
        )
        for r in rows:
            yield f"{r['entity_type']}:{r['entity_id']}", r

    def collect_access_logs(self) -> Collected:
        d = self.access_lookback_days
        rows = self._sql(
            f"""
            WITH ev AS (
              SELECT source_table_full_name AS table_full_name, 1 AS r, 0 AS w,
                     event_time AS last_read, CAST(NULL AS TIMESTAMP) AS last_write, CAST(NULL AS STRING) AS job_id
              FROM system.access.table_lineage
              WHERE event_date >= date_sub(current_date(), {d}) AND source_table_full_name IS NOT NULL
              UNION ALL
              SELECT target_table_full_name, 0, 1, CAST(NULL AS TIMESTAMP), event_time,
                     CASE WHEN entity_type = 'JOB' THEN entity_id END
              FROM system.access.table_lineage
              WHERE event_date >= date_sub(current_date(), {d}) AND target_table_full_name IS NOT NULL
            )
            SELECT table_full_name, sum(r) AS reads, sum(w) AS writes,
                   max(last_read) AS last_read, max(last_write) AS last_write,
                   collect_set(job_id) AS writer_job_ids
            FROM ev WHERE 1 = 1 {self._catalog_filter('table_full_name')}
            GROUP BY table_full_name"""
        )
        scan = self._scan_stats([r["table_full_name"] for r in sorted(rows, key=lambda x: -(x["reads"] or 0))])
        for r in rows:
            r["window_days"] = d
            r.update(scan.get(r["table_full_name"], {}))
            yield r["table_full_name"], r

    def _scan_stats(self, tables: list[str]) -> dict[str, dict]:
        """Optional: pruning effectiveness per table from system.query.history (text match)."""
        if not self.config.get("collect_scan_stats", False):
            return {}
        out: dict[str, dict] = {}
        for t in tables[: int(self.config.get("scan_stats_max_tables", 50))]:
            try:
                r = self._sql(
                    f"""
                    SELECT count(*) AS scans,
                           avg(CASE WHEN read_files > 0 AND pruned_files = 0 THEN 1.0 ELSE 0.0 END) AS unpruned_scan_ratio
                    FROM system.query.history
                    WHERE start_time >= current_timestamp() - INTERVAL {self.lookback_days} DAYS
                      AND statement_type = 'SELECT'
                      AND lower(statement_text) LIKE {sql_literal('%' + t.lower() + '%')}"""
                )[0]
                out[t] = {"scans": r["scans"], "unpruned_scan_ratio": r["unpruned_scan_ratio"]}
            except Exception as e:  # noqa: BLE001
                log.warning("scan stats failed for %s: %s", t, e)
        return out

    _INCIDENT_STATES = "('CANCELED','CANCELLED','FAILED','ERROR','TIMED_OUT','TIMEDOUT')"

    def collect_costs(self) -> Collected:
        """Daily list cost per resource/SKU, with spend of canceled/failed pipeline updates and job runs split out
        (`incident_cost`) so FactStore can estimate a robust steady-state monthly cost."""
        d = self.lookback_days
        usage = f"""
            SELECT u.workspace_id, u.sku_name, u.billing_origin_product, u.usage_date,
                   coalesce(u.usage_metadata.job_id, u.usage_metadata.dlt_pipeline_id,
                            u.usage_metadata.warehouse_id, u.usage_metadata.cluster_id, 'unattributed') AS resource_id,
                   CASE WHEN u.usage_metadata.job_id IS NOT NULL THEN 'job'
                        WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'pipeline'
                        WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN 'warehouse'
                        WHEN u.usage_metadata.cluster_id IS NOT NULL THEN 'cluster'
                        ELSE 'other' END AS resource_type,
                   u.usage_metadata.job_id AS job_id, u.usage_metadata.job_run_id AS run_id,
                   u.usage_metadata.dlt_pipeline_id AS pipeline_id, u.usage_metadata.dlt_update_id AS update_id,
                   u.usage_quantity, u.usage_unit,
                   u.usage_quantity * p.pricing.effective_list.default AS list_cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
              ON u.sku_name = p.sku_name AND u.cloud = p.cloud
             AND u.usage_end_time >= p.price_start_time
             AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
            WHERE u.usage_date >= date_sub(current_date(), {d}) {self._ws('u.workspace_id')}"""
        agg = """u.workspace_id, u.sku_name, u.resource_id, u.resource_type, u.usage_date,
                   first(u.billing_origin_product) AS billing_origin_product,
                   sum(u.usage_quantity) AS usage_quantity, first(u.usage_unit) AS usage_unit, sum(u.list_cost) AS list_cost"""
        group = "GROUP BY u.workspace_id, u.sku_name, u.resource_id, u.resource_type, u.usage_date"
        try:
            rows = self._sql(
                f"""
                WITH u AS ({usage}),
                pu AS (SELECT workspace_id, pipeline_id, update_id, max_by(result_state, period_end_time) AS result_state
                       FROM system.lakeflow.pipeline_update_timeline
                       WHERE period_end_time >= date_sub(current_date(), {d + 1}) GROUP BY ALL),
                jr AS (SELECT workspace_id, job_id, run_id, max_by(result_state, period_end_time) AS result_state
                       FROM system.lakeflow.job_run_timeline
                       WHERE period_end_time >= date_sub(current_date(), {d + 1}) GROUP BY ALL)
                SELECT {agg},
                       sum(CASE WHEN upper(coalesce(pu.result_state, jr.result_state, '')) IN {self._INCIDENT_STATES}
                                THEN u.list_cost ELSE 0 END) AS incident_cost
                FROM u
                LEFT JOIN pu ON u.workspace_id = pu.workspace_id AND u.pipeline_id = pu.pipeline_id AND u.update_id = pu.update_id
                LEFT JOIN jr ON u.workspace_id = jr.workspace_id AND u.job_id = jr.job_id AND u.run_id = jr.run_id
                {group}"""
            )
        except Exception as e:  # noqa: BLE001 - lakeflow system tables not enabled: daily cost without incident split
            self.warn(f"run-state cost split unavailable ({str(e)[:200]}); using daily cost only")
            rows = self._sql(f"WITH u AS ({usage}) SELECT {agg}, 0.0 AS incident_cost FROM u {group}")
        for r in rows:
            yield f"{r['workspace_id']}:{r['resource_id']}:{r['sku_name']}:{r['usage_date']}", r
