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

    def supported_entities(self) -> set[str]:
        return {
            "workspaces", "clusters", "jobs", "tasks", "pipelines", "schemas", "tables",
            "storage_stats", "compute_metrics", "access_logs", "costs",
        }

    def test_connection(self) -> bool:
        self.w.current_user.me()
        return True

    # ------------------------------------------------------------------ workspace objects
    def collect_workspaces(self) -> Collected:
        ws_id = str(self.w.get_workspace_id())
        yield ws_id, {"workspace_id": ws_id, "host": self.w.config.host}

    def collect_clusters(self) -> Collected:
        skip_job = self.config.get("skip_job_clusters", True)
        for c in self.w.clusters.list():
            d = c.as_dict()
            if skip_job and d.get("cluster_source") == "JOB":
                continue
            yield d["cluster_id"], d

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

    def collect_pipelines(self) -> Collected:
        details = self.config.get("pipeline_details", False)
        for p in self.w.pipelines.list_pipelines():
            d = p.as_dict()
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
        return [c.name for c in self.w.catalogs.list() if c.name and c.name not in EXCLUDED_CATALOGS]

    def _schemas(self, catalog: str):
        inc = self.config.get("schema_include") or ["*"]
        exc = self.config.get("schema_exclude") or ["information_schema"]
        for s in self.w.schemas.list(catalog_name=catalog):
            if any(fnmatch.fnmatch(s.name, p) for p in inc) and not any(fnmatch.fnmatch(s.name, p) for p in exc):
                yield s

    def collect_schemas(self) -> Collected:
        for cat in self._catalogs():
            for s in self._schemas(cat):
                d = s.as_dict()
                yield d.get("full_name") or f"{cat}.{s.name}", d

    def collect_tables(self) -> Collected:
        max_tables = int(self.config.get("max_tables", 500))
        n = 0
        for cat in self._catalogs():
            for s in self._schemas(cat):
                for t in self.w.tables.list(catalog_name=cat, schema_name=s.name):
                    if n >= max_tables:
                        log.warning("max_tables=%s reached; remaining tables skipped", max_tables)
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
        d = self.lookback_days
        rows = self._sql(
            f"""
            SELECT cluster_id,
                   count(DISTINCT instance_id)                                  AS instances,
                   avg(cpu_user_percent + cpu_system_percent)                   AS avg_cpu_pct,
                   percentile_approx(cpu_user_percent + cpu_system_percent, 0.95) AS p95_cpu_pct,
                   avg(mem_used_percent)                                        AS avg_mem_pct,
                   min(start_time) AS period_start, max(end_time) AS period_end
            FROM system.compute.node_timeline
            WHERE start_time >= current_timestamp() - INTERVAL {d} DAYS
            GROUP BY cluster_id"""
        )
        for r in rows:
            yield r["cluster_id"], r

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

    def collect_costs(self) -> Collected:
        d = self.lookback_days
        rows = self._sql(
            f"""
            SELECT u.workspace_id, u.sku_name, u.billing_origin_product,
                   coalesce(u.usage_metadata.job_id, u.usage_metadata.dlt_pipeline_id,
                            u.usage_metadata.warehouse_id, u.usage_metadata.cluster_id, 'unattributed') AS resource_id,
                   CASE WHEN u.usage_metadata.job_id IS NOT NULL THEN 'job'
                        WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'pipeline'
                        WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN 'warehouse'
                        WHEN u.usage_metadata.cluster_id IS NOT NULL THEN 'cluster'
                        ELSE 'other' END AS resource_type,
                   min(u.usage_date) AS period_start, max(u.usage_date) AS period_end,
                   sum(u.usage_quantity) AS usage_quantity, first(u.usage_unit) AS usage_unit,
                   sum(u.usage_quantity * p.pricing.effective_list.default) AS list_cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
              ON u.sku_name = p.sku_name AND u.cloud = p.cloud
             AND u.usage_end_time >= p.price_start_time
             AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
            WHERE u.usage_date >= date_sub(current_date(), {d})
            GROUP BY ALL"""
        )
        for r in rows:
            yield f"{r['workspace_id']}:{r['resource_id']}:{r['sku_name']}", r
