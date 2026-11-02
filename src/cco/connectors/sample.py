"""Synthetic connector emitting Databricks-shaped payloads.

Used by tests and demos so the whole flow (ingest -> ... -> approval) runs without
any cloud credentials. Each sample table is engineered to trigger specific checks.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from cco.connectors.base import BaseConnector, Collected
from cco.models.canonical import Provider
from cco.utils import GB, MB, utcnow


class SampleConnector(BaseConnector):
    name = "sample"
    provider = Provider.DATABRICKS

    def __init__(self, config: dict | None = None, now: datetime | None = None) -> None:
        super().__init__(config)
        self.now = now or utcnow()
        self.ws = "1234567890123456"

    def supported_entities(self) -> set[str]:
        return {"workspaces", "clusters", "jobs", "tasks", "pipelines", "tables", "access_logs", "costs", "compute_metrics"}

    # ------------------------------------------------------------------ helpers
    def _ago(self, days: float) -> str:
        return (self.now - timedelta(days=days)).isoformat()

    def _ms(self, days: float) -> int:
        return int((self.now - timedelta(days=days)).timestamp() * 1000)

    def _op(self, version: int, days_ago: float, operation: str, **metrics: Any) -> dict:
        return {
            "version": version,
            "timestamp": self._ago(days_ago),
            "operation": operation,
            "operationParameters": {},
            "operationMetrics": {k: str(v) for k, v in metrics.items()},
        }

    def _table(self, full_name: str, *, size: float, files: int, columns: int = 20, partition_cols=(),
               partitions: list | None = None, small_files: int | None = None, history: list | None = None,
               props: dict | None = None, created_days: int = 400, modified_days: float = 1) -> dict:
        cat, sch, name = full_name.split(".")
        loc = f"s3://demo-lakehouse/{cat}/{sch}/{name}"
        detail = {
            "format": "delta", "name": full_name, "location": loc,
            "createdAt": self._ago(created_days), "lastModified": self._ago(modified_days),
            "partitionColumns": list(partition_cols), "clusteringColumns": [],
            "numFiles": files, "sizeInBytes": int(size), "properties": props or {},
        }
        delta: dict[str, Any] = {"detail": detail, "history": history or []}
        if partitions is not None or small_files is not None:
            delta["file_stats"] = {"total_files": files, "small_files": small_files, "size_bytes": int(size), "partitions": partitions or []}
        return {
            "full_name": full_name, "catalog_name": cat, "schema_name": sch, "name": name,
            "table_type": "MANAGED", "data_source_format": "DELTA", "storage_location": loc, "owner": "data-engineering",
            "columns": [{"name": f"col_{i}", "type_name": "STRING"} for i in range(columns)],
            "created_at": self._ms(created_days), "updated_at": self._ms(modified_days), "properties": {}, "delta": delta,
        }

    # ------------------------------------------------------------------ tables
    def _tables(self) -> list[dict]:
        t: list[dict] = []

        # 1. Streaming-style appends -> thousands of tiny files, hot partitions
        parts = [{"partition": {"sale_date": (self.now - timedelta(days=i)).date().isoformat()}, "num_files": 1200,
                  "small_files": 1150, "size_bytes": int(1200 * 1.5 * MB), "last_modified": self._ago(i)} for i in range(10)]
        hist = [self._op(100 - i, i * 0.5, "WRITE", numFiles=200, numOutputBytes=int(200 * 1.5 * MB), numOutputRows=50000) for i in range(60)]
        t.append(self._table("demo.sales.fact_transactions", size=12000 * 1.5 * MB, files=12000, partition_cols=["sale_date"],
                             partitions=parts, small_files=11500, history=hist))

        # 2. Cold legacy table: unread, unmodified, partitions beyond retention
        parts = [{"partition": {"order_month": f"m{i:03d}"}, "num_files": 100, "small_files": 0,
                  "size_bytes": int(400 * GB / 36), "last_modified": self._ago(400 + 30 * i)} for i in range(36)]
        t.append(self._table("demo.archive.legacy_orders", size=400 * GB, files=3600, partition_cols=["order_month"],
                             partitions=parts, small_files=0, created_days=1500, modified_days=400,
                             history=[self._op(5, 400, "WRITE", numFiles=100, numOutputBytes=int(11 * GB))]))

        # 3. Heavy MERGE target: write amplification, tombstones, long retention, unpruned scans
        hist = [self._op(500 - i, i * 3, "MERGE", numTargetFilesAdded=400, numTargetFilesRemoved=392,
                         numTargetBytesAdded=int(50 * GB), numTargetBytesRemoved=int(49 * GB)) for i in range(40)]
        t.append(self._table("demo.web.clickstream_events", size=2048 * GB, files=16000, history=hist,
                             props={"delta.deletedFileRetentionDuration": "interval 90 days"}))

        # 4. Partition skew: one region holds ~93% of the data
        parts = [{"partition": {"store_region": "WEST"}, "num_files": 900, "small_files": 0, "size_bytes": int(900 * GB), "last_modified": self._ago(1)}]
        parts += [{"partition": {"store_region": f"R{i}"}, "num_files": 80, "small_files": 0, "size_bytes": int(10 * GB),
                   "last_modified": self._ago(1)} for i in range(7)]
        hist = [self._op(60 - i, i, "WRITE", numFiles=50, numOutputBytes=int(50 * 128 * MB)) for i in range(30)]
        t.append(self._table("demo.store.inventory_snapshot", size=970 * GB, files=1460, partition_cols=["store_region"],
                             partitions=parts, small_files=0, history=hist))

        # 5. Healthy dimension - should produce no findings
        hist = [self._op(30 - i, i, "WRITE", numFiles=1, numOutputBytes=int(100 * MB)) for i in range(30)]
        t.append(self._table("demo.product.dim_product", size=2 * GB, files=20, history=hist))

        # 6. Wide table, auto-optimize already on, still small files
        hist = [self._op(90 - i, i, "WRITE", numFiles=20, numOutputBytes=int(20 * 4 * MB)) for i in range(30)]
        t.append(self._table("demo.customer.customer_360_wide", size=5000 * 4 * MB, files=5000, columns=250, small_files=4800, history=hist,
                             props={"delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "true"}))

        # 7. Written daily by a job, never read
        hist = [self._op(200 - i, i, "WRITE", numFiles=13, numOutputBytes=int(1.6 * GB)) for i in range(30)]
        t.append(self._table("demo.ops.audit_write_only", size=50 * GB, files=400, history=hist, created_days=200))

        # 8. Retention below safe minimum
        t.append(self._table("demo.tmp.scratch_short_retention", size=5 * GB, files=50,
                             props={"delta.deletedFileRetentionDuration": "interval 1 days"},
                             history=[self._op(3, 2, "WRITE", numFiles=10, numOutputBytes=int(1 * GB))]))
        return t

    # ------------------------------------------------------------------ collectors
    def collect_workspaces(self) -> Collected:
        yield self.ws, {"workspace_id": self.ws, "host": "https://demo.cloud.databricks.com"}

    def collect_clusters(self) -> Collected:
        yield "0101-allpurpose", {"cluster_id": "0101-allpurpose", "cluster_name": "shared-analytics", "node_type_id": "i3.2xlarge",
                                  "driver_node_type_id": "i3.2xlarge", "autoscale": {"min_workers": 2, "max_workers": 20},
                                  "autotermination_minutes": 0, "spark_version": "15.4.x-scala2.12", "cluster_source": "UI",
                                  "state": "RUNNING", "custom_tags": {"team": "analytics"}}

    def _jobs(self) -> list[dict]:
        return [
            {"job_id": 1001, "creator_user_name": "etl@demo.com", "settings": {"name": "sales_ingest", "schedule": {"quartz_cron_expression": "0 */5 * * * ?"},
             "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": "/ingest"}, "job_cluster_key": "jc"}]}},
            {"job_id": 1002, "creator_user_name": "etl@demo.com", "settings": {"name": "clickstream_merge", "schedule": {"quartz_cron_expression": "0 0 * * * ?"},
             "tasks": [{"task_key": "merge", "spark_python_task": {"python_file": "merge.py"}, "job_cluster_key": "jc"}]}},
            {"job_id": 1003, "creator_user_name": "ops@demo.com", "settings": {"name": "audit_export", "schedule": {"quartz_cron_expression": "0 0 2 * * ?"},
             "tasks": [{"task_key": "export", "sql_task": {"query": {"query_id": "q1"}}, "existing_cluster_id": "0101-allpurpose"}]}},
        ]

    def collect_jobs(self) -> Collected:
        for j in self._jobs():
            yield j["job_id"], j

    def collect_tasks(self) -> Collected:
        for j in self._jobs():
            for task in j["settings"]["tasks"]:
                yield f"{j['job_id']}:{task['task_key']}", {**task, "job_id": j["job_id"]}

    def collect_pipelines(self) -> Collected:
        yield "p-001", {"pipeline_id": "p-001", "name": "bronze_events", "state": "IDLE", "creator_user_name": "etl@demo.com"}

    def collect_tables(self) -> Collected:
        for t in self._tables():
            yield t["full_name"], t

    def collect_access_logs(self) -> Collected:
        rows = [
            ("demo.sales.fact_transactions", 900, 60, 0.2, 0.1, ["1001"], 300, 0.45),
            ("demo.web.clickstream_events", 2000, 40, 0.1, 0.5, ["1002"], 500, 0.7),
            ("demo.store.inventory_snapshot", 400, 30, 0.5, 1, [], 100, 0.2),
            ("demo.product.dim_product", 5000, 30, 0.01, 1, [], 1000, 0.05),
            ("demo.customer.customer_360_wide", 800, 30, 0.3, 1, [], 200, 0.1),
            ("demo.ops.audit_write_only", 0, 30, None, 1, ["1003"], 0, None),
            ("demo.tmp.scratch_short_retention", 50, 10, 1, 2, [], 10, 0.0),
        ]
        for name, reads, writes, last_read, last_write, jobs, scans, unpruned in rows:
            yield name, {
                "table_full_name": name, "reads": reads, "writes": writes,
                "last_read": self._ago(last_read) if last_read is not None else None,
                "last_write": self._ago(last_write), "writer_job_ids": jobs, "window_days": 90,
                "scans": scans, "unpruned_scan_ratio": unpruned,
            }

    def collect_costs(self) -> Collected:
        for rid, rtype, amount in (("1001", "job", 1200.0), ("1002", "job", 4500.0), ("1003", "job", 800.0), ("0101-allpurpose", "cluster", 2000.0)):
            yield f"{self.ws}:{rid}:JOBS_COMPUTE", {
                "workspace_id": self.ws, "sku_name": "PREMIUM_JOBS_COMPUTE", "resource_id": rid, "resource_type": rtype,
                "period_start": self._ago(30), "period_end": self._ago(0), "usage_quantity": amount / 0.15,
                "usage_unit": "DBU", "list_cost": amount,
            }

    def collect_compute_metrics(self) -> Collected:
        yield "0101-allpurpose", {"cluster_id": "0101-allpurpose", "instances": 12, "avg_cpu_pct": 14.0, "p95_cpu_pct": 41.0,
                                  "avg_mem_pct": 33.0, "period_start": self._ago(30), "period_end": self._ago(0)}
