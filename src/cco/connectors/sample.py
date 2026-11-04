"""Synthetic connector emitting Databricks-shaped payloads.

Used by tests and demos so the whole flow (ingest -> ... -> approval) runs without
any cloud credentials. Each sample table / cluster / job / pipeline is engineered to
trigger specific checks of agents A-D (see the scenario comments).
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
        return {"workspaces", "clusters", "pools", "jobs", "tasks", "pipelines", "tables", "access_logs", "costs",
                "compute_metrics", "job_run_stats", "pipeline_run_stats", "lineage"}

    # ------------------------------------------------------------------ helpers
    def _ago(self, days: float) -> str:
        return (self.now - timedelta(days=days)).isoformat()

    def _ms(self, days: float) -> int:
        return int((self.now - timedelta(days=days)).timestamp() * 1000)

    def _op(self, version: int, days_ago: float, operation: str, params: dict | None = None, **metrics: Any) -> dict:
        return {
            "version": version,
            "timestamp": self._ago(days_ago),
            "operation": operation,
            "operationParameters": params or {},
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
        hist = [self._op(60 - i, i, "WRITE", {"mode": "Overwrite"}, numFiles=50, numOutputBytes=int(50 * 128 * MB)) for i in range(30)]
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

    def _clusters(self) -> list[dict]:
        def c(cid, name, node, *, source="UI", workers=None, auto=None, at=None, avail=None, mode="USER_ISOLATION"):
            d = {"cluster_id": cid, "cluster_name": name, "node_type_id": node, "driver_node_type_id": node, "spark_version": "15.4.x-scala2.12",
                 "cluster_source": source, "state": "RUNNING", "data_security_mode": mode, "autotermination_minutes": at}
            if auto:
                d["autoscale"] = {"min_workers": auto[0], "max_workers": auto[1]}
            else:
                d["num_workers"] = workers
            if avail:
                d["aws_attributes"] = {"availability": avail, "first_on_demand": 1}
            return d

        return [
            # B: always-on, idle nights/weekends, no auto-termination, previous-gen nodes, low p95
            c("0101-allpurpose", "shared-analytics", "i3.2xlarge", auto=(2, 20), at=0),
            # B: fixed 8 workers, autoscaling disabled
            c("0102-ds-fixed", "ds-team-fixed", "m6i.4xlarge", workers=8, at=120),
            # B: autoscaling 4-4 (ineffective) + low concurrency; consolidation candidate with 0104
            c("0103-bi-adhoc", "bi-adhoc", "m6i.xlarge", auto=(4, 4), at=30),
            c("0104-eng-sandbox", "eng-sandbox", "m6i.2xlarge", auto=(1, 8), at=20),
            # job clusters
            c("job-1001-run", "job-1001-run", "m6i.large", source="JOB", workers=2, avail="SPOT_WITH_FALLBACK", mode="SINGLE_USER"),
            c("job-1002-run", "job-1002-run", "i3.4xlarge", source="JOB", workers=16, avail="ON_DEMAND", mode="SINGLE_USER"),
        ]

    def collect_clusters(self) -> Collected:
        for c in self._clusters():
            yield c["cluster_id"], c

    def collect_pools(self) -> Collected:
        # B: pool with warm idle capacity never used + two pools for the same node type
        for pid, name, idle_min, used, idle in (("pool-a", "warm-m6i-a", 4, 0, 4), ("pool-b", "warm-m6i-b", 2, 3, 2)):
            yield pid, {"instance_pool_id": pid, "instance_pool_name": name, "node_type_id": "m6i.xlarge", "min_idle_instances": idle_min,
                        "idle_instance_autotermination_minutes": 60, "stats": {"used_count": used, "idle_count": idle}}

    def _jobs(self) -> list[dict]:
        return [
            {"job_id": 1001, "creator_user_name": "etl@demo.com", "settings": {"name": "sales_ingest", "schedule": {"quartz_cron_expression": "0 */5 * * * ?"},
             "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": "/ingest"}, "job_cluster_key": "jc"}]}},
            {"job_id": 1002, "creator_user_name": "etl@demo.com", "settings": {"name": "clickstream_merge", "schedule": {"quartz_cron_expression": "0 0 * * * ?"},
             "tasks": [{"task_key": "merge", "spark_python_task": {"python_file": "merge.py"}, "job_cluster_key": "jc"}]}},
            {"job_id": 1003, "creator_user_name": "ops@demo.com", "settings": {"name": "audit_export", "schedule": {"quartz_cron_expression": "0 0 2 * * ?"},
             "tasks": [{"task_key": "export", "sql_task": {"query": {"query_id": "q1"}}, "existing_cluster_id": "0101-allpurpose"}]}},
            *self._extra_jobs(),
        ]

    def _extra_jobs(self) -> list[dict]:
        def nb(path, **params):
            return {"task_key": "main", "notebook_task": {"notebook_path": path, "base_parameters": params}}

        def job(jid, name, cron, tasks, creator="etl@demo.com"):
            return {"job_id": jid, "creator_user_name": creator,
                    "settings": {"name": name, "schedule": {"quartz_cron_expression": cron} if cron else None, "tasks": tasks}}

        snap = "/Workspace/Users/{u}/.bundle/dtc_etl/{t}/files/src/customer_snapshot"
        return [
            # C: same logic deployed three times (prod + two personal dev bundle copies)
            job(1004, "[dev alice] customer_snapshot", "0 0 6 * * ?", [nb(snap.format(u="alice@demo.com", t="dev"), env="{{job.parameters.env}}")], "alice@demo.com"),
            job(1005, "[dev bob] customer_snapshot", "0 0 6 * * ?", [nb(snap.format(u="bob@demo.com", t="dev"), env="{{job.parameters.env}}")], "bob@demo.com"),
            job(1006, "customer_snapshot", "0 0 6 * * ?", [nb(snap.format(u="sp-deployer", t="prod"), env="{{job.parameters.env}}")], "sp-deployer"),
            # C: downstream of sales_ingest on a parallel hourly clock; re-reads unchanged inputs
            job(1007, "store_kpi_rollup", "0 0 * * * ?", [nb("/Repos/etl/kpi/store_kpi_rollup")]),
            # C: same source -> target lineage as sales_ingest (1001)
            job(1008, "sales_ingest_v2", "0 */10 * * * ?", [nb("/Repos/etl/ingest/sales_ingest_v2")]),
            # C: rebuilds inventory_snapshot (overwrite) every night in the busiest window, queues
            job(1009, "inventory_full_rebuild", "0 0 2 * * ?", [nb("/Repos/etl/inventory/full_rebuild")]),
            # D: triggers pipeline p-001 hourly
            job(1010, "bronze_events_trigger", "0 0 * * * ?", [{"task_key": "refresh", "pipeline_task": {"pipeline_id": "p-001"}}]),
        ]

    def collect_jobs(self) -> Collected:
        for j in self._jobs():
            yield j["job_id"], j

    def collect_tasks(self) -> Collected:
        for j in self._jobs():
            for task in j["settings"]["tasks"]:
                yield f"{j['job_id']}:{task['task_key']}", {**task, "job_id": j["job_id"]}

    def collect_pipelines(self) -> Collected:
        lib = "/Workspace/Users/{u}/.bundle/etl/{t}/files/pipelines/{n}"

        def p(pid, name, libpath, continuous=False, dev=False):
            return pid, {"pipeline_id": pid, "name": name, "state": "IDLE", "creator_user_name": "etl@demo.com", "pipeline_type": "ETL_PIPELINE",
                         "spec": {"libraries": [{"notebook": {"path": libpath}}], "continuous": continuous, "development": dev,
                                  "serverless": True, "catalog": "demo"}}

        yield p("p-001", "bronze_events", lib.format(u="sp-deployer", t="prod", n="bronze_events"))      # D: too frequent + full refreshes
        yield p("p-002", "[dev carol] bronze_events", lib.format(u="carol@demo.com", t="dev", n="bronze_events"), dev=True)  # D: duplicate
        yield p("p-003", "legacy_feed", lib.format(u="sp-deployer", t="prod", n="legacy_feed"))          # D: inactive
        yield p("p-004", "marketing_attribution", lib.format(u="sp-deployer", t="prod", n="attribution"), continuous=True)  # D: unused + continuous
        yield p("p-005", "orders_ingest", lib.format(u="sp-deployer", t="prod", n="orders_ingest"))      # D: duplicate ingestion
        yield p("p-006", "store_metrics", lib.format(u="sp-deployer", t="prod", n="store_metrics"))      # D: critical + unstable

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
            # upstream / downstream tables of the workload scenarios
            ("demo.web.raw_clicks", 100, 30, 1, 3, [], None, None),
            ("demo.web.bronze_events", 500, 720, 0.1, 0.05, [], None, None),
            ("demo.mkt.attribution", 0, 300, None, 0.1, [], None, None),
            ("demo.store.kpi_rollup", 300, 720, 0.2, 0.05, ["1007"], None, None),
            ("demo.sales.orders_bronze", 100, 30, 1, 1, [], None, None),
            ("demo.store.store_metrics", 5000, 30, 0.01, 1, [], None, None),
        ]
        for name, reads, writes, last_read, last_write, jobs, scans, unpruned in rows:
            yield name, {
                "table_full_name": name, "reads": reads, "writes": writes,
                "last_read": self._ago(last_read) if last_read is not None else None,
                "last_write": self._ago(last_write), "writer_job_ids": jobs, "window_days": 90,
                "scans": scans, "unpruned_scan_ratio": unpruned,
            }

    def collect_costs(self) -> Collected:
        costs = (("1001", "job", 1200.0), ("1002", "job", 4500.0), ("1003", "job", 800.0), ("0101-allpurpose", "cluster", 2000.0),
                 ("0102-ds-fixed", "cluster", 1500.0), ("0103-bi-adhoc", "cluster", 300.0), ("0104-eng-sandbox", "cluster", 250.0),
                 ("1004", "job", 350.0), ("1005", "job", 300.0), ("1006", "job", 400.0), ("1007", "job", 450.0),
                 ("1008", "job", 600.0), ("1009", "job", 900.0), ("1010", "job", 50.0),
                 ("p-001", "pipeline", 1500.0), ("p-002", "pipeline", 400.0), ("p-004", "pipeline", 2500.0),
                 ("p-005", "pipeline", 700.0), ("p-006", "pipeline", 300.0))
        for rid, rtype, amount in costs:
            yield f"{self.ws}:{rid}:JOBS_COMPUTE", {
                "workspace_id": self.ws, "sku_name": "PREMIUM_JOBS_COMPUTE", "resource_id": rid, "resource_type": rtype,
                "period_start": self._ago(30), "period_end": self._ago(0), "usage_quantity": amount / 0.15,
                "usage_unit": "DBU", "list_cost": amount,
            }

    def collect_compute_metrics(self) -> Collected:
        # (cluster, avg_cpu, p95_cpu, avg_mem, p95_mem, idle, offhours_idle, offhours_share, worker_node_h, driver_node_h, active_h)
        rows = (
            ("0101-allpurpose", 14, 30, 33, 45, 0.62, 0.85, 0.45, 3000, 700, 700),
            ("0102-ds-fixed", 18, 55, 40, 60, 0.30, 0.40, 0.10, 1600, 200, 200),
            ("0103-bi-adhoc", 12, 35, 30, 45, 0.40, 0.50, 0.10, 400, 100, 100),
            ("0104-eng-sandbox", 9, 25, 25, 35, 0.45, 0.50, 0.10, 300, 80, 80),
            ("job-1001-run", 45, 75, 50, 70, 0.20, 0.20, 0.30, 1300, 650, 650),
            ("job-1002-run", 15, 30, 35, 45, 0.55, 0.55, 0.30, 2880, 180, 180),
        )
        for cid, cpu, p95, mem, p95m, idle, oidle, oshare, wh, dh, ah in rows:
            yield cid, {"cluster_id": cid, "w_avg_cpu_pct": cpu, "w_p95_cpu_pct": p95, "w_avg_mem_pct": mem, "w_p95_mem_pct": p95m,
                        "w_idle_ratio": idle, "w_offhours_idle_ratio": oidle, "w_offhours_share": oshare, "w_node_hours": wh,
                        "d_node_hours": dh, "active_hours": ah, "window_days": 30, "instances": 12,
                        "period_start": self._ago(30), "period_end": self._ago(0)}

    # ------------------------------------------------------------------ run history + lineage
    @staticmethod
    def _uniform(per_hour: int) -> dict[int, int]:
        return {h: per_hour for h in range(24)}

    def _stats(self, *, runs, window=30, success=1.0, avg_run=600.0, std=60.0, exec_s=None, queue=0.0, setup=30.0,
               hist=None, compute=(), task_runs=None, retries=0, retried=0, retried_ok=0, full_refreshes=0,
               last_days=0.05, trigger="PERIODIC") -> dict:
        return {"runs": runs, "window_days": window, "success_rate": success, "avg_run_s": avg_run, "std_run_s": std,
                "avg_exec_s": exec_s if exec_s is not None else avg_run - setup, "avg_queue_s": queue, "avg_setup_s": setup,
                "total_hours": runs * avg_run / 3600, "first_run": self._ago(window), "last_run": self._ago(last_days),
                "task_runs": task_runs if task_runs is not None else runs, "retries": retries, "retried_task_runs": retried,
                "retried_then_succeeded": retried_ok, "full_refreshes": full_refreshes, "trigger_types": {trigger: runs},
                "start_hour_histogram": hist or {}, "compute_ids": list(compute), "workspace_id": self.ws}

    def collect_job_run_stats(self) -> Collected:
        s = self._stats
        rows = {
            1001: s(runs=8640, avg_run=280, setup=120, exec_s=150, hist=self._uniform(360), compute=["job-1001-run"]),
            1002: s(runs=720, success=0.97, avg_run=900, setup=60, hist=self._uniform(30), compute=["job-1002-run"],
                    task_runs=900, retries=200, retried=150, retried_ok=15),
            1003: s(runs=30, success=0.6, avg_run=1800, hist={2: 30}, compute=["0101-allpurpose"]),
            1004: s(runs=30, hist={6: 30}), 1005: s(runs=25, hist={6: 25}), 1006: s(runs=30, hist={6: 30}),
            1007: s(runs=720, avg_run=300, hist=self._uniform(30)),
            1008: s(runs=4320, avg_run=200, hist=self._uniform(180)),
            1009: s(runs=30, avg_run=3600, std=4500, queue=400, hist={2: 30}),
            1010: s(runs=720, avg_run=120, hist=self._uniform(30)),
        }
        for jid, st in rows.items():
            yield str(jid), {"job_id": jid, **st}

    def collect_pipeline_run_stats(self) -> Collected:
        s = self._stats
        rows = {
            "p-001": s(runs=720, success=0.98, full_refreshes=300, hist=self._uniform(30), trigger="JOB_TASK"),
            "p-002": s(runs=100, hist={9: 100}, trigger="USER_ACTION"),
            "p-003": s(runs=2, window=180, last_days=120, trigger="USER_ACTION"),
            "p-004": s(runs=1, avg_run=30 * 86400, trigger="USER_ACTION"),
            "p-005": s(runs=30, hist={1: 30}),
            "p-006": s(runs=30, success=0.8, hist={3: 30}),
        }
        for pid, st in rows.items():
            yield pid, {"pipeline_id": pid, **st}

    def collect_lineage(self) -> Collected:
        land = "path:/Volumes/demo/raw/landing"
        rows = (
            ("JOB", "1001", [f"{land}/sales/2026/10/22/batch_001.json"], ["demo.sales.fact_transactions"]),
            ("JOB", "1008", [f"{land}/sales/2026/10/22/batch_001.json"], ["demo.sales.fact_transactions"]),
            ("JOB", "1002", ["demo.web.raw_clicks"], ["demo.web.clickstream_events"]),
            ("JOB", "1003", ["demo.web.clickstream_events"], ["demo.ops.audit_write_only"]),
            ("JOB", "1007", ["demo.sales.fact_transactions"], ["demo.store.kpi_rollup"]),
            ("JOB", "1009", ["demo.sales.fact_transactions"], ["demo.store.inventory_snapshot"]),
            ("PIPELINE", "p-001", [f"{land}/events/2026/10/23/a.json", "demo.web.raw_clicks"], ["demo.web.bronze_events"]),
            ("PIPELINE", "p-004", ["demo.sales.fact_transactions"], ["demo.mkt.attribution"]),
            ("PIPELINE", "p-005", [f"{land}/sales/2026/10/23/orders.json"], ["demo.sales.orders_bronze"]),
            ("PIPELINE", "p-006", ["demo.sales.fact_transactions"], ["demo.store.store_metrics"]),
        )
        for etype, eid, src, tgt in rows:
            yield f"{etype}:{eid}", {"entity_type": etype, "entity_id": eid, "sources": src, "targets": tgt,
                                     "last_event": self._ago(0.1), "workspace_id": self.ws}
