"""ETL / normalization: raw connector payloads -> canonical objects.

Normalizers are registered per (provider, entity). Adding a source = adding
functions here (or passing `extra` to Normalizer) - nothing downstream changes.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from cco.connectors.base import RawRecord
from cco.etl.fingerprint import logical_name, logical_path, task_fingerprint, task_source
from cco.models.canonical import (
    Account, CanonicalObject, Cluster, ComputePool, CostMetric, Job, Lineage, PartitionStats, Pipeline, Provider,
    RunStats, StorageObject, Table, TableOperation, Task, UsageMetric, Workspace, stable_id,
)
from cco.utils import to_dt, to_float, to_int

_JOB_SCOPED = re.compile(r"\{\{\s*job\.(parameters|name|id)")

log = logging.getLogger(__name__)

NormalizerFn = Callable[[RawRecord], Iterable[CanonicalObject]]
_REGISTRY: dict[tuple[str, str], NormalizerFn] = {}

TASK_TYPES = ("notebook_task", "spark_python_task", "python_wheel_task", "spark_jar_task", "pipeline_task",
              "sql_task", "dbt_task", "run_job_task", "for_each_task", "condition_task", "spark_submit_task")


def normalizes(provider: str, entity: str) -> Callable[[NormalizerFn], NormalizerFn]:
    def deco(fn: NormalizerFn) -> NormalizerFn:
        _REGISTRY[(provider, entity)] = fn
        return fn

    return deco


@dataclass
class NormalizationResult:
    objects: list[CanonicalObject] = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)
    skipped: Counter = field(default_factory=Counter)
    errors: list[dict] = field(default_factory=list)


class Normalizer:
    def __init__(self, extra: dict[tuple[str, str], NormalizerFn] | None = None) -> None:
        self.registry = {**_REGISTRY, **(extra or {})}

    def normalize(self, records: Iterable[RawRecord]) -> NormalizationResult:
        res = NormalizationResult()
        for rec in records:
            fn = self.registry.get((rec.provider, rec.entity))
            if fn is None:
                res.skipped[f"{rec.provider}.{rec.entity}"] += 1
                continue
            try:
                for obj in fn(rec):
                    res.objects.append(obj)
                    res.counts[obj.OBJECT_TYPE] += 1
            except Exception as e:  # noqa: BLE001
                res.errors.append({"provider": rec.provider, "entity": rec.entity, "source_id": rec.source_id, "error": str(e)[:300]})
        return res


def _metric(provider: Provider, resource_id: str, resource_type: str, name: str, value: Any, **kw: Any) -> UsageMetric:
    return UsageMetric(
        id=stable_id(provider.value, resource_id, name), provider=provider, name=name, resource_id=resource_id,
        resource_type=resource_type, metric_name=name, value=to_float(value, 0.0), **kw,
    )


# ============================================================================ Databricks
DBX = Provider.DATABRICKS


@normalizes("databricks", "workspaces")
def _dbx_workspace(r: RawRecord):
    p = r.payload
    yield Workspace(id=str(p["workspace_id"]), provider=DBX, name=p.get("host") or str(p["workspace_id"]), url=p.get("host"))


def _availability(p: dict) -> str | None:
    for k in ("aws_attributes", "azure_attributes", "gcp_attributes"):
        a = p.get(k) or {}
        if a.get("availability"):
            return str(a["availability"])
    return None


def _cluster_spec(nc: dict) -> dict[str, Any]:
    """Compact view of a new_cluster spec (job clusters)."""
    auto = nc.get("autoscale") or {}
    return {
        "node_type": nc.get("node_type_id"), "driver_node_type": nc.get("driver_node_type_id"),
        "num_workers": nc.get("num_workers"), "min_workers": auto.get("min_workers"), "max_workers": auto.get("max_workers"),
        "availability": _availability(nc), "pool_id": nc.get("instance_pool_id"), "runtime": nc.get("spark_version"),
        "photon": nc.get("runtime_engine") == "PHOTON" if nc.get("runtime_engine") else None,
    }


@normalizes("databricks", "clusters")
def _dbx_cluster(r: RawRecord):
    p = r.payload
    if p.get("_source") == "system":  # system.compute.clusters shape
        aws = p.get("aws_attributes") or {}
        yield Cluster(
            id=p["cluster_id"], provider=DBX, name=p.get("cluster_name") or "", workspace_id=str(p.get("workspace_id") or "") or None,
            tags=p.get("tags") or {}, node_type=p.get("worker_node_type"), driver_node_type=p.get("driver_node_type"),
            num_workers=to_int(p.get("worker_count")), min_workers=to_int(p.get("min_autoscale_workers")),
            max_workers=to_int(p.get("max_autoscale_workers")), autotermination_minutes=to_int(p.get("auto_termination_minutes")),
            runtime_version=p.get("dbr_version"), cluster_source=p.get("cluster_source"), policy_id=p.get("policy_id"),
            owner=p.get("owned_by"), availability=_availability(p), driver_pool_id=p.get("driver_instance_pool_id"),
            worker_pool_id=p.get("worker_instance_pool_id"), worker_cores=to_float(p.get("worker_cores")),
            worker_memory_mb=to_float(p.get("worker_memory_mb")), deleted=p.get("delete_time") is not None,
            attributes={"data_security_mode": p.get("data_security_mode"), "first_on_demand": aws.get("first_on_demand")},
        )
        return
    auto = p.get("autoscale") or {}
    yield Cluster(
        id=p["cluster_id"], provider=DBX, name=p.get("cluster_name", ""), tags=p.get("custom_tags") or {},
        node_type=p.get("node_type_id"), driver_node_type=p.get("driver_node_type_id"), num_workers=to_int(p.get("num_workers")),
        min_workers=to_int(auto.get("min_workers")), max_workers=to_int(auto.get("max_workers")),
        autotermination_minutes=to_int(p.get("autotermination_minutes")), runtime_version=p.get("spark_version"),
        cluster_source=p.get("cluster_source"), state=p.get("state"), policy_id=p.get("policy_id"),
        photon=(p.get("runtime_engine") == "PHOTON") if p.get("runtime_engine") else None,
        owner=p.get("creator_user_name"), availability=_availability(p), driver_pool_id=p.get("driver_instance_pool_id"),
        worker_pool_id=p.get("instance_pool_id"), worker_cores=to_float(p.get("worker_cores")),
        worker_memory_mb=to_float(p.get("worker_memory_mb")),
        attributes={"data_security_mode": p.get("data_security_mode"), "first_on_demand": (p.get("aws_attributes") or {}).get("first_on_demand")},
    )


@normalizes("databricks", "pools")
def _dbx_pool(r: RawRecord):
    p = r.payload
    st = p.get("stats") or {}
    yield ComputePool(
        id=p["instance_pool_id"], provider=DBX, name=p.get("instance_pool_name", ""), node_type=p.get("node_type_id"),
        min_idle=to_int(p.get("min_idle_instances"), 0), max_capacity=to_int(p.get("max_capacity")),
        idle_autotermination_minutes=to_int(p.get("idle_instance_autotermination_minutes")),
        used_count=to_int(st.get("used_count"), 0) + to_int(st.get("pending_used_count"), 0),
        idle_count=to_int(st.get("idle_count"), 0) + to_int(st.get("pending_idle_count"), 0),
        availability=_availability(p), tags=p.get("custom_tags") or {},
    )


def _trigger_kind(s: dict) -> str:
    trig = s.get("trigger") or {}
    if trig.get("file_arrival"):
        return "file_arrival"
    if trig.get("table_update") or trig.get("table"):
        return "table_update"
    if trig.get("periodic"):
        return "periodic"
    if s.get("continuous"):
        return "continuous"
    if s.get("schedule"):
        return "cron"
    return "manual"   # API / external scheduler (e.g. enterprise orchestrators)


@normalizes("databricks", "jobs")
def _dbx_job(r: RawRecord):
    p = r.payload
    s = p.get("settings") or {}
    sched = s.get("schedule") or {}
    tasks = s.get("tasks") or []
    pause = sched.get("pause_status") or (s.get("trigger") or {}).get("pause_status") or (s.get("continuous") or {}).get("pause_status")
    fps = sorted(fp for fp in (task_fingerprint(t) for t in tasks) if fp)
    # Generic notebooks/wheels are often driven by job-level parameters (source_location, table, ...):
    # identical tasks with different job parameter defaults are NOT duplicates.
    jparams = sorted((str(x.get("name")), str(x.get("default"))) for x in s.get("parameters") or [])
    if fps and jparams:
        fps.append("jobparams:" + stable_id(*[f"{k}={v}" for k, v in jparams]))
    elif fps and _JOB_SCOPED.search(json.dumps(tasks, default=str)):
        # tasks read {{job.parameters.*}} / {{job.name}} but the defaults are not in the payload (jobs/list omits
        # them): behaviour depends on the job itself, so only copies of the SAME logical job can be duplicates.
        fps.append("jobscoped:" + stable_id(logical_name(s.get("name"))))
    sources = sorted({src for _, src, _ in (task_source(t) for t in tasks) if src})
    yield Job(
        id=str(p["job_id"]), provider=DBX, name=s.get("name", ""), tags=s.get("tags") or {},
        creator=p.get("creator_user_name"), run_as=p.get("run_as_user_name"),
        schedule=sched.get("quartz_cron_expression"), paused=(pause == "PAUSED") if pause else None,
        task_count=len(tasks),
        attributes={
            "logical_name": logical_name(s.get("name")),
            "trigger_kind": _trigger_kind(s),
            "max_concurrent_runs": s.get("max_concurrent_runs"),
            "job_clusters": [{"key": jc.get("job_cluster_key"), **_cluster_spec(jc.get("new_cluster") or {})} for jc in s.get("job_clusters") or []],
            "existing_cluster_ids": sorted({t["existing_cluster_id"] for t in tasks if t.get("existing_cluster_id")}),
            "uses_serverless": any(not (t.get("existing_cluster_id") or t.get("job_cluster_key") or t.get("new_cluster")) for t in tasks
                                   if "pipeline_task" not in t and "condition_task" not in t and "run_job_task" not in t),
            "task_fingerprints": fps,
            "task_sources": sources,
            "pipeline_ids": sorted({(t.get("pipeline_task") or {}).get("pipeline_id") for t in tasks if t.get("pipeline_task")}),
            "max_retries": max([to_int(t.get("max_retries"), 0) for t in tasks] or [0]),
            "deployment": (s.get("deployment") or {}).get("kind"),
            "queue_enabled": (s.get("queue") or {}).get("enabled"),
        },
    )


@normalizes("databricks", "tasks")
def _dbx_task(r: RawRecord):
    p = r.payload
    ttype = next((t for t in TASK_TYPES if t in p), None)
    compute = p.get("existing_cluster_id") or p.get("job_cluster_key") or ("serverless" if not p.get("new_cluster") else "new_cluster")
    _, src, _ = task_source(p)
    yield Task(
        id=r.source_id, provider=DBX, name=p.get("task_key", ""), job_id=str(p.get("job_id")), task_key=p.get("task_key", ""),
        task_type=ttype, compute_ref=compute, depends_on=[d.get("task_key") for d in p.get("depends_on") or []],
        attributes={
            "fingerprint": task_fingerprint(p), "logical_source": src,
            "max_retries": to_int(p.get("max_retries"), 0), "min_retry_interval_millis": p.get("min_retry_interval_millis"),
            "retry_on_timeout": p.get("retry_on_timeout"),
            "new_cluster": _cluster_spec(p["new_cluster"]) if p.get("new_cluster") else None,
        },
    )


@normalizes("databricks", "pipelines")
def _dbx_pipeline(r: RawRecord):
    p = r.payload
    spec = p.get("spec") or {}
    sysset = p.get("sys_settings") or {}

    def pick(k: str) -> Any:
        return spec.get(k) if spec.get(k) is not None else sysset.get(k)

    libs = [logical_path((lib.get("notebook") or lib.get("file") or {}).get("path")) for lib in spec.get("libraries") or []]
    el = pick("event_log") or {}
    event_log = None
    if isinstance(el, dict) and el.get("name"):
        parts = (el.get("catalog") or spec.get("catalog"), el.get("schema") or spec.get("schema") or spec.get("target"), el["name"])
        event_log = ".".join(str(x) for x in parts if x)
    yield Pipeline(
        id=p["pipeline_id"], provider=DBX, name=p.get("name", ""), state=p.get("state"), creator=p.get("creator_user_name"),
        continuous=pick("continuous"), serverless=pick("serverless"), photon=pick("photon"), edition=pick("edition"),
        tags=p.get("sys_tags") or spec.get("tags") or {},
        attributes={
            "logical_name": logical_name(p.get("name")), "development": pick("development"),
            "pipeline_type": p.get("pipeline_type"), "run_as": p.get("run_as_user_name"),
            "libraries": sorted(x for x in libs if x), "target": spec.get("target") or spec.get("schema"), "catalog": spec.get("catalog"),
            "event_log_table": event_log,
        },
    )


def _ops(history: list[dict]) -> list[TableOperation]:
    out = []
    for h in history or []:
        metrics = {k: v for k, v in ((k, to_float(v)) for k, v in (h.get("operationMetrics") or {}).items()) if v is not None}
        out.append(TableOperation(version=to_int(h.get("version")), timestamp=to_dt(h.get("timestamp")),
                                  operation=str(h.get("operation") or "").upper(), metrics=metrics,
                                  parameters=h.get("operationParameters") or {}))
    return out


@normalizes("databricks", "tables")
def _dbx_table(r: RawRecord):
    p = r.payload
    delta = p.get("delta") or {}
    det = delta.get("detail") or {}
    fs = delta.get("file_stats") or {}
    props = {**(p.get("properties") or {}), **(det.get("properties") or {})}
    partitions = [
        PartitionStats(values=x.get("partition") or {}, num_files=to_int(x.get("num_files"), 0), size_bytes=to_int(x.get("size_bytes"), 0),
                       small_files=to_int(x.get("small_files")), last_modified=to_dt(x.get("last_modified")))
        for x in fs.get("partitions") or []
    ]
    full = p.get("full_name") or r.source_id
    table = Table(
        id=full, provider=DBX, name=full, catalog=p.get("catalog_name"), schema=p.get("schema_name"), table_name=p.get("name"),
        table_type=p.get("table_type"), table_format=(p.get("data_source_format") or det.get("format") or "").upper() or None,
        location=p.get("storage_location") or det.get("location"), owner=p.get("owner"),
        partition_columns=list(det.get("partitionColumns") or []), clustering_columns=list(det.get("clusteringColumns") or []),
        num_columns=len(p.get("columns") or []) or None, num_files=to_int(det.get("numFiles")), size_bytes=to_int(det.get("sizeInBytes")),
        small_files=to_int(fs.get("small_files")), created_at=to_dt(p.get("created_at") or det.get("createdAt")),
        last_modified=to_dt(det.get("lastModified") or p.get("updated_at")), properties=props, partitions=partitions,
        history=_ops(delta.get("history") or []),
        attributes={k: delta[k] for k in ("detail_error", "history_error", "file_stats_error") if k in delta},
    )
    yield table
    if table.size_bytes is not None:
        yield StorageObject(id=f"storage:{full}", provider=DBX, name=full, location=table.location, size_bytes=table.size_bytes,
                            object_count=table.num_files, last_modified=table.last_modified, linked_resource_id=full)


@normalizes("databricks", "storage_stats")
def _dbx_storage(r: RawRecord):
    p = r.payload
    yield StorageObject(id=f"extloc:{p.get('name')}", provider=DBX, name=p.get("name", ""), location=p.get("url"),
                        attributes={"credential_name": p.get("credential_name"), "read_only": p.get("read_only")})


@normalizes("databricks", "compute_metrics")
def _dbx_compute(r: RawRecord):
    p = r.payload
    cid = p["cluster_id"]
    kw = dict(period_start=to_dt(p.get("period_start")), period_end=to_dt(p.get("period_end")))
    has_workers = (to_float(p.get("w_node_hours")) or 0) > 0
    pre = "w_" if has_workers else "d_"
    for name, unit in (("avg_cpu_pct", "%"), ("p95_cpu_pct", "%"), ("avg_mem_pct", "%"), ("p95_mem_pct", "%"),
                       ("avg_cpu_wait_pct", "%"), ("idle_ratio", "ratio"), ("offhours_idle_ratio", "ratio"), ("offhours_share", "ratio")):
        v = p.get(pre + name, p.get(name))
        if v is not None:
            yield _metric(DBX, cid, "cluster", name, v, unit=unit, **kw)
    node_hours = p.get("node_hours")
    if node_hours is None and (p.get("w_node_hours") is not None or p.get("d_node_hours") is not None):
        node_hours = (to_float(p.get("w_node_hours")) or 0) + (to_float(p.get("d_node_hours")) or 0)
    for name, v, unit in (("node_hours", node_hours, "h"), ("worker_node_hours", p.get("w_node_hours"), "h"),
                          ("active_hours", p.get("active_hours"), "h"), ("instances", p.get("instances"), "count"),
                          ("worker_instances", p.get("worker_instances"), "count"), ("window_days", p.get("window_days"), "d")):
        if v is not None:
            yield _metric(DBX, cid, "cluster", name, v, unit=unit, **kw)


def _hist(h: Any) -> dict[int, int]:
    return {int(k): int(v) for k, v in (h or {}).items() if k is not None}


def _run_stats(p: dict, rid: str, rtype: str) -> RunStats:
    return RunStats(
        id=f"runstats:{rtype}:{rid}", provider=DBX, name=rid, workspace_id=str(p.get("workspace_id") or "") or None,
        resource_id=rid, resource_type=rtype, window_days=to_int(p.get("window_days"), 30), runs=to_int(p.get("runs"), 0),
        success_rate=to_float(p.get("success_rate")), avg_run_s=to_float(p.get("avg_run_s")), std_run_s=to_float(p.get("std_run_s")),
        avg_exec_s=to_float(p.get("avg_exec_s")), avg_queue_s=to_float(p.get("avg_queue_s")), avg_setup_s=to_float(p.get("avg_setup_s")),
        total_hours=to_float(p.get("total_hours")), first_run=to_dt(p.get("first_run")), last_run=to_dt(p.get("last_run")),
        task_runs=to_int(p.get("task_runs"), 0), retries=to_int(p.get("retries"), 0),
        retried_task_runs=to_int(p.get("retried_task_runs"), 0), retried_then_succeeded=to_int(p.get("retried_then_succeeded"), 0),
        full_refreshes=to_int(p.get("full_refreshes"), 0), trigger_types={str(k): int(v) for k, v in (p.get("trigger_types") or {}).items()},
        start_hour_histogram=_hist(p.get("start_hour_histogram")), compute_ids=[str(c) for c in p.get("compute_ids") or []],
    )


@normalizes("databricks", "job_run_stats")
def _dbx_job_runs(r: RawRecord):
    yield _run_stats(r.payload, str(r.payload["job_id"]), "job")


@normalizes("databricks", "pipeline_run_stats")
def _dbx_pipeline_runs(r: RawRecord):
    yield _run_stats(r.payload, str(r.payload["pipeline_id"]), "pipeline")


@normalizes("databricks", "lineage")
def _dbx_lineage(r: RawRecord):
    p = r.payload
    etype = str(p.get("entity_type") or "job").lower()
    yield Lineage(
        id=f"lineage:{etype}:{p['entity_id']}", provider=DBX, name=str(p["entity_id"]), workspace_id=str(p.get("workspace_id") or "") or None,
        resource_id=str(p["entity_id"]), resource_type=etype, sources=sorted(p.get("sources") or []),
        targets=sorted(p.get("targets") or []), last_event=to_dt(p.get("last_event")),
    )


@normalizes("databricks", "access_logs")
def _dbx_access(r: RawRecord):
    p = r.payload
    t = p["table_full_name"]
    window = to_int(p.get("window_days"), 90)
    yield _metric(DBX, t, "table", "read_events", p.get("reads"), unit="events",
                  attributes={"last_event_time": p.get("last_read"), "window_days": window})
    yield _metric(DBX, t, "table", "write_events", p.get("writes"), unit="events",
                  attributes={"last_event_time": p.get("last_write"), "window_days": window,
                              "writer_job_ids": [str(j) for j in (p.get("writer_job_ids") or []) if j]})
    if p.get("scans") is not None:
        yield _metric(DBX, t, "table", "scan_count", p.get("scans"), unit="queries")
    if p.get("unpruned_scan_ratio") is not None:
        yield _metric(DBX, t, "table", "unpruned_scan_ratio", p.get("unpruned_scan_ratio"), unit="ratio")


@normalizes("databricks", "costs")
def _dbx_cost(r: RawRecord):
    p = r.payload
    day = to_dt(p.get("usage_date"))
    attrs: dict[str, Any] = {"billing_origin_product": p.get("billing_origin_product"), "pricing": "list"}
    if day is not None:  # daily rows -> robust estimator in FactStore.cost_details
        attrs["granularity"] = "day"
        attrs["incident_amount"] = to_float(p.get("incident_cost"), 0.0)
    yield CostMetric(
        id=stable_id("dbx", r.source_id), provider=DBX, name=p.get("sku_name", ""), workspace_id=str(p.get("workspace_id")),
        resource_id=str(p.get("resource_id")), resource_type=p.get("resource_type", "other"), amount=to_float(p.get("list_cost"), 0.0),
        sku=p.get("sku_name"), usage_quantity=to_float(p.get("usage_quantity")), usage_unit=p.get("usage_unit"),
        period_start=day or to_dt(p.get("period_start")), period_end=day or to_dt(p.get("period_end")),
        attributes=attrs,
    )


# ============================================================================ AWS
@normalizes("aws", "accounts")
def _aws_account(r: RawRecord):
    yield Account(id=r.payload["account_id"], provider=Provider.AWS, name=r.payload.get("arn", ""), kind="account")


@normalizes("aws", "costs")
def _aws_cost(r: RawRecord):
    p = r.payload
    yield CostMetric(id=stable_id("aws", r.source_id), provider=Provider.AWS, name=p["service"], resource_id=p["service"],
                     resource_type="service", amount=to_float(p.get("amount"), 0.0), currency=p.get("currency") or "USD",
                     usage_quantity=to_float(p.get("usage_quantity")), period_start=to_dt(p.get("period_start")),
                     period_end=to_dt(p.get("period_end")), attributes={"tag": p.get("tag")})


@normalizes("aws", "storage_stats")
def _aws_storage(r: RawRecord):
    p = r.payload
    yield StorageObject(id=f"s3://{p['bucket']}", provider=Provider.AWS, name=p["bucket"], location=f"s3://{p['bucket']}",
                        size_bytes=to_int(p.get("size_bytes")), object_count=to_int(p.get("object_count")), storage_class="S3")


@normalizes("aws", "compute_metrics")
def _aws_compute(r: RawRecord):
    p = r.payload
    yield _metric(Provider.AWS, p["instance_id"], "instance", "instance_running", 1 if p.get("state") == "running" else 0,
                  attributes={k: p.get(k) for k in ("instance_type", "lifecycle", "cluster_id")})


# ============================================================================ Azure
@normalizes("azure", "accounts")
def _az_account(r: RawRecord):
    yield Account(id=r.payload["subscription_id"], provider=Provider.AZURE, name=r.payload["subscription_id"], kind="subscription")


@normalizes("azure", "costs")
def _az_cost(r: RawRecord):
    p = r.payload
    rid = p.get("ResourceId") or p.get("ServiceName") or r.source_id
    yield CostMetric(id=stable_id("azure", r.source_id), provider=Provider.AZURE, name=p.get("ServiceName", ""), resource_id=rid,
                     resource_type="resource", amount=to_float(p.get("Cost") or p.get("totalCost"), 0.0), currency=p.get("Currency") or "USD",
                     period_start=to_dt(p.get("period_start")), period_end=to_dt(p.get("period_end")))


@normalizes("azure", "storage_stats")
def _az_storage(r: RawRecord):
    p = r.payload
    yield StorageObject(id=p["id"], provider=Provider.AZURE, name=p.get("name", ""), size_bytes=to_int(p.get("size_bytes")),
                        storage_class=p.get("access_tier"), attributes={"kind": p.get("kind"), "location": p.get("location")})


# ============================================================================ GCP
@normalizes("gcp", "accounts")
def _gcp_account(r: RawRecord):
    yield Account(id=r.payload["project_id"], provider=Provider.GCP, name=r.payload["project_id"], kind="project")


@normalizes("gcp", "costs")
def _gcp_cost(r: RawRecord):
    p = r.payload
    yield CostMetric(id=stable_id("gcp", r.source_id), provider=Provider.GCP, name=p.get("service", ""), account_id=p.get("project_id"),
                     resource_id=f"{p.get('project_id')}:{p.get('service')}", resource_type="service", amount=to_float(p.get("amount"), 0.0),
                     currency=p.get("currency") or "USD", sku=p.get("sku"), usage_quantity=to_float(p.get("usage_quantity")),
                     usage_unit=p.get("usage_unit"), period_start=to_dt(p.get("period_start")), period_end=to_dt(p.get("period_end")))


@normalizes("gcp", "storage_stats")
def _gcp_storage(r: RawRecord):
    p = r.payload
    yield StorageObject(id=f"gs://{p['bucket']}", provider=Provider.GCP, name=p["bucket"], location=f"gs://{p['bucket']}",
                        storage_class=p.get("storage_class"), attributes={"lifecycle_rules": p.get("lifecycle_rules")})


# ============================================================================ Snowflake
SF = Provider.SNOWFLAKE


@normalizes("snowflake", "accounts")
def _sf_account(r: RawRecord):
    yield Account(id=r.payload["account"], provider=SF, name=r.payload["account"], kind="account", attributes={"region": r.payload.get("region")})


@normalizes("snowflake", "tables")
def _sf_table(r: RawRecord):
    p = r.payload
    yield Table(
        id=r.source_id, provider=SF, name=r.source_id, catalog=p.get("table_catalog"), schema=p.get("table_schema"),
        table_name=p.get("table_name"), table_type=p.get("table_type"), table_format="SNOWFLAKE",
        size_bytes=to_int(p.get("active_bytes") or p.get("bytes")), created_at=to_dt(p.get("created")),
        last_modified=to_dt(p.get("last_altered")), clustering_columns=[p["clustering_key"]] if p.get("clustering_key") else [],
        attributes={k: p.get(k) for k in ("time_travel_bytes", "failsafe_bytes", "retained_for_clone_bytes", "retention_time", "row_count")},
    )


@normalizes("snowflake", "access_logs")
def _sf_access(r: RawRecord):
    p = r.payload
    yield _metric(SF, p["table_full_name"], "table", "read_events", p.get("reads"),
                  attributes={"last_event_time": p.get("last_read"), "window_days": p.get("window_days")})


@normalizes("snowflake", "costs")
def _sf_cost(r: RawRecord):
    p = r.payload
    yield CostMetric(id=stable_id("sf", r.source_id), provider=SF, name=p["warehouse_name"], resource_id=p["warehouse_name"],
                     resource_type="warehouse", amount=to_float(p.get("amount"), 0.0), usage_quantity=to_float(p.get("credits")),
                     usage_unit="credits", period_start=to_dt(p.get("period_start")), period_end=to_dt(p.get("period_end")))


@normalizes("snowflake", "compute_metrics")
def _sf_compute(r: RawRecord):
    p = r.payload
    for name in ("avg_running", "avg_queued"):
        if p.get(name) is not None:
            yield _metric(SF, p["warehouse_name"], "warehouse", name, p[name])
