"""ETL / normalization: raw connector payloads -> canonical objects.

Normalizers are registered per (provider, entity). Adding a source = adding
functions here (or passing `extra` to Normalizer) - nothing downstream changes.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from cco.connectors.base import RawRecord
from cco.models.canonical import (
    Account, CanonicalObject, Cluster, CostMetric, Job, PartitionStats, Pipeline, Provider,
    StorageObject, Table, TableOperation, Task, UsageMetric, Workspace, stable_id,
)
from cco.utils import to_dt, to_float, to_int

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


@normalizes("databricks", "clusters")
def _dbx_cluster(r: RawRecord):
    p = r.payload
    auto = p.get("autoscale") or {}
    yield Cluster(
        id=p["cluster_id"], provider=DBX, name=p.get("cluster_name", ""), tags=p.get("custom_tags") or {},
        node_type=p.get("node_type_id"), driver_node_type=p.get("driver_node_type_id"), num_workers=p.get("num_workers"),
        min_workers=auto.get("min_workers"), max_workers=auto.get("max_workers"),
        autotermination_minutes=p.get("autotermination_minutes"), runtime_version=p.get("spark_version"),
        cluster_source=p.get("cluster_source"), state=p.get("state"), policy_id=p.get("policy_id"),
        photon=(p.get("runtime_engine") == "PHOTON") if p.get("runtime_engine") else None,
        attributes={"data_security_mode": p.get("data_security_mode")},
    )


@normalizes("databricks", "jobs")
def _dbx_job(r: RawRecord):
    p = r.payload
    s = p.get("settings") or {}
    sched = s.get("schedule") or {}
    yield Job(
        id=str(p["job_id"]), provider=DBX, name=s.get("name", ""), tags=s.get("tags") or {},
        creator=p.get("creator_user_name"), run_as=p.get("run_as_user_name"),
        schedule=sched.get("quartz_cron_expression"), paused=(sched.get("pause_status") == "PAUSED") if sched else None,
        task_count=len(s.get("tasks") or []),
    )


@normalizes("databricks", "tasks")
def _dbx_task(r: RawRecord):
    p = r.payload
    ttype = next((t for t in TASK_TYPES if t in p), None)
    compute = p.get("existing_cluster_id") or p.get("job_cluster_key") or ("serverless" if not p.get("new_cluster") else "new_cluster")
    yield Task(
        id=r.source_id, provider=DBX, name=p.get("task_key", ""), job_id=str(p.get("job_id")), task_key=p.get("task_key", ""),
        task_type=ttype, compute_ref=compute, depends_on=[d.get("task_key") for d in p.get("depends_on") or []],
    )


@normalizes("databricks", "pipelines")
def _dbx_pipeline(r: RawRecord):
    p = r.payload
    spec = p.get("spec") or {}
    yield Pipeline(
        id=p["pipeline_id"], provider=DBX, name=p.get("name", ""), state=p.get("state"), creator=p.get("creator_user_name"),
        continuous=spec.get("continuous"), serverless=spec.get("serverless"), photon=spec.get("photon"), edition=spec.get("edition"),
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
    for name, unit in (("avg_cpu_pct", "%"), ("p95_cpu_pct", "%"), ("avg_mem_pct", "%"), ("instances", "count")):
        if p.get(name) is not None:
            yield _metric(DBX, cid, "cluster", name, p[name], unit=unit, **kw)


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
    yield CostMetric(
        id=stable_id("dbx", r.source_id), provider=DBX, name=p.get("sku_name", ""), workspace_id=str(p.get("workspace_id")),
        resource_id=str(p.get("resource_id")), resource_type=p.get("resource_type", "other"), amount=to_float(p.get("list_cost"), 0.0),
        sku=p.get("sku_name"), usage_quantity=to_float(p.get("usage_quantity")), usage_unit=p.get("usage_unit"),
        period_start=to_dt(p.get("period_start")), period_end=to_dt(p.get("period_end")),
        attributes={"billing_origin_product": p.get("billing_origin_product"), "pricing": "list"},
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
