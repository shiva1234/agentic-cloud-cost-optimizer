"""Causal + usage + cost facts.

The FactStore indexes canonical objects and derives analysis-ready facts:
  * usage facts  - reads/writes/scans per resource (from lineage + query history)
  * cost facts   - monthly cost per job/cluster/warehouse (from billing)
  * causal links - table <- writer jobs -> cost, so storage issues can be priced in compute $
Agents consume facts only; they never parse raw payloads.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

from cco.models.canonical import CanonicalObject, CostMetric, Table, TableOperation, UsageMetric
from cco.utils import GB, MB, days_since, parse_interval_days, to_dt, utcnow

WRITE_OPS = {
    "WRITE", "MERGE", "UPDATE", "DELETE", "STREAMING UPDATE", "COPY INTO", "TRUNCATE",
    "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT",
}
_MIN_DT = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _first(m: dict[str, float], *keys: str) -> float:
    for k in keys:
        if m.get(k) is not None:
            return float(m[k])
    return 0.0


def added_bytes(op: TableOperation) -> float:
    return _first(op.metrics, "numOutputBytes", "numTargetBytesAdded", "numAddedBytes")


def removed_bytes(op: TableOperation) -> float:
    return _first(op.metrics, "numTargetBytesRemoved", "numRemovedBytes")


def added_files(op: TableOperation) -> float:
    return _first(op.metrics, "numFiles", "numTargetFilesAdded", "numAddedFiles", "numOutputFiles")


@dataclass
class DeltaTableFacts:
    table_id: str
    # layout
    num_files: int = 0
    size_bytes: int = 0
    avg_file_size: float = 0.0
    small_files: int = 0
    small_file_ratio: float = 0.0
    small_file_ratio_estimated: bool = True
    num_columns: int | None = None
    partition_columns: list[str] = field(default_factory=list)
    clustering_columns: list[str] = field(default_factory=list)
    num_partitions: int = 0
    max_files_per_partition: int = 0
    avg_files_per_partition: float = 0.0
    partition_skew_ratio: float | None = None
    old_partitions: int = 0
    old_partition_bytes: int = 0
    # lifecycle
    table_age_days: float | None = None
    last_modified: datetime | None = None
    days_since_modified: float | None = None
    last_accessed: datetime | None = None
    days_since_accessed: float | None = None
    access_data_available: bool = False
    access_window_days: int = 90
    no_access_in_window: bool = False
    reads_in_window: float = 0.0
    writes_in_window: float = 0.0
    read_write_ratio: float | None = None
    scan_count: float = 0.0
    unpruned_scan_ratio: float | None = None
    writer_job_ids: list[str] = field(default_factory=list)
    # write behaviour (from DESCRIBE HISTORY)
    history_ops: int = 0
    write_ops: int = 0
    bytes_written: float = 0.0
    avg_write_file_size: float | None = None
    write_amplification: float | None = None
    growth_ratio_30d: float | None = None
    days_since_vacuum: float | None = None
    days_since_optimize: float | None = None
    tombstoned_bytes_est: float = 0.0
    vacuumable_bytes_est: float = 0.0
    # properties
    deleted_file_retention_days: float = 7.0
    log_retention_days: float = 30.0
    optimize_write: bool = False
    auto_compact: bool = False
    deletion_vectors: bool = False
    target_file_size: str | None = None
    # $$
    monthly_storage_cost: float = 0.0
    attributed_compute_cost: float = 0.0


class FactStore:
    COST_DEFAULTS: dict[str, Any] = {
        "min_observation_days": 14,    # never extrapolate a month from fewer days than this
        "outlier_cap_x_median": 5.0,   # cap each day at N x median active-day cost (0 disables winsorization)
    }

    def __init__(self, objects: Iterable[CanonicalObject], now: datetime | None = None, storage_cost_per_gb_month: float = 0.023,
                 cost_estimation: dict[str, Any] | None = None) -> None:
        self.now = now or utcnow()
        self.storage_cost_per_gb_month = storage_cost_per_gb_month
        self.cost_cfg: dict[str, Any] = {**self.COST_DEFAULTS, **(cost_estimation or {})}
        self._cost_cache: dict[str, dict[str, Any]] = {}
        self.objects: dict[str, dict[str, CanonicalObject]] = defaultdict(dict)
        self.usage: dict[str, dict[str, UsageMetric]] = defaultdict(dict)
        self.costs: dict[str, list[CostMetric]] = defaultdict(list)
        self.access_data_available = False
        self.access_window_days = 90
        self._job_writes: dict[str, set[str]] | None = None
        self._cache: dict[tuple, DeltaTableFacts] = {}
        self._workloads: Any = None
        for o in objects:
            self.add(o)

    @property
    def workloads(self) -> Any:
        """Cluster / job / pipeline facts (built lazily, shared by the workload agents)."""
        import threading

        lock = self.__dict__.setdefault("_wl_lock", threading.Lock())
        with lock:  # agents run in parallel threads; build once
            if self._workloads is None:
                from cco.etl.workload_facts import WorkloadFactStore

                self._workloads = WorkloadFactStore(self)
            return self._workloads

    # ------------------------------------------------------------------ indexing
    def add(self, o: CanonicalObject) -> None:
        self.objects[o.OBJECT_TYPE][o.id] = o
        if isinstance(o, UsageMetric):
            self.usage[o.resource_id][o.metric_name] = o
            if o.metric_name == "read_events":
                self.access_data_available = True
                self.access_window_days = int(o.attributes.get("window_days") or self.access_window_days)
        elif isinstance(o, CostMetric):
            self.costs[o.resource_id].append(o)
            self.__dict__.setdefault("_cost_cache", {}).pop(o.resource_id, None)
        self._job_writes = None
        self._workloads = None
        self._cache.clear()

    def of_type(self, object_type: str) -> list[CanonicalObject]:
        return list(self.objects.get(object_type, {}).values())

    def get(self, object_type: str, obj_id: str) -> CanonicalObject | None:
        return self.objects.get(object_type, {}).get(obj_id)

    def available_types(self) -> set[str]:
        return {t for t, v in self.objects.items() if v}

    def tables(self, table_format: str | None = None) -> list[Table]:
        ts = [t for t in self.of_type("table") if isinstance(t, Table)]
        if table_format:
            ts = [t for t in ts if (t.table_format or "").upper() == table_format.upper()]
        return ts

    def summary(self) -> dict[str, int]:
        return {t: len(v) for t, v in self.objects.items()}

    # ------------------------------------------------------------------ cost & causal facts
    def monthly_cost(self, resource_id: str) -> float:
        """Robust steady-state monthly cost (see `cost_details`)."""
        return self.cost_details(resource_id)["monthly_cost"]

    def incident_cost(self, resource_id: str) -> float:
        """Spend from canceled / failed runs or updates in the cost window (excluded from `monthly_cost`)."""
        return self.cost_details(resource_id)["incident_cost"]

    def cost_details(self, resource_id: str) -> dict[str, Any]:
        """Monthly cost estimate that is robust to one-off spikes.

        * Daily rows (attributes.granularity == 'day'): canceled/failed-run spend is split out as `incident_cost`;
          the remaining daily steady-state cost is winsorized at `outlier_cap_x_median` x the median active-day cost,
          summed, and scaled to 30 days over max(observed window, `min_observation_days`). Days without usage count as 0.
        * Period rows (other providers / aggregated billing): amount x 30 / period_days, as before.
        """
        rid = str(resource_id)
        cache = self.__dict__.setdefault("_cost_cache", {})
        if rid in cache:
            return cache[rid]
        cfg = {**self.COST_DEFAULTS, **getattr(self, "cost_cfg", {})}
        rows = self.costs.get(rid, [])
        daily = [c for c in rows if c.attributes.get("granularity") == "day" and c.period_start]
        period = [c for c in rows if c not in daily]
        out: dict[str, Any] = {"monthly_cost": sum(c.monthly_amount for c in period), "incident_cost": 0.0,
                               "trimmed_cost": 0.0, "raw_monthly_cost": None, "observed_days": None, "active_days": None,
                               "basis": "period_average" if period else None}
        out["incident_cost"] = sum(float(c.attributes.get("incident_amount") or 0.0) for c in period)
        if daily:
            steady: dict[Any, float] = defaultdict(float)
            incident = 0.0
            for c in daily:
                inc = min(max(float(c.attributes.get("incident_amount") or 0.0), 0.0), max(c.amount, 0.0))
                incident += inc
                steady[c.period_start.date()] += c.amount - inc
            days = sorted(steady)
            end = max(days[-1], self.now.date())
            observed = (end - days[0]).days + 1
            window = max(observed, int(cfg["min_observation_days"]))
            active = [v for v in steady.values() if v > 0]
            cap_x = float(cfg["outlier_cap_x_median"] or 0)
            cap = median(active) * cap_x if active and cap_x > 0 else None
            total = sum(steady.values())
            kept = sum(min(v, cap) if cap is not None else v for v in steady.values())
            out["monthly_cost"] += kept / window * 30.0
            out["incident_cost"] += incident
            out["trimmed_cost"] = max(0.0, total - kept)
            out["raw_monthly_cost"] = (total + incident) / max(1, (days[-1] - days[0]).days) * 30.0
            out["observed_days"], out["active_days"] = observed, len(active)
            out["basis"] = "winsorized_daily" if not period else "winsorized_daily+period_average"
        cache[rid] = out
        return out

    def writer_jobs(self, table_id: str) -> list[str]:
        m = self.usage.get(table_id, {}).get("write_events")
        return list(m.attributes.get("writer_job_ids") or []) if m else []

    def tables_written_by(self, job_id: str) -> set[str]:
        if self._job_writes is None:
            idx: dict[str, set[str]] = defaultdict(set)
            for rid, metrics in self.usage.items():
                m = metrics.get("write_events")
                for j in (m.attributes.get("writer_job_ids") or []) if m else []:
                    idx[str(j)].add(rid)
            self._job_writes = idx
        return self._job_writes.get(str(job_id), set())

    def attributed_compute_cost(self, table_id: str) -> float:
        """Monthly writer-job cost attributed evenly across the tables each job writes."""
        total = 0.0
        for j in self.writer_jobs(table_id):
            n = max(1, len(self.tables_written_by(j)))
            total += self.monthly_cost(j) / n
        return total

    # ------------------------------------------------------------------ Delta table facts
    def table_facts(self, table: Table, *, small_file_bytes: int = 32 * MB, stale_partition_days: int = 180,
                    write_lookback_days: int = 30) -> DeltaTableFacts:
        key = (table.id, small_file_bytes, stale_partition_days, write_lookback_days)
        if key in self._cache:
            return self._cache[key]
        now = self.now
        f = DeltaTableFacts(table_id=table.id)
        f.num_files = int(table.num_files or 0)
        f.size_bytes = int(table.size_bytes or 0)
        f.avg_file_size = f.size_bytes / f.num_files if f.num_files else 0.0
        f.num_columns = table.num_columns
        f.partition_columns = list(table.partition_columns)
        f.clustering_columns = list(table.clustering_columns)
        f.table_age_days = days_since(table.created_at, now)
        f.last_modified = table.last_modified
        f.days_since_modified = days_since(table.last_modified, now)

        props = {str(k).lower(): v for k, v in (table.properties or {}).items()}
        f.deleted_file_retention_days = parse_interval_days(props.get("delta.deletedfileretentionduration"), 7.0)
        f.log_retention_days = parse_interval_days(props.get("delta.logretentionduration"), 30.0)
        f.optimize_write = str(props.get("delta.autooptimize.optimizewrite", "")).lower() == "true"
        f.auto_compact = str(props.get("delta.autooptimize.autocompact", "")).lower() in ("true", "auto", "legacy")
        f.deletion_vectors = str(props.get("delta.enabledeletionvectors", "")).lower() == "true"
        f.target_file_size = props.get("delta.targetfilesize")

        self._history_facts(f, table, small_file_bytes, write_lookback_days)
        self._small_file_facts(f, table, small_file_bytes)
        self._partition_facts(f, table, stale_partition_days)
        self._access_facts(f, table)

        f.monthly_storage_cost = f.size_bytes / GB * self.storage_cost_per_gb_month
        f.attributed_compute_cost = self.attributed_compute_cost(table.id)
        self._cache[key] = f
        return f

    def _history_facts(self, f: DeltaTableFacts, table: Table, small: int, lookback: int) -> None:
        now = self.now
        hist = sorted((o for o in table.history if o.timestamp), key=lambda o: o.timestamp)
        f.history_ops = len(hist)
        vac = [o.timestamp for o in hist if o.operation.startswith("VACUUM END")]
        opt = [o.timestamp for o in hist if o.operation == "OPTIMIZE"]
        last_vacuum = max(vac) if vac else None
        f.days_since_vacuum = days_since(last_vacuum, now)
        f.days_since_optimize = days_since(max(opt) if opt else None, now)

        cutoff = now - timedelta(days=lookback)
        writes = [o for o in hist if o.operation in WRITE_OPS and o.timestamp >= cutoff]
        f.write_ops = len(writes)
        written = sum(added_bytes(o) for o in writes)
        removed = sum(removed_bytes(o) for o in writes)
        files = sum(added_files(o) for o in writes)
        f.bytes_written = written
        f.avg_write_file_size = written / files if files else None
        net = written - removed
        if written > 0:
            f.write_amplification = 100.0 if net <= 0 else min(100.0, written / net)
        base = f.size_bytes - net
        f.growth_ratio_30d = net / base if base > 0 and written > 0 else None

        retention_cutoff = now - timedelta(days=f.deleted_file_retention_days)
        since = last_vacuum or _MIN_DT
        after_vac = [o for o in hist if o.timestamp > since]
        f.tombstoned_bytes_est = sum(removed_bytes(o) for o in after_vac)
        f.vacuumable_bytes_est = sum(removed_bytes(o) for o in after_vac if o.timestamp <= retention_cutoff)

    def _small_file_facts(self, f: DeltaTableFacts, table: Table, small: int) -> None:
        if not f.num_files:
            return
        if table.small_files is not None:
            f.small_files = int(table.small_files)
            f.small_file_ratio = min(1.0, f.small_files / f.num_files)
            f.small_file_ratio_estimated = False
            return
        # Estimate: files added by small writes since the last OPTIMIZE, or the average file size
        opt = [o.timestamp for o in table.history if o.operation == "OPTIMIZE" and o.timestamp]
        last_opt = max(opt) if opt else _MIN_DT
        small_added = sum(
            added_files(o) for o in table.history
            if o.operation in WRITE_OPS and o.timestamp and o.timestamp > last_opt
            and added_files(o) > 0 and added_bytes(o) / added_files(o) < small
        )
        est_hist = min(1.0, small_added / f.num_files)
        est_avg = 0.9 if f.avg_file_size < small / 2 else 0.6 if f.avg_file_size < small else 0.0
        f.small_file_ratio = max(est_hist, est_avg)
        f.small_files = int(round(f.small_file_ratio * f.num_files))
        f.small_file_ratio_estimated = True

    def _partition_facts(self, f: DeltaTableFacts, table: Table, stale_days: int) -> None:
        parts = table.partitions
        if not parts:
            return
        files = [p.num_files for p in parts]
        sizes = [p.size_bytes for p in parts]
        f.num_partitions = len(parts)
        f.max_files_per_partition = max(files)
        f.avg_files_per_partition = sum(files) / len(files)
        med = median(sizes)
        f.partition_skew_ratio = (max(sizes) / med) if med > 0 else None
        cutoff = self.now - timedelta(days=stale_days)
        old = [p for p in parts if p.last_modified and p.last_modified < cutoff]
        f.old_partitions = len(old)
        f.old_partition_bytes = sum(p.size_bytes for p in old)

    def _access_facts(self, f: DeltaTableFacts, table: Table) -> None:
        u = self.usage.get(table.id, {})
        reads, writes = u.get("read_events"), u.get("write_events")
        f.access_data_available = self.access_data_available
        f.access_window_days = int((reads.attributes.get("window_days") if reads else None) or self.access_window_days)
        f.reads_in_window = reads.value if reads else 0.0
        f.writes_in_window = writes.value if writes else 0.0
        f.last_accessed = to_dt(reads.attributes.get("last_event_time")) if reads else None
        f.days_since_accessed = days_since(f.last_accessed, self.now)
        f.no_access_in_window = f.access_data_available and f.reads_in_window == 0
        f.read_write_ratio = (f.reads_in_window / f.writes_in_window) if f.writes_in_window else None
        f.scan_count = u["scan_count"].value if "scan_count" in u else 0.0
        f.unpruned_scan_ratio = u["unpruned_scan_ratio"].value if "unpruned_scan_ratio" in u else None
        f.writer_job_ids = self.writer_jobs(table.id)

    def facts_as_dict(self, f: DeltaTableFacts) -> dict[str, Any]:
        from cco.models.canonical import to_jsonable

        return to_jsonable(f)
