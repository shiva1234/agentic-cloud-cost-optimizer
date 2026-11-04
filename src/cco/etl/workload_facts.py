"""Workload facts: clusters, jobs and pipelines joined with utilization, run history, lineage, usage and cost.

Built once per FactStore (see `FactStore.workloads`) and shared by the compute, job and pipeline agents.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from cco.models.canonical import Cluster, ComputePool, Job, Lineage, Pipeline, RunStats
from cco.utils import days_since

if TYPE_CHECKING:  # pragma: no cover
    from cco.etl.facts import FactStore


@dataclass
class ClusterFacts:
    cluster_id: str
    name: str = ""
    source: str | None = None
    is_job_cluster: bool = False
    owner: str | None = None
    node_type: str | None = None
    driver_node_type: str | None = None
    worker_cores: float | None = None
    worker_memory_mb: float | None = None
    num_workers: int | None = None
    min_workers: int | None = None
    max_workers: int | None = None
    autoscale: bool = False
    autotermination_minutes: int | None = None
    availability: str | None = None
    pool_ids: list[str] = field(default_factory=list)
    runtime: str | None = None
    policy_id: str | None = None
    data_security_mode: str | None = None
    # utilization (worker nodes when present, else driver)
    metrics_available: bool = False
    avg_cpu: float | None = None
    p95_cpu: float | None = None
    avg_mem: float | None = None
    p95_mem: float | None = None
    cpu_wait: float | None = None
    idle_ratio: float | None = None
    offhours_idle_ratio: float | None = None
    offhours_share: float | None = None
    node_hours: float = 0.0
    active_hours: float = 0.0
    avg_nodes: float | None = None
    window_days: int = 30
    # usage / cost
    job_ids: list[str] = field(default_factory=list)
    monthly_cost: float = 0.0

    @property
    def utilization_pct(self) -> float | None:
        return self.avg_cpu

    @property
    def useful_node_hours(self) -> float:
        return self.node_hours * (1 - (self.idle_ratio or 0))

    @property
    def always_on_ratio(self) -> float:
        return self.active_hours / max(1.0, self.window_days * 24.0)

    @property
    def configured_max_workers(self) -> int:
        return int(self.max_workers if self.autoscale else (self.num_workers or 0))


@dataclass
class JobFacts:
    job_id: str
    name: str = ""
    logical_name: str = ""
    creator: str | None = None
    run_as: str | None = None
    trigger_kind: str = "manual"
    schedule: str | None = None
    paused: bool | None = None
    task_count: int = 0
    task_fingerprints: list[str] = field(default_factory=list)
    task_sources: list[str] = field(default_factory=list)
    pipeline_ids: list[str] = field(default_factory=list)
    max_retries: int = 0
    job_clusters: list[dict] = field(default_factory=list)
    existing_cluster_ids: list[str] = field(default_factory=list)
    uses_serverless: bool = False
    # runs
    run_stats_available: bool = False
    runs: int = 0
    window_days: int = 30
    runs_per_day: float = 0.0
    success_rate: float | None = None
    avg_run_s: float | None = None
    cv_run: float | None = None
    avg_exec_s: float | None = None
    avg_queue_s: float | None = None
    avg_setup_s: float | None = None
    setup_ratio: float | None = None
    total_hours: float = 0.0
    last_run: datetime | None = None
    trigger_types: dict[str, int] = field(default_factory=dict)
    hour_hist: dict[int, int] = field(default_factory=dict)
    task_runs: int = 0
    retry_ratio: float = 0.0
    retry_success_ratio: float | None = None
    # compute
    compute_ids: list[str] = field(default_factory=list)
    job_cluster_ids: list[str] = field(default_factory=list)
    all_purpose_ids: list[str] = field(default_factory=list)
    compute_metrics_available: bool = False
    node_hours: float = 0.0
    avg_cpu: float | None = None
    p95_cpu: float | None = None
    avg_mem: float | None = None
    p95_mem: float | None = None
    idle_ratio: float | None = None
    availability: list[str] = field(default_factory=list)
    node_types: list[str] = field(default_factory=list)
    max_workers_seen: int = 0
    # lineage / usage
    lineage_available: bool = False
    sources: list[str] = field(default_factory=list)
    targets: list[str] = field(default_factory=list)
    source_writes_per_day: float | None = None
    target_reads: float | None = None
    # $$
    monthly_cost: float = 0.0          # robust steady-state estimate (see FactStore.cost_details)
    incident_cost: float = 0.0         # canceled / failed run spend in the cost window (not in monthly_cost)

    @property
    def cost_per_run(self) -> float | None:
        monthly_runs = self.runs_per_day * 30
        return self.monthly_cost / monthly_runs if monthly_runs else None

    @property
    def compute_hours_per_output(self) -> float | None:
        return self.total_hours / len(self.targets) if self.targets else None

    @property
    def externally_triggered(self) -> bool:
        """Runs come via API (ONETIME) - typically an enterprise scheduler; schedule changes happen there."""
        tot = sum(self.trigger_types.values())
        return bool(tot) and self.trigger_types.get("ONETIME", 0) / tot >= 0.5


@dataclass
class PipelineFacts:
    pipeline_id: str
    name: str = ""
    logical_name: str = ""
    state: str | None = None
    continuous: bool | None = None
    development: bool | None = None
    serverless: bool | None = None
    edition: str | None = None
    libraries: list[str] = field(default_factory=list)
    creator: str | None = None
    run_stats_available: bool = False
    runs: int = 0
    window_days: int = 90
    runs_per_day: float = 0.0
    success_rate: float | None = None
    full_refreshes: int = 0
    full_refresh_ratio: float = 0.0
    last_run: datetime | None = None
    days_since_run: float | None = None
    total_hours: float = 0.0
    trigger_types: dict[str, int] = field(default_factory=dict)
    hour_hist: dict[int, int] = field(default_factory=dict)
    lineage_available: bool = False
    sources: list[str] = field(default_factory=list)
    targets: list[str] = field(default_factory=list)
    final_targets: list[str] = field(default_factory=list)
    consumer_reads: float | None = None
    source_writes_per_day: float | None = None
    triggering_job_ids: list[str] = field(default_factory=list)
    event_log_table: str | None = None  # from the pipeline spec's event_log setting (if published to UC)
    monthly_cost: float = 0.0           # robust steady-state estimate (see FactStore.cost_details)
    incident_cost: float = 0.0          # canceled / failed update spend in the cost window (not in monthly_cost)
    cost_trimmed: float = 0.0           # outlier spend removed by the estimator
    cost_basis: str | None = None


def _wavg(pairs: list[tuple[float | None, float]]) -> float | None:
    pairs = [(v, w) for v, w in pairs if v is not None and w > 0]
    tot = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / tot if tot else None


class WorkloadFactStore:
    def __init__(self, store: "FactStore") -> None:
        self.store = store
        self.now = store.now
        self.pools: list[ComputePool] = [p for p in store.of_type("compute_pool") if isinstance(p, ComputePool)]
        self._run_stats = {(o.resource_type, o.resource_id): o for o in store.of_type("run_stats") if isinstance(o, RunStats)}
        self._lineage = {(o.resource_type, o.resource_id): o for o in store.of_type("lineage") if isinstance(o, Lineage)}
        self.lineage_available = bool(self._lineage)
        self.run_stats_available = bool(self._run_stats)
        self.clusters: dict[str, ClusterFacts] = {}
        self.jobs: dict[str, JobFacts] = {}
        self.pipelines: dict[str, PipelineFacts] = {}
        self._build_clusters()
        self._build_jobs()
        self._build_pipelines()

    # ------------------------------------------------------------------ helpers
    def _metric(self, rid: str, name: str) -> float | None:
        m = self.store.usage.get(rid, {}).get(name)
        return float(m.value) if m is not None else None

    def table_writes_per_day(self, table: str) -> float | None:
        m = self.store.usage.get(table, {}).get("write_events")
        if m is None:
            return None
        return float(m.value) / max(1, int(m.attributes.get("window_days") or self.store.access_window_days))

    def table_reads(self, table: str) -> float | None:
        if not self.store.access_data_available:
            return None
        m = self.store.usage.get(table, {}).get("read_events")
        return float(m.value) if m is not None else 0.0

    def lineage(self, rtype: str, rid: str) -> Lineage | None:
        return self._lineage.get((rtype, rid))

    def run_stats(self, rtype: str, rid: str) -> RunStats | None:
        return self._run_stats.get((rtype, rid))

    # ------------------------------------------------------------------ clusters
    def _build_clusters(self) -> None:
        for c in self.store.of_type("cluster"):
            if not isinstance(c, Cluster):
                continue
            f = ClusterFacts(
                cluster_id=c.id, name=c.name, source=c.cluster_source, is_job_cluster=c.is_job_cluster, owner=c.owner,
                node_type=c.node_type, driver_node_type=c.driver_node_type, worker_cores=c.worker_cores,
                worker_memory_mb=c.worker_memory_mb, num_workers=c.num_workers, min_workers=c.min_workers,
                max_workers=c.max_workers, autoscale=c.autoscale, autotermination_minutes=c.autotermination_minutes,
                availability=c.availability, pool_ids=[p for p in (c.driver_pool_id, c.worker_pool_id) if p],
                runtime=c.runtime_version, policy_id=c.policy_id, data_security_mode=c.attributes.get("data_security_mode"),
                monthly_cost=self.store.monthly_cost(c.id),
            )
            m = lambda n, _id=c.id: self._metric(_id, n)  # noqa: E731
            if m("avg_cpu_pct") is not None:
                f.metrics_available = True
                f.avg_cpu, f.p95_cpu = m("avg_cpu_pct"), m("p95_cpu_pct")
                f.avg_mem, f.p95_mem = m("avg_mem_pct"), m("p95_mem_pct")
                f.cpu_wait, f.idle_ratio = m("avg_cpu_wait_pct"), m("idle_ratio")
                f.offhours_idle_ratio, f.offhours_share = m("offhours_idle_ratio"), m("offhours_share")
                f.node_hours = m("node_hours") or 0.0
                f.active_hours = m("active_hours") or 0.0
                f.window_days = int(m("window_days") or 30)
                wh = m("worker_node_hours")
                f.avg_nodes = (wh / f.active_hours) if wh and f.active_hours else None
            self.clusters[c.id] = f

    # ------------------------------------------------------------------ jobs
    def _build_jobs(self) -> None:
        cluster_jobs: dict[str, set[str]] = defaultdict(set)
        for j in self.store.of_type("job"):
            if not isinstance(j, Job):
                continue
            a = j.attributes
            f = JobFacts(
                job_id=j.id, name=j.name, logical_name=a.get("logical_name") or j.name.lower(), creator=j.creator, run_as=j.run_as,
                trigger_kind=a.get("trigger_kind") or ("cron" if j.schedule else "manual"), schedule=j.schedule, paused=j.paused,
                task_count=j.task_count, task_fingerprints=list(a.get("task_fingerprints") or []),
                task_sources=list(a.get("task_sources") or []), pipeline_ids=list(a.get("pipeline_ids") or []),
                max_retries=int(a.get("max_retries") or 0), job_clusters=list(a.get("job_clusters") or []),
                existing_cluster_ids=list(a.get("existing_cluster_ids") or []), uses_serverless=bool(a.get("uses_serverless")),
                monthly_cost=self.store.monthly_cost(j.id), incident_cost=self.store.incident_cost(j.id),
            )
            rs = self.run_stats("job", j.id)
            if rs:
                f.run_stats_available = True
                f.runs, f.window_days = rs.runs, rs.window_days
                f.runs_per_day = rs.runs_per_day
                f.success_rate, f.avg_run_s = rs.success_rate, rs.avg_run_s
                f.cv_run = (rs.std_run_s / rs.avg_run_s) if rs.std_run_s is not None and rs.avg_run_s else None
                f.avg_exec_s, f.avg_queue_s, f.avg_setup_s = rs.avg_exec_s, rs.avg_queue_s, rs.avg_setup_s
                if rs.avg_setup_s is not None and rs.avg_exec_s:
                    f.setup_ratio = rs.avg_setup_s / (rs.avg_setup_s + rs.avg_exec_s)
                f.total_hours = rs.total_hours or 0.0
                f.last_run, f.trigger_types, f.hour_hist = rs.last_run, dict(rs.trigger_types), dict(rs.start_hour_histogram)
                f.task_runs = rs.task_runs
                f.retry_ratio = rs.retries / rs.task_runs if rs.task_runs else 0.0
                f.retry_success_ratio = rs.retried_then_succeeded / rs.retried_task_runs if rs.retried_task_runs else None
                f.compute_ids = list(rs.compute_ids)
            for cid in set(f.compute_ids) | set(f.existing_cluster_ids):
                cf = self.clusters.get(cid)
                if cf is None:
                    continue
                cluster_jobs[cid].add(j.id)
                (f.job_cluster_ids if cf.is_job_cluster else f.all_purpose_ids).append(cid)
            jc = [self.clusters[c] for c in f.job_cluster_ids if self.clusters[c].metrics_available]
            if jc:
                f.compute_metrics_available = True
                w = [c.node_hours for c in jc]
                f.node_hours = sum(w)
                f.avg_cpu = _wavg([(c.avg_cpu, c.node_hours) for c in jc])
                f.p95_cpu = _wavg([(c.p95_cpu, c.node_hours) for c in jc])
                f.avg_mem = _wavg([(c.avg_mem, c.node_hours) for c in jc])
                f.p95_mem = _wavg([(c.p95_mem, c.node_hours) for c in jc])
                f.idle_ratio = _wavg([(c.idle_ratio, c.node_hours) for c in jc])
            cl = [self.clusters[c] for c in f.job_cluster_ids]
            f.availability = sorted({c.availability for c in cl if c.availability} | {s["availability"] for s in f.job_clusters if s.get("availability")})
            f.node_types = sorted({c.node_type for c in cl if c.node_type} | {s["node_type"] for s in f.job_clusters if s.get("node_type")})
            f.max_workers_seen = max([c.configured_max_workers for c in cl] + [int(s.get("max_workers") or s.get("num_workers") or 0) for s in f.job_clusters] or [0])
            lin = self.lineage("job", j.id)
            if lin:
                f.lineage_available = True
                f.sources, f.targets = list(lin.sources), list(lin.targets)
                spd = [x for x in (self.table_writes_per_day(s) for s in f.sources if not s.startswith("path:")) if x is not None]
                f.source_writes_per_day = max(spd) if spd else None
                reads = [self.table_reads(t) for t in f.targets if not t.startswith("path:")]
                f.target_reads = sum(r for r in reads if r is not None) if reads and all(r is not None for r in reads) else None
            self.jobs[j.id] = f
        for cid, jobs in cluster_jobs.items():
            self.clusters[cid].job_ids = sorted(jobs)

    # ------------------------------------------------------------------ pipelines
    def _build_pipelines(self) -> None:
        trig: dict[str, set[str]] = defaultdict(set)
        for jf in self.jobs.values():
            for pid in jf.pipeline_ids:
                trig[pid].add(jf.job_id)
        for p in self.store.of_type("pipeline"):
            if not isinstance(p, Pipeline):
                continue
            a = p.attributes
            f = PipelineFacts(
                pipeline_id=p.id, name=p.name, logical_name=a.get("logical_name") or p.name.lower(), state=p.state,
                continuous=p.continuous, development=a.get("development"), serverless=p.serverless, edition=p.edition,
                libraries=list(a.get("libraries") or []), creator=p.creator, triggering_job_ids=sorted(trig.get(p.id, set())),
                event_log_table=a.get("event_log_table"),
            )
            cd = self.store.cost_details(p.id)
            f.monthly_cost, f.incident_cost = cd["monthly_cost"], cd["incident_cost"]
            f.cost_trimmed, f.cost_basis = cd["trimmed_cost"], cd["basis"]
            rs = self.run_stats("pipeline", p.id)
            if rs:
                f.run_stats_available = True
                f.runs, f.window_days, f.runs_per_day = rs.runs, rs.window_days, rs.runs_per_day
                f.success_rate, f.full_refreshes = rs.success_rate, rs.full_refreshes
                f.full_refresh_ratio = rs.full_refreshes / rs.runs if rs.runs else 0.0
                f.last_run, f.total_hours = rs.last_run, rs.total_hours or 0.0
                f.trigger_types, f.hour_hist = dict(rs.trigger_types), dict(rs.start_hour_histogram)
            f.days_since_run = days_since(f.last_run, self.now) if f.last_run else None
            lin = self.lineage("pipeline", p.id)
            if lin:
                f.lineage_available = True
                f.sources, f.targets = list(lin.sources), list(lin.targets)
                own = set(f.sources)
                f.final_targets = [t for t in f.targets if t not in own and not t.startswith("path:")]
                reads = [self.table_reads(t) for t in f.final_targets]
                if reads and all(r is not None for r in reads):
                    f.consumer_reads = sum(reads)
                spd = [x for x in (self.table_writes_per_day(s) for s in f.sources if s not in set(f.targets) and not s.startswith("path:")) if x is not None]
                f.source_writes_per_day = max(spd) if spd else None
            self.pipelines[p.id] = f

    def summary(self) -> dict[str, Any]:
        return {"clusters": len(self.clusters), "jobs": len(self.jobs), "pipelines": len(self.pipelines), "pools": len(self.pools),
                "run_stats": len(self._run_stats), "lineage": len(self._lineage)}
