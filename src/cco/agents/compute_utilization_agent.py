"""Agent B - Compute Utilization.

Purpose: detect underutilized, oversized or expensive compute.

Cluster-level checks (all-purpose clusters)
  cluster_idle_ratio            nodes idle (<5% CPU) most of the time -> auto-termination / pause
  start_stop_window             always-on cluster idle nights/weekends -> scheduled start/stop
  autoscaling_disabled          fixed-size multi-worker cluster -> enable autoscaling
  autoscaling_ineffective       min workers ~= max workers -> lower min
  overprovisioned_all_purpose   p95 CPU and memory low -> fewer / smaller workers
  low_concurrency               big shared cluster used by <= 1 workload at low CPU -> consolidate
  old_node_generation           previous-generation instance families -> current generation
Job-level checks (job clusters, aggregated over the job's runs)
  job_cluster_oversized         jobs running on too-large clusters
  job_runtime_vs_work           cluster hours vs useful work (idle while the job runs)
  jobs_on_all_purpose           scheduled jobs on all-purpose compute -> job clusters
  startup_shutdown_waste        setup time is a large share of run time
  on_demand_batch               reliable batch job on on-demand nodes -> spot with fallback
  queue_time                    runs wait in queue
Fleet-level checks
  pool_fragmentation            several pools for one node type / idle warm instances
  consolidation                 several low-utilization all-purpose clusters with compatible config

Metrics reported: utilization %, active vs idle hours, queue time, autoscaling effectiveness,
avg/p95 CPU and memory, workloads per cluster, cost per job run, cluster hours vs useful work.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Any

from cco.agents.base import AgentContext, AgentResult, register_agent
from cco.agents.workload_base import CheckOut, WorkloadAgent
from cco.etl.facts import FactStore
from cco.etl.workload_facts import ClusterFacts, JobFacts
from cco.models.canonical import ActionType as A
from cco.models.canonical import Risk, Severity

DEFAULTS: dict[str, Any] = {
    "min_node_hours": 10,
    "min_monthly_cost": 25,
    "idle_ratio": 0.5,
    "max_autotermination_minutes": 60,
    "recommended_autotermination_minutes": 30,
    "always_on_ratio": 0.6,
    "offhours_min_share": 0.3,
    "offhours_idle_ratio": 0.7,
    "autoscale_min_fixed_workers": 2,
    "autoscale_min_ratio": 0.8,
    "overprovisioned_p95_cpu": 40,
    "overprovisioned_p95_mem": 50,
    "target_cpu_pct": 70,
    "target_mem_pct": 80,
    "min_resize_factor": 0.25,
    "low_concurrency_max_cpu": 20,
    "low_concurrency_min_workers": 4,
    "job_min_runs": 3,
    "job_idle_ratio": 0.5,
    "startup_setup_ratio": 0.3,
    "startup_min_runs": 20,
    "queue_seconds": 300,
    "spot_min_success_rate": 0.9,
    "vm_cost_ratio": 1.0,          # cloud VM $ per $ of DBUs (VM cost is not in system.billing)
    "spot_discount": 0.6,
    "all_purpose_premium": 0.45,   # share saved moving work from all-purpose to jobs compute
    "pool_vm_hourly_cost": 0.5,
    "node_generation_upgrades": {"m4": "m6i", "c4": "c6i", "r4": "r6i", "i3": "i4i", "d2": "d3", "m5": "m6i", "c5": "c6i", "r5": "r6i"},
    "node_generation_savings": 0.1,
}


def _family(node_type: str | None) -> str | None:
    m = re.match(r"^([a-z]+\d+)", (node_type or "").lower())
    return m.group(1) if m else None


@register_agent
class ComputeUtilizationAgent(WorkloadAgent):
    name = "compute_utilization"
    domain = "compute"
    description = "Detects idle, oversized and expensive clusters and job compute (idle ratio, autoscaling, spot, pools, sizing)."
    required_object_types = {"cluster"}
    priority = 20
    DEFAULTS = DEFAULTS

    def is_applicable(self, facts: FactStore) -> tuple[bool, str]:
        wl = facts.workloads
        n = sum(1 for c in wl.clusters.values() if c.metrics_available)
        return (n > 0 or bool(wl.pools), f"{n} clusters with utilization metrics, {len(wl.pools)} pools")

    def cluster_checks(self) -> list:
        return [self.check_cluster_idle_ratio, self.check_start_stop_window, self.check_autoscaling_disabled,
                self.check_autoscaling_effectiveness, self.check_overprovisioned_all_purpose,
                self.check_low_concurrency, self.check_old_node_generation]

    def job_checks(self) -> list:
        return [self.check_job_cluster_oversized, self.check_job_runtime_vs_work, self.check_jobs_on_all_purpose,
                self.check_startup_waste, self.check_on_demand_batch, self.check_queue_time]

    def analyze(self, facts: FactStore, ctx: AgentContext) -> AgentResult:
        res = AgentResult(agent=self.name)
        wl = facts.workloads
        self._wl = wl
        self._low_concurrency: list[ClusterFacts] = []
        ap = [c for c in wl.clusters.values() if not c.is_job_cluster and c.metrics_available
              and c.node_hours >= float(self.t["min_node_hours"]) and self.in_scope(ctx, c.cluster_id, c.name)]
        jobs = [j for j in wl.jobs.values() if j.runs >= int(self.t["job_min_runs"]) and self.in_scope(ctx, j.job_id, j.name)]
        n_c = self.run_checks(res, ctx, ap, self.cluster_checks(), key=lambda c: c.cluster_id)
        n_j = self.run_checks(res, ctx, jobs, self.job_checks(), key=lambda j: j.job_id)
        for out in (self.check_pool_fragmentation(), self.check_consolidation()):
            self.emit(res, ctx, out)
        util = [c.avg_cpu for c in wl.clusters.values() if c.metrics_available and c.avg_cpu is not None]
        node_h = sum(c.node_hours for c in wl.clusters.values())
        useful_h = sum(c.useful_node_hours for c in wl.clusters.values())
        res.stats = {
            "all_purpose_clusters_analyzed": n_c, "jobs_analyzed": n_j, "pools": len(wl.pools),
            "fleet_avg_cpu_pct": round(sum(util) / len(util), 1) if util else None,
            "fleet_node_hours": round(node_h, 1), "fleet_useful_node_hours": round(useful_h, 1),
            "fleet_useful_work_ratio": round(useful_h / node_h, 3) if node_h else None,
            "findings": len(res.findings), "recommendations": len(res.recommendations),
        }
        return res

    # ------------------------------------------------------------------ metrics helpers
    def _cluster_metrics(self, c: ClusterFacts) -> dict[str, Any]:
        return {
            "utilization_pct": c.avg_cpu, "p95_cpu_pct": c.p95_cpu, "avg_mem_pct": c.avg_mem, "p95_mem_pct": c.p95_mem,
            "idle_ratio": c.idle_ratio, "node_hours": round(c.node_hours, 1), "active_hours": round(c.active_hours, 1),
            "idle_node_hours": round(c.node_hours * (c.idle_ratio or 0), 1), "useful_node_hours": round(c.useful_node_hours, 1),
            "avg_nodes": c.avg_nodes, "configured_max_workers": c.configured_max_workers, "workloads": len(c.job_ids),
            "monthly_cost": round(c.monthly_cost, 2),
        }

    def _job_metrics(self, j: JobFacts) -> dict[str, Any]:
        return {
            "runs": j.runs, "runs_per_day": round(j.runs_per_day, 2), "cost_per_run": round(j.cost_per_run, 2) if j.cost_per_run else None,
            "avg_run_min": round((j.avg_run_s or 0) / 60, 1), "avg_queue_s": j.avg_queue_s, "avg_setup_s": j.avg_setup_s,
            "setup_ratio": j.setup_ratio, "node_hours": round(j.node_hours, 1), "utilization_pct": j.avg_cpu,
            "p95_cpu_pct": j.p95_cpu, "p95_mem_pct": j.p95_mem, "idle_ratio": j.idle_ratio, "monthly_cost": round(j.monthly_cost, 2),
        }

    def _resize_factor(self, p95_cpu: float | None, p95_mem: float | None) -> float:
        cpu = (p95_cpu or 0) / float(self.t["target_cpu_pct"])
        mem = (p95_mem or 0) / float(self.t["target_mem_pct"])
        return min(1.0, max(float(self.t["min_resize_factor"]), cpu, mem))

    # ------------------------------------------------------------------ cluster checks
    def check_cluster_idle_ratio(self, c: ClusterFacts) -> CheckOut:
        if c.idle_ratio is None or c.idle_ratio < float(self.t["idle_ratio"]):
            return None
        sev = Severity.HIGH if c.idle_ratio >= 0.7 and c.monthly_cost >= 500 else Severity.MEDIUM
        f = self.finding(c.cluster_id, "cluster", "cluster_idle_ratio", f"Cluster {c.name} idle {self.ratio(c.idle_ratio)} of node time",
                         f"{c.node_hours * c.idle_ratio:,.0f} of {c.node_hours:,.0f} node-hours had <{5}% CPU; auto-termination = "
                         f"{c.autotermination_minutes or 'disabled'} min.", sev, **self._cluster_metrics(c))
        recs = []
        at, mx = c.autotermination_minutes, int(self.t["max_autotermination_minutes"])
        target = int(self.t["recommended_autotermination_minutes"])
        if not at or at > mx or c.idle_ratio >= 0.7:
            new_at = min(target, at) if at else target
            recs.append(self.rec(
                c.cluster_id, "cluster", A.SET_AUTOTERMINATION, f"Pause idle cluster {c.name}: auto-terminate after {new_at} min",
                "Idle all-purpose clusters bill DBUs and VMs while doing nothing; a shorter auto-termination window stops idle time.",
                severity=sev, confidence=0.8, savings=c.monthly_cost * c.idle_ratio * 0.6, monthly_cost=c.monthly_cost,
                basis="monthly cost x idle ratio x 60% recoverable", proposed={"autotermination_minutes": new_at},
                current_autotermination_minutes=at))
        return f, recs

    def check_start_stop_window(self, c: ClusterFacts) -> CheckOut:
        if (c.always_on_ratio < float(self.t["always_on_ratio"]) or (c.offhours_share or 0) < float(self.t["offhours_min_share"])
                or (c.offhours_idle_ratio or 0) < float(self.t["offhours_idle_ratio"])):
            return None
        f = self.finding(c.cluster_id, "cluster", "start_stop_window", f"Cluster {c.name} runs nights/weekends while idle",
                         f"Up {self.ratio(c.always_on_ratio)} of the window; {self.ratio(c.offhours_share)} of node time is off-hours and "
                         f"{self.ratio(c.offhours_idle_ratio)} of that is idle.", Severity.MEDIUM, **self._cluster_metrics(c),
                         offhours_share=c.offhours_share, offhours_idle_ratio=c.offhours_idle_ratio)
        return f, [self.rec(
            c.cluster_id, "cluster", A.SCHEDULE_START_STOP, f"Schedule start/stop window for {c.name} (business hours)",
            "Stop the cluster outside business hours (scheduled job or policy) and rely on auto-start for ad-hoc use.",
            severity=Severity.MEDIUM, confidence=0.7, monthly_cost=c.monthly_cost,
            savings=c.monthly_cost * (c.offhours_share or 0) * (c.offhours_idle_ratio or 0) * 0.8,
            basis="monthly cost x off-hours share x off-hours idle ratio x 80%",
            proposed={"run_window": "weekdays 07:00-19:00"})]

    def check_autoscaling_disabled(self, c: ClusterFacts) -> CheckOut:
        if c.autoscale or (c.num_workers or 0) < int(self.t["autoscale_min_fixed_workers"]):
            return None
        util = max((c.avg_cpu or 0) / float(self.t["target_cpu_pct"]), (c.avg_mem or 0) / float(self.t["target_mem_pct"]))
        new_min = max(1, math.ceil((c.num_workers or 1) * min(1.0, util)))
        f = self.finding(c.cluster_id, "cluster", "autoscaling_disabled", f"Autoscaling disabled on {c.name} ({c.num_workers} fixed workers)",
                         f"Average CPU {self.pct(c.avg_cpu)} / memory {self.pct(c.avg_mem)}: the fixed fleet is rarely fully used.",
                         Severity.MEDIUM, **self._cluster_metrics(c))
        return f, [self.rec(
            c.cluster_id, "cluster", A.ENABLE_AUTOSCALING, f"Enable autoscaling on {c.name} ({new_min}-{c.num_workers} workers)",
            "Autoscaling releases workers when load drops instead of paying for the peak size all the time.",
            severity=Severity.MEDIUM, confidence=0.65, monthly_cost=c.monthly_cost, savings=c.monthly_cost * max(0.0, 1 - util) * 0.5,
            basis="monthly cost x (1 - utilization vs target) x 50%",
            proposed={"autoscale": {"min_workers": new_min, "max_workers": c.num_workers}})]

    def check_autoscaling_effectiveness(self, c: ClusterFacts) -> CheckOut:
        if not c.autoscale or not c.max_workers or c.max_workers < 2:
            return None
        mn, mx = c.min_workers or 0, c.max_workers
        if mn < float(self.t["autoscale_min_ratio"]) * mx:
            return None
        new_min = max(1, mx // 4)
        f = self.finding(c.cluster_id, "cluster", "autoscaling_ineffective", f"Autoscaling on {c.name} is effectively fixed ({mn}-{mx})",
                         f"Min workers is {mn / mx:.0%} of max, so the cluster can never scale down; average nodes {c.avg_nodes or 'n/a'}.",
                         Severity.LOW, **self._cluster_metrics(c), autoscaling_effectiveness=round(1 - mn / mx, 2))
        return f, [self.rec(
            c.cluster_id, "cluster", A.TUNE_AUTOSCALING, f"Lower min workers on {c.name} to {new_min}",
            "A low minimum lets autoscaling release capacity between bursts.", severity=Severity.LOW, confidence=0.6,
            monthly_cost=c.monthly_cost, savings=c.monthly_cost * (mn - new_min) / mx * 0.4 * (1 - (c.avg_cpu or 0) / 100),
            basis="monthly cost x removable min share x 40% x idle share",
            proposed={"autoscale": {"min_workers": new_min, "max_workers": mx}})]

    def check_overprovisioned_all_purpose(self, c: ClusterFacts) -> CheckOut:
        if c.p95_cpu is None or c.p95_cpu >= float(self.t["overprovisioned_p95_cpu"]):
            return None
        if c.p95_mem is not None and c.p95_mem >= float(self.t["overprovisioned_p95_mem"]):
            return None
        factor = self._resize_factor(c.p95_cpu, c.p95_mem)
        cur = c.configured_max_workers
        proposed = ({"max_workers" if c.autoscale else "num_workers": max(1, math.ceil(cur * factor))} if cur >= 2
                    else {"node_type": f"smaller than {c.node_type}"})
        sev = Severity.HIGH if c.monthly_cost >= 1000 else Severity.MEDIUM
        f = self.finding(c.cluster_id, "cluster", "overprovisioned_all_purpose", f"Over-provisioned all-purpose cluster {c.name}",
                         f"p95 CPU {self.pct(c.p95_cpu)}, p95 memory {self.pct(c.p95_mem)} on {cur or 'single-node'} x {c.node_type}.",
                         sev, **self._cluster_metrics(c), resize_factor=round(factor, 2))
        return f, [self.rec(
            c.cluster_id, "cluster", A.RESIZE_CLUSTER, f"Right-size {c.name} to ~{factor:.0%} of current capacity",
            "Peak (p95) utilization stays far below capacity; fewer or smaller workers handle the same load.",
            severity=sev, risk=Risk.MEDIUM, confidence=0.65, monthly_cost=c.monthly_cost, savings=c.monthly_cost * (1 - factor) * 0.8,
            basis="monthly cost x (1 - resize factor) x 80%", proposed=proposed)]

    def check_low_concurrency(self, c: ClusterFacts) -> CheckOut:
        if (c.avg_cpu is None or c.avg_cpu >= float(self.t["low_concurrency_max_cpu"]) or len(c.job_ids) > 1
                or c.configured_max_workers < int(self.t["low_concurrency_min_workers"])):
            return None
        self._low_concurrency.append(c)
        f = self.finding(c.cluster_id, "cluster", "low_concurrency", f"Low concurrency on shared cluster {c.name}",
                         f"{c.configured_max_workers}-worker all-purpose cluster serves {len(c.job_ids)} scheduled workload(s) at "
                         f"{self.pct(c.avg_cpu)} average CPU.", Severity.LOW, **self._cluster_metrics(c))
        return f, []  # consolidation recommendation is emitted fleet-wide (check_consolidation)

    def check_old_node_generation(self, c: ClusterFacts) -> CheckOut:
        return self._node_generation(c.cluster_id, "cluster", c.name, [c.node_type], c.monthly_cost)

    def _node_generation(self, rid: str, rtype: str, name: str, node_types: list[str | None], cost: float) -> CheckOut:
        upgrades = self.t["node_generation_upgrades"]
        old = sorted({n for n in node_types if _family(n) in upgrades})
        if not old or cost < float(self.t["min_monthly_cost"]):
            return None
        target = {n: re.sub(r"^[a-z]+\d+", upgrades[_family(n)], n) for n in old}
        f = self.finding(rid, rtype, "old_node_generation", f"{name} uses previous-generation instances ({', '.join(old)})",
                         "Newer instance families give more performance per dollar at similar list price.", Severity.LOW,
                         node_types=old, monthly_cost=round(cost, 2))
        return f, [self.rec(
            rid, rtype, A.CHANGE_NODE_TYPE, f"Move {name} to current-generation nodes ({', '.join(target.values())})",
            "Current-generation (or Graviton) families are typically 10-20% cheaper per unit of work; validate runtime support.",
            severity=Severity.LOW, risk=Risk.MEDIUM, confidence=0.5, monthly_cost=cost, savings=cost * float(self.t["node_generation_savings"]),
            basis="monthly cost x 10% price-performance gain", proposed={"node_type": target})]

    # ------------------------------------------------------------------ job checks
    def check_job_cluster_oversized(self, j: JobFacts) -> CheckOut:
        if (not j.compute_metrics_available or j.node_hours < float(self.t["min_node_hours"]) or j.p95_cpu is None
                or j.p95_cpu >= float(self.t["overprovisioned_p95_cpu"])
                or (j.p95_mem is not None and j.p95_mem >= float(self.t["overprovisioned_p95_mem"]))):
            return None
        factor = self._resize_factor(j.p95_cpu, j.p95_mem)
        cur = j.max_workers_seen
        proposed = {"workers": max(1, math.ceil(cur * factor))} if cur >= 2 else {"node_type": f"smaller than {', '.join(j.node_types)}"}
        sev = Severity.HIGH if j.monthly_cost >= 1000 else Severity.MEDIUM
        f = self.finding(j.job_id, "job", "job_cluster_oversized", f"Job {j.name} runs on a too-large cluster",
                         f"Across {j.runs} runs: p95 CPU {self.pct(j.p95_cpu)}, p95 memory {self.pct(j.p95_mem)} on up to {cur} x "
                         f"{', '.join(j.node_types) or 'n/a'}.", sev, **self._job_metrics(j), resize_factor=round(factor, 2))
        return f, [self.rec(
            j.job_id, "job", A.RESIZE_CLUSTER, f"Right-size job cluster for {j.name} to ~{factor:.0%}",
            "The job never uses most of its cluster; a smaller job cluster finishes in similar time for less.",
            severity=sev, risk=Risk.MEDIUM, confidence=0.65, monthly_cost=j.monthly_cost, savings=j.monthly_cost * (1 - factor) * 0.8,
            basis="monthly job cost x (1 - resize factor) x 80%", proposed=proposed, related=j.job_cluster_ids[:20])]

    def check_job_runtime_vs_work(self, j: JobFacts) -> CheckOut:
        if (not j.compute_metrics_available or j.idle_ratio is None or j.idle_ratio < float(self.t["job_idle_ratio"])
                or j.node_hours < float(self.t["min_node_hours"])):
            return None
        f = self.finding(j.job_id, "job", "job_runtime_vs_work", f"Job {j.name}: cluster idle {self.ratio(j.idle_ratio)} of its runtime",
                         f"{j.node_hours * j.idle_ratio:,.0f} of {j.node_hours:,.0f} node-hours had no meaningful CPU (driver-only work, "
                         "waits/sleeps, sequential tasks on a multi-node cluster).", Severity.MEDIUM, **self._job_metrics(j),
                         useful_node_hours=round(j.node_hours * (1 - j.idle_ratio), 1))
        return f, [self.rec(
            j.job_id, "job", A.RESIZE_CLUSTER, f"Shrink or switch {j.name} to single-node / serverless compute",
            "Most cluster-hours do no work; fewer workers (or serverless, which bills only for work) remove the idle share.",
            severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.6, monthly_cost=j.monthly_cost,
            savings=j.monthly_cost * j.idle_ratio * 0.6, basis="monthly job cost x idle ratio x 60%", related=j.job_cluster_ids[:20])]

    def check_jobs_on_all_purpose(self, j: JobFacts) -> CheckOut:
        if not j.all_purpose_ids:
            return None
        clusters = [self._wl.clusters[c] for c in j.all_purpose_ids]
        ap_cost = sum(c.monthly_cost for c in clusters)
        share = min(1.0, j.total_hours / max(1e-6, sum(c.active_hours for c in clusters))) if any(c.active_hours for c in clusters) else 0.5
        savings = ap_cost * share * float(self.t["all_purpose_premium"])
        f = self.finding(j.job_id, "job", "jobs_on_all_purpose", f"Scheduled job {j.name} runs on all-purpose compute",
                         f"{j.runs} runs in {j.window_days}d on {', '.join(c.name or c.cluster_id for c in clusters)}; all-purpose DBUs cost "
                         "~2x jobs compute and the job competes with interactive users.", Severity.MEDIUM,
                         **self._job_metrics(j), all_purpose_share_of_cluster=round(share, 2))
        return f, [self.rec(
            j.job_id, "job", A.CONVERT_TO_JOB_CLUSTER, f"Move {j.name} to a job cluster or serverless jobs compute",
            "Job clusters are billed at the cheaper Jobs Compute rate, are isolated, and terminate when the run ends.",
            severity=Severity.MEDIUM, confidence=0.75, savings=savings, monthly_cost=ap_cost,
            basis="all-purpose cost x job's share of cluster time x 45% rate difference", related=j.all_purpose_ids)]

    def check_startup_waste(self, j: JobFacts) -> CheckOut:
        if (j.uses_serverless or j.setup_ratio is None or j.setup_ratio < float(self.t["startup_setup_ratio"])
                or j.runs < int(self.t["startup_min_runs"])):
            return None
        f = self.finding(j.job_id, "job", "startup_shutdown_waste", f"Job {j.name}: {self.ratio(j.setup_ratio)} of run time is cluster startup",
                         f"Average setup {j.avg_setup_s or 0:.0f}s vs execution {j.avg_exec_s or 0:.0f}s over {j.runs} runs "
                         f"({j.runs_per_day:.1f}/day).", Severity.MEDIUM, **self._job_metrics(j))
        return f, [self.rec(
            j.job_id, "job", A.REDUCE_STARTUP_OVERHEAD, f"Cut startup overhead for {j.name} (serverless, pools, or batch runs)",
            "Frequent short runs pay cluster spin-up each time; serverless starts in seconds, pools keep warm VMs, "
            "or fewer/larger batches amortize startup.", severity=Severity.MEDIUM, confidence=0.6, monthly_cost=j.monthly_cost,
            savings=j.monthly_cost * j.setup_ratio * 0.5, basis="monthly job cost x setup share x 50%")]

    def check_on_demand_batch(self, j: JobFacts) -> CheckOut:
        if (not j.availability or any("SPOT" in a for a in j.availability) or j.monthly_cost < float(self.t["min_monthly_cost"])
                or (j.success_rate is not None and j.success_rate < float(self.t["spot_min_success_rate"]))):
            return None
        savings = j.monthly_cost * float(self.t["vm_cost_ratio"]) * float(self.t["spot_discount"]) * 0.7
        f = self.finding(j.job_id, "job", "on_demand_batch", f"Batch job {j.name} uses only on-demand instances",
                         f"Availability {', '.join(j.availability)}; success rate {self.ratio(j.success_rate)} - a good spot candidate.",
                         Severity.LOW, **self._job_metrics(j))
        return f, [self.rec(
            j.job_id, "job", A.USE_SPOT, f"Use spot workers with on-demand fallback for {j.name}",
            "Batch job clusters tolerate spot reclamation; keep the driver on-demand (first_on_demand=1) and fall back automatically.",
            severity=Severity.LOW, confidence=0.6, savings=savings,
            basis="estimated cloud VM cost (vm_cost_ratio x DBU cost) x spot discount x 70% worker share (VM cost not in billing tables)",
            proposed={"availability": "SPOT_WITH_FALLBACK", "first_on_demand": 1})]

    def check_queue_time(self, j: JobFacts) -> CheckOut:
        if j.avg_queue_s is None or j.avg_queue_s < float(self.t["queue_seconds"]):
            return None
        f = self.finding(j.job_id, "job", "queue_time", f"Job {j.name} waits {j.avg_queue_s / 60:.1f} min in queue per run",
                         "Runs are queued by max_concurrent_runs or workspace concurrency limits, delaying downstream SLAs.",
                         Severity.LOW, **self._job_metrics(j))
        return f, [self.rec(
            j.job_id, "job", A.SHIFT_SCHEDULE, f"Stagger or re-window {j.name} to avoid queueing",
            "Move the schedule away from the busiest window (or raise max_concurrent_runs if overlap is intended).",
            severity=Severity.LOW, confidence=0.5, monthly_cost=j.monthly_cost)]

    # ------------------------------------------------------------------ fleet checks
    def check_pool_fragmentation(self) -> list[CheckOut]:
        out: list[CheckOut] = []
        by_type: dict[str, list] = defaultdict(list)
        for p in self._wl.pools:
            by_type[p.node_type or "?"].append(p)
            if p.min_idle > 0 and p.used_count == 0:
                cost = p.min_idle * 730 * float(self.t["pool_vm_hourly_cost"])
                f = self.finding(p.id, "pool", "pool_idle_capacity", f"Pool {p.name} keeps {p.min_idle} warm instances with no usage",
                                 f"min_idle_instances={p.min_idle}, used={p.used_count}, idle={p.idle_count}.", Severity.LOW,
                                 min_idle=p.min_idle, used=p.used_count, idle=p.idle_count)
                out.append((f, [self.rec(p.id, "pool", A.REDUCE_POOL_IDLE, f"Set min idle instances of pool {p.name} to 0",
                                         "Idle pool instances bill cloud VM cost even when no cluster uses them.", severity=Severity.LOW,
                                         confidence=0.7, savings=cost, basis="min_idle x 730h x pool_vm_hourly_cost",
                                         proposed={"min_idle_instances": 0})]))
        for node_type, pools in by_type.items():
            if len(pools) < 2:
                continue
            gid = f"pools:{node_type}"
            f = self.finding(gid, "pool_group", "pool_fragmentation", f"{len(pools)} pools for node type {node_type}",
                             "Separate pools for the same node type fragment warm capacity, so each needs its own idle buffer.",
                             Severity.LOW, pools=[p.name for p in pools], idle_instances=sum(p.idle_count for p in pools))
            out.append((f, [self.rec(gid, "pool_group", A.CONSOLIDATE_POOLS, f"Consolidate {len(pools)} {node_type} pools into one",
                                     "One shared pool per node type maximizes reuse of warm instances.", severity=Severity.LOW,
                                     confidence=0.6, related=[p.id for p in pools],
                                     savings=sum(p.min_idle for p in pools[1:]) * 730 * float(self.t["pool_vm_hourly_cost"]),
                                     basis="min_idle of the removed pools x 730h x VM hourly cost")]))
        return out

    def check_consolidation(self) -> list[CheckOut]:
        groups: dict[tuple, list[ClusterFacts]] = defaultdict(list)
        for c in self._low_concurrency:
            groups[(_family(c.node_type), (c.runtime or "").split(".")[0], c.data_security_mode)].append(c)
        out: list[CheckOut] = []
        for key, cs in groups.items():
            if len(cs) < 2:
                continue
            gid = "clusters:" + ":".join(str(k) for k in key)
            cost = sum(c.monthly_cost for c in cs)
            f = self.finding(gid, "cluster_group", "consolidation", f"{len(cs)} low-utilization all-purpose clusters with compatible config",
                             f"{', '.join(c.name for c in cs)}: average CPU "
                             f"{sum((c.avg_cpu or 0) for c in cs) / len(cs):.0f}%, combined {self.money(cost)}/month.",
                             Severity.MEDIUM, clusters=[c.cluster_id for c in cs], monthly_cost=round(cost, 2))
            out.append((f, [self.rec(gid, "cluster_group", A.CONSOLIDATE_CLUSTERS, f"Consolidate {len(cs)} clusters into one shared autoscaling cluster",
                                     "Fewer, shared clusters (same access mode/runtime) raise concurrency and cut idle baseline nodes.",
                                     severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.55, monthly_cost=cost,
                                     savings=cost * (1 - 1 / len(cs)) * 0.5, basis="combined cost x (1 - 1/n) x 50%",
                                     related=[c.cluster_id for c in cs])]))
        return out
