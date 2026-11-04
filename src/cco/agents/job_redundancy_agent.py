"""Agent C - Jobs / Workflow Redundancy.

Purpose: detect duplicate or inefficient orchestration.

Fleet-level checks (compare jobs with each other)
  duplicate_jobs                   identical task logic (fingerprints) in several jobs
  same_logic_multiple_deployments  same logical job deployed several times (per-user bundle copies, teams)
  same_lineage                     same upstream + same downstream datasets -> same transformation
  overlapping_schedules            downstream job scheduled in parallel with its upstream -> chain
  peak_window_contention           expensive job in the busiest window with queueing -> move window
Per-job checks
  unnecessary_retries              retries that rarely succeed
  reprocess_unchanged_inputs       runs more often than its inputs change
  full_reprocessing                targets fully overwritten each run -> incremental
  low_value_output                 cost with no consumed output
  failing_job                      low success rate wastes compute
  duration_variance                unstable runtime (informational)

Metrics: overlap frequency (start-hour cosine), dependency similarity (Jaccard), run duration CV,
retry ratio, success rate, source->target lineage, compute hours per output.
"""
from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Any

from cco.agents.base import AgentContext, AgentResult, register_agent
from cco.agents.workload_base import CheckOut, WorkloadAgent, connected_groups
from cco.etl.facts import WRITE_OPS, FactStore
from cco.etl.fingerprint import cosine, jaccard
from cco.etl.workload_facts import JobFacts
from cco.models.canonical import ActionType as A
from cco.models.canonical import Risk, Severity, Table

DEFAULTS: dict[str, Any] = {
    "min_runs": 3,
    "min_monthly_cost": 10,
    "lineage_jaccard": 0.9,
    "schedule_overlap_cosine": 0.8,
    "max_pairs_per_key": 50,
    "retry_ratio": 0.2,
    "retry_success_max": 0.3,
    "min_task_runs": 10,
    "reprocess_ratio": 1.5,
    "reprocess_min_runs_per_day": 2,
    "overwrite_share": 0.8,
    "low_value_min_cost": 100,
    "duration_cv": 1.0,
    "duration_min_runs": 10,
    "failing_success_rate": 0.8,
    "failing_min_runs": 10,
    "peak_hours": 3,
    "peak_share": 0.5,
    "peak_queue_s": 120,
    "shift_min_cost": 200,
}
TIME_TRIGGERS = {"cron", "periodic", "manual"}
OVERWRITE_OPS = {"CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE TABLE AS SELECT", "TRUNCATE"}


@register_agent
class JobRedundancyAgent(WorkloadAgent):
    name = "job_redundancy"
    domain = "orchestration"
    description = "Detects duplicate jobs, overlapping schedules, wasted retries, reprocessing of unchanged inputs and low-value jobs."
    required_object_types = {"job"}
    priority = 30
    DEFAULTS = DEFAULTS

    def is_applicable(self, facts: FactStore) -> tuple[bool, str]:
        n = len(facts.workloads.jobs)
        return (n > 0, f"{n} jobs ({'with' if facts.workloads.run_stats_available else 'without'} run history)")

    def job_checks(self) -> list:
        return [self.check_unnecessary_retries, self.check_reprocess_unchanged_inputs, self.check_full_reprocessing,
                self.check_low_value_output, self.check_failing_job, self.check_duration_variance]

    def analyze(self, facts: FactStore, ctx: AgentContext) -> AgentResult:
        res = AgentResult(agent=self.name)
        self._facts = facts
        wl = facts.workloads
        jobs = [j for j in wl.jobs.values() if self.in_scope(ctx, j.job_id, j.name)]
        active = [j for j in jobs if j.runs >= int(self.t["min_runs"]) and not j.paused]
        self._flagged: set[str] = set()
        for out in (self.check_duplicate_jobs(jobs, active),
                    self.check_overlapping_schedules(active), self.check_peak_window(active)):
            self.emit(res, ctx, out)
        self.run_checks(res, ctx, active, self.job_checks(), key=lambda j: j.job_id)
        retry = [j.retry_ratio for j in active if j.task_runs]
        res.stats = {
            "jobs": len(jobs), "active_jobs": len(active), "jobs_with_lineage": sum(1 for j in jobs if j.lineage_available),
            "avg_retry_ratio": round(sum(retry) / len(retry), 3) if retry else None,
            "findings": len(res.findings), "recommendations": len(res.recommendations),
        }
        return res

    def _m(self, j: JobFacts) -> dict[str, Any]:
        return {
            "runs": j.runs, "runs_per_day": round(j.runs_per_day, 2), "success_rate": j.success_rate,
            "retry_ratio": round(j.retry_ratio, 3), "duration_cv": round(j.cv_run, 2) if j.cv_run is not None else None,
            "monthly_cost": round(j.monthly_cost, 2), "cost_per_run": round(j.cost_per_run, 2) if j.cost_per_run else None,
            "compute_hours_per_output": round(j.compute_hours_per_output, 2) if j.compute_hours_per_output else None,
            "trigger_kind": j.trigger_kind, "externally_triggered": j.externally_triggered,
        }

    @staticmethod
    def _keeper(group: list[JobFacts]) -> JobFacts:
        return sorted(group, key=lambda j: (-j.runs, -j.monthly_cost, j.job_id))[0]

    def _sched_note(self, j: JobFacts) -> str:
        return (" Runs are triggered externally (API); change the trigger in the external scheduler." if j.externally_triggered else "")

    # ------------------------------------------------------------------ fleet checks
    # duplicate-relationship kinds, strongest first:
    #   kind -> (check name, action, confidence, savings share, savings basis, human-readable reason)
    DUP_KINDS: dict[str, tuple] = {
        "same_code_same_name": ("same_logic_multiple_deployments", A.CONSOLIDATE_DEPLOYMENTS, 0.8, 0.9,
                                "duplicate job monthly cost x 90%", "identical task logic deployed several times"),
        "same_code": ("duplicate_jobs", A.MERGE_JOBS, 0.8, 0.9, "duplicate job monthly cost x 90%", "identical task logic"),
        "same_lineage": ("same_lineage", A.MERGE_JOBS, 0.7, 0.8, "redundant job monthly cost x 80%",
                         "same source-to-target lineage (the same tables are computed more than once)"),
        "same_name_sources": ("same_logic_multiple_deployments", A.CONSOLIDATE_DEPLOYMENTS, 0.5, 0.5,
                              "copy's monthly cost x 50%", "same code and sources with different parameters/owners"),
    }
    DUP_RANK = {k: i for i, k in enumerate(DUP_KINDS)}

    def check_duplicate_jobs(self, jobs: list[JobFacts], active: list[JobFacts] | None = None) -> list[CheckOut]:
        """Duplicate jobs: identical task logic, the same logical job deployed several times, or the same source->target
        lineage. All mutually-duplicate jobs form ONE set -> ONE finding on the keeper listing every copy; each copy gets
        its own merge/retire recommendation linked to that finding."""
        jm = {j.job_id: j for j in jobs}
        edges: list[tuple[str, str, str, float]] = []  # (a, b, kind, similarity)
        by_fp: dict[tuple, list[JobFacts]] = defaultdict(list)
        for j in jobs:
            if j.task_fingerprints:
                by_fp[tuple(j.task_fingerprints)].append(j)
        seen: set[str] = set()
        for group in by_fp.values():
            if len(group) < 2:
                continue
            kind = "same_code_same_name" if len({j.logical_name for j in group}) == 1 else "same_code"
            ids = sorted(j.job_id for j in group)
            seen.update(ids)
            edges += [(ids[0], other, kind, 1.0) for other in ids[1:]]
        by_name: dict[tuple, list[JobFacts]] = defaultdict(list)  # same logical job + same sources, different parameters
        for j in jobs:
            if j.task_sources and j.job_id not in seen:
                by_name[(j.logical_name, tuple(j.task_sources))].append(j)
        for group in by_name.values():
            ids = sorted(j.job_id for j in group)
            edges += [(ids[0], other, "same_name_sources", 1.0) for other in ids[1:]]
        by_target: dict[str, list[JobFacts]] = defaultdict(list)
        for j in (active if active is not None else jobs):
            if j.lineage_available and j.targets:
                for t in j.targets:
                    by_target[t].append(j)
        pairs: set[tuple[str, str]] = set()
        cap = int(self.t["max_pairs_per_key"])
        for group in by_target.values():
            for a, b in list(combinations(sorted(group, key=lambda x: x.job_id), 2))[:cap]:
                pairs.add((a.job_id, b.job_id))
        thr = float(self.t["lineage_jaccard"])
        for a_id, b_id in sorted(pairs):
            a, b = jm[a_id], jm[b_id]
            js, jt = jaccard(a.sources, b.sources), jaccard(a.targets, b.targets)
            if js >= thr and jt >= thr:
                edges.append((a_id, b_id, "same_lineage", min(js, jt)))
        return [self._dup_set([jm[i] for i in members], es) for members, es in connected_groups(edges)]

    def _dup_set(self, group: list[JobFacts], edges: list[tuple[str, str, str, float]]) -> CheckOut:
        keeper = self._keeper(group)
        dups = sorted((d for d in group if d.job_id != keeper.job_id), key=lambda d: d.job_id)
        self._flagged.update(d.job_id for d in dups)
        dup_ids = [d.job_id for d in dups]
        kinds = sorted({e[2] for e in edges}, key=self.DUP_RANK.get)
        set_kind = kinds[0]

        def kind_of(job_id: str) -> str:  # strongest relationship this copy has inside the set
            own = [e[2] for e in edges if job_id in (e[0], e[1])]
            return min(own, key=self.DUP_RANK.get) if own else set_kind

        dup_cost = sum(d.monthly_cost for d in dups)
        lineage = "same_lineage" in kinds
        names = ", ".join(d.name for d in dups[:5]) + (f" (+{len(dups) - 5} more)" if len(dups) > 5 else "")
        why = "; ".join(self.DUP_KINDS[k][5] for k in kinds)
        shared = sorted(set.intersection(*(set(j.targets) for j in group)))[:10] if lineage else None
        sev = Severity.HIGH if lineage or dup_cost >= 500 else Severity.MEDIUM
        f = self.finding(keeper.job_id, "job", self.DUP_KINDS[set_kind][0],
                         f"Job {keeper.name} has {len(dups)} duplicate(s): {names}",
                         f"{len(group)} jobs with {why}. Keep {keeper.name} ({keeper.runs} runs) and retire or merge the "
                         f"{len(dups)} cop{'y' if len(dups) == 1 else 'ies'} ({self.money(dup_cost)}/month).", sev,
                         resource_metrics=self._m(keeper), keeper=keeper.job_id, duplicates=dup_ids,
                         duplicate_names=[d.name for d in dups][:20], group_size=len(group), match_kinds=kinds,
                         duplicates_monthly_cost=round(dup_cost, 2),
                         dependency_similarity=round(min(e[3] for e in edges), 2), shared_targets=shared or None)
        recs = []
        for d in dups:
            k = kind_of(d.job_id)
            _, action, conf, share, basis, _ = self.DUP_KINDS[k]
            title = {"same_code_same_name": f"Retire duplicate deployment {d.name} (same logic as {keeper.name})",
                     "same_code": f"Merge duplicate job {d.name} into {keeper.name}",
                     "same_lineage": f"Merge {d.name} into {keeper.name} (same source-to-target lineage)",
                     "same_name_sources": f"Consolidate deployment {d.name} with {keeper.name}"}[k]
            why_rec = {"same_lineage": "Two writers of the same tables from the same inputs waste compute and risk conflicting writes.",
                       "same_name_sources": "The same logic is deployed several times; parameterize one job instead of maintaining copies."
                       }.get(k, "Identical tasks run more than once; keep one owner/schedule and pause the copies "
                                "(per-user dev bundle deployments should not keep active schedules).")
            recs.append(self.rec(d.job_id, "job", action, title, why_rec,
                                 severity=Severity.HIGH if k == "same_lineage" or d.monthly_cost >= 500 else Severity.MEDIUM,
                                 risk=Risk.MEDIUM, confidence=conf, monthly_cost=d.monthly_cost, savings=d.monthly_cost * share,
                                 basis=basis, related=[keeper.job_id], keeper_job_id=keeper.job_id,
                                 group_key=f"jobdup:{keeper.job_id}"))
        return f, recs

    def check_overlapping_schedules(self, jobs: list[JobFacts]) -> list[CheckOut]:
        out: list[CheckOut] = []
        writers: dict[str, list[JobFacts]] = defaultdict(list)
        for j in jobs:
            for t in j.targets:
                writers[t].append(j)
        thr = float(self.t["schedule_overlap_cosine"])
        done: set[str] = set()
        for b in jobs:  # b = downstream candidate
            if b.trigger_kind not in TIME_TRIGGERS or b.job_id in done or b.job_id in self._flagged or not b.hour_hist:
                continue
            best = None
            for s in b.sources:
                for a in writers.get(s, []):
                    if a.job_id == b.job_id or not a.hour_hist:
                        continue
                    ov = cosine(a.hour_hist, b.hour_hist)
                    if ov >= thr and (best is None or ov > best[1]):
                        best = (a, ov)
            if not best:
                continue
            a, ov = best
            done.add(b.job_id)
            shared = sorted(set(a.targets) & set(b.sources))[:10]
            f = self.finding(b.job_id, "job", "overlapping_schedules", f"{b.name} is scheduled in parallel with its upstream {a.name}",
                             f"{b.name} reads {len(shared)} table(s) written by {a.name}; their start-hour profiles overlap {ov:.0%}, so "
                             "runs either read stale data or race the writer.", Severity.MEDIUM, resource_metrics=self._m(b),
                             overlap_frequency=round(ov, 2), upstream_job=a.job_id, shared_tables=shared)
            out.append((f, [self.rec(b.job_id, "job", A.CHAIN_JOBS, f"Chain {b.name} after {a.name} (run-job task or table-update trigger)",
                                     "Event-driven chaining runs the downstream exactly once per upstream refresh instead of on a parallel clock."
                                     + self._sched_note(b), severity=Severity.MEDIUM, confidence=0.6, monthly_cost=b.monthly_cost,
                                     savings=b.monthly_cost * max(0.0, 1 - a.runs_per_day / b.runs_per_day) * 0.7 if b.runs_per_day > a.runs_per_day else 0.0,
                                     basis="downstream cost x share of runs beyond upstream refreshes x 70%",
                                     related=[a.job_id], proposed={"trigger": {"table_update": {"table_names": shared}}})]))
        return out

    def check_peak_window(self, jobs: list[JobFacts]) -> list[CheckOut]:
        load: dict[int, int] = defaultdict(int)
        for j in jobs:
            for h, n in j.hour_hist.items():
                load[h] += n
        if not load:
            return []
        peak = {h for h, _ in sorted(load.items(), key=lambda x: -x[1])[: int(self.t["peak_hours"])]}
        out: list[CheckOut] = []
        for j in jobs:
            tot = sum(j.hour_hist.values())
            if not tot or j.monthly_cost < float(self.t["shift_min_cost"]) or (j.avg_queue_s or 0) < float(self.t["peak_queue_s"]):
                continue
            share = sum(n for h, n in j.hour_hist.items() if h in peak) / tot
            if share < float(self.t["peak_share"]):
                continue
            f = self.finding(j.job_id, "job", "peak_window_contention", f"Expensive job {j.name} runs in the busiest window",
                             f"{share:.0%} of runs start in peak hours {sorted(peak)} (UTC) and wait {j.avg_queue_s:.0f}s in queue.",
                             Severity.LOW, resource_metrics=self._m(j), peak_hours=sorted(peak), peak_share=round(share, 2))
            out.append((f, [self.rec(j.job_id, "job", A.SHIFT_SCHEDULE, f"Move {j.name} to a lower-contention window",
                                     "Off-peak windows reduce queueing and contention for shared capacity and spot interruptions." + self._sched_note(j),
                                     severity=Severity.LOW, confidence=0.5, monthly_cost=j.monthly_cost,
                                     proposed={"avoid_hours_utc": sorted(peak)})]))
        return out

    # ------------------------------------------------------------------ per-job checks
    def check_unnecessary_retries(self, j: JobFacts) -> CheckOut:
        if j.task_runs < int(self.t["min_task_runs"]) or j.retry_ratio < float(self.t["retry_ratio"]):
            return None
        rsr = j.retry_success_ratio
        if rsr is not None and rsr > float(self.t["retry_success_max"]):
            return None
        wasted = j.retry_ratio / (1 + j.retry_ratio) * (1 - (rsr or 0))
        f = self.finding(j.job_id, "job", "unnecessary_retries", f"Job {j.name}: {j.retry_ratio:.0%} retry ratio, retries rarely succeed",
                         f"{j.task_runs} task runs; retried task runs eventually succeeded {self.ratio(rsr)} of the time; "
                         f"max_retries={j.max_retries}.", Severity.MEDIUM, resource_metrics=self._m(j), retry_success_ratio=rsr)
        return f, [self.rec(j.job_id, "job", A.REDUCE_RETRIES, f"Reduce retry policy for {j.name} and fix the root failure",
                            "Retries that fail again burn a full task run each time; cap retries (e.g. 1 with backoff) and alert instead.",
                            severity=Severity.MEDIUM, confidence=0.7, monthly_cost=j.monthly_cost, savings=j.monthly_cost * wasted * 0.8,
                            basis="monthly cost x failed-retry share of attempts x 80%", proposed={"max_retries": 1})]

    def check_reprocess_unchanged_inputs(self, j: JobFacts) -> CheckOut:
        if (j.trigger_kind not in TIME_TRIGGERS or j.source_writes_per_day is None
                or j.runs_per_day < float(self.t["reprocess_min_runs_per_day"])):
            return None
        writes = j.source_writes_per_day
        if j.runs_per_day <= float(self.t["reprocess_ratio"]) * max(writes, 1e-9):
            return None
        unchanged = max(0.0, 1 - writes / j.runs_per_day)
        sev = Severity.HIGH if j.monthly_cost * unchanged >= 500 else Severity.MEDIUM
        f = self.finding(j.job_id, "job", "reprocess_unchanged_inputs", f"Job {j.name} runs {j.runs_per_day:.1f}x/day; inputs change {writes:.1f}x/day",
                         f"~{unchanged:.0%} of runs process unchanged upstream tables.", sev, resource_metrics=self._m(j),
                         source_writes_per_day=round(writes, 2), unchanged_run_share=round(unchanged, 2))
        return f, [self.rec(j.job_id, "job", A.TRIGGER_ON_CHANGE, f"Trigger {j.name} on upstream table updates instead of a clock",
                            "A table-update (or file-arrival) trigger runs only when inputs change; incremental reads avoid rescanning."
                            + self._sched_note(j), severity=sev, confidence=0.7, monthly_cost=j.monthly_cost,
                            savings=j.monthly_cost * unchanged * 0.7, basis="monthly cost x unchanged-run share x 70%",
                            proposed={"trigger": {"table_update": {"table_names": [s for s in j.sources if not s.startswith('path:')][:10]}}})]

    def check_full_reprocessing(self, j: JobFacts) -> CheckOut:
        if j.monthly_cost < float(self.t["min_monthly_cost"]) or not j.targets:
            return None
        writes = over = 0
        for t in j.targets:
            tbl = self._facts.get("table", t)
            if not isinstance(tbl, Table):
                continue
            for op in tbl.history:
                if op.operation in WRITE_OPS:
                    writes += 1
                    mode = str((op.parameters or {}).get("mode", "")).lower()
                    over += int(op.operation in OVERWRITE_OPS or mode == "overwrite")
        if writes < 5 or over / writes < float(self.t["overwrite_share"]):
            return None
        f = self.finding(j.job_id, "job", "full_reprocessing", f"Job {j.name} rebuilds its targets on every run",
                         f"{over}/{writes} recent writes to its target tables are full overwrites.", Severity.MEDIUM, resource_metrics=self._m(j),
                         overwrite_share=round(over / writes, 2))
        return f, [self.rec(j.job_id, "job", A.ADD_INCREMENTAL, f"Make {j.name} incremental (MERGE / streaming / materialized view)",
                            "Processing only new or changed rows avoids recomputing history each run.", severity=Severity.MEDIUM,
                            risk=Risk.MEDIUM, confidence=0.5, monthly_cost=j.monthly_cost, savings=j.monthly_cost * 0.4,
                            basis="monthly cost x 40% typical incremental saving")]

    def check_low_value_output(self, j: JobFacts) -> CheckOut:
        if not j.lineage_available or j.monthly_cost < float(self.t["low_value_min_cost"]) or j.job_id in self._flagged:
            return None
        table_targets = [t for t in j.targets if not t.startswith("path:")]
        if table_targets and j.target_reads == 0:
            f = self.finding(j.job_id, "job", "low_value_output", f"Job {j.name} writes {len(table_targets)} table(s) nobody reads",
                             f"No reads of its outputs in the access window while costing {self.money(j.monthly_cost)}/month.",
                             Severity.HIGH if j.monthly_cost >= 500 else Severity.MEDIUM, resource_metrics=self._m(j), targets=table_targets[:10])
            return f, [self.rec(j.job_id, "job", A.DECOMMISSION_JOB, f"Pause or decommission {j.name} (outputs unused)",
                                "Confirm with owners; pause the schedule first (reversible), then retire the job and its tables.",
                                severity=f.severity, risk=Risk.MEDIUM, confidence=0.6, monthly_cost=j.monthly_cost,
                                savings=j.monthly_cost * 0.9, basis="monthly job cost x 90%", related=table_targets[:20])]
        if not j.targets and j.sources:
            f = self.finding(j.job_id, "job", "low_value_output", f"Job {j.name} reads data but writes no tracked output",
                             "No Unity Catalog targets in lineage; it may export externally or be a leftover.", Severity.LOW, resource_metrics=self._m(j))
            return f, [self.rec(j.job_id, "job", A.REVIEW, f"Review purpose of {j.name} (no tracked outputs)",
                                "Verify the job still produces value (external export, alerts) before keeping its schedule.",
                                severity=Severity.LOW, confidence=0.3, monthly_cost=j.monthly_cost)]
        return None

    def check_failing_job(self, j: JobFacts) -> CheckOut:
        if j.success_rate is None or j.runs < int(self.t["failing_min_runs"]) or j.success_rate >= float(self.t["failing_success_rate"]):
            return None
        f = self.finding(j.job_id, "job", "failing_job", f"Job {j.name} succeeds only {j.success_rate:.0%} of runs",
                         f"{j.runs} runs in {j.window_days}d; failed runs consume compute without producing output.",
                         Severity.HIGH if j.success_rate < 0.5 else Severity.MEDIUM, resource_metrics=self._m(j))
        return f, [self.rec(j.job_id, "job", A.FIX_FAILING_WORKLOAD, f"Fix recurring failures in {j.name}",
                            "Fix the root cause (or pause the schedule) so failed runs stop consuming compute.",
                            severity=f.severity, confidence=0.7, monthly_cost=j.monthly_cost,
                            savings=j.monthly_cost * (1 - j.success_rate) * 0.8, basis="monthly cost x failure share x 80%")]

    def check_duration_variance(self, j: JobFacts) -> CheckOut:
        if j.cv_run is None or j.runs < int(self.t["duration_min_runs"]) or j.cv_run < float(self.t["duration_cv"]):
            return None
        f = self.finding(j.job_id, "job", "duration_variance", f"Job {j.name} has unstable runtime (CV {j.cv_run:.1f})",
                         f"Average {(j.avg_run_s or 0) / 60:.1f} min with high variance: data skew, contention or variable input size.",
                         Severity.INFO, resource_metrics=self._m(j))
        return f, []
