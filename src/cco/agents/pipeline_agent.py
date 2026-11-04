"""Agent D - Pipeline / Data Flow Optimization.

Purpose: detect stale or redundant data pipelines (Lakeflow Spark Declarative Pipelines).

Per-pipeline checks
  pipeline_inactive          no updates for a long time
  no_downstream_consumers    final outputs are never read (pipelines producing unused data)
  scheduled_too_frequently   updates far more often than inputs change / continuous without need
  processing_same_data       frequent full refreshes re-process all data
  critical_pipeline_unstable heavily consumed pipeline with failures
Fleet checks
  duplicate_pipelines        same source code or same source->target lineage as another pipeline
  duplicate_ingestion        several pipelines/jobs ingest the same raw data (evidence-based, layout-agnostic)
  lineage_incomplete         (info) a pipeline writes tables but lineage shows no data sources -> ingestion checks skipped
  failed_update_spend        spend from canceled / failed updates, reported as an incident (not steady-state cost)

Duplicate-ingestion evidence is purely structural (no project-specific folder names):
  * sources are compared at the granularity lineage observed - the same file/table is a match, sibling files are not;
  * paths collapse to a directory root only with data-partition evidence: trailing date segments (yyyy/mm/dd,
    yyyy-mm-dd), hive key=value partitions, or >= N distinct data files under one directory read by the same workload;
  * self-references are dropped: sources that are also targets of the same workload and the pipeline's own event log;
  * code / config artifacts (.yaml, .py, .jar, ...) read at startup are not ingested data.
"""
from __future__ import annotations

import fnmatch
import re
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from typing import Any, Iterable

from cco.agents.base import AgentContext, AgentResult, register_agent
from cco.agents.workload_base import CheckOut, WorkloadAgent
from cco.agents.workload_base import connected_groups as _connected_groups
from cco.etl.facts import FactStore
from cco.etl.fingerprint import jaccard
from cco.etl.workload_facts import PipelineFacts
from cco.models.canonical import ActionType as A
from cco.models.canonical import Risk, Severity

DEFAULTS: dict[str, Any] = {
    "inactivity_days": 60,
    "lineage_jaccard": 0.8,
    "ingestion_path_jaccard": 0.8,
    "ingestion_target_jaccard_max": 0.5,
    "too_frequent_ratio": 3.0,
    "min_runs_per_day_frequency": 4,
    "full_refresh_ratio": 0.3,
    "full_refresh_min_runs": 5,
    "critical_min_consumer_reads": 1000,
    "critical_max_failure_rate": 0.1,
    "critical_min_runs": 5,
    "min_monthly_cost": 10,
    "max_pairs_per_key": 50,
    # ---- duplicate-ingestion evidence (structural; no project-specific names)
    "multi_file_root_min_files": 3,  # >= N distinct data files in one directory read by one workload => that dir is a dataset root
    "max_workloads_per_root": 4,     # a source read by more workloads is shared reference data, not duplicated ingestion
    # file formats that are code / configuration artifacts, never ingested data (format-level, not folder names)
    "non_data_file_extensions": ["yaml", "yml", "toml", "ini", "cfg", "conf", "properties", "py", "pyc", "sql", "scala",
                                 "r", "ipynb", "sh", "jar", "whl", "egg", "md"],
    # optional org-specific glob overrides (e.g. "*/shared_reference/*"); EMPTY by default - set in settings.yaml
    "ingestion_exclude_patterns": [],
    "emit_lineage_gaps": True,       # info finding when a pipeline's ingestion sources are missing from lineage
    "incident_min_cost": 50,         # report canceled/failed-update spend above this as an incident
}

_EXT = re.compile(r"\.([A-Za-z0-9]{1,8})$")
_YEAR = re.compile(r"^(?:19|20)\d{2}$")
_ISO_DATE = re.compile(r"^(?:19|20)\d{2}-\d{2}(?:-\d{2})?(?:[T_ ]\d{2}.*)?$")
_SMALL_NUM = re.compile(r"^\d{1,2}$")  # mm / dd / hh - only counts after a year or hive segment gives evidence
_HIVE = re.compile(r"^[^=/]+=[^/]*$")

# strength of a source match (higher = more specific evidence)
BASIS_RANK = {"exact_table": 3, "exact_path": 3, "partitioned_root": 2, "multi_file_root": 1}


def _path(src: str) -> str | None:
    return src[5:].rstrip("/") if src.startswith("path:") else None


def _ext(segment: str) -> str | None:
    m = _EXT.search(segment)
    return m.group(1).lower() if m else None


def partition_root(path: str) -> str | None:
    """Directory above trailing data-partition segments, or None without structural partition evidence.

    '/v/landing/sales/2026/10/22/a.json' -> '/v/landing/sales';  's3://b/x/dt=2026-10-22/p.parquet' -> 's3://b/x';
    '/v/cfg/a.yaml' -> None (a plain file is compared as itself).
    """
    segs = path.rstrip("/").split("/")
    if len(segs) > 1 and _ext(segs[-1]):
        segs = segs[:-1]
    i, evidence = len(segs), False
    while i > 1:
        s = segs[i - 1]
        if _YEAR.match(s) or _ISO_DATE.match(s) or _HIVE.match(s):
            evidence = True
        elif not _SMALL_NUM.match(s):
            break
        i -= 1
    root = "/".join(segs[:i])
    return root if evidence and root else None


def ingestion_root(src: str) -> str | None:
    """Comparison key of a storage source: its partitioned dataset root when there is partition evidence, else the
    exact observed path. 'path:/V/landing/sales/2026/10/22/a.json' -> '/V/landing/sales'; tables -> None."""
    p = _path(src)
    if not p:
        return None
    return partition_root(p) or p


def _id_forms(ids: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for i in ids:
        i = str(i or "").lower()
        if len(i) >= 8:  # only ids specific enough to be embedded in a table name (UUID-style pipeline ids)
            out |= {i, i.replace("-", "_"), i.replace("-", "")}
    return out


@dataclass
class SourceEvidence:
    keys: dict[str, str] = field(default_factory=dict)      # comparison key -> basis (see BASIS_RANK)
    dropped: dict[str, int] = field(default_factory=dict)   # reason -> count

    @property
    def path_keys(self) -> set[str]:
        return {k for k, b in self.keys.items() if b != "exact_table"}


def source_evidence(sources: Iterable[str], targets: Iterable[str], *, owner_ids: Iterable[str] = (),
                    event_log_tables: Iterable[str | None] = (), non_data_ext: Iterable[str] = (),
                    exclude_patterns: Iterable[str] = (), multi_file_min: int = 3) -> SourceEvidence:
    """Reduce a workload's observed lineage sources to comparison keys using structural evidence only."""
    tset = {t.lower() for t in targets}
    ids = _id_forms(owner_ids)
    logs = {t.lower() for t in event_log_tables if t}
    ndx = {e.lower().lstrip(".") for e in non_data_ext}
    pats = [p for p in exclude_patterns if p]
    ev = SourceEvidence()
    dropped: dict[str, int] = defaultdict(int)

    def put(key: str, basis: str) -> None:
        old = ev.keys.get(key)
        if old is None or BASIS_RANK[basis] < BASIS_RANK[old]:  # keep the weaker evidence (conservative)
            ev.keys[key] = basis

    plain: list[str] = []
    for s in dict.fromkeys(sources):
        low = s.lower()
        if low in tset:
            dropped["own_target"] += 1
            continue
        p = _path(s)
        if p is None:
            if low in logs or any(i in low for i in ids):
                dropped["own_event_log"] += 1
            else:
                put(s, "exact_table")
            continue
        if not p:
            continue
        if any(fnmatch.fnmatch(p, pat) or fnmatch.fnmatch(s, pat) for pat in pats):
            dropped["user_excluded"] += 1
            continue
        if (_ext(p.rsplit("/", 1)[-1]) or "") in ndx:
            dropped["non_data_file"] += 1
            continue
        root = partition_root(p)
        if root:
            put(root, "partitioned_root")
        else:
            plain.append(p)
    files_by_dir: dict[str, set[str]] = defaultdict(set)
    for p in plain:
        d, _, leaf = p.rpartition("/")
        if d and _ext(leaf):
            files_by_dir[d].add(p)
    for p in plain:
        d = p.rpartition("/")[0]
        if len(files_by_dir.get(d, ())) >= max(2, multi_file_min):
            put(d, "multi_file_root")
        else:
            put(p, "exact_path")
    ev.dropped = dict(dropped)
    return ev


@register_agent
class PipelineOptimizationAgent(WorkloadAgent):
    name = "pipeline_optimization"
    domain = "pipelines"
    description = "Detects inactive, unused, duplicate and over-scheduled pipelines and duplicate ingestion paths."
    required_object_types = {"pipeline"}
    priority = 40
    DEFAULTS = DEFAULTS

    def is_applicable(self, facts: FactStore) -> tuple[bool, str]:
        n = len(facts.workloads.pipelines)
        return (n > 0, f"{n} pipelines")

    def pipeline_checks(self) -> list:
        return [self.check_inactive, self.check_no_downstream_consumers, self.check_scheduled_too_frequently,
                self.check_processing_same_data, self.check_critical_pipeline, self.check_incident_spend]

    def analyze(self, facts: FactStore, ctx: AgentContext) -> AgentResult:
        res = AgentResult(agent=self.name)
        wl = facts.workloads
        self._wl = wl
        pipes = [p for p in wl.pipelines.values() if self.in_scope(ctx, p.pipeline_id, p.name)]
        self._flagged: set[str] = set()
        for out in (self.check_duplicate_pipelines(pipes), self.check_duplicate_ingestion(pipes)):
            self.emit(res, ctx, out)
        self.run_checks(res, ctx, pipes, self.pipeline_checks(), key=lambda p: p.pipeline_id)
        res.stats = {
            "pipelines": len(pipes), "with_run_history": sum(1 for p in pipes if p.run_stats_available),
            "with_lineage": sum(1 for p in pipes if p.lineage_available),
            "continuous": sum(1 for p in pipes if p.continuous),
            "findings": len(res.findings), "recommendations": len(res.recommendations),
        }
        return res

    def _m(self, p: PipelineFacts) -> dict[str, Any]:
        return {
            "updates": p.runs, "updates_per_day": round(p.runs_per_day, 2), "success_rate": p.success_rate,
            "full_refresh_ratio": round(p.full_refresh_ratio, 2), "days_since_update": round(p.days_since_run, 1) if p.days_since_run is not None else None,
            "consumer_reads": p.consumer_reads, "source_writes_per_day": p.source_writes_per_day, "continuous": p.continuous,
            "monthly_cost": round(p.monthly_cost, 2), "triggering_jobs": p.triggering_job_ids[:10],
            "cost_basis": p.cost_basis, "incident_cost": round(p.incident_cost, 2) if p.incident_cost else None,
            "outlier_cost_trimmed": round(p.cost_trimmed, 2) if p.cost_trimmed else None,
        }

    @staticmethod
    def _keeper(group: list[PipelineFacts]) -> PipelineFacts:
        return sorted(group, key=lambda p: (-(p.consumer_reads or 0), -p.runs, p.pipeline_id))[0]

    # ------------------------------------------------------------------ per pipeline
    def check_inactive(self, p: PipelineFacts) -> CheckOut:
        if not self._wl.run_stats_available or p.continuous:
            return None
        days = int(self.t["inactivity_days"])
        if p.run_stats_available and p.days_since_run is not None and p.days_since_run < days:
            return None
        last = f"{p.days_since_run:.0f} days ago" if p.days_since_run is not None else "not in the lookback window"
        f = self.finding(p.pipeline_id, "pipeline", "pipeline_inactive", f"Pipeline {p.name} inactive (last update {last})",
                         f"No updates for at least {days} days; its tables are likely stale and its definition is clutter.",
                         Severity.LOW, resource_metrics=self._m(p))
        return f, [self.rec(p.pipeline_id, "pipeline", A.ARCHIVE_PIPELINE, f"Archive inactive pipeline {p.name}",
                            "Confirm with the owner, then delete the pipeline (keep or archive its tables) to stop accidental refreshes.",
                            severity=Severity.LOW, risk=Risk.MEDIUM, reversible=False, confidence=0.7,
                            related=p.triggering_job_ids, monthly_cost=p.monthly_cost)]

    def check_no_downstream_consumers(self, p: PipelineFacts) -> CheckOut:
        if not p.lineage_available or not p.final_targets or p.consumer_reads is None or p.consumer_reads > 0 or p.runs == 0:
            return None
        if p.pipeline_id in self._flagged:
            return None
        sev = Severity.HIGH if p.monthly_cost >= 500 else Severity.MEDIUM
        f = self.finding(p.pipeline_id, "pipeline", "no_downstream_consumers", f"Pipeline {p.name} produces data nobody reads",
                         f"{len(p.final_targets)} output table(s) had no reads in the access window, yet the pipeline ran {p.runs} updates.",
                         sev, resource_metrics=self._m(p), final_targets=p.final_targets[:10])
        return f, [self.rec(p.pipeline_id, "pipeline", A.PAUSE_PIPELINE, f"Pause pipeline {p.name} (no downstream consumers)",
                            "Pause the triggering schedule/job first (reversible); archive the pipeline if no owner objects.",
                            severity=sev, risk=Risk.MEDIUM, confidence=0.65, monthly_cost=p.monthly_cost, savings=p.monthly_cost * 0.9,
                            basis="monthly pipeline cost x 90%", related=p.triggering_job_ids + p.final_targets[:10])]

    def check_scheduled_too_frequently(self, p: PipelineFacts) -> CheckOut:
        if p.continuous:
            if p.source_writes_per_day is not None and p.source_writes_per_day > 24:
                return None  # sources change constantly: continuous is justified
            f = self.finding(p.pipeline_id, "pipeline", "scheduled_too_frequently", f"Continuous pipeline {p.name} with slow-changing inputs",
                             f"Inputs change {p.source_writes_per_day if p.source_writes_per_day is not None else 'an unknown number of'} times/day; "
                             "continuous mode keeps compute running 24x7.", Severity.MEDIUM, resource_metrics=self._m(p))
            return f, [self.rec(p.pipeline_id, "pipeline", A.REDUCE_FREQUENCY, f"Switch {p.name} from continuous to triggered",
                                "Run triggered updates on a table-update/file-arrival trigger (or a schedule matching data latency SLAs).",
                                severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.55, monthly_cost=p.monthly_cost,
                                savings=p.monthly_cost * 0.6, basis="monthly cost x 60% (continuous -> triggered)",
                                proposed={"continuous": False, "trigger": "table_update"})]
        if (p.runs_per_day < float(self.t["min_runs_per_day_frequency"]) or p.source_writes_per_day is None
                or p.runs_per_day <= float(self.t["too_frequent_ratio"]) * max(p.source_writes_per_day, 1e-9)):
            return None
        unchanged = max(0.0, 1 - p.source_writes_per_day / p.runs_per_day)
        f = self.finding(p.pipeline_id, "pipeline", "scheduled_too_frequently", f"Pipeline {p.name} updates {p.runs_per_day:.1f}x/day for {p.source_writes_per_day:.1f} input changes/day",
                         f"~{unchanged:.0%} of updates find no new upstream data.", Severity.MEDIUM, resource_metrics=self._m(p),
                         unchanged_update_share=round(unchanged, 2))
        return f, [self.rec(p.pipeline_id, "pipeline", A.TRIGGER_ON_CHANGE, f"Trigger {p.name} on upstream changes (reduce polling)",
                            "Use a table-update / file-arrival trigger on the triggering job, or lower the schedule frequency; "
                            "for streaming sources set maxFilesPerTrigger / maxBytesPerTrigger as backpressure.",
                            severity=Severity.MEDIUM, confidence=0.65, monthly_cost=p.monthly_cost, savings=p.monthly_cost * unchanged * 0.7,
                            basis="monthly cost x no-new-data update share x 70%", related=p.triggering_job_ids)]

    def check_processing_same_data(self, p: PipelineFacts) -> CheckOut:
        if p.runs < int(self.t["full_refresh_min_runs"]) or p.full_refresh_ratio < float(self.t["full_refresh_ratio"]):
            return None
        f = self.finding(p.pipeline_id, "pipeline", "processing_same_data", f"Pipeline {p.name}: {p.full_refresh_ratio:.0%} of updates are full refreshes",
                         f"{p.full_refreshes} of {p.runs} updates recomputed all data from scratch.", Severity.MEDIUM, resource_metrics=self._m(p))
        return f, [self.rec(p.pipeline_id, "pipeline", A.ADD_INCREMENTAL, f"Stop routine full refreshes of {p.name}",
                            "Use incremental refresh (streaming tables / incrementalizable MVs); reserve full refresh for schema or logic changes.",
                            severity=Severity.MEDIUM, confidence=0.6, monthly_cost=p.monthly_cost,
                            savings=p.monthly_cost * p.full_refresh_ratio * 0.5, basis="monthly cost x full-refresh share x 50%")]

    def check_critical_pipeline(self, p: PipelineFacts) -> CheckOut:
        if (p.consumer_reads is None or p.consumer_reads < float(self.t["critical_min_consumer_reads"]) or p.success_rate is None
                or p.runs < int(self.t["critical_min_runs"]) or 1 - p.success_rate <= float(self.t["critical_max_failure_rate"])):
            return None
        f = self.finding(p.pipeline_id, "pipeline", "critical_pipeline_unstable", f"Critical pipeline {p.name} fails {1 - p.success_rate:.0%} of updates",
                         f"Outputs are read {p.consumer_reads:,.0f} times in the window; failures delay many consumers.",
                         Severity.MEDIUM, resource_metrics=self._m(p))
        return f, [self.rec(p.pipeline_id, "pipeline", A.PRIORITIZE_PIPELINE, f"Prioritize {p.name}: reliability and capacity first",
                            "Give critical pipelines dedicated/serverless performance-optimized compute, failure alerts and retries, "
                            "and schedule them ahead of low-value workloads.", severity=Severity.MEDIUM, confidence=0.6,
                            monthly_cost=p.monthly_cost, related=p.triggering_job_ids)]

    def check_incident_spend(self, p: PipelineFacts) -> CheckOut:
        """Spend from canceled / failed updates is an incident, excluded from the steady-state monthly cost."""
        if p.incident_cost < float(self.t["incident_min_cost"]):
            return None
        sev = Severity.HIGH if p.incident_cost >= 1000 else Severity.MEDIUM if p.incident_cost >= 200 else Severity.LOW
        f = self.finding(p.pipeline_id, "pipeline", "failed_update_spend",
                         f"Pipeline {p.name}: {self.money(p.incident_cost)} spent on canceled/failed updates",
                         f"{self.money(p.incident_cost)} of billed usage in the cost window came from updates that ended canceled or failed. "
                         f"It is excluded from the steady-state estimate ({self.money(p.monthly_cost)}/month). Look for stuck or runaway "
                         "updates and add update timeouts and failure alerts.", sev, resource_metrics=self._m(p))
        return f, []

    # ------------------------------------------------------------------ fleet
    def check_duplicate_pipelines(self, pipes: list[PipelineFacts]) -> list[CheckOut]:
        """Duplicate pipelines (same code - incl. copies deployed by different users - or same source->target lineage).

        All mutually-duplicate pipelines form ONE set -> ONE finding on the keeper listing every copy; each copy gets its
        own retire recommendation, linked to that finding (per-copy, so a retire action never supersedes the keeper).
        """
        pm = {p.pipeline_id: p for p in pipes}
        edges: list[tuple[str, str, str, float]] = []
        by_code: dict[tuple, list[PipelineFacts]] = defaultdict(list)
        for p in pipes:
            if p.libraries:
                by_code[tuple(p.libraries)].append(p)
        for group in by_code.values():
            ids = sorted(p.pipeline_id for p in group)
            edges += [(ids[0], other, "same source code", 1.0) for other in ids[1:]]
        by_target: dict[str, list[PipelineFacts]] = defaultdict(list)
        for p in pipes:
            for t in p.final_targets or p.targets:
                by_target[t].append(p)
        thr = float(self.t["lineage_jaccard"])
        pairs: set[tuple[str, str]] = set()
        for g in by_target.values():
            for a, b in list(combinations(sorted(g, key=lambda x: x.pipeline_id), 2))[: int(self.t["max_pairs_per_key"])]:
                pairs.add((a.pipeline_id, b.pipeline_id))
        for a_id, b_id in sorted(pairs):
            a, b = pm[a_id], pm[b_id]
            sim = min(jaccard(a.sources, b.sources), jaccard(a.targets, b.targets))
            if sim >= thr:
                edges.append((a_id, b_id, "same source-to-target lineage", sim))
        return [self._dup([pm[i] for i in members], {e[2] for e in es}, min(e[3] for e in es))
                for members, es in _connected_groups(edges)]

    def _dup(self, group: list[PipelineFacts], whys: set[str], sim: float) -> CheckOut:
        keeper = self._keeper(group)
        dups = sorted((d for d in group if d.pipeline_id != keeper.pipeline_id), key=lambda d: d.pipeline_id)
        self._flagged.update(d.pipeline_id for d in dups)
        dup_ids = [d.pipeline_id for d in dups]
        dup_cost = sum(d.monthly_cost for d in dups)
        why = " and ".join(sorted(whys))
        names = ", ".join(d.name for d in dups[:5]) + (f" (+{len(dups) - 5} more)" if len(dups) > 5 else "")
        f = self.finding(keeper.pipeline_id, "pipeline", "duplicate_pipelines",
                         f"Pipeline {keeper.name} has {len(dups)} duplicate(s): {names}",
                         f"{len(group)} pipelines with {why} (similarity {sim:.2f}). Keep {keeper.name} and retire the "
                         f"{len(dups)} cop{'y' if len(dups) == 1 else 'ies'} ({self.money(dup_cost)}/month).",
                         Severity.HIGH if dup_cost >= 500 else Severity.MEDIUM, resource_metrics=self._m(keeper),
                         keeper=keeper.pipeline_id, duplicates=dup_ids, duplicate_names=[d.name for d in dups][:20],
                         group_size=len(group), duplicates_monthly_cost=round(dup_cost, 2), dependency_similarity=round(sim, 2))
        recs = [self.rec(d.pipeline_id, "pipeline", A.DEDUPLICATE_PIPELINE, f"Deduplicate {d.name} into {keeper.name}",
                         "Keep one pipeline per dataset; point consumers at the keeper's tables and retire the copy.",
                         severity=Severity.HIGH if d.monthly_cost >= 500 else Severity.MEDIUM, risk=Risk.MEDIUM,
                         confidence=0.7 if sim >= 0.99 else 0.55, monthly_cost=d.monthly_cost, savings=d.monthly_cost * 0.9,
                         basis="duplicate pipeline cost x 90%",
                         related=[keeper.pipeline_id] + [x for x in dup_ids if x != d.pipeline_id] + d.triggering_job_ids,
                         group_key=f"pipedup:{keeper.pipeline_id}", keeper=keeper.pipeline_id)
                for d in dups]
        return f, recs

    def _evidence(self, sources: list[str], targets: list[str], owner_ids: Iterable[str] = (),
                  event_logs: Iterable[str | None] = ()) -> SourceEvidence:
        pats = list(self.t.get("ingestion_exclude_patterns") or [])
        # legacy override key (segment names) - still honoured if a user sets it, empty by default
        pats += [f"*/{s}/*" for s in (self.t.get("ingestion_exclude_segments") or [])]
        return source_evidence(sources, targets, owner_ids=owner_ids, event_log_tables=event_logs,
                               non_data_ext=self.t.get("non_data_file_extensions") or [], exclude_patterns=pats,
                               multi_file_min=int(self.t["multi_file_root_min_files"]))

    @staticmethod
    def _match_basis(ka: dict[str, str], kb: dict[str, str], shared: list[str]) -> tuple[str, float]:
        rank = min(min(BASIS_RANK[ka[k]], BASIS_RANK[kb[k]]) for k in shared)
        if rank >= 3:
            return ("identical_sources", 0.75) if set(ka) == set(kb) else ("shared_exact_sources", 0.65)
        if rank == 2:
            return "shared_partitioned_root", 0.55
        return "shared_multi_file_root", 0.45

    def _lineage_gap(self, p: PipelineFacts, ev: SourceEvidence, writes: list[str]) -> CheckOut:
        why = ", ".join(f"{n} {r.replace('_', ' ')}" for r, n in sorted(ev.dropped.items())) or "no sources recorded"
        f = self.finding(p.pipeline_id, "pipeline", "lineage_incomplete", f"Pipeline {p.name}: ingestion sources not visible in lineage",
                         f"Writes {len(writes)} table(s) but lineage shows no data sources after removing self-references and "
                         f"non-data files ({why}). Duplicate-ingestion analysis was skipped for this pipeline.",
                         Severity.INFO, written_tables=writes[:10], dropped_sources=ev.dropped or None,
                         observed_sources=len(p.sources), monthly_cost=round(p.monthly_cost, 2))
        return f, []

    def check_duplicate_ingestion(self, pipes: list[PipelineFacts]) -> list[CheckOut]:
        """Pipelines and jobs ingesting the same raw data into different targets (structural evidence only)."""
        out: list[CheckOut] = []
        # (kind, id, name, evidence, targets, monthly_cost, related)
        wls: list[tuple[str, str, str, SourceEvidence, set[str], float, list[str]]] = []
        for p in pipes:
            if not p.lineage_available:
                continue
            ev = self._evidence(p.sources, p.targets, owner_ids=[p.pipeline_id], event_logs=[p.event_log_table])
            writes = list(p.targets)  # a self-read streaming table is still an output of this pipeline
            if not ev.keys:  # lineage-completeness gate: writes data, but no visible data source
                if writes and self.t.get("emit_lineage_gaps", True):
                    out.append(self._lineage_gap(p, ev, writes))
                continue
            if ev.path_keys:
                wls.append(("pipeline", p.pipeline_id, p.name, ev, set(writes), p.monthly_cost, p.triggering_job_ids))
        for j in self._wl.jobs.values():
            if j.pipeline_ids or not j.lineage_available:
                continue  # pipeline-triggering jobs are represented by their pipelines
            ev = self._evidence(j.sources, j.targets, owner_ids=[j.job_id])
            if ev.path_keys:
                wls.append(("job", j.job_id, j.name, ev, set(j.targets), j.monthly_cost, []))
        by_key: dict[str, list[int]] = defaultdict(list)
        for i, w in enumerate(wls):
            for k in w[3].path_keys:
                by_key[k].append(i)
        pairs: set[tuple[int, int]] = set()
        for idx in by_key.values():
            if len(idx) < 2 or len(idx) > int(self.t["max_workloads_per_root"]):
                continue
            for a, b in list(combinations(sorted(idx), 2))[: int(self.t["max_pairs_per_key"])]:
                pairs.add((a, b))
        edges: list[tuple[int, int, str, float, float, list[str]]] = []
        for a, b in sorted(pairs):
            wa, wb = wls[a], wls[b]
            if wa[1] in self._flagged or wb[1] in self._flagged:
                continue  # already reported as a duplicate pipeline
            ka, kb = wa[3].keys, wb[3].keys
            ps, pt = jaccard(ka, kb), jaccard(wa[4], wb[4])
            if ps < float(self.t["ingestion_path_jaccard"]) or pt > float(self.t["ingestion_target_jaccard_max"]):
                continue
            shared = sorted(set(ka) & set(kb) & (wa[3].path_keys | wb[3].path_keys))
            if not shared:
                continue
            basis, conf = self._match_basis(ka, kb, shared)
            edges.append((a, b, basis, conf, ps, shared))
        for members, es in _connected_groups(edges):
            out.append(self._ingest_dup([wls[i] for i in members], es))
        return out

    def _ingest_dup(self, group: list[tuple], edges: list[tuple]) -> CheckOut:
        """ONE finding per set of workloads ingesting the same raw data; one recommendation per redundant copy."""
        keeper = max(group, key=lambda w: (w[5], w[1]))  # keep the primary (most expensive) ingestion
        dups = sorted((w for w in group if w is not keeper), key=lambda w: w[1])
        dup_ids = [w[1] for w in dups]
        weakest = min(edges, key=lambda e: e[3])  # report the weakest evidence in the set (conservative)
        basis, conf, ps = weakest[2], weakest[3], min(e[4] for e in edges)
        shared = sorted({s for e in edges for s in e[5]})
        dup_cost = sum(w[5] for w in dups)
        names = ", ".join(w[2] for w in dups[:5]) + (f" (+{len(dups) - 5} more)" if len(dups) > 5 else "")
        f = self.finding(keeper[1], keeper[0], "duplicate_ingestion",
                         f"{keeper[0].title()} {keeper[2]}: same raw data also ingested by {len(dups)} other workload(s): {names}",
                         f"{len(group)} workloads read {', '.join(shared[:3])} ({basis.replace('_', ' ')}, source similarity "
                         f"{ps:.2f}) into different targets. Ingest once in {keeper[2]}; the others read its bronze tables.",
                         Severity.MEDIUM, match_basis=basis, shared_sources=shared[:10], keeper=keeper[1],
                         duplicates=dup_ids, duplicate_names=[w[2] for w in dups][:20], duplicate_kinds=[w[0] for w in dups],
                         group_size=len(group), ingestion_paths=sorted(keeper[3].path_keys)[:10],
                         dependency_similarity=round(ps, 2), duplicates_monthly_cost=round(dup_cost, 2),
                         monthly_cost=round(keeper[5], 2))
        recs = [self.rec(d[1], d[0], A.DEDUPLICATE_PIPELINE if d[0] == "pipeline" else A.MERGE_JOBS,
                         f"Ingest once: reuse {keeper[2]}'s bronze tables instead of re-ingesting in {d[2]}",
                         "One bronze ingestion per source; downstream flows read the shared bronze table.",
                         severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=conf, monthly_cost=d[5],
                         savings=d[5] * 0.6, basis="duplicate ingestion cost x 60%",
                         related=[keeper[1]] + [x for x in dup_ids if x != d[1]] + d[6],
                         match_basis=basis, keeper=keeper[1], dropped_sources=d[3].dropped or None,
                         group_key=f"ingestdup:{keeper[1]}")
                for d in dups]
        return f, recs
