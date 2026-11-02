"""Agent A - Delta Table Optimization.

Purpose: detect Delta table inefficiencies and storage bloat.

Checks (each is an independent, unit-testable method):
  too_many_small_files        small-file ratio above threshold
  low_average_file_size       average file size below the small-file threshold
  compaction_needed           hot partitions with too many files
  low_quality_write_pattern   writers emitting tiny files
  high_write_amplification    MERGE/UPDATE rewriting far more bytes than they add
  stale_partitions            cold partitions not modified for N days
  data_beyond_retention       partitions older than the business retention policy
  unpruned_scans              frequent full scans (no file/partition pruning)
  vacuum_opportunity          tombstoned files past retention never vacuumed
  retention_misconfigured     deletedFileRetentionDuration too long / too short
  partition_skew              one partition dominates the table
  table_inactive              no reads in the access window (archive / stop writer)
  wide_small_file_table       wide schema + small files -> larger compaction window
  fast_growth                 informational growth trend
"""
from __future__ import annotations

import fnmatch
import logging
from typing import Any, Callable, Optional

from cco.agents.base import AgentContext, AgentResult, BaseAgent, register_agent
from cco.etl.facts import DeltaTableFacts, FactStore
from cco.models.canonical import ActionType, Finding, Recommendation, Risk, Severity, Table
from cco.utils import GB, MB, days_since, human_bytes, quote_fqn, quote_ident, sql_literal, to_float

log = logging.getLogger(__name__)

DEFAULT_THRESHOLDS: dict[str, Any] = {
    "small_file_bytes": 32 * MB,
    "target_file_bytes": 128 * MB,
    "min_files_for_optimize": 50,
    "small_file_ratio": 0.4,
    "max_files_per_partition": 1000,
    "write_file_bytes": 8 * MB,
    "min_write_ops": 10,
    "write_lookback_days": 30,
    "write_amplification": 5.0,
    "stale_partition_days": 180,
    "stale_partition_min_share": 0.2,
    "data_retention_days": 730,
    "inactivity_days": 90,
    "max_deleted_file_retention_days": 30,
    "min_deleted_file_retention_days": 7,
    "vacuum_min_reclaim_gb": 10,
    "vacuum_interval_days": 7,
    "partition_skew_ratio": 10.0,
    "min_partitions_for_skew": 5,
    "unpruned_scan_ratio": 0.5,
    "min_scans": 20,
    "wide_table_columns": 100,
    "wide_table_target_file_size": "256mb",
    "growth_ratio_30d": 0.5,
    "storage_cost_per_gb_month": 0.023,
    "archive_cost_per_gb_month": 0.004,
    "optimize_read_savings_factor": 0.15,
}

CheckOut = Optional[tuple[Finding, list[Recommendation]]]


@register_agent
class DeltaTableOptimizationAgent(BaseAgent):
    name = "delta_table_optimization"
    domain = "storage"
    description = "Detects Delta table inefficiencies and storage bloat (small files, compaction, vacuum, retention, partitions, inactivity)."
    required_object_types = {"table"}
    priority = 10

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.t: dict[str, Any] = {**DEFAULT_THRESHOLDS, **(self.config.get("thresholds") or {})}

    # ------------------------------------------------------------------ orchestration
    def is_applicable(self, facts: FactStore) -> tuple[bool, str]:
        n = len(facts.tables("DELTA"))
        return (n > 0, f"{n} Delta tables in fact store" if n else "no Delta tables in fact store")

    def checks(self) -> list[Callable[[Table, DeltaTableFacts, FactStore], CheckOut]]:
        return [
            self.check_small_files, self.check_low_avg_file_size, self.check_compaction_need,
            self.check_low_quality_writes, self.check_write_amplification, self.check_stale_partitions,
            self.check_data_beyond_retention, self.check_unpruned_scans, self.check_vacuum_opportunity,
            self.check_retention, self.check_partition_skew, self.check_inactivity,
            self.check_wide_small_file_table, self.check_growth,
        ]

    def analyze(self, facts: FactStore, ctx: AgentContext) -> AgentResult:
        result = AgentResult(agent=self.name)
        tables = facts.tables("DELTA")
        patterns = ctx.scope.get("resources")
        if patterns:
            tables = [t for t in tables if any(fnmatch.fnmatch(t.id, p) for p in patterns)]
        flagged = 0
        for table in tables:
            try:
                tf = facts.table_facts(table, small_file_bytes=int(self.t["small_file_bytes"]),
                                       stale_partition_days=int(self.t["stale_partition_days"]),
                                       write_lookback_days=int(self.t["write_lookback_days"]))
                hit = False
                for check in self.checks():
                    out = check(table, tf, facts)
                    if not out:
                        continue
                    finding, recs = out
                    hit = True
                    result.findings.append(finding)
                    for r in recs:
                        r.finding_ids.append(finding.id)
                        r.run_id = ctx.run_id
                    result.recommendations.extend(recs)
                flagged += int(hit)
            except Exception as e:  # noqa: BLE001
                log.exception("delta agent failed on %s", table.id)
                result.errors.append({"resource_id": table.id, "error": str(e)[:300]})
        result.stats = {"tables_analyzed": len(tables), "tables_flagged": flagged,
                        "findings": len(result.findings), "recommendations": len(result.recommendations)}
        return result

    # ------------------------------------------------------------------ helpers
    def _finding(self, table: Table, check: str, title: str, detail: str, severity: Severity, **metrics: Any) -> Finding:
        return Finding(agent=self.name, check=check, resource_id=table.id, resource_type="table",
                       title=title, detail=detail, severity=severity, metrics=metrics)

    def _rec(self, table: Table, action: ActionType, title: str, rationale: str, *, severity: Severity,
             command: str | None = None, risk: Risk = Risk.LOW, reversible: bool = True, confidence: float = 0.7,
             savings: float = 0.0, basis: str = "", **metrics: Any) -> Recommendation:
        return Recommendation(resource_id=table.id, resource_type="table", action_type=action, title=title, rationale=rationale,
                              agent=self.name, command=command, severity=severity, risk=risk, reversible=reversible,
                              confidence=confidence, estimated_monthly_savings=round(max(0.0, savings), 2),
                              savings_basis=basis, metrics=metrics)

    def _price(self) -> float:
        return float(self.t["storage_cost_per_gb_month"])

    def _optimize_savings(self, tf: DeltaTableFacts) -> tuple[float, str]:
        factor = float(self.t["optimize_read_savings_factor"])
        s = tf.attributed_compute_cost * tf.small_file_ratio * factor
        return s, f"heuristic: attributed compute ${tf.attributed_compute_cost:,.0f}/mo x small-file ratio {tf.small_file_ratio:.0%} x {factor:.0%} file-overhead reduction"

    def _auto_optimize_rec(self, table: Table, tf: DeltaTableFacts, severity: Severity, why: str) -> Recommendation:
        return self._rec(
            table, ActionType.ENABLE_AUTO_OPTIMIZE, f"Enable optimized writes + auto compaction on {table.id}",
            why, severity=severity, confidence=0.75,
            command=(f"ALTER TABLE {quote_fqn(table.id)} SET TBLPROPERTIES "
                     "('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"),
            basis="prevents future small files; savings realised via fewer OPTIMIZE runs and faster reads",
        )

    # ------------------------------------------------------------------ checks: file layout
    def check_small_files(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.num_files < t["min_files_for_optimize"] or tf.small_file_ratio < t["small_file_ratio"]:
            return None
        sev = Severity.HIGH if tf.small_file_ratio >= 0.8 and tf.num_files >= 1000 else Severity.MEDIUM
        est = " (estimated from write history)" if tf.small_file_ratio_estimated else ""
        f = self._finding(table, "too_many_small_files", "Too many small files",
                          f"{tf.small_files:,} of {tf.num_files:,} files ({tf.small_file_ratio:.0%}) are smaller than "
                          f"{human_bytes(t['small_file_bytes'])}{est}.", sev,
                          total_files=tf.num_files, small_files=tf.small_files, small_file_ratio=round(tf.small_file_ratio, 3),
                          estimated=tf.small_file_ratio_estimated)
        savings, basis = self._optimize_savings(tf)
        rec = self._rec(table, ActionType.OPTIMIZE, f"Run OPTIMIZE on {table.id}",
                        f"Compact {tf.small_files:,} small files into ~{human_bytes(t['target_file_bytes'])} files to cut file-listing and task overhead.",
                        severity=sev, command=f"OPTIMIZE {quote_fqn(table.id)}",
                        confidence=0.65 if tf.small_file_ratio_estimated else 0.85, savings=savings, basis=basis)
        return f, [rec]

    def check_low_avg_file_size(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.num_files < t["min_files_for_optimize"] or tf.avg_file_size >= t["small_file_bytes"]:
            return None
        sev = Severity.HIGH if tf.avg_file_size < t["small_file_bytes"] / 8 else Severity.MEDIUM
        f = self._finding(table, "low_average_file_size", "Very low average file size",
                          f"Average file size is {human_bytes(tf.avg_file_size)} across {tf.num_files:,} files "
                          f"(target ~{human_bytes(t['target_file_bytes'])}).", sev,
                          avg_file_size=round(tf.avg_file_size), total_files=tf.num_files, size_bytes=tf.size_bytes)
        if not (tf.optimize_write and tf.auto_compact):
            recs = [self._auto_optimize_rec(table, tf, sev, "Average file size is far below target; fix it at write time instead of repeatedly compacting.")]
        else:
            savings, basis = self._optimize_savings(tf)
            recs = [self._rec(table, ActionType.OPTIMIZE, f"Run OPTIMIZE on {table.id}",
                              "Auto-optimize is already on but files are still small - schedule an explicit OPTIMIZE.",
                              severity=sev, command=f"OPTIMIZE {quote_fqn(table.id)}", confidence=0.8, savings=savings, basis=basis)]
        return f, recs

    def check_compaction_need(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        limit = int(self.t["max_files_per_partition"])
        if not tf.num_partitions or tf.max_files_per_partition <= limit:
            return None
        hot = sorted((p for p in table.partitions if p.num_files > limit), key=lambda p: -p.num_files)[:10]
        preds = []
        for p in hot:
            conds = [f"{quote_ident(k)} IS NULL" if v is None else f"{quote_ident(k)} = {sql_literal(v)}" for k, v in p.values.items()]
            preds.append("(" + " AND ".join(conds) + ")")
        f = self._finding(table, "compaction_needed", "Partitions need compaction",
                          f"{len(hot)} partition(s) exceed {limit:,} files (max {tf.max_files_per_partition:,}; "
                          f"avg {tf.avg_files_per_partition:,.0f} files/partition).", Severity.MEDIUM,
                          max_files_per_partition=tf.max_files_per_partition, avg_files_per_partition=round(tf.avg_files_per_partition, 1),
                          hot_partitions=[p.values for p in hot])
        savings, basis = self._optimize_savings(tf)
        rec = self._rec(table, ActionType.OPTIMIZE, f"Run OPTIMIZE on hot partitions of {table.id}",
                        "Targeted compaction of the partitions with the most files (cheaper than a full-table OPTIMIZE).",
                        severity=Severity.MEDIUM, command=f"OPTIMIZE {quote_fqn(table.id)} WHERE {' OR '.join(preds)}",
                        confidence=0.85, savings=savings, basis=basis)
        return f, [rec]

    def check_low_quality_writes(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.write_ops < t["min_write_ops"] or tf.avg_write_file_size is None or tf.avg_write_file_size >= t["write_file_bytes"]:
            return None
        jobs = ", ".join(tf.writer_job_ids) or "unknown writers"
        f = self._finding(table, "low_quality_write_pattern", "Writers produce tiny files",
                          f"{tf.write_ops} writes in the last {t['write_lookback_days']} days averaged {human_bytes(tf.avg_write_file_size)} "
                          f"per output file (writer jobs: {jobs}).", Severity.MEDIUM,
                          write_ops=tf.write_ops, avg_write_file_size=round(tf.avg_write_file_size), writer_job_ids=tf.writer_job_ids)
        recs: list[Recommendation] = []
        if not tf.optimize_write:
            recs.append(self._auto_optimize_rec(table, tf, Severity.MEDIUM, "Optimized writes coalesce output into fewer, larger files."))
        recs.append(self._rec(table, ActionType.REVIEW_UPSTREAM_JOB, f"Tune writer jobs for {table.id}",
                              f"Batch micro-batches (longer trigger interval), repartition/coalesce before write, or switch to "
                              f"liquid clustering. Writer jobs: {jobs}.", severity=Severity.LOW, confidence=0.6,
                              writer_job_ids=tf.writer_job_ids))
        return f, recs

    def check_write_amplification(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        amp = tf.write_amplification
        if tf.write_ops < t["min_write_ops"] or amp is None or amp < t["write_amplification"]:
            return None
        sev = Severity.HIGH if amp >= 4 * t["write_amplification"] else Severity.MEDIUM
        f = self._finding(table, "high_write_amplification", "High write amplification",
                          f"Writes rewrote {human_bytes(tf.bytes_written)} to add a net {human_bytes(tf.bytes_written / amp)} "
                          f"(~{amp:,.0f}x amplification) over {t['write_lookback_days']} days.", sev,
                          write_amplification=round(amp, 1), bytes_written=tf.bytes_written, write_ops=tf.write_ops)
        savings = tf.attributed_compute_cost * 0.25 * (1 - 1 / amp)
        basis = f"heuristic: 25% of attributed writer compute ${tf.attributed_compute_cost:,.0f}/mo scaled by (1 - 1/amplification)"
        recs: list[Recommendation] = []
        if not tf.deletion_vectors:
            recs.append(self._rec(table, ActionType.ENABLE_DELETION_VECTORS, f"Enable deletion vectors on {table.id}",
                                  "Deletion vectors let MERGE/UPDATE/DELETE mark rows instead of rewriting whole files.",
                                  severity=sev, risk=Risk.MEDIUM, confidence=0.7, savings=savings, basis=basis,
                                  command=f"ALTER TABLE {quote_fqn(table.id)} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"))
        if not tf.clustering_columns:
            recs.append(self._rec(table, ActionType.LIQUID_CLUSTERING, f"Cluster {table.id} on MERGE keys",
                                  "Liquid clustering on the MERGE join keys (and tighter MERGE predicates) shrinks the set of files touched per write.",
                                  severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.55,
                                  savings=savings if tf.deletion_vectors else 0.0, basis=basis if tf.deletion_vectors else ""))
        return (f, recs) if recs else (f, [])

    # ------------------------------------------------------------------ checks: lifecycle
    def check_stale_partitions(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if not tf.num_partitions or not tf.old_partitions or not tf.size_bytes:
            return None
        share = tf.old_partition_bytes / tf.size_bytes
        if share < t["stale_partition_min_share"]:
            return None
        gb = tf.old_partition_bytes / GB
        savings = gb * (self._price() - float(t["archive_cost_per_gb_month"]))
        sev = Severity.HIGH if gb >= 100 else Severity.MEDIUM
        f = self._finding(table, "stale_partitions", "Stale partitions",
                          f"{tf.old_partitions} of {tf.num_partitions} partitions ({human_bytes(tf.old_partition_bytes)}, {share:.0%} of the table) "
                          f"have not been modified in {t['stale_partition_days']} days.", sev,
                          old_partitions=tf.old_partitions, old_partition_bytes=tf.old_partition_bytes, share=round(share, 3))
        rec = self._rec(table, ActionType.ARCHIVE_PARTITIONS, f"Archive cold partitions of {table.id}",
                        "Move cold partitions to an archive table / cheaper storage tier (deep clone + delete, or lifecycle policy).",
                        severity=sev, risk=Risk.MEDIUM, confidence=0.6, savings=savings,
                        basis=f"{gb:,.0f} GB x (${self._price()}/GB hot - ${t['archive_cost_per_gb_month']}/GB archive)")
        return f, [rec]

    def check_data_beyond_retention(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        props = {str(k).lower(): v for k, v in table.properties.items()}
        retention = to_float(props.get("cco.data_retention_days"), to_float(self.t.get("data_retention_days")))
        if not retention or not table.partitions:
            return None
        expired = [p for p in table.partitions if p.last_modified and (days_since(p.last_modified, facts.now) or 0) > retention]
        if not expired:
            return None
        size = sum(p.size_bytes for p in expired)
        f = self._finding(table, "data_beyond_retention", "Idle data older than retention period",
                          f"{len(expired)} partition(s) ({human_bytes(size)}) are older than the {retention:,.0f}-day retention policy.",
                          Severity.MEDIUM, expired_partitions=len(expired), expired_bytes=size, retention_days=retention)
        rec = self._rec(table, ActionType.PURGE_EXPIRED_DATA, f"Purge data past retention in {table.id}",
                        "After data-owner / legal sign-off: DELETE expired partitions, then VACUUM after the retention window.",
                        severity=Severity.MEDIUM, risk=Risk.HIGH, reversible=False, confidence=0.5,
                        savings=size / GB * self._price(), basis=f"{size / GB:,.0f} GB x ${self._price()}/GB-month")
        return f, [rec]

    def check_unpruned_scans(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.scan_count < t["min_scans"] or tf.unpruned_scan_ratio is None or tf.unpruned_scan_ratio < t["unpruned_scan_ratio"]:
            return None
        sev = Severity.HIGH if tf.old_partitions else Severity.MEDIUM
        stale = f" {tf.old_partitions} stale partitions are scanned on every such query." if tf.old_partitions else ""
        f = self._finding(table, "unpruned_scans", "Frequent full scans",
                          f"{tf.unpruned_scan_ratio:.0%} of {tf.scan_count:,.0f} queries read files without any pruning.{stale}", sev,
                          scans=tf.scan_count, unpruned_scan_ratio=tf.unpruned_scan_ratio, old_partitions=tf.old_partitions)
        if tf.partition_columns:
            rec = self._rec(table, ActionType.PARTITION_REDESIGN, f"Improve partition pruning on {table.id}",
                            f"Queries are not filtering on partition column(s) {tf.partition_columns}. Add partition predicates in "
                            f"consumers or re-layout on the columns actually used in filters.", severity=sev, risk=Risk.MEDIUM, confidence=0.55)
        else:
            rec = self._rec(table, ActionType.LIQUID_CLUSTERING, f"Enable data skipping on {table.id}",
                            "Apply liquid clustering (CLUSTER BY) on the most common filter columns so file-level statistics can prune scans.",
                            severity=sev, risk=Risk.MEDIUM, confidence=0.55)
        return f, [rec]

    def check_vacuum_opportunity(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.vacuumable_bytes_est < float(t["vacuum_min_reclaim_gb"]) * GB:
            return None
        if tf.days_since_vacuum is not None and tf.days_since_vacuum < t["vacuum_interval_days"]:
            return None
        gb = tf.vacuumable_bytes_est / GB
        last = "never (in visible history)" if tf.days_since_vacuum is None else f"{tf.days_since_vacuum:,.0f} days ago"
        sev = Severity.HIGH if gb >= 250 else Severity.MEDIUM
        f = self._finding(table, "vacuum_opportunity", "VACUUM opportunity",
                          f"~{human_bytes(tf.vacuumable_bytes_est)} of removed files are older than the {tf.deleted_file_retention_days:,.0f}-day "
                          f"retention and can be reclaimed. Last VACUUM: {last}.", sev,
                          vacuumable_bytes_est=tf.vacuumable_bytes_est, tombstoned_bytes_est=tf.tombstoned_bytes_est,
                          days_since_vacuum=tf.days_since_vacuum)
        rec = self._rec(table, ActionType.VACUUM, f"Run VACUUM on {table.id}",
                        "Physically delete unreferenced files past the retention window (respects the table's retention setting).",
                        severity=sev, command=f"VACUUM {quote_fqn(table.id)}", risk=Risk.MEDIUM, reversible=False, confidence=0.6,
                        savings=gb * self._price(), basis=f"~{gb:,.0f} GB (estimated from DESCRIBE HISTORY) x ${self._price()}/GB-month")
        return f, [rec]

    def check_retention(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        d = tf.deleted_file_retention_days
        hi, lo = float(t["max_deleted_file_retention_days"]), float(t["min_deleted_file_retention_days"])
        if d > hi:
            held = tf.tombstoned_bytes_est * (1 - hi / d)
            f = self._finding(table, "retention_too_long", "Deleted-file retention is very long",
                              f"delta.deletedFileRetentionDuration is {d:,.0f} days; removed files are kept far longer than typical "
                              f"time-travel needs ({hi:,.0f} days).", Severity.MEDIUM, retention_days=d, tombstoned_bytes_est=tf.tombstoned_bytes_est)
            rec = self._rec(table, ActionType.SET_RETENTION, f"Reduce deleted-file retention on {table.id} to {hi:,.0f} days",
                            "Shorter retention lets VACUUM reclaim storage sooner. Confirm no consumer needs older time travel.",
                            severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.6, savings=held / GB * self._price(),
                            basis=f"share of tombstoned bytes held beyond {hi:,.0f} days",
                            command=(f"ALTER TABLE {quote_fqn(table.id)} SET TBLPROPERTIES "
                                     f"('delta.deletedFileRetentionDuration' = 'interval {int(hi)} days')"))
            return f, [rec]
        if d < lo:
            f = self._finding(table, "retention_too_short", "Deleted-file retention below safe minimum",
                              f"delta.deletedFileRetentionDuration is {d:,.1f} days (< {lo:,.0f}); VACUUM may break time travel "
                              f"and long-running readers/streams.", Severity.MEDIUM, retention_days=d)
            rec = self._rec(table, ActionType.SET_RETENTION, f"Extend deleted-file retention on {table.id} to {lo:,.0f} days",
                            "Protects concurrent readers, streaming checkpoints and recovery; storage impact is minimal.",
                            severity=Severity.MEDIUM, risk=Risk.LOW, confidence=0.85,
                            command=(f"ALTER TABLE {quote_fqn(table.id)} SET TBLPROPERTIES "
                                     f"('delta.deletedFileRetentionDuration' = 'interval {int(lo)} days')"))
            return f, [rec]
        return None

    def check_partition_skew(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if tf.num_partitions < t["min_partitions_for_skew"] or not tf.partition_skew_ratio or tf.partition_skew_ratio < t["partition_skew_ratio"]:
            return None
        top = max(table.partitions, key=lambda p: p.size_bytes)
        f = self._finding(table, "partition_skew", "Partition skew / hot partition",
                          f"Largest partition {top.values} holds {human_bytes(top.size_bytes)} - {tf.partition_skew_ratio:,.0f}x the median partition.",
                          Severity.MEDIUM, partition_skew_ratio=round(tf.partition_skew_ratio, 1), hot_partition=top.values,
                          hot_partition_bytes=top.size_bytes)
        rec = self._rec(table, ActionType.LIQUID_CLUSTERING, f"Replace skewed partitioning on {table.id}",
                        f"Partitioning by {tf.partition_columns} is heavily skewed. Migrate to liquid clustering (or add a secondary key) "
                        "so files are evenly sized and writes are not bottlenecked on one partition.",
                        severity=Severity.MEDIUM, risk=Risk.MEDIUM, confidence=0.6)
        return f, [rec]

    def check_inactivity(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if not tf.access_data_available:
            return None  # never infer inactivity without access logs
        thr = float(t["inactivity_days"])
        inactive = (tf.no_access_in_window and tf.access_window_days >= thr) or (tf.days_since_accessed or 0) >= thr
        if not inactive:
            return None
        last = f"{tf.days_since_accessed:,.0f} days ago" if tf.days_since_accessed is not None else f"not in the last {tf.access_window_days} days"
        if tf.writes_in_window > 0:
            savings = tf.attributed_compute_cost + tf.monthly_storage_cost
            f = self._finding(table, "write_only_table", "Table is written but never read",
                              f"{tf.writes_in_window:,.0f} writes but no reads (last read {last}); writer jobs: {tf.writer_job_ids or 'unknown'}.",
                              Severity.HIGH, writes=tf.writes_in_window, writer_job_ids=tf.writer_job_ids,
                              attributed_compute_cost=tf.attributed_compute_cost)
            rec = self._rec(table, ActionType.REVIEW_UPSTREAM_JOB, f"Pause or decommission writers of {table.id}",
                            f"Nobody reads this table. Confirm with the owner, then pause writer job(s) {tf.writer_job_ids} and archive the table.",
                            severity=Severity.HIGH, risk=Risk.MEDIUM, confidence=0.7, savings=savings,
                            basis=f"writer compute ${tf.attributed_compute_cost:,.0f}/mo + storage ${tf.monthly_storage_cost:,.2f}/mo",
                            writer_job_ids=tf.writer_job_ids)
            return f, [rec]
        if (tf.days_since_modified or 0) < thr:
            return None
        gb = tf.size_bytes / GB
        sev = Severity.HIGH if gb >= 100 else Severity.LOW if gb < 1 else Severity.MEDIUM
        f = self._finding(table, "table_inactive", "Table access inactivity",
                          f"No reads (last read {last}) and no modifications for {tf.days_since_modified:,.0f} days; {human_bytes(tf.size_bytes)} at rest.",
                          sev, days_since_modified=tf.days_since_modified, days_since_accessed=tf.days_since_accessed, size_bytes=tf.size_bytes)
        rec = self._rec(table, ActionType.ARCHIVE_TABLE, f"Archive cold table {table.id}",
                        "Deep-clone to an archive location / cheaper tier, or drop after owner confirmation. Check downstream lineage first.",
                        severity=sev, risk=Risk.HIGH, confidence=0.65,
                        savings=gb * (self._price() - float(t["archive_cost_per_gb_month"])),
                        basis=f"{gb:,.0f} GB x (${self._price()} hot - ${t['archive_cost_per_gb_month']} archive)/GB-month")
        return f, [rec]

    def check_wide_small_file_table(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        t = self.t
        if not tf.num_columns or tf.num_columns < t["wide_table_columns"] or tf.target_file_size:
            return None
        if tf.num_files < t["min_files_for_optimize"] or tf.avg_file_size >= t["small_file_bytes"]:
            return None
        size = t["wide_table_target_file_size"]
        f = self._finding(table, "wide_small_file_table", "Wide table with small files",
                          f"{tf.num_columns} columns with {human_bytes(tf.avg_file_size)} average files - each file yields tiny column chunks.",
                          Severity.MEDIUM, num_columns=tf.num_columns, avg_file_size=round(tf.avg_file_size))
        rec = self._rec(table, ActionType.SET_TARGET_FILE_SIZE, f"Use a larger compaction window for {table.id}",
                        f"Set delta.targetFileSize = {size} and compact on a schedule (e.g. nightly OPTIMIZE) rather than per write.",
                        severity=Severity.MEDIUM, confidence=0.6,
                        command=f"ALTER TABLE {quote_fqn(table.id)} SET TBLPROPERTIES ('delta.targetFileSize' = {sql_literal(size)})")
        return f, [rec]

    def check_growth(self, table: Table, tf: DeltaTableFacts, facts: FactStore) -> CheckOut:
        if tf.growth_ratio_30d is None or tf.growth_ratio_30d < self.t["growth_ratio_30d"]:
            return None
        f = self._finding(table, "fast_growth", "Fast table growth",
                          f"Table grew ~{tf.growth_ratio_30d:.0%} in {self.t['write_lookback_days']} days; monitor retention and layout.",
                          Severity.INFO, growth_ratio_30d=round(tf.growth_ratio_30d, 3))
        return f, []
