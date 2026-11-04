"""Persistence for findings, recommendations (with approval state) and the audit log.

Re-running the analysis must never silently overwrite a human decision, so
`merge_existing` keeps pending/approved/rejected state for recurring recommendations.

Reporting data model (every attribute is stored once; only join keys repeat across tables):

  cco_runs                 1 row / run            run_id                               when, status, request
  cco_checks               1 row / check          check_name                           agent, domain
  cco_resources            1 row / resource       resource_type, resource_id           name, workspace, owner, first/last seen
  cco_resource_snapshots   1 row / run x resource run_id, resource_type, resource_id   monthly_cost + resource metrics
  cco_findings             1 row / run x finding  run_id, finding_id                   check, resource, severity, text, check metrics
  cco_finding_resources    1 row / extra member   run_id, finding_id, resource         the copies in a duplicate set
  cco_recommendations      1 row / rec (current)  recommendation_id                    action, savings, score, approval status
  cco_queue                1 row / run x group    run_id, group_id                     rank, priority, combined savings
  cco_queue_members        1 row / run x rec      run_id, group_id, recommendation_id  (bridge)
  cco_approval_audit       1 row / event          event_time, recommendation_id        actor, event, details

Views for dashboards (denormalized, read-only): v_findings, v_latest_findings, v_recommendations, v_queue, v_run_summary.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import timedelta
from typing import Any, Iterable

log = logging.getLogger(__name__)

from cco.models.canonical import Finding, Recommendation, RecommendationStatus
from cco.utils import quote_fqn, sql_literal, utcnow

KEEP_STATE = {RecommendationStatus.PENDING_APPROVAL, RecommendationStatus.APPROVED}


def merge_existing(existing: Recommendation | None, new: Recommendation, resurface_rejected_after_days: int = 30) -> Recommendation:
    if existing is None:
        return new
    new.created_at = existing.created_at
    if existing.status in KEEP_STATE:
        new.status, new.approvals = existing.status, existing.approvals
    elif existing.status == RecommendationStatus.REJECTED:
        rejected_at = (existing.rejection or {}).get("at")
        if rejected_at and utcnow() - existing.updated_at < timedelta(days=resurface_rejected_after_days):
            new.status, new.rejection = existing.status, existing.rejection
    return new


class RecommendationRepository(ABC):
    resurface_rejected_after_days: int = 30

    @abstractmethod
    def get(self, rec_id: str) -> Recommendation | None: ...

    @abstractmethod
    def list(self, status: RecommendationStatus | None = None) -> list[Recommendation]: ...

    @abstractmethod
    def _write(self, recs: list[Recommendation]) -> None: ...

    @abstractmethod
    def save_findings(self, run_id: str, findings: Iterable[Finding]) -> None: ...

    @abstractmethod
    def audit(self, rec_id: str, event: str, actor: str, details: dict[str, Any] | None = None) -> None: ...

    def upsert(self, recs: Iterable[Recommendation]) -> list[Recommendation]:
        merged = [merge_existing(self.get(r.id), r, self.resurface_rejected_after_days) for r in recs]
        self._write(merged)
        return merged

    def update(self, rec: Recommendation) -> None:
        rec.updated_at = utcnow()
        self._write([rec])

    def save_queue(self, run_id: str, items: Iterable[Any]) -> None:  # noqa: B027 - optional for custom repos
        """Persist the ranked Optimization Queue snapshot of a run (QueueItem objects)."""

    def save_run(self, run: dict[str, Any]) -> None:  # noqa: B027 - optional for custom repos
        """Upsert one run row (run_id, started_at, finished_at, status, request, agents_run, error_message)."""

    def save_catalog(self, run_id: str, resources: Iterable[dict], checks: Iterable[dict]) -> None:  # noqa: B027
        """Upsert resource and check dimensions seen in a run."""


# ----------------------------------------------------------------------------- row shaping (shared by all repositories)
TABLES = {
    "runs": "cco_runs", "checks": "cco_checks", "resources": "cco_resources", "resource_snapshots": "cco_resource_snapshots",
    "findings": "cco_findings", "finding_resources": "cco_finding_resources", "recommendations": "cco_recommendations",
    "queue": "cco_queue", "queue_members": "cco_queue_members", "audit": "cco_approval_audit",
}
# keys describing a duplicate set; stored relationally (cco_finding_resources + cco_resources), not in metrics JSON
SET_METRIC_KEYS = {"keeper", "duplicates", "duplicate_names", "duplicate_kinds", "group_size", "duplicates_monthly_cost",
                   "duplicate_of", "other"}
# Recommendation fields kept as typed columns; everything else goes to `details` (JSON)
REC_COLUMNS = ("resource_type", "resource_id", "action_type", "agent", "title", "rationale", "command", "severity", "risk",
               "reversible", "confidence", "estimated_monthly_savings", "savings_basis", "score", "priority", "status",
               "superseded_by", "finding_ids", "related_resources", "depends_on", "created_at", "updated_at")
REC_DETAILS = ("metrics", "score_breakdown", "llm_summary", "llm_notes", "approvals", "rejection", "execution_result")


def _json(v: Any) -> str | None:
    return None if v in (None, {}, []) else json.dumps(v, default=str, sort_keys=True)


def finding_members(f: Finding) -> list[tuple[str, str]]:
    """(resource_type, resource_id) of the OTHER members of a duplicate-set finding (the keeper is the finding's resource)."""
    kinds = f.metrics.get("duplicate_kinds") or []
    return [(kinds[i] if i < len(kinds) else f.resource_type, str(d)) for i, d in enumerate(f.metrics.get("duplicates") or [])]


def finding_rows(run_id: str, findings: Iterable[Finding]) -> tuple[list[dict], list[dict], list[dict]]:
    """Split findings into (cco_findings, cco_finding_resources, cco_resource_snapshots) rows.

    Resource-level metrics (cost, runs, success rate...) are identical for every finding on a resource, so they are
    written once per run x resource; finding metrics keep only what is specific to the check.
    """
    rows, members = [], []
    snaps: dict[tuple[str, str], dict] = {}
    for f in findings:
        rk = set(f.resource_metric_keys) | {"monthly_cost"}
        s = snaps.setdefault((f.resource_type, f.resource_id), {"run_id": run_id, "resource_type": f.resource_type,
                                                              "resource_id": f.resource_id, "monthly_cost": None, "m": {}})
        for k in rk & set(f.metrics):
            if k == "monthly_cost":
                if s["monthly_cost"] is None and isinstance(f.metrics[k], (int, float)):
                    s["monthly_cost"] = float(f.metrics[k])
            else:
                s["m"].setdefault(k, f.metrics[k])
        own = {k: v for k, v in f.metrics.items() if k not in rk and k not in SET_METRIC_KEYS}
        rows.append({"run_id": run_id, "finding_id": f.id, "check_name": f.check, "resource_type": f.resource_type,
                     "resource_id": f.resource_id, "severity": f.severity.value, "title": f.title, "detail": f.detail,
                     "metrics": _json(own), "created_at": f.created_at})
        members += [{"run_id": run_id, "finding_id": f.id, "resource_type": t, "resource_id": i, "role": "duplicate"}
                    for t, i in finding_members(f)]
    snap_rows = [{"run_id": s["run_id"], "resource_type": s["resource_type"], "resource_id": s["resource_id"],
                  "monthly_cost": s["monthly_cost"], "metrics": _json(s["m"])} for s in snaps.values()]
    return rows, members, snap_rows


def rec_row(r: Recommendation) -> dict[str, Any]:
    d = r.to_dict()
    row = {k: d[k] for k in REC_COLUMNS}
    row.update(recommendation_id=r.id, last_run_id=r.run_id, created_at=r.created_at, updated_at=r.updated_at,
               reversible=bool(r.reversible), confidence=float(r.confidence), score=float(r.score),
               estimated_monthly_savings=float(r.estimated_monthly_savings),
               details=_json({k: d[k] for k in REC_DETAILS if d.get(k) not in (None, {}, [])}))
    return row


def rec_from_row(row: dict[str, Any]) -> Recommendation:
    d = {k: row.get(k) for k in REC_COLUMNS}
    d.update(json.loads(row.get("details") or "{}"))
    d.update(id=row["recommendation_id"], run_id=row.get("last_run_id"))
    for k in ("finding_ids", "related_resources", "depends_on"):
        d[k] = list(d.get(k) or [])
    return Recommendation.from_dict({k: v for k, v in d.items() if v is not None})


def queue_rows(run_id: str, items: Iterable[Any]) -> tuple[list[dict], list[dict]]:
    """QueueItems -> (cco_queue, cco_queue_members). Titles, resources, agents, actions and statuses are NOT copied:
    they are attributes of the member recommendations (see v_queue)."""
    q, m = [], []
    for i in items:
        d = i.to_dict()
        q.append({"run_id": run_id, **{k: d[k] for k in ("group_id", "rank", "priority", "group_score", "combined_monthly_savings",
                                                        "impact", "confidence", "max_risk", "primary_recommendation_id")}})
        m += [{"run_id": run_id, "group_id": d["group_id"], "recommendation_id": rid} for rid in d["recommendation_ids"]]
    return q, m


class InMemoryRecommendationRepository(RecommendationRepository):
    def __init__(self) -> None:
        self.recs: dict[str, Recommendation] = {}
        self.audit_log: list[dict] = []
        self.tables: dict[str, list[dict]] = defaultdict(list)  # same row shapes as the Delta tables
        self.runs: dict[str, dict] = {}
        self.resources: dict[tuple[str, str], dict] = {}
        self.checks: dict[str, dict] = {}

    @property
    def findings(self) -> list[dict]:
        return self.tables["findings"]

    def save_queue(self, run_id: str, items: Iterable[Any]) -> None:
        q, m = queue_rows(run_id, items)
        self.tables["queue"] += q
        self.tables["queue_members"] += m

    def save_run(self, run: dict[str, Any]) -> None:
        self.runs[run["run_id"]] = {**self.runs.get(run["run_id"], {}), **run}

    def save_catalog(self, run_id: str, resources: Iterable[dict], checks: Iterable[dict]) -> None:
        for r in resources:
            key = (r["resource_type"], r["resource_id"])
            old = self.resources.get(key, {"first_seen_run_id": run_id})
            self.resources[key] = {**old, **{k: v for k, v in r.items() if v is not None}, "last_seen_run_id": run_id}
        for c in checks:
            self.checks[c["check_name"]] = dict(c)

    def get(self, rec_id: str) -> Recommendation | None:
        return self.recs.get(rec_id)

    def list(self, status: RecommendationStatus | None = None) -> list[Recommendation]:
        return [r for r in self.recs.values() if status is None or r.status == status]

    def _write(self, recs: list[Recommendation]) -> None:
        for r in recs:
            self.recs[r.id] = r

    def save_findings(self, run_id: str, findings: Iterable[Finding]) -> None:
        rows, members, snaps = finding_rows(run_id, findings)
        self.tables["findings"] += rows
        self.tables["finding_resources"] += members
        self.tables["resource_snapshots"] += snaps

    def audit(self, rec_id: str, event: str, actor: str, details: dict[str, Any] | None = None) -> None:
        self.audit_log.append({"event_time": utcnow(), "recommendation_id": rec_id, "event": event, "actor": actor,
                               "details": details or {}})


# ----------------------------------------------------------------------------- Delta / Unity Catalog
# (DDL, primary key, cluster-by, comment). NOT NULL + informational PRIMARY KEY help Genie / BI tools infer joins.
DDL: dict[str, tuple[str, str | None, str | None, str]] = {
    "runs": ("run_id STRING NOT NULL, started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, request STRING, "
             "agents_run ARRAY<STRING>, error_message STRING", "run_id", None,
             "One row per optimizer run."),
    "checks": ("check_name STRING NOT NULL, agent STRING, domain STRING, updated_at TIMESTAMP", "check_name", None,
               "Check catalog: which agent / domain owns each check."),
    "resources": ("resource_type STRING NOT NULL, resource_id STRING NOT NULL, resource_name STRING, workspace_id STRING, "
                  "owner STRING, run_as STRING, first_seen_run_id STRING, last_seen_run_id STRING, updated_at TIMESTAMP",
                  "resource_type, resource_id", None, "Resources (jobs, pipelines, clusters, tables...) referenced by findings or recommendations."),
    "resource_snapshots": ("run_id STRING NOT NULL, resource_type STRING NOT NULL, resource_id STRING NOT NULL, "
                           "monthly_cost DOUBLE, metrics STRING", "run_id, resource_type, resource_id", "run_id",
                           "Per run x resource: monthly cost and resource-level metrics (runs, success rate, ...). JSON metrics."),
    "findings": ("run_id STRING NOT NULL, finding_id STRING NOT NULL, check_name STRING, resource_type STRING, resource_id STRING, "
                 "severity STRING, title STRING, detail STRING, metrics STRING, created_at TIMESTAMP", "run_id, finding_id", "run_id",
                 "Per run x finding. finding_id is stable across runs (check + resource). metrics = check-specific JSON."),
    "finding_resources": ("run_id STRING NOT NULL, finding_id STRING NOT NULL, resource_type STRING NOT NULL, "
                          "resource_id STRING NOT NULL, role STRING", "run_id, finding_id, resource_type, resource_id", "run_id",
                          "Other members of a duplicate-set finding (the keeper is the finding's own resource)."),
    "recommendations": ("recommendation_id STRING NOT NULL, resource_type STRING, resource_id STRING, action_type STRING, agent STRING, "
                        "title STRING, rationale STRING, command STRING, severity STRING, risk STRING, reversible BOOLEAN, "
                        "confidence DOUBLE, estimated_monthly_savings DOUBLE, savings_basis STRING, score DOUBLE, priority STRING, "
                        "status STRING, superseded_by STRING, finding_ids ARRAY<STRING>, related_resources ARRAY<STRING>, "
                        "depends_on ARRAY<STRING>, details STRING, last_run_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP",
                        "recommendation_id", None, "Current state of each recommendation (stable id = resource + action) incl. approval status."),
    "queue": ("run_id STRING NOT NULL, group_id STRING NOT NULL, rank INT, priority STRING, group_score DOUBLE, "
              "combined_monthly_savings DOUBLE, impact DOUBLE, confidence DOUBLE, max_risk STRING, primary_recommendation_id STRING",
              "run_id, group_id", "run_id", "Optimization queue per run: one row per group of related recommendations."),
    "queue_members": ("run_id STRING NOT NULL, group_id STRING NOT NULL, recommendation_id STRING NOT NULL",
                      "run_id, group_id, recommendation_id", "run_id", "Bridge: recommendations in each queue group per run."),
    "audit": ("event_time TIMESTAMP, recommendation_id STRING, event STRING, actor STRING, details STRING", None, None,
              "Approval / execution audit trail (append only)."),
}
LEGACY = ("recommendations", "findings", "approval_audit", "optimization_queue")


def _df_schema(ddl: str) -> str:
    return ddl.replace(" NOT NULL", "")


class DeltaRecommendationRepository(RecommendationRepository):
    """Normalized Unity Catalog tables (see module docstring) + reporting views."""

    def __init__(self, spark: Any, catalog: str, schema: str, migrate_legacy: bool = True) -> None:
        self.spark = spark
        self.base = f"{catalog}.{schema}"
        self.t = {k: quote_fqn(f"{self.base}.{v}") for k, v in TABLES.items()}
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_fqn(self.base)}")
        for key, (ddl, pk, cluster, comment) in DDL.items():
            name = TABLES[key]
            cons = f", CONSTRAINT {name}_pk PRIMARY KEY ({pk})" if pk else ""
            clus = f" CLUSTER BY ({cluster})" if cluster else ""
            sql = f"CREATE TABLE IF NOT EXISTS {self.t[key]} ({ddl}{cons}){clus} COMMENT {sql_literal(comment)}"
            try:
                spark.sql(sql)
            except Exception:  # noqa: BLE001 - informational constraints unsupported (e.g. non-UC) -> plain table
                spark.sql(f"CREATE TABLE IF NOT EXISTS {self.t[key]} ({ddl}){clus} COMMENT {sql_literal(comment)}")
        self._create_views()
        if migrate_legacy:
            try:
                self.migrate_legacy()
            except Exception:  # noqa: BLE001 - never block a run on a best-effort migration
                log.exception("legacy table migration skipped")

    # ------------------------------------------------------------------ write helpers
    def _name(self, key: str) -> str:
        return self.t[key].replace("`", "")

    def _df(self, key: str, rows: list[dict]) -> Any:
        return self.spark.createDataFrame(rows, schema=_df_schema(DDL[key][0]))

    def _replace_run(self, key: str, run_id: str, rows: list[dict]) -> None:
        """Idempotent per-run write: re-saving a run replaces its rows instead of duplicating them."""
        self.spark.sql(f"DELETE FROM {self.t[key]} WHERE run_id = {sql_literal(run_id)}")
        if rows:
            self._df(key, rows).write.mode("append").saveAsTable(self._name(key))

    def _merge(self, key: str, rows: list[dict], on: list[str], update: list[str], insert_only: dict[str, str] | None = None) -> None:
        if not rows:
            return
        view = f"_cco_{key}_src"
        self._df(key, rows).createOrReplaceTempView(view)
        cols = [c.split()[0] for c in DDL[key][0].split(", ")]
        ins = {c: (insert_only or {}).get(c, f"s.{c}") for c in cols}
        self.spark.sql(
            f"MERGE INTO {self.t[key]} t USING {view} s ON {' AND '.join(f't.{c} = s.{c}' for c in on)} "
            f"WHEN MATCHED THEN UPDATE SET {', '.join(f'{c} = s.{c}' for c in update)} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(ins)}) VALUES ({', '.join(ins.values())})")

    # ------------------------------------------------------------------ runs / dimensions
    def save_run(self, run: dict[str, Any]) -> None:
        cols = [c.split()[0] for c in DDL["runs"][0].split(", ")]
        row = {c: run.get(c) for c in cols}
        upd = [c for c in cols if c != "run_id" and run.get(c) is not None]
        self._merge("runs", [row], ["run_id"], upd or ["status"])

    def save_catalog(self, run_id: str, resources: Iterable[dict], checks: Iterable[dict]) -> None:
        now = utcnow()
        res = [{**r, "first_seen_run_id": run_id, "last_seen_run_id": run_id, "updated_at": now} for r in resources]
        # keep known attributes when this run could not resolve them (coalesce), never move first_seen
        if res:
            self._df("resources", res).createOrReplaceTempView("_cco_resources_src")
            self.spark.sql(
                f"MERGE INTO {self.t['resources']} t USING _cco_resources_src s "
                "ON t.resource_type = s.resource_type AND t.resource_id = s.resource_id "
                "WHEN MATCHED THEN UPDATE SET resource_name = coalesce(s.resource_name, t.resource_name), "
                "workspace_id = coalesce(s.workspace_id, t.workspace_id), owner = coalesce(s.owner, t.owner), "
                "run_as = coalesce(s.run_as, t.run_as), last_seen_run_id = s.last_seen_run_id, updated_at = s.updated_at "
                "WHEN NOT MATCHED THEN INSERT *")
        self._merge("checks", [{**c, "updated_at": now} for c in checks], ["check_name"], ["agent", "domain", "updated_at"])

    # ------------------------------------------------------------------ facts
    def save_findings(self, run_id: str, findings: Iterable[Finding]) -> None:
        rows, members, snaps = finding_rows(run_id, findings)
        self._replace_run("findings", run_id, rows)
        self._replace_run("finding_resources", run_id, members)
        self._replace_run("resource_snapshots", run_id, snaps)

    def save_queue(self, run_id: str, items: Iterable[Any]) -> None:
        q, m = queue_rows(run_id, items)
        self._replace_run("queue", run_id, q)
        self._replace_run("queue_members", run_id, m)

    # ------------------------------------------------------------------ recommendations (current state)
    def get(self, rec_id: str) -> Recommendation | None:
        rows = self.spark.sql(f"SELECT * FROM {self.t['recommendations']} WHERE recommendation_id = {sql_literal(rec_id)}").collect()
        return rec_from_row(rows[0].asDict()) if rows else None

    def list(self, status: RecommendationStatus | None = None) -> list[Recommendation]:
        where = f" WHERE status = {sql_literal(RecommendationStatus(status).value)}" if status else ""
        rows = self.spark.sql(f"SELECT * FROM {self.t['recommendations']}{where} ORDER BY score DESC").collect()
        return [rec_from_row(r.asDict()) for r in rows]

    def upsert(self, recs: Iterable[Recommendation]) -> list[Recommendation]:
        recs = list(recs)
        if not recs:
            return []
        ids = ", ".join(sql_literal(r.id) for r in recs)
        existing = {row.recommendation_id: rec_from_row(row.asDict()) for row in self.spark.sql(
            f"SELECT * FROM {self.t['recommendations']} WHERE recommendation_id IN ({ids})").collect()}
        merged = [merge_existing(existing.get(r.id), r, self.resurface_rejected_after_days) for r in recs]
        self._write(merged)
        return merged

    def _write(self, recs: list[Recommendation]) -> None:
        if recs:
            cols = [c.split()[0] for c in DDL["recommendations"][0].split(", ")]
            self._merge("recommendations", [rec_row(r) for r in recs], ["recommendation_id"], [c for c in cols if c != "recommendation_id"])

    def audit(self, rec_id: str, event: str, actor: str, details: dict[str, Any] | None = None) -> None:
        row = [{"event_time": utcnow(), "recommendation_id": rec_id, "event": event, "actor": actor,
                "details": json.dumps(details or {}, default=str)}]
        self._df("audit", row).write.mode("append").saveAsTable(self._name("audit"))

    # ------------------------------------------------------------------ reporting views
    def _create_views(self) -> None:
        t, b = self.t, self.base
        v = {k: quote_fqn(f"{b}.{k}") for k in ("v_findings", "v_latest_findings", "v_recommendations", "v_queue", "v_run_summary")}
        latest = f"(SELECT max_by(run_id, started_at) AS run_id FROM {t['runs']} WHERE status = 'succeeded')"
        stmts = [
            f"""CREATE OR REPLACE VIEW {v['v_findings']} COMMENT 'Findings with run, check, resource, cost and duplicate members' AS
            SELECT f.run_id, r.started_at AS run_started_at, (f.run_id = l.run_id) AS is_latest_run, f.finding_id,
                   c.agent, c.domain, f.check_name, f.severity, f.resource_type, f.resource_id, res.resource_name, res.owner,
                   res.workspace_id, s.monthly_cost, f.title, f.detail, f.metrics, s.metrics AS resource_metrics,
                   coalesce(m.duplicate_count, 0) AS duplicate_count, m.duplicate_ids, m.duplicate_names, f.created_at
            FROM {t['findings']} f
            LEFT JOIN {t['runs']} r ON r.run_id = f.run_id
            CROSS JOIN {latest} l
            LEFT JOIN {t['checks']} c ON c.check_name = f.check_name
            LEFT JOIN {t['resources']} res ON res.resource_type = f.resource_type AND res.resource_id = f.resource_id
            LEFT JOIN {t['resource_snapshots']} s
                   ON s.run_id = f.run_id AND s.resource_type = f.resource_type AND s.resource_id = f.resource_id
            LEFT JOIN (SELECT fr.run_id, fr.finding_id, count(*) AS duplicate_count,
                              collect_list(fr.resource_id) AS duplicate_ids,
                              collect_list(coalesce(d.resource_name, fr.resource_id)) AS duplicate_names
                       FROM {t['finding_resources']} fr
                       LEFT JOIN {t['resources']} d ON d.resource_type = fr.resource_type AND d.resource_id = fr.resource_id
                       GROUP BY fr.run_id, fr.finding_id) m ON m.run_id = f.run_id AND m.finding_id = f.finding_id""",
            f"""CREATE OR REPLACE VIEW {v['v_latest_findings']} COMMENT 'Findings of the latest successful run' AS
            SELECT * FROM {v['v_findings']} WHERE is_latest_run""",
            f"""CREATE OR REPLACE VIEW {v['v_recommendations']} COMMENT 'Current recommendations with resource and latest queue position' AS
            SELECT rec.recommendation_id, rec.status, rec.priority, rec.score, q.rank AS queue_rank, qm.group_id,
                   rec.action_type, rec.agent, rec.severity, rec.risk, rec.reversible, rec.confidence,
                   rec.estimated_monthly_savings, rec.savings_basis, rec.resource_type, rec.resource_id, res.resource_name,
                   res.owner, res.workspace_id, rec.title, rec.rationale, rec.command, rec.superseded_by, rec.finding_ids,
                   rec.related_resources, rec.depends_on, rec.details, rec.last_run_id, (rec.last_run_id = l.run_id) AS in_latest_run,
                   rec.created_at, rec.updated_at
            FROM {t['recommendations']} rec
            CROSS JOIN {latest} l
            LEFT JOIN {t['resources']} res ON res.resource_type = rec.resource_type AND res.resource_id = rec.resource_id
            LEFT JOIN {t['queue_members']} qm ON qm.run_id = l.run_id AND qm.recommendation_id = rec.recommendation_id
            LEFT JOIN {t['queue']} q ON q.run_id = qm.run_id AND q.group_id = qm.group_id""",
            f"""CREATE OR REPLACE VIEW {v['v_queue']} COMMENT 'Optimization queue per run with member recommendations' AS
            SELECT q.run_id, r.started_at AS run_started_at, (q.run_id = l.run_id) AS is_latest_run, q.rank, q.group_id,
                   p.title AS primary_title, q.priority, q.group_score, q.combined_monthly_savings, q.impact, q.confidence,
                   q.max_risk, q.primary_recommendation_id, count(qm.recommendation_id) AS recommendations,
                   collect_list(qm.recommendation_id) AS recommendation_ids, array_distinct(collect_list(rec.resource_id)) AS resources,
                   array_distinct(collect_list(rec.agent)) AS agents, array_distinct(collect_list(rec.action_type)) AS actions,
                   array_distinct(collect_list(rec.status)) AS current_statuses
            FROM {t['queue']} q
            CROSS JOIN {latest} l
            LEFT JOIN {t['runs']} r ON r.run_id = q.run_id
            LEFT JOIN {t['recommendations']} p ON p.recommendation_id = q.primary_recommendation_id
            LEFT JOIN {t['queue_members']} qm ON qm.run_id = q.run_id AND qm.group_id = q.group_id
            LEFT JOIN {t['recommendations']} rec ON rec.recommendation_id = qm.recommendation_id
            GROUP BY ALL""",
            f"""CREATE OR REPLACE VIEW {v['v_run_summary']} COMMENT 'One row per run: status, volume, cost in scope, queued savings' AS
            SELECT r.run_id, r.started_at, r.finished_at, r.status, r.agents_run, r.error_message,
                   (r.run_id = l.run_id) AS is_latest_run,
                   (SELECT count(*) FROM {t['findings']} f WHERE f.run_id = r.run_id) AS findings,
                   (SELECT count(*) FROM {t['findings']} f WHERE f.run_id = r.run_id AND f.severity IN ('high', 'critical')) AS high_findings,
                   (SELECT count(*) FROM {t['resource_snapshots']} s WHERE s.run_id = r.run_id) AS resources_flagged,
                   (SELECT round(sum(s.monthly_cost), 2) FROM {t['resource_snapshots']} s WHERE s.run_id = r.run_id) AS flagged_monthly_cost,
                   (SELECT count(*) FROM {t['queue']} q WHERE q.run_id = r.run_id) AS queue_items,
                   (SELECT round(sum(q.combined_monthly_savings), 2) FROM {t['queue']} q WHERE q.run_id = r.run_id) AS queued_monthly_savings
            FROM {t['runs']} r CROSS JOIN {latest} l""",
        ]
        for s in stmts:
            try:
                self.spark.sql(s)
            except Exception:  # noqa: BLE001 - views are a convenience layer; never block persistence
                log.exception("could not create reporting view")

    # ------------------------------------------------------------------ one-time migration from the old flat tables
    def _exists(self, name: str) -> bool:
        cat, sch = self.base.split(".")
        return bool(self.spark.sql(
            f"SELECT 1 FROM {quote_fqn(cat)}.information_schema.tables WHERE table_schema = {sql_literal(sch)} "
            f"AND table_name = {sql_literal(name)}").collect())

    def _empty(self, key: str) -> bool:
        return not self.spark.sql(f"SELECT 1 FROM {self.t[key]} LIMIT 1").collect()

    def migrate_legacy(self) -> dict[str, int]:
        """Copy the old flat tables (recommendations / findings / approval_audit / optimization_queue) into the new model.

        Runs only into EMPTY new tables, never modifies or drops the old tables. Approval decisions are preserved.
        """
        old = {n: quote_fqn(f"{self.base}.{n}") for n in LEGACY if self._exists(n)}
        done: dict[str, int] = {}
        if "recommendations" in old and self._empty("recommendations"):
            recs = [Recommendation.from_dict(json.loads(r.payload))
                    for r in self.spark.sql(f"SELECT payload FROM {old['recommendations']}").collect()]
            self._write(recs)
            done["recommendations"] = len(recs)
        if "approval_audit" in old and self._empty("audit"):
            self.spark.sql(f"INSERT INTO {self.t['audit']} SELECT event_time, rec_id, event, actor, details FROM {old['approval_audit']}")
            done["audit"] = 1
        if "findings" in old and self._empty("findings"):
            o = old["findings"]
            fid = "substr(sha1(concat_ws('|', 'finding', agent, check_name, resource_type, resource_id)), 1, 16)"
            self.spark.sql(f"""INSERT INTO {self.t['runs']} (run_id, started_at, finished_at, status)
                SELECT run_id, min(created_at), max(created_at), 'succeeded' FROM {o}
                WHERE run_id NOT IN (SELECT run_id FROM {self.t['runs']}) GROUP BY run_id""")
            self.spark.sql(f"""MERGE INTO {self.t['checks']} t USING (SELECT check_name, max(agent) AS agent FROM {o} GROUP BY check_name) s
                ON t.check_name = s.check_name WHEN NOT MATCHED THEN INSERT (check_name, agent, updated_at)
                VALUES (s.check_name, s.agent, current_timestamp())""")
            self.spark.sql(f"""MERGE INTO {self.t['resources']} t USING (SELECT resource_type, resource_id, min(run_id) AS f, max(run_id) AS l
                FROM {o} GROUP BY ALL) s ON t.resource_type = s.resource_type AND t.resource_id = s.resource_id
                WHEN NOT MATCHED THEN INSERT (resource_type, resource_id, first_seen_run_id, last_seen_run_id, updated_at)
                VALUES (s.resource_type, s.resource_id, s.f, s.l, current_timestamp())""")
            self.spark.sql(f"""INSERT INTO {self.t['findings']} SELECT run_id, {fid}, check_name, resource_type, resource_id,
                severity, title, detail, metrics, created_at FROM {o}
                QUALIFY row_number() OVER (PARTITION BY run_id, {fid} ORDER BY created_at DESC) = 1""")
            self.spark.sql(f"""INSERT INTO {self.t['resource_snapshots']} SELECT run_id, resource_type, resource_id,
                max(try_cast(get_json_object(metrics, '$.monthly_cost') AS DOUBLE)), NULL FROM {o} GROUP BY ALL""")
            done["findings"] = 1
        if "optimization_queue" in old and self._empty("queue"):
            o = old["optimization_queue"]
            self.spark.sql(f"""INSERT INTO {self.t['queue']} SELECT run_id, group_id, rank, priority, group_score,
                combined_monthly_savings, impact, confidence, max_risk, primary_recommendation_id FROM {o}
                QUALIFY row_number() OVER (PARTITION BY run_id, group_id ORDER BY created_at DESC) = 1""")
            self.spark.sql(f"""INSERT INTO {self.t['queue_members']} SELECT DISTINCT run_id, group_id, rid FROM {o}
                LATERAL VIEW explode(from_json(recommendation_ids, 'array<string>')) x AS rid""")
            done["queue"] = 1
        if done:
            log.info("migrated legacy optimizer tables: %s", done)
        return done
