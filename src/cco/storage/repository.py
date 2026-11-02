"""Persistence for findings, recommendations (with approval state) and the audit log.

Re-running the analysis must never silently overwrite a human decision, so
`merge_existing` keeps pending/approved/rejected state for recurring recommendations.
"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Iterable

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


class InMemoryRecommendationRepository(RecommendationRepository):
    def __init__(self) -> None:
        self.recs: dict[str, Recommendation] = {}
        self.findings: list[dict] = []
        self.audit_log: list[dict] = []

    def get(self, rec_id: str) -> Recommendation | None:
        return self.recs.get(rec_id)

    def list(self, status: RecommendationStatus | None = None) -> list[Recommendation]:
        return [r for r in self.recs.values() if status is None or r.status == status]

    def _write(self, recs: list[Recommendation]) -> None:
        for r in recs:
            self.recs[r.id] = r

    def save_findings(self, run_id: str, findings: Iterable[Finding]) -> None:
        self.findings.extend({**f.to_dict(), "run_id": run_id} for f in findings)

    def audit(self, rec_id: str, event: str, actor: str, details: dict[str, Any] | None = None) -> None:
        self.audit_log.append({"event_time": utcnow(), "rec_id": rec_id, "event": event, "actor": actor, "details": details or {}})


REC_SCHEMA = (
    "id STRING, run_id STRING, resource_id STRING, resource_type STRING, action_type STRING, agent STRING, "
    "status STRING, priority STRING, score DOUBLE, estimated_monthly_savings DOUBLE, severity STRING, risk STRING, "
    "title STRING, command STRING, payload STRING, updated_at TIMESTAMP"
)
FINDING_SCHEMA = (
    "run_id STRING, id STRING, agent STRING, check_name STRING, resource_id STRING, resource_type STRING, "
    "severity STRING, title STRING, detail STRING, metrics STRING, created_at TIMESTAMP"
)
AUDIT_SCHEMA = "event_time TIMESTAMP, rec_id STRING, event STRING, actor STRING, details STRING"


class DeltaRecommendationRepository(RecommendationRepository):
    """Unity Catalog tables: recommendations (MERGE on id), findings (append), approval_audit (append)."""

    def __init__(self, spark: Any, catalog: str, schema: str) -> None:
        self.spark = spark
        base = f"{catalog}.{schema}"
        self.t_recs, self.t_findings, self.t_audit = (quote_fqn(f"{base}.{t}") for t in ("recommendations", "findings", "approval_audit"))
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_fqn(base)}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.t_recs} ({REC_SCHEMA})")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.t_findings} ({FINDING_SCHEMA})")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.t_audit} ({AUDIT_SCHEMA})")

    @staticmethod
    def _row(r: Recommendation) -> dict[str, Any]:
        return {
            "id": r.id, "run_id": r.run_id, "resource_id": r.resource_id, "resource_type": r.resource_type,
            "action_type": r.action_type.value, "agent": r.agent, "status": r.status.value, "priority": r.priority,
            "score": float(r.score), "estimated_monthly_savings": float(r.estimated_monthly_savings),
            "severity": r.severity.value, "risk": r.risk.value, "title": r.title, "command": r.command,
            "payload": json.dumps(r.to_dict(), default=str), "updated_at": r.updated_at,
        }

    def get(self, rec_id: str) -> Recommendation | None:
        rows = self.spark.sql(f"SELECT payload FROM {self.t_recs} WHERE id = {sql_literal(rec_id)}").collect()
        return Recommendation.from_dict(json.loads(rows[0].payload)) if rows else None

    def list(self, status: RecommendationStatus | None = None) -> list[Recommendation]:
        where = f" WHERE status = {sql_literal(RecommendationStatus(status).value)}" if status else ""
        rows = self.spark.sql(f"SELECT payload FROM {self.t_recs}{where} ORDER BY score DESC").collect()
        return [Recommendation.from_dict(json.loads(r.payload)) for r in rows]

    def upsert(self, recs: Iterable[Recommendation]) -> list[Recommendation]:
        recs = list(recs)
        if not recs:
            return []
        ids = ", ".join(sql_literal(r.id) for r in recs)
        existing = {
            row.id: Recommendation.from_dict(json.loads(row.payload))
            for row in self.spark.sql(f"SELECT id, payload FROM {self.t_recs} WHERE id IN ({ids})").collect()
        }
        merged = [merge_existing(existing.get(r.id), r, self.resurface_rejected_after_days) for r in recs]
        self._write(merged)
        return merged

    def _write(self, recs: list[Recommendation]) -> None:
        if not recs:
            return
        df = self.spark.createDataFrame([self._row(r) for r in recs], schema=REC_SCHEMA)
        df.createOrReplaceTempView("_cco_rec_updates")
        self.spark.sql(
            f"""MERGE INTO {self.t_recs} t USING _cco_rec_updates s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""
        )

    def save_findings(self, run_id: str, findings: Iterable[Finding]) -> None:
        rows = [
            {"run_id": run_id, "id": f.id, "agent": f.agent, "check_name": f.check, "resource_id": f.resource_id,
             "resource_type": f.resource_type, "severity": f.severity.value, "title": f.title, "detail": f.detail,
             "metrics": json.dumps(f.metrics, default=str), "created_at": f.created_at}
            for f in findings
        ]
        if rows:
            self.spark.createDataFrame(rows, schema=FINDING_SCHEMA).write.mode("append").saveAsTable(self.t_findings.replace("`", ""))

    def audit(self, rec_id: str, event: str, actor: str, details: dict[str, Any] | None = None) -> None:
        row = [{"event_time": utcnow(), "rec_id": rec_id, "event": event, "actor": actor, "details": json.dumps(details or {}, default=str)}]
        self.spark.createDataFrame(row, schema=AUDIT_SCHEMA).write.mode("append").saveAsTable(self.t_audit.replace("`", ""))
