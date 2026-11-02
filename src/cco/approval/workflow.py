"""Human-in-the-loop approval workflow.

State machine:
  PROPOSED --submit--> PENDING_APPROVAL --approve(xN)--> APPROVED --executor--> EXECUTED | FAILED
                                       +--reject-----> REJECTED
Nothing is ever executed unless it is APPROVED. High-risk items need two distinct approvers.
Every transition is written to the audit log.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from cco.models.canonical import Recommendation, RecommendationStatus, Risk
from cco.storage.repository import RecommendationRepository
from cco.utils import utcnow

S = RecommendationStatus


class ApprovalError(RuntimeError):
    pass


@dataclass
class ApprovalPolicy:
    min_score_for_approval: float = 25.0
    dual_approval_risks: set[str] = field(default_factory=lambda: {"high"})

    @classmethod
    def from_config(cls, approval_cfg: dict | None, scoring_cfg: dict | None = None) -> "ApprovalPolicy":
        a, s = approval_cfg or {}, scoring_cfg or {}
        return cls(float(s.get("min_score_for_approval", 25)), set(a.get("dual_approval_risks", ["high"])))

    def required_approvals(self, rec: Recommendation) -> int:
        return 2 if Risk(rec.risk).value in self.dual_approval_risks else 1


class ApprovalWorkflow:
    def __init__(self, repo: RecommendationRepository, policy: ApprovalPolicy | None = None) -> None:
        self.repo = repo
        self.policy = policy or ApprovalPolicy()

    def submit(self, recs: Iterable[Recommendation], actor: str = "system") -> list[Recommendation]:
        """Persist recommendations; those above the score threshold enter the approval queue."""
        prepared = []
        for r in recs:
            if r.status == S.PROPOSED and r.score >= self.policy.min_score_for_approval:
                r.status = S.PENDING_APPROVAL
            prepared.append(r)
        stored = self.repo.upsert(prepared)
        for r in stored:
            if r.status == S.PENDING_APPROVAL:
                self.repo.audit(r.id, "queued_for_approval", actor, {"score": r.score, "priority": r.priority})
        return stored

    def pending(self) -> list[Recommendation]:
        return sorted(self.repo.list(S.PENDING_APPROVAL), key=lambda r: -r.score)

    def approve(self, rec_id: str, approver: str, comment: str = "") -> Recommendation:
        rec = self._get(rec_id)
        if rec.status != S.PENDING_APPROVAL:
            raise ApprovalError(f"{rec_id} is {rec.status.value}; only pending items can be approved")
        if any(a["approver"] == approver for a in rec.approvals):
            raise ApprovalError(f"{approver} already approved {rec_id}")
        rec.approvals.append({"approver": approver, "comment": comment, "at": utcnow().isoformat()})
        needed = self.policy.required_approvals(rec)
        if len(rec.approvals) >= needed:
            rec.status = S.APPROVED
        self.repo.update(rec)
        self.repo.audit(rec.id, "approved" if rec.status == S.APPROVED else "partially_approved", approver,
                        {"comment": comment, "approvals": len(rec.approvals), "required": needed})
        return rec

    def reject(self, rec_id: str, approver: str, reason: str) -> Recommendation:
        rec = self._get(rec_id)
        if rec.status not in (S.PENDING_APPROVAL, S.PROPOSED, S.APPROVED):
            raise ApprovalError(f"{rec_id} is {rec.status.value}; cannot reject")
        rec.status = S.REJECTED
        rec.rejection = {"by": approver, "reason": reason, "at": utcnow().isoformat()}
        self.repo.update(rec)
        self.repo.audit(rec.id, "rejected", approver, {"reason": reason})
        return rec

    def _get(self, rec_id: str) -> Recommendation:
        rec = self.repo.get(rec_id)
        if rec is None:
            raise ApprovalError(f"recommendation {rec_id} not found")
        return rec
