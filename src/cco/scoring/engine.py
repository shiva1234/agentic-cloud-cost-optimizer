"""Scoring + recommendation engine: applies rules, ranks, and (re)scores after LLM review."""
from __future__ import annotations

from typing import Iterable

from cco.models.canonical import Recommendation
from cco.scoring.rules import ScoringRules


class ScoringEngine:
    def __init__(self, rules: ScoringRules | None = None, min_score_for_approval: float = 25.0) -> None:
        self.rules = rules or ScoringRules()
        self.min_score_for_approval = min_score_for_approval

    @classmethod
    def from_config(cls, cfg: dict | None) -> "ScoringEngine":
        cfg = cfg or {}
        return cls(ScoringRules.from_config(cfg), float(cfg.get("min_score_for_approval", 25)))

    def score(self, recs: Iterable[Recommendation]) -> list[Recommendation]:
        out = []
        for r in recs:
            llm_delta = r.score_breakdown.get("llm_confidence_delta")
            r.score, r.score_breakdown = self.rules.score(r)
            if llm_delta is not None:  # keep the audit trail of the AI adjustment
                r.score_breakdown["llm_confidence_delta"] = llm_delta
            r.priority = self.rules.priority(r.score)
            out.append(r)
        return self.rank(out)

    @staticmethod
    def rank(recs: Iterable[Recommendation]) -> list[Recommendation]:
        return sorted(recs, key=lambda r: (-r.score, -r.estimated_monthly_savings, r.resource_id))

    def actionable(self, recs: Iterable[Recommendation]) -> list[Recommendation]:
        return [r for r in recs if r.score >= self.min_score_for_approval]
