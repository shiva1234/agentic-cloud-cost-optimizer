"""Scoring rules: transparent, additive point system (0-100) so every score is explainable.

  severity points   info 0 | low 10 | medium 25 | high 40 | critical 50
  savings points    0-30, log-scaled up to `savings_cap_usd`/month
  confidence        multiplies (severity + savings) by 0.5 + 0.5 * confidence
  reversibility     +5 if the action can be undone
  automation        +5 if a validated command exists (cheap to act on)
  risk penalty      low 0 | medium 8 | high 20
  prod penalty      -5 when the resource matches a production pattern
"""
from __future__ import annotations

import fnmatch
import math
from dataclasses import dataclass, field

from cco.models.canonical import Recommendation

SEVERITY_POINTS = {"info": 0, "low": 10, "medium": 25, "high": 40, "critical": 50}
RISK_PENALTY = {"low": 0, "medium": 8, "high": 20}


@dataclass
class ScoringRules:
    savings_cap_usd: float = 10_000.0
    max_savings_points: float = 30.0
    reversible_bonus: float = 5.0
    automation_bonus: float = 5.0
    prod_penalty: float = 5.0
    prod_resource_patterns: list[str] = field(default_factory=lambda: ["*prod*"])
    priority_bands: tuple[tuple[float, str], ...] = ((70, "P1"), (50, "P2"), (30, "P3"))

    @classmethod
    def from_config(cls, cfg: dict | None) -> "ScoringRules":
        cfg = cfg or {}
        known = {k: v for k, v in cfg.items() if k in cls.__dataclass_fields__}
        return cls(**known)

    def savings_points(self, savings: float) -> float:
        if savings <= 0:
            return 0.0
        return min(self.max_savings_points, self.max_savings_points * math.log10(1 + savings) / math.log10(1 + self.savings_cap_usd))

    def is_prod(self, resource_id: str) -> bool:
        return any(fnmatch.fnmatch(resource_id.lower(), p.lower()) for p in self.prod_resource_patterns)

    def score(self, r: Recommendation) -> tuple[float, dict[str, float]]:
        sev = SEVERITY_POINTS[r.severity.value]
        sav = self.savings_points(r.estimated_monthly_savings)
        conf_mult = 0.5 + 0.5 * max(0.0, min(1.0, r.confidence))
        parts = {
            "severity": sev,
            "savings": round(sav, 2),
            "confidence_multiplier": round(conf_mult, 3),
            "reversible": self.reversible_bonus if r.reversible else 0.0,
            "automation": self.automation_bonus if r.executable else 0.0,
            "risk_penalty": -RISK_PENALTY[r.risk.value],
            "prod_penalty": -self.prod_penalty if self.is_prod(r.resource_id) else 0.0,
        }
        total = (sev + sav) * conf_mult + parts["reversible"] + parts["automation"] + parts["risk_penalty"] + parts["prod_penalty"]
        return round(max(0.0, min(100.0, total)), 1), parts

    def priority(self, score: float) -> str:
        for threshold, label in self.priority_bands:
            if score >= threshold:
                return label
        return "P4"
