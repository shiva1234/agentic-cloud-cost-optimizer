"""Optimization Queue: ONE prioritized list across all agents.

The Coordinator has already de-duplicated, suppressed superseded recommendations and assigned
a group_id to overlapping recommendations (same resource, or linked via related_resources such as
duplicate jobs, consolidated clusters, a table and its writer job). Here each group becomes one
queue item:

  combined savings  per resource: cost x (1 - prod(1 - s_i / cost)) when the resource's monthly cost is
                    known (savings of overlapping actions don't simply add up), else sum; then summed
                    across resources of the group
  impact            combined savings x savings-weighted confidence
  group score       best member score, +5 when 2+ agents corroborate the same group
  order             priority band (from group score), then impact, then group score
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from cco.models.canonical import Recommendation
from cco.scoring.rules import ScoringRules

RISK_ORDER = {"low": 0, "medium": 1, "high": 2}
CORROBORATION_BONUS = 5.0


@dataclass
class QueueItem:
    rank: int
    group_id: str
    title: str
    priority: str
    group_score: float
    combined_monthly_savings: float
    impact: float
    confidence: float
    max_risk: str
    primary_recommendation_id: str
    recommendation_ids: list[str] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)
    agents: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    statuses: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class OptimizationQueue:
    items: list[QueueItem] = field(default_factory=list)

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def top(self, n: int = 20) -> list[QueueItem]:
        return self.items[:n]

    def refresh_statuses(self, recs: Iterable[Recommendation]) -> None:
        by_id = {r.id: r for r in recs}
        for it in self.items:
            st: dict[str, int] = defaultdict(int)
            for rid in it.recommendation_ids:
                if rid in by_id:
                    st[by_id[rid].status.value] += 1
            it.statuses = dict(st)


def combined_savings(recs: list[Recommendation]) -> float:
    per_res: dict[str, list[Recommendation]] = defaultdict(list)
    for r in recs:
        per_res[r.resource_id].append(r)
    total = 0.0
    for items in per_res.values():
        cost = max((float(r.metrics.get("resource_monthly_cost") or 0) for r in items), default=0.0)
        if cost > 0:
            remaining = 1.0
            for r in items:
                remaining *= 1 - min(1.0, r.estimated_monthly_savings / cost)
            total += cost * (1 - remaining)
        else:
            total += sum(r.estimated_monthly_savings for r in items)
    return round(total, 2)


def _gid(r: Recommendation) -> str:
    """Coordinator-assigned group, else one group per resource."""
    return r.group_id or f"resource:{r.resource_id}"


def build_optimization_queue(recs: list[Recommendation], rules: ScoringRules | None = None) -> OptimizationQueue:
    rules = rules or ScoringRules()
    groups: dict[str, list[Recommendation]] = defaultdict(list)
    for r in recs:
        groups[_gid(r)].append(r)
    items: list[QueueItem] = []
    for gid, members in groups.items():
        members.sort(key=lambda r: (-r.score, -r.estimated_monthly_savings, r.id))
        primary = members[0]
        agents = sorted({r.agent for r in members})
        gscore = min(100.0, primary.score + (CORROBORATION_BONUS if len(agents) > 1 else 0.0))
        sav = combined_savings(members)
        w = sum(r.estimated_monthly_savings for r in members)
        conf = (sum(r.confidence * r.estimated_monthly_savings for r in members) / w if w > 0
                else sum(r.confidence for r in members) / len(members))
        title = primary.title if len(members) == 1 else f"{primary.title} (+{len(members) - 1} related)"
        items.append(QueueItem(
            rank=0, group_id=gid, title=title, priority=rules.priority(gscore), group_score=round(gscore, 1),
            combined_monthly_savings=sav, impact=round(sav * conf, 2), confidence=round(conf, 3),
            max_risk=max((r.risk.value for r in members), key=lambda x: RISK_ORDER.get(x, 0)),
            primary_recommendation_id=primary.id, recommendation_ids=[r.id for r in members],
            resources=sorted({r.resource_id for r in members}), agents=agents,
            actions=sorted({r.action_type.value for r in members}),
        ))
    items.sort(key=lambda it: (it.priority, -it.impact, -it.group_score, it.group_id))
    by_group = {it.group_id: it for it in items}
    for i, it in enumerate(items, 1):
        it.rank = i
    for r in recs:
        it = by_group.get(_gid(r))
        r.queue_rank = it.rank if it else None
    q = OptimizationQueue(items)
    q.refresh_statuses(recs)
    return q
