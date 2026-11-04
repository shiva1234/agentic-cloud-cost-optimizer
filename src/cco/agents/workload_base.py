"""Shared plumbing for the workload agents (compute, jobs, pipelines)."""
from __future__ import annotations

import fnmatch
import logging
from collections import defaultdict
from typing import Any, Callable, ClassVar, Iterable, Optional

from cco.agents.base import AgentContext, AgentResult, BaseAgent
from cco.models.canonical import ActionType, Finding, Recommendation, Risk, Severity

log = logging.getLogger(__name__)

CheckOut = Optional[tuple[Finding, list[Recommendation]]]


def connected_groups(edges: list[tuple]) -> list[tuple[list, list[tuple]]]:
    """Union-find over (a, b, ...) edges -> [(sorted members, edges of that set)], one entry per connected set.

    A~B, A~C, B~D all land in ONE set {A, B, C, D}, so a duplicate set is reported once - never per pair or per member.
    """
    parent: dict[Any, Any] = {}

    def find(x: Any) -> Any:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for e in edges:
        ra, rb = find(e[0]), find(e[1])
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    comp_edges: dict[Any, list[tuple]] = defaultdict(list)
    for e in edges:
        comp_edges[find(e[0])].append(e)
    members: dict[Any, set] = defaultdict(set)
    for x in list(parent):
        members[find(x)].add(x)
    return [(sorted(members[r]), comp_edges[r]) for r in sorted(comp_edges)]


class WorkloadAgent(BaseAgent):
    DEFAULTS: ClassVar[dict[str, Any]] = {}

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.t: dict[str, Any] = {**self.DEFAULTS, **(self.config.get("thresholds") or {})}

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def in_scope(ctx: AgentContext, *keys: str | None) -> bool:
        patterns = ctx.scope.get("resources")
        if not patterns:
            return True
        return any(k and fnmatch.fnmatch(str(k), p) for k in keys for p in patterns)

    def finding(self, rid: str, rtype: str, check: str, title: str, detail: str, severity: Severity, *,
                resource_metrics: dict[str, Any] | None = None, **metrics: Any) -> Finding:
        """resource_metrics: attributes of the resource itself (cost, runs, success rate...), identical for every finding on
        that resource in a run. They stay in Finding.metrics in memory but are stored ONCE per resource and run."""
        rm = {k: v for k, v in (resource_metrics or {}).items() if v is not None and k not in metrics}
        m = {**rm, **{k: v for k, v in metrics.items() if v is not None}}
        return Finding(agent=self.name, check=check, resource_id=str(rid), resource_type=rtype, title=title,
                       detail=detail, severity=severity, metrics=m, resource_metric_keys=sorted(rm))

    def rec(self, rid: str, rtype: str, action: ActionType, title: str, rationale: str, *, severity: Severity,
            risk: Risk = Risk.LOW, reversible: bool = True, confidence: float = 0.6, savings: float = 0.0, basis: str = "",
            related: Iterable[str] = (), monthly_cost: float | None = None, proposed: dict | None = None,
            command: str | None = None, **metrics: Any) -> Recommendation:
        m = {k: v for k, v in metrics.items() if v is not None}
        if monthly_cost is not None:
            m["resource_monthly_cost"] = round(float(monthly_cost), 2)
            savings = min(savings, float(monthly_cost)) if monthly_cost > 0 else savings
        if proposed:
            m["proposed_change"] = proposed
        return Recommendation(
            resource_id=str(rid), resource_type=rtype, action_type=action, title=title, rationale=rationale, agent=self.name,
            command=command, severity=severity, risk=risk, reversible=reversible, confidence=confidence,
            estimated_monthly_savings=round(max(0.0, savings), 2), savings_basis=basis, metrics=m,
            related_resources=sorted({str(r) for r in related if r and str(r) != str(rid)}),
        )

    def run_checks(self, result: AgentResult, ctx: AgentContext, items: Iterable[Any],
                   checks: list[Callable[[Any], CheckOut]], key: Callable[[Any], str]) -> int:
        n = 0
        for item in items:
            n += 1
            for check in checks:
                try:
                    self.emit(result, ctx, check(item))
                except Exception as e:  # noqa: BLE001
                    log.exception("%s.%s failed on %s", self.name, check.__name__, key(item))
                    result.errors.append({"resource_id": key(item), "check": check.__name__, "error": str(e)[:300]})
        return n

    @staticmethod
    def emit(result: AgentResult, ctx: AgentContext, out: CheckOut | list[CheckOut]) -> None:
        for o in (out if isinstance(out, list) else [out]):
            if not o:
                continue
            finding, recs = o
            result.findings.append(finding)
            for r in recs:
                r.finding_ids.append(finding.id)
                r.run_id = ctx.run_id
            result.recommendations.extend(recs)

    def money(self, x: float | None) -> str:
        return f"${(x or 0):,.0f}"

    def pct(self, x: float | None) -> str:
        return "n/a" if x is None else f"{x:.0f}%"

    def ratio(self, x: float | None) -> str:
        return "n/a" if x is None else f"{x * 100:.0f}%"
