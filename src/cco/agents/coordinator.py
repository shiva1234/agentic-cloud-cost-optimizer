"""Coordinator: executes the plan and merges every agent's output into ONE report.

Responsibilities:
  * run agents (in parallel) with isolated failures
  * de-duplicate recommendations for the same (resource, action)
  * resolve cross-recommendation conflicts (e.g. don't OPTIMIZE a table we propose to archive)
  * order dependent actions (e.g. shorten retention before VACUUM)
  * build a summary for the UI / API / alerts
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from cco.agents.base import AgentContext, AgentResult, BaseAgent
from cco.agents.planner import ExecutionPlan
from cco.etl.facts import FactStore
from cco.models.canonical import ActionType, Finding, Recommendation, Severity
from cco.utils import utcnow

log = logging.getLogger(__name__)

# Tuning actions that make no sense once a table is proposed for archival
SUPPRESSED_BY_ARCHIVE = {
    ActionType.OPTIMIZE, ActionType.ENABLE_AUTO_OPTIMIZE, ActionType.SET_TARGET_FILE_SIZE,
    ActionType.LIQUID_CLUSTERING, ActionType.PARTITION_REDESIGN, ActionType.ARCHIVE_PARTITIONS,
    ActionType.ENABLE_DELETION_VECTORS,
}


@dataclass
class AnalysisReport:
    run_id: str
    plan: ExecutionPlan
    findings: list[Finding] = field(default_factory=list)
    recommendations: list[Recommendation] = field(default_factory=list)
    suppressed: list[dict[str, Any]] = field(default_factory=list)
    agent_stats: dict[str, dict] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    executive_summary: str | None = None
    generated_at: datetime = field(default_factory=utcnow)

    def findings_by_id(self) -> dict[str, Finding]:
        return {f.id: f for f in self.findings}


class Coordinator:
    def __init__(self, agents: dict[str, BaseAgent], max_workers: int = 4) -> None:
        self.agents = agents
        self.max_workers = max_workers

    def execute(self, plan: ExecutionPlan, facts: FactStore, ctx: AgentContext) -> AnalysisReport:
        report = AnalysisReport(run_id=ctx.run_id, plan=plan)
        results = self._run(plan, facts, ctx)
        recs: list[Recommendation] = []
        for res in results:
            report.findings.extend(res.findings)
            recs.extend(res.recommendations)
            report.agent_stats[res.agent] = res.stats
            report.errors.extend({**e, "agent": res.agent} for e in res.errors)
        merged = self._dedupe(recs)
        report.recommendations = self._resolve_conflicts(merged, report)
        report.summary = self._summarize(report)
        return report

    def _run(self, plan: ExecutionPlan, facts: FactStore, ctx: AgentContext) -> list[AgentResult]:
        def run_one(task) -> AgentResult:
            agent = self.agents[task.agent]
            try:
                actx = AgentContext(run_id=ctx.run_id, config=ctx.config, scope={**ctx.scope, **task.scope}, now=ctx.now)
                return agent.analyze(facts, actx)
            except Exception as e:  # noqa: BLE001
                log.exception("agent %s crashed", task.agent)
                return AgentResult(agent=task.agent, errors=[{"error": str(e)[:500]}])

        if not plan.tasks:
            return []
        with ThreadPoolExecutor(max_workers=max(1, min(self.max_workers, len(plan.tasks)))) as pool:
            return list(pool.map(run_one, plan.tasks))

    @staticmethod
    def _dedupe(recs: list[Recommendation]) -> list[Recommendation]:
        by_id: dict[str, Recommendation] = {}
        for r in recs:
            cur = by_id.get(r.id)
            if cur is None:
                by_id[r.id] = r
                continue
            # keep the stronger recommendation, absorb evidence from the other
            keep, other = (r, cur) if r.severity.rank > cur.severity.rank else (cur, r)
            keep.finding_ids = list(dict.fromkeys(keep.finding_ids + other.finding_ids))
            if other.rationale not in keep.rationale:
                keep.rationale = f"{keep.rationale} Also: {other.rationale}"
            keep.estimated_monthly_savings = max(keep.estimated_monthly_savings, other.estimated_monthly_savings)
            keep.confidence = min(0.95, max(keep.confidence, other.confidence) + 0.05)  # corroborated by 2+ checks
            keep.command = keep.command or other.command
            by_id[r.id] = keep
        return list(by_id.values())

    @staticmethod
    def _resolve_conflicts(recs: list[Recommendation], report: AnalysisReport) -> list[Recommendation]:
        per_resource: dict[str, list[Recommendation]] = defaultdict(list)
        for r in recs:
            per_resource[r.resource_id].append(r)
        out: list[Recommendation] = []
        for rid, items in per_resource.items():
            actions = {r.action_type: r for r in items}
            archive = actions.get(ActionType.ARCHIVE_TABLE)
            for r in items:
                if archive and r.action_type in SUPPRESSED_BY_ARCHIVE:
                    report.suppressed.append({"id": r.id, "resource_id": rid, "action_type": r.action_type.value,
                                              "reason": "table proposed for archival"})
                    archive.finding_ids = list(dict.fromkeys(archive.finding_ids + r.finding_ids))
                    continue
                out.append(r)
            vac, ret = actions.get(ActionType.VACUUM), actions.get(ActionType.SET_RETENTION)
            if vac and ret and ret.id not in vac.depends_on:
                vac.depends_on.append(ret.id)
                vac.rationale += " Run after the retention change is applied."
            purge = actions.get(ActionType.PURGE_EXPIRED_DATA)
            if archive and purge and purge.id not in archive.depends_on:
                archive.depends_on.append(purge.id)
        return out

    @staticmethod
    def _summarize(report: AnalysisReport) -> dict[str, Any]:
        recs = report.recommendations
        sev = Counter(f.severity.value for f in report.findings)
        by_resource: dict[str, float] = defaultdict(float)
        for r in recs:
            by_resource[r.resource_id] += r.estimated_monthly_savings
        return {
            "agents_run": report.plan.agent_names,
            "findings": len(report.findings),
            "recommendations": len(recs),
            "suppressed": len(report.suppressed),
            "findings_by_severity": {s.value: sev.get(s.value, 0) for s in Severity},
            "recommendations_by_action": dict(Counter(r.action_type.value for r in recs)),
            "estimated_monthly_savings": round(sum(r.estimated_monthly_savings for r in recs), 2),
            "top_resources_by_savings": sorted(
                ({"resource_id": k, "savings": round(v, 2)} for k, v in by_resource.items()), key=lambda x: -x["savings"]
            )[:10],
            "errors": len(report.errors),
        }
