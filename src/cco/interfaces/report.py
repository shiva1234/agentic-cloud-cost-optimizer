"""Report rendering for UI / notebooks / API."""
from __future__ import annotations

from typing import Any

from cco.agents.coordinator import AnalysisReport


def recommendations_table(report: AnalysisReport) -> list[dict[str, Any]]:
    return [
        {
            "priority": r.priority, "score": r.score, "resource_id": r.resource_id, "action": r.action_type.value,
            "title": r.title, "severity": r.severity.value, "risk": r.risk.value, "confidence": r.confidence,
            "est_monthly_savings": r.estimated_monthly_savings, "status": r.status.value, "command": r.command,
            "summary": r.llm_summary, "id": r.id, "agent": r.agent, "resource_type": r.resource_type,
            "queue_rank": r.queue_rank, "group_id": r.group_id, "related_resources": ", ".join(r.related_resources[:10]),
        }
        for r in report.recommendations
    ]


def queue_table(report: AnalysisReport) -> list[dict[str, Any]]:
    return [
        {"rank": q.rank, "priority": q.priority, "title": q.title, "combined_monthly_savings": q.combined_monthly_savings,
         "impact": q.impact, "group_score": q.group_score, "confidence": q.confidence, "max_risk": q.max_risk,
         "agents": ", ".join(q.agents), "actions": ", ".join(q.actions), "resources": ", ".join(q.resources[:10]),
         "n_recommendations": len(q.recommendation_ids), "statuses": ", ".join(f"{k}={v}" for k, v in q.statuses.items()),
         "group_id": q.group_id}
        for q in report.queue
    ]


def findings_table(report: AnalysisReport) -> list[dict[str, Any]]:
    return [
        {"severity": f.severity.value, "agent": f.agent, "resource_type": f.resource_type, "resource_id": f.resource_id,
         "check": f.check, "title": f.title, "detail": f.detail, "id": f.id}
        for f in sorted(report.findings, key=lambda f: -f.severity.rank)
    ]


def to_markdown(report: AnalysisReport, top_n: int = 20) -> str:
    s = report.summary
    lines = [
        f"# Cloud Cost Optimization Report - run `{report.run_id}`",
        "",
        report.executive_summary or "",
        "",
        f"* Agents run: {', '.join(s.get('agents_run', [])) or 'none'}",
        f"* Findings: {s.get('findings', 0)} | Recommendations: {s.get('recommendations', 0)} | Suppressed: {s.get('suppressed', 0)}",
        f"* Estimated monthly savings: ${s.get('estimated_monthly_savings', 0):,.2f} (sum of recommendations); "
        f"overlap-adjusted queue total: ${s.get('queue_combined_monthly_savings', 0):,.2f}",
        f"* Recommendations by agent: {s.get('recommendations_by_agent', {})}",
        "",
    ]
    if report.queue:
        lines += ["## Optimization Queue", "", "| # | Priority | Item | Agents | Savings/mo | Impact | Risk |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for q in report.queue[:top_n]:
            lines.append(f"| {q.rank} | {q.priority} | {q.title} | {', '.join(q.agents)} | ${q.combined_monthly_savings:,.2f} | {q.impact:,.2f} | {q.max_risk} |")
        lines.append("")
    lines += ["## Recommendations", "", "| Priority | Score | Resource | Action | Savings/mo | Risk | Status |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for r in report.recommendations[:top_n]:
        lines.append(f"| {r.priority} | {r.score} | {r.resource_id} | {r.action_type.value} | ${r.estimated_monthly_savings:,.2f} | {r.risk.value} | {r.status.value} |")
    return "\n".join(lines)


def to_spark_dataframes(report: AnalysisReport, spark: Any) -> dict[str, Any]:
    """Convenience for notebooks: display(dfs['recommendations'])."""
    import pandas as pd

    return {
        "recommendations": spark.createDataFrame(pd.DataFrame(recommendations_table(report))) if report.recommendations else None,
        "findings": spark.createDataFrame(pd.DataFrame(findings_table(report))) if report.findings else None,
        "queue": spark.createDataFrame(pd.DataFrame(queue_table(report))) if report.queue else None,
    }
