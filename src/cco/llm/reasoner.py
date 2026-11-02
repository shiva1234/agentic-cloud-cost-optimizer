"""AI reasoning layer - runs AFTER rule-based scoring.

Guardrails:
  * the LLM only sees findings/recommendations produced by deterministic rules
  * it may summarise, flag risks and nudge confidence by at most +/- max_confidence_delta
  * it can NOT add recommendations, change actions or edit SQL commands
  * any failure falls back to deterministic template summaries
"""
from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Any

from cco.agents.coordinator import AnalysisReport
from cco.models.canonical import Recommendation

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior FinOps reviewer for a Databricks lakehouse.
You receive recommendations produced by deterministic rules, with their evidence.
For EACH item return a short plain-English summary for an approver, concrete risks or prerequisites,
a confidence_delta between -{d} and {d} (negative if evidence is weak or risky), and needs_human_review (bool).
Never propose new actions and never modify commands.
Reply with ONLY JSON: {{"executive_summary": str, "items": [{{"id": str, "summary": str, "risks": [str],
"confidence_delta": float, "needs_human_review": bool}}]}}"""


def extract_json(text: str | None) -> Any:
    if not text:
        return None
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return None
    return None


class LLMClient(ABC):
    @abstractmethod
    def complete(self, system: str, user: str) -> str: ...


class NullLLM(LLMClient):
    def complete(self, system: str, user: str) -> str:
        return ""


class DatabricksServingLLM(LLMClient):
    """Databricks Model Serving / Foundation Model APIs chat endpoint."""

    def __init__(self, endpoint: str, max_tokens: int = 2500, temperature: float = 0.1, workspace_client: Any = None) -> None:
        self.endpoint = endpoint
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._w = workspace_client

    def complete(self, system: str, user: str) -> str:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        w = self._w or WorkspaceClient()
        resp = w.serving_endpoints.query(
            name=self.endpoint,
            messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system), ChatMessage(role=ChatMessageRole.USER, content=user)],
            max_tokens=self.max_tokens,
            temperature=self.temperature,
        )
        return resp.choices[0].message.content


def build_llm(cfg: dict | None) -> LLMClient:
    cfg = cfg or {}
    if not cfg.get("enabled"):
        return NullLLM()
    if cfg.get("provider", "databricks") == "databricks":
        return DatabricksServingLLM(cfg.get("endpoint", "databricks-meta-llama-3-3-70b-instruct"),
                                    int(cfg.get("max_tokens", 2500)), float(cfg.get("temperature", 0.1)))
    raise ValueError(f"unsupported llm provider {cfg.get('provider')}")


class LLMReasoner:
    def __init__(self, client: LLMClient | None = None, max_items: int = 25, max_confidence_delta: float = 0.2) -> None:
        self.client = client or NullLLM()
        self.max_items = max_items
        self.max_delta = max_confidence_delta

    @classmethod
    def from_config(cls, cfg: dict | None, client: LLMClient | None = None) -> "LLMReasoner":
        cfg = cfg or {}
        return cls(client or build_llm(cfg), int(cfg.get("max_items", 25)), float(cfg.get("max_confidence_delta", 0.2)))

    # ------------------------------------------------------------------ public
    def enrich(self, report: AnalysisReport) -> AnalysisReport:
        recs = report.recommendations[: self.max_items]
        for r in report.recommendations:
            r.llm_summary = self._template(r, report)
        report.executive_summary = self._template_exec(report)
        if not recs or isinstance(self.client, NullLLM):
            return report
        try:
            raw = self.client.complete(SYSTEM_PROMPT.format(d=self.max_delta), json.dumps(self._payload(recs, report), default=str))
            data = extract_json(raw)
            if not isinstance(data, dict):
                raise ValueError("LLM did not return a JSON object")
            self._apply(data, recs)
            if data.get("executive_summary"):
                report.executive_summary = str(data["executive_summary"])[:4000]
        except Exception as e:  # noqa: BLE001
            log.warning("LLM reasoning failed, keeping template summaries: %s", e)
            report.errors.append({"agent": "llm_reasoner", "error": str(e)[:300]})
        return report

    # ------------------------------------------------------------------ internals
    def _payload(self, recs: list[Recommendation], report: AnalysisReport) -> dict[str, Any]:
        fmap = report.findings_by_id()
        return {
            "items": [
                {
                    "id": r.id, "resource": r.resource_id, "action": r.action_type.value, "title": r.title,
                    "rationale": r.rationale, "command": r.command, "severity": r.severity.value, "risk": r.risk.value,
                    "reversible": r.reversible, "rule_confidence": r.confidence, "rule_score": r.score,
                    "estimated_monthly_savings": r.estimated_monthly_savings, "savings_basis": r.savings_basis,
                    "evidence": [{"check": fmap[f].check, "detail": fmap[f].detail, "metrics": fmap[f].metrics}
                                 for f in r.finding_ids if f in fmap],
                }
                for r in recs
            ],
            "summary": report.summary,
        }

    def _apply(self, data: dict[str, Any], recs: list[Recommendation]) -> None:
        by_id = {r.id: r for r in recs}
        for item in data.get("items") or []:
            r = by_id.get(str(item.get("id")))
            if r is None:
                continue  # ignore anything the model invented
            if item.get("summary"):
                r.llm_summary = str(item["summary"])[:1500]
            r.llm_notes = [str(x)[:300] for x in (item.get("risks") or [])][:5]
            try:
                delta = max(-self.max_delta, min(self.max_delta, float(item.get("confidence_delta") or 0)))
            except (TypeError, ValueError):
                delta = 0.0
            r.score_breakdown["llm_confidence_delta"] = round(delta, 3)
            r.confidence = round(max(0.05, min(0.99, r.confidence + delta)), 3)
            if item.get("needs_human_review"):
                r.llm_notes.append("LLM flagged for careful human review")

    @staticmethod
    def _template(r: Recommendation, report: AnalysisReport) -> str:
        fmap = report.findings_by_id()
        evidence = "; ".join(fmap[f].detail for f in r.finding_ids if f in fmap)
        savings = f" Est. savings ${r.estimated_monthly_savings:,.0f}/month." if r.estimated_monthly_savings else ""
        return f"{r.title}: {evidence}{savings} Risk: {r.risk.value}{'' if r.reversible else ', not reversible'}."

    @staticmethod
    def _template_exec(report: AnalysisReport) -> str:
        s = report.summary
        top = ", ".join(x["resource_id"] for x in s.get("top_resources_by_savings", [])[:3]) or "n/a"
        return (f"{s.get('findings', 0)} findings and {s.get('recommendations', 0)} recommendations; estimated "
                f"${s.get('estimated_monthly_savings', 0):,.0f}/month. Top resources: {top}.")
