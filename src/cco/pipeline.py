"""End-to-end orchestration.

  1. metadata collection     connectors -> ingestion -> raw metadata store
  2. ETL / normalization     raw -> canonical objects -> causal/usage/cost facts
  3. multi-agent analysis    planner -> specialist agents -> coordinator (one merged report)
  4. rule-based scoring      deterministic points + priority bands
  5. AI reasoning layer      LLM summaries + bounded confidence adjustment, then re-score
  6. optimization queue      cross-agent groups ranked by impact (cco.agents.queue)
     human approval          persisted queue; nothing executes without approval
  7. alerts / UI / API       notify approvers, render report
  8. (optional) actions      executes ONLY previously-approved, validated commands
"""
from __future__ import annotations

import argparse
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from cco.actions.executor import ActionExecutor, ExecutionResult
from cco.agents import AgentContext, AnalysisReport, Coordinator, Planner, build_agents, build_optimization_queue
from cco.approval.workflow import ApprovalPolicy, ApprovalWorkflow
from cco.config import get_path, load_config
from cco.connectors.base import BaseConnector
from cco.connectors.registry import build_connectors
from cco.etl.facts import FactStore
from cco.etl.normalizer import Normalizer
from cco.ingestion.collector import IngestionReport, MetadataCollector
from cco.interfaces.alerts import Notifier, build_notifier, notify_high_priority
from cco.interfaces.report import to_markdown
from cco.llm.reasoner import LLMClient, LLMReasoner
from cco.scoring.engine import ScoringEngine
from cco.storage.raw_store import DeltaRawStore, InMemoryRawStore, RawMetadataStore
from cco.storage.repository import (DeltaRecommendationRepository, InMemoryRecommendationRepository, RecommendationRepository,
                                    finding_members)
from cco.utils import utcnow

log = logging.getLogger(__name__)


def _spark() -> Any:
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def build_raw_store(cfg: dict, spark: Any = None) -> RawMetadataStore:
    s = cfg.get("storage") or {}
    if s.get("mode") == "delta":
        return DeltaRawStore(spark or _spark(), s["catalog"], s["schema"])
    return InMemoryRawStore()


def build_repository(cfg: dict, spark: Any = None) -> RecommendationRepository:
    s = cfg.get("storage") or {}
    repo: RecommendationRepository
    if s.get("mode") == "delta":
        repo = DeltaRecommendationRepository(spark or _spark(), s["catalog"], s["schema"])
    else:
        repo = InMemoryRecommendationRepository()
    repo.resurface_rejected_after_days = int(get_path(cfg, "approval.resurface_rejected_after_days", 30))
    return repo


@dataclass
class PipelineResult:
    run_id: str
    ingestion: IngestionReport
    normalization: dict[str, Any]
    facts_summary: dict[str, int]
    report: AnalysisReport
    queued_for_approval: int
    alerts_sent: int
    executions: list[ExecutionResult] = field(default_factory=list)

    def markdown(self) -> str:
        return to_markdown(self.report)


class CostOptimizationPipeline:
    def __init__(self, config: dict, *, spark: Any = None, connectors: list[BaseConnector] | None = None,
                 raw_store: RawMetadataStore | None = None, repository: RecommendationRepository | None = None,
                 llm: LLMClient | None = None, notifier: Notifier | None = None) -> None:
        self.cfg = config
        self.spark = spark
        conn_kwargs = {"databricks": {"spark": spark}} if spark is not None else {}
        self.connectors = connectors if connectors is not None else build_connectors(config, **conn_kwargs)
        self.raw_store = raw_store or build_raw_store(config, spark)
        self.repo = repository or build_repository(config, spark)
        self.agents = build_agents(config)
        self.scoring = ScoringEngine.from_config(config.get("scoring"))
        self.reasoner = LLMReasoner.from_config(config.get("llm"), client=llm)
        self.planner = Planner(self.agents, llm=self.reasoner.client if config.get("llm", {}).get("enabled") else None)
        self.coordinator = Coordinator(self.agents, max_workers=int(get_path(config, "agents.max_workers", 4)))
        self.workflow = ApprovalWorkflow(self.repo, ApprovalPolicy.from_config(config.get("approval"), config.get("scoring")))
        self.notifier = notifier or build_notifier(config.get("alerts"))

    def run(self, request: dict[str, Any] | None = None, execute_actions: bool = False, run_id: str | None = None) -> PipelineResult:
        run_id = run_id or f"{utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        started = utcnow()
        self._record_run(run_id, started, None, "running", request)
        try:
            res = self._run(run_id, request, execute_actions)
        except Exception as e:
            self._record_run(run_id, started, utcnow(), "failed", request, error=f"{type(e).__name__}: {e}"[:2000])
            raise
        self._record_run(run_id, started, utcnow(), "succeeded", request, agents=res.report.plan.agent_names)
        return res

    def _record_run(self, run_id: str, started: Any, finished: Any, status: str, request: dict | None,
                    agents: list[str] | None = None, error: str | None = None) -> None:
        try:
            self.repo.save_run({"run_id": run_id, "started_at": started, "finished_at": finished, "status": status,
                                "request": json.dumps(request, default=str) if request else None,
                                "agents_run": list(agents) if agents is not None else None, "error_message": error})
        except Exception:  # noqa: BLE001 - run bookkeeping must never mask the run itself
            log.exception("could not record run %s", run_id)

    def _catalog(self, objects: list[Any], report: AnalysisReport) -> tuple[list[dict], list[dict]]:
        """Resource + check dimension rows for everything the run's findings / recommendations reference."""
        refs = {(f.resource_type, f.resource_id) for f in report.findings}
        refs |= {m for f in report.findings for m in finding_members(f)}
        refs |= {(r.resource_type, r.resource_id) for r in report.recommendations}
        by_id: dict[str, list[Any]] = {}
        for o in objects:
            by_id.setdefault(str(o.id), []).append(o)
        resources = []
        for rtype, rid in sorted(refs):
            cands = by_id.get(rid) or []
            o = next((c for c in cands if c.object_type == rtype), cands[0] if cands else None)
            owner = (getattr(o, "owner", None) or getattr(o, "creator", None)) if o else None
            run_as = (getattr(o, "run_as", None) or o.attributes.get("run_as")) if o else None
            resources.append({"resource_type": rtype, "resource_id": rid, "resource_name": (o.name or None) if o else None,
                              "workspace_id": str(o.workspace_id) if o and o.workspace_id else None,
                              "owner": str(owner) if owner else None, "run_as": str(run_as) if run_as else None})
        checks = {f.check: {"check_name": f.check, "agent": f.agent,
                            "domain": getattr(self.agents.get(f.agent), "domain", None)} for f in report.findings}
        return resources, sorted(checks.values(), key=lambda c: c["check_name"])

    def _run(self, run_id: str, request: dict[str, Any] | None, execute_actions: bool) -> PipelineResult:
        log.info("run %s: 1/8 metadata collection", run_id)
        ingestion = MetadataCollector(self.connectors, self.raw_store).run(run_id)

        log.info("run %s: 2/8 normalization + facts", run_id)
        norm = Normalizer().normalize(self.raw_store.read(run_id))
        price = float(get_path(self.cfg, "agents.delta_table_optimization.thresholds.storage_cost_per_gb_month", 0.023))
        facts = FactStore(norm.objects, storage_cost_per_gb_month=price,
                          cost_estimation=get_path(self.cfg, "cost_estimation", None) or {})

        log.info("run %s: 3/8 multi-agent analysis", run_id)
        plan = self.planner.plan(facts, request)
        ctx = AgentContext(run_id=run_id, config=self.cfg, now=facts.now)
        report = self.coordinator.execute(plan, facts, ctx)

        log.info("run %s: 4/8 rule-based scoring", run_id)
        report.recommendations = self.scoring.score(report.recommendations)

        log.info("run %s: 5/8 AI reasoning", run_id)
        report = self.reasoner.enrich(report)
        report.recommendations = self.scoring.score(report.recommendations)  # re-score with adjusted confidence

        log.info("run %s: 6/8 optimization queue + human approval", run_id)
        queue = build_optimization_queue(report.recommendations, self.scoring.rules)
        stored = self.workflow.submit(report.recommendations)
        report.recommendations = self.scoring.rank(stored)
        queue.refresh_statuses(stored)
        report.queue = queue.items
        report.summary["queue_items"] = len(queue)
        report.summary["queue_combined_monthly_savings"] = round(sum(i.combined_monthly_savings for i in queue), 2)
        self.repo.save_findings(run_id, report.findings)
        self.repo.save_queue(run_id, queue.items)
        self.repo.save_catalog(run_id, *self._catalog(norm.objects, report))
        queued = sum(1 for r in stored if r.status.value == "pending_approval")

        log.info("run %s: 7/8 alerts", run_id)
        alerts = notify_high_priority(report, self.notifier, get_path(self.cfg, "alerts.min_priority", "P2"))

        executions: list[ExecutionResult] = []
        if execute_actions:
            log.info("run %s: 8/8 executing previously approved actions", run_id)
            executions = ActionExecutor.from_config(self.repo, self.cfg.get("actions"), self.spark).execute_approved()

        return PipelineResult(
            run_id=run_id, ingestion=ingestion,
            normalization={"counts": dict(norm.counts), "skipped": dict(norm.skipped), "errors": norm.errors},
            facts_summary=facts.summary(), report=report, queued_for_approval=queued, alerts_sent=alerts, executions=executions,
        )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Cloud cost optimizer")
    p.add_argument("--config", default=None, help="path to settings.yaml")
    p.add_argument("--sample", action="store_true", help="use synthetic sample connector + in-memory storage")
    p.add_argument("--execute-actions", action="store_true", help="run APPROVED actions (respects actions.dry_run)")
    p.add_argument("--agents", nargs="*", default=None)
    p.add_argument("--resources", nargs="*", default=None, help="glob filters, e.g. prod.sales.*")
    p.add_argument("--goal", default=None)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    overrides = None
    if a.sample:
        overrides = {"storage": {"mode": "memory"}, "llm": {"enabled": False},
                     "connectors": {k: {"enabled": False} for k in ("databricks", "aws", "azure", "gcp", "snowflake")} | {"sample": {"enabled": True}}}
    cfg = load_config(a.config, overrides)
    request = {k: v for k, v in {"agents": a.agents, "resources": a.resources, "goal": a.goal}.items() if v}
    res = CostOptimizationPipeline(cfg).run(request, execute_actions=a.execute_actions)
    print(res.markdown())
    print(json.dumps({"run_id": res.run_id, "queued_for_approval": res.queued_for_approval,
                      "ingestion_errors": res.ingestion.errors}, default=str, indent=2))


if __name__ == "__main__":
    main()
