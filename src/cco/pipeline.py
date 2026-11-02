"""End-to-end orchestration.

  1. metadata collection     connectors -> ingestion -> raw metadata store
  2. ETL / normalization     raw -> canonical objects -> causal/usage/cost facts
  3. multi-agent analysis    planner -> specialist agents -> coordinator (one merged report)
  4. rule-based scoring      deterministic points + priority bands
  5. AI reasoning layer      LLM summaries + bounded confidence adjustment, then re-score
  6. human approval          persisted queue; nothing executes without approval
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
from cco.agents import AgentContext, AnalysisReport, Coordinator, Planner, build_agents
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
from cco.storage.repository import DeltaRecommendationRepository, InMemoryRecommendationRepository, RecommendationRepository
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
        log.info("run %s: 1/8 metadata collection", run_id)
        ingestion = MetadataCollector(self.connectors, self.raw_store).run(run_id)

        log.info("run %s: 2/8 normalization + facts", run_id)
        norm = Normalizer().normalize(self.raw_store.read(run_id))
        price = float(get_path(self.cfg, "agents.delta_table_optimization.thresholds.storage_cost_per_gb_month", 0.023))
        facts = FactStore(norm.objects, storage_cost_per_gb_month=price)

        log.info("run %s: 3/8 multi-agent analysis", run_id)
        plan = self.planner.plan(facts, request)
        ctx = AgentContext(run_id=run_id, config=self.cfg, now=facts.now)
        report = self.coordinator.execute(plan, facts, ctx)

        log.info("run %s: 4/8 rule-based scoring", run_id)
        report.recommendations = self.scoring.score(report.recommendations)

        log.info("run %s: 5/8 AI reasoning", run_id)
        report = self.reasoner.enrich(report)
        report.recommendations = self.scoring.score(report.recommendations)  # re-score with adjusted confidence

        log.info("run %s: 6/8 human approval queue", run_id)
        stored = self.workflow.submit(report.recommendations)
        report.recommendations = self.scoring.rank(stored)
        self.repo.save_findings(run_id, report.findings)
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
