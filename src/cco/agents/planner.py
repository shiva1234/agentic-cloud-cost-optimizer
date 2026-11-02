"""Planner: decides WHICH agents run, in what order, and why.

Rule-based by default (enabled + requested + data available). Optionally an LLM can
map a natural-language goal ("why is storage growing in prod?") onto agent names;
the LLM can only pick from the registered catalog, never invent agents.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from cco.agents.base import BaseAgent
from cco.etl.facts import FactStore

log = logging.getLogger(__name__)


@dataclass
class PlannedTask:
    agent: str
    reason: str
    priority: int
    scope: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionPlan:
    tasks: list[PlannedTask] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)
    goal: str | None = None

    @property
    def agent_names(self) -> list[str]:
        return [t.agent for t in self.tasks]


class Planner:
    def __init__(self, agents: dict[str, BaseAgent], llm: Any = None) -> None:
        self.agents = agents
        self.llm = llm

    def plan(self, facts: FactStore, request: dict[str, Any] | None = None) -> ExecutionPlan:
        """request: {agents?: [...], domains?: [...], resources?: [glob...], goal?: str}"""
        request = dict(request or {})
        plan = ExecutionPlan(goal=request.get("goal"))
        wanted = set(request.get("agents") or [])
        if not wanted and request.get("goal") and self.llm is not None:
            wanted = self._agents_for_goal(request["goal"])
        domains = set(request.get("domains") or [])
        scope = {"resources": request["resources"]} if request.get("resources") else {}

        for name, agent in sorted(self.agents.items(), key=lambda kv: kv[1].priority):
            if wanted and name not in wanted:
                plan.skipped.append({"agent": name, "reason": "not requested"})
                continue
            if domains and agent.domain not in domains:
                plan.skipped.append({"agent": name, "reason": f"domain '{agent.domain}' not requested"})
                continue
            ok, why = agent.is_applicable(facts)
            if not ok:
                plan.skipped.append({"agent": name, "reason": why})
                continue
            plan.tasks.append(PlannedTask(agent=name, reason=why, priority=agent.priority, scope=scope))
        return plan

    def _agents_for_goal(self, goal: str) -> set[str]:
        catalog = [a.describe() for a in self.agents.values()]
        system = "You route cost-optimization requests to specialist agents. Reply with JSON: {\"agents\": [names]} using only names from the catalog."
        user = json.dumps({"goal": goal, "catalog": catalog})
        try:
            from cco.llm.reasoner import extract_json

            data = extract_json(self.llm.complete(system, user)) or {}
            picked = {a for a in data.get("agents", []) if a in self.agents}
            return picked or set(self.agents)
        except Exception as e:  # noqa: BLE001
            log.warning("LLM planning failed (%s); running all applicable agents", e)
            return set(self.agents)
