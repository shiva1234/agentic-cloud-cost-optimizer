"""Specialist agent contract + registry.

An agent is a deterministic analyser over the FactStore: it emits Findings (what is
wrong, with evidence) and candidate Recommendations (what to do). Scoring, LLM
reasoning and approval happen downstream, so agents stay simple and testable.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from cco.etl.facts import FactStore
from cco.models.canonical import Finding, Recommendation
from cco.utils import utcnow


@dataclass
class AgentContext:
    run_id: str
    config: dict[str, Any] = field(default_factory=dict)
    scope: dict[str, Any] = field(default_factory=dict)  # e.g. {"resources": ["prod.sales.*"]}
    now: datetime = field(default_factory=utcnow)


@dataclass
class AgentResult:
    agent: str
    findings: list[Finding] = field(default_factory=list)
    recommendations: list[Recommendation] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)


class BaseAgent(ABC):
    name: ClassVar[str] = "base"
    domain: ClassVar[str] = "general"
    description: ClassVar[str] = ""
    required_object_types: ClassVar[set[str]] = set()
    priority: ClassVar[int] = 100  # lower runs first

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = dict(config or {})

    def is_applicable(self, facts: FactStore) -> tuple[bool, str]:
        missing = self.required_object_types - facts.available_types()
        if missing:
            return False, f"missing object types: {sorted(missing)}"
        return True, "required data available"

    @abstractmethod
    def analyze(self, facts: FactStore, ctx: AgentContext) -> AgentResult: ...

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "domain": self.domain, "description": self.description,
                "requires": sorted(self.required_object_types), "priority": self.priority}


AGENT_REGISTRY: dict[str, type[BaseAgent]] = {}


def register_agent(cls: type[BaseAgent]) -> type[BaseAgent]:
    AGENT_REGISTRY[cls.name] = cls
    return cls


def build_agents(config: dict[str, Any]) -> dict[str, BaseAgent]:
    """Instantiate every registered agent whose config section is not explicitly disabled."""
    agents_cfg = config.get("agents") or {}
    out: dict[str, BaseAgent] = {}
    for name, cls in AGENT_REGISTRY.items():
        acfg = agents_cfg.get(name) or {}
        if acfg.get("enabled", True):
            out[name] = cls(acfg)
    return out
