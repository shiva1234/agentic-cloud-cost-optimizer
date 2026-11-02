"""Agent orchestration layer. Importing this package registers all specialist agents.

Roadmap (4 specialists first, then expand):
  A. delta_table_optimization   - implemented (cco.agents.delta_table_agent)
  B/C/D. add new modules here and decorate the class with @register_agent
"""
from cco.agents.base import AGENT_REGISTRY, AgentContext, AgentResult, BaseAgent, build_agents, register_agent  # noqa: F401
from cco.agents.delta_table_agent import DeltaTableOptimizationAgent  # noqa: F401
from cco.agents.planner import ExecutionPlan, Planner  # noqa: F401
from cco.agents.coordinator import AnalysisReport, Coordinator  # noqa: F401
