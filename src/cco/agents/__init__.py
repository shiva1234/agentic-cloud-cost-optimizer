"""Agent orchestration layer. Importing this package registers all specialist agents.

Specialists (add more: new module + @register_agent + import here):
  A. delta_table_optimization   cco.agents.delta_table_agent
  B. compute_utilization        cco.agents.compute_utilization_agent
  C. job_redundancy             cco.agents.job_redundancy_agent
  D. pipeline_optimization      cco.agents.pipeline_agent
The Coordinator merges their output; cco.agents.queue builds the single Optimization Queue.
"""
from cco.agents.base import AGENT_REGISTRY, AgentContext, AgentResult, BaseAgent, build_agents, register_agent  # noqa: F401
from cco.agents.delta_table_agent import DeltaTableOptimizationAgent  # noqa: F401
from cco.agents.compute_utilization_agent import ComputeUtilizationAgent  # noqa: F401
from cco.agents.job_redundancy_agent import JobRedundancyAgent  # noqa: F401
from cco.agents.pipeline_agent import PipelineOptimizationAgent  # noqa: F401
from cco.agents.planner import ExecutionPlan, Planner  # noqa: F401
from cco.agents.coordinator import AnalysisReport, Coordinator  # noqa: F401
from cco.agents.queue import OptimizationQueue, QueueItem, build_optimization_queue  # noqa: F401
