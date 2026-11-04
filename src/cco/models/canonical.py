"""Canonical, cloud-agnostic object model.

Every connector's raw payload is normalized into these objects so that agents,
scoring and the approval workflow never depend on a specific cloud/vendor API.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar

from cco.utils import to_dt, utcnow


def stable_id(*parts: Any) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def new_id() -> str:
    return uuid.uuid4().hex[:16]


def to_jsonable(o: Any) -> Any:
    if is_dataclass(o) and not isinstance(o, type):
        return {f.name: to_jsonable(getattr(o, f.name)) for f in fields(o)}
    if isinstance(o, Enum):
        return o.value
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {str(k): to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [to_jsonable(v) for v in o]
    return o


# ----------------------------------------------------------------------------- enums
class Provider(str, Enum):
    DATABRICKS = "databricks"
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    SNOWFLAKE = "snowflake"


_SEVERITY_ORDER = ["info", "low", "medium", "high", "critical"]


class Severity(str, Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return _SEVERITY_ORDER.index(self.value)


class Risk(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ActionType(str, Enum):
    OPTIMIZE = "optimize"
    VACUUM = "vacuum"
    SET_RETENTION = "set_retention"
    ENABLE_AUTO_OPTIMIZE = "enable_auto_optimize"
    SET_TARGET_FILE_SIZE = "set_target_file_size"
    ENABLE_DELETION_VECTORS = "enable_deletion_vectors"
    PARTITION_REDESIGN = "partition_redesign"
    LIQUID_CLUSTERING = "liquid_clustering"
    ARCHIVE_PARTITIONS = "archive_partitions"
    PURGE_EXPIRED_DATA = "purge_expired_data"
    ARCHIVE_TABLE = "archive_table"
    REVIEW_UPSTREAM_JOB = "review_upstream_job"
    REVIEW = "review"
    # compute (agent B)
    RESIZE_CLUSTER = "resize_cluster"
    ENABLE_AUTOSCALING = "enable_autoscaling"
    TUNE_AUTOSCALING = "tune_autoscaling"
    SET_AUTOTERMINATION = "set_autotermination"
    CONVERT_TO_JOB_CLUSTER = "convert_to_job_cluster"
    USE_SPOT = "use_spot"
    CHANGE_NODE_TYPE = "change_node_type"
    CONSOLIDATE_CLUSTERS = "consolidate_clusters"
    SCHEDULE_START_STOP = "schedule_start_stop"
    REDUCE_STARTUP_OVERHEAD = "reduce_startup_overhead"
    CONSOLIDATE_POOLS = "consolidate_pools"
    REDUCE_POOL_IDLE = "reduce_pool_idle"
    # jobs / workflows (agent C)
    MERGE_JOBS = "merge_jobs"
    CHAIN_JOBS = "chain_jobs"
    REDUCE_RETRIES = "reduce_retries"
    ADD_INCREMENTAL = "add_incremental"
    TRIGGER_ON_CHANGE = "trigger_on_change"
    SHIFT_SCHEDULE = "shift_schedule"
    DECOMMISSION_JOB = "decommission_job"
    FIX_FAILING_WORKLOAD = "fix_failing_workload"
    CONSOLIDATE_DEPLOYMENTS = "consolidate_deployments"
    # pipelines (agent D)
    PAUSE_PIPELINE = "pause_pipeline"
    ARCHIVE_PIPELINE = "archive_pipeline"
    DEDUPLICATE_PIPELINE = "deduplicate_pipeline"
    REDUCE_FREQUENCY = "reduce_frequency"
    PRIORITIZE_PIPELINE = "prioritize_pipeline"


# Actions that retire a resource; they supersede tuning actions on the same resource
DECOMMISSION_ACTIONS = {
    ActionType.ARCHIVE_TABLE, ActionType.DECOMMISSION_JOB, ActionType.MERGE_JOBS,
    ActionType.PAUSE_PIPELINE, ActionType.ARCHIVE_PIPELINE, ActionType.DEDUPLICATE_PIPELINE,
    ActionType.CONSOLIDATE_DEPLOYMENTS,
}


class RecommendationStatus(str, Enum):
    PROPOSED = "proposed"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    FAILED = "failed"


# ----------------------------------------------------------------------------- canonical objects
@dataclass(kw_only=True)
class CanonicalObject:
    OBJECT_TYPE: ClassVar[str] = "object"

    id: str
    provider: Provider
    name: str = ""
    workspace_id: str | None = None
    account_id: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime = field(default_factory=utcnow)

    @property
    def object_type(self) -> str:
        return self.OBJECT_TYPE

    def to_dict(self) -> dict[str, Any]:
        d = to_jsonable(self)
        d["object_type"] = self.OBJECT_TYPE
        return d


@dataclass(kw_only=True)
class Workspace(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "workspace"
    url: str | None = None
    region: str | None = None
    cloud: str | None = None


@dataclass(kw_only=True)
class Account(CanonicalObject):
    """Cloud account / subscription / project / Snowflake account."""

    OBJECT_TYPE: ClassVar[str] = "account"
    kind: str = "account"


@dataclass(kw_only=True)
class Cluster(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "cluster"
    node_type: str | None = None
    driver_node_type: str | None = None
    num_workers: int | None = None
    min_workers: int | None = None
    max_workers: int | None = None
    autotermination_minutes: int | None = None
    runtime_version: str | None = None
    cluster_source: str | None = None
    state: str | None = None
    policy_id: str | None = None
    photon: bool | None = None
    owner: str | None = None
    availability: str | None = None          # ON_DEMAND | SPOT | SPOT_WITH_FALLBACK ...
    driver_pool_id: str | None = None
    worker_pool_id: str | None = None
    worker_cores: float | None = None
    worker_memory_mb: float | None = None
    deleted: bool = False

    @property
    def is_job_cluster(self) -> bool:
        return (self.cluster_source or "").upper() in ("JOB", "PIPELINE", "PIPELINE_MAINTENANCE")

    @property
    def autoscale(self) -> bool:
        return self.max_workers is not None and self.max_workers > 0


@dataclass(kw_only=True)
class Job(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "job"
    creator: str | None = None
    run_as: str | None = None
    schedule: str | None = None
    paused: bool | None = None
    task_count: int = 0


@dataclass(kw_only=True)
class Task(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "task"
    job_id: str = ""
    task_key: str = ""
    task_type: str | None = None
    compute_ref: str | None = None
    depends_on: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class Pipeline(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "pipeline"
    state: str | None = None
    continuous: bool | None = None
    serverless: bool | None = None
    photon: bool | None = None
    edition: str | None = None
    creator: str | None = None


@dataclass(kw_only=True)
class ComputePool(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "compute_pool"
    node_type: str | None = None
    min_idle: int = 0
    max_capacity: int | None = None
    idle_autotermination_minutes: int | None = None
    used_count: int = 0
    idle_count: int = 0
    availability: str | None = None


@dataclass(kw_only=True)
class RunStats(CanonicalObject):
    """Aggregated run history for a job or pipeline over a lookback window."""

    OBJECT_TYPE: ClassVar[str] = "run_stats"
    resource_id: str = ""
    resource_type: str = "job"            # job | pipeline
    window_days: int = 30
    runs: int = 0
    success_rate: float | None = None
    avg_run_s: float | None = None
    std_run_s: float | None = None
    avg_exec_s: float | None = None
    avg_queue_s: float | None = None
    avg_setup_s: float | None = None
    total_hours: float | None = None
    first_run: datetime | None = None
    last_run: datetime | None = None
    task_runs: int = 0
    retries: int = 0
    retried_task_runs: int = 0
    retried_then_succeeded: int = 0
    full_refreshes: int = 0
    trigger_types: dict[str, int] = field(default_factory=dict)
    start_hour_histogram: dict[int, int] = field(default_factory=dict)
    compute_ids: list[str] = field(default_factory=list)

    @property
    def runs_per_day(self) -> float:
        return self.runs / max(1, self.window_days)


@dataclass(kw_only=True)
class Lineage(CanonicalObject):
    """Observed source/target datasets of a workload (job, pipeline)."""

    OBJECT_TYPE: ClassVar[str] = "lineage"
    resource_id: str = ""
    resource_type: str = "job"
    sources: list[str] = field(default_factory=list)
    targets: list[str] = field(default_factory=list)
    last_event: datetime | None = None


@dataclass
class PartitionStats:
    values: dict[str, Any]
    num_files: int = 0
    size_bytes: int = 0
    small_files: int | None = None
    last_modified: datetime | None = None


@dataclass
class TableOperation:
    version: int | None
    timestamp: datetime | None
    operation: str
    metrics: dict[str, float] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class Table(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "table"
    catalog: str | None = None
    schema: str | None = None
    table_name: str | None = None
    table_type: str | None = None
    table_format: str | None = None
    location: str | None = None
    owner: str | None = None
    partition_columns: list[str] = field(default_factory=list)
    clustering_columns: list[str] = field(default_factory=list)
    num_columns: int | None = None
    num_files: int | None = None
    size_bytes: int | None = None
    small_files: int | None = None
    created_at: datetime | None = None
    last_modified: datetime | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    partitions: list[PartitionStats] = field(default_factory=list)
    history: list[TableOperation] = field(default_factory=list)

    @property
    def is_delta(self) -> bool:
        return (self.table_format or "").upper() == "DELTA"


@dataclass(kw_only=True)
class StorageObject(CanonicalObject):
    """Bucket / container / external location / table storage footprint."""

    OBJECT_TYPE: ClassVar[str] = "storage_object"
    location: str | None = None
    size_bytes: int | None = None
    object_count: int | None = None
    storage_class: str | None = None
    last_modified: datetime | None = None
    linked_resource_id: str | None = None


@dataclass(kw_only=True)
class UsageMetric(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "usage_metric"
    resource_id: str = ""
    resource_type: str = ""
    metric_name: str = ""
    value: float = 0.0
    unit: str | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None


@dataclass(kw_only=True)
class CostMetric(CanonicalObject):
    OBJECT_TYPE: ClassVar[str] = "cost_metric"
    resource_id: str = ""
    resource_type: str = ""
    amount: float = 0.0
    currency: str = "USD"
    sku: str | None = None
    usage_quantity: float | None = None
    usage_unit: str | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None

    @property
    def period_days(self) -> float:
        if self.period_start and self.period_end:
            return max(1.0, (self.period_end - self.period_start).total_seconds() / 86400.0)
        return 30.0

    @property
    def monthly_amount(self) -> float:
        return self.amount * 30.0 / self.period_days


# ----------------------------------------------------------------------------- analysis outputs
@dataclass(kw_only=True)
class Finding:
    agent: str
    check: str
    resource_id: str
    resource_type: str
    title: str
    detail: str
    severity: Severity = Severity.MEDIUM
    metrics: dict[str, Any] = field(default_factory=dict)
    # keys of `metrics` that describe the resource itself (stored once per resource and run, not per finding)
    resource_metric_keys: list[str] = field(default_factory=list)
    id: str = ""
    created_at: datetime = field(default_factory=utcnow)

    def __post_init__(self) -> None:
        self.severity = Severity(self.severity)
        if not self.id:
            # deterministic: the same check on the same resource keeps one id across runs (trend / compare runs)
            self.id = stable_id("finding", self.agent, self.check, self.resource_type, self.resource_id)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass(kw_only=True)
class Recommendation:
    resource_id: str
    resource_type: str
    action_type: ActionType
    title: str
    rationale: str
    agent: str
    command: str | None = None
    finding_ids: list[str] = field(default_factory=list)
    severity: Severity = Severity.MEDIUM
    risk: Risk = Risk.LOW
    reversible: bool = True
    confidence: float = 0.5
    estimated_monthly_savings: float = 0.0
    savings_basis: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    related_resources: list[str] = field(default_factory=list)
    group_id: str | None = None
    queue_rank: int | None = None
    superseded_by: str | None = None
    score: float = 0.0
    priority: str = "P4"
    score_breakdown: dict[str, float] = field(default_factory=dict)
    llm_summary: str | None = None
    llm_notes: list[str] = field(default_factory=list)
    status: RecommendationStatus = RecommendationStatus.PROPOSED
    approvals: list[dict[str, Any]] = field(default_factory=list)
    rejection: dict[str, Any] | None = None
    execution_result: dict[str, Any] | None = None
    run_id: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    id: str = ""

    def __post_init__(self) -> None:
        self.action_type = ActionType(self.action_type)
        self.severity = Severity(self.severity)
        self.risk = Risk(self.risk)
        self.status = RecommendationStatus(self.status)
        if not self.id:
            # deterministic: the same issue on the same resource keeps one id across runs
            self.id = stable_id(self.resource_id, self.action_type.value)

    @property
    def executable(self) -> bool:
        return bool(self.command)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Recommendation":
        names = {f.name for f in fields(cls)}
        data = {k: v for k, v in d.items() if k in names}
        for k in ("created_at", "updated_at"):
            if k in data:
                data[k] = to_dt(data[k]) or utcnow()
        return cls(**data)
