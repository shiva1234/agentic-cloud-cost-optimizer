"""Optional action automation.

Safety layers (all must pass):
  1. status must be APPROVED (human decision)
  2. action type must be in the configured allow-list
  3. command must match a strict per-action regex (no multi-statement / arbitrary SQL)
  4. dependencies (depends_on) must already be EXECUTED
  5. dry_run=True by default - commands are only logged
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from cco.models.canonical import ActionType, Recommendation, RecommendationStatus
from cco.storage.repository import RecommendationRepository
from cco.utils import utcnow

log = logging.getLogger(__name__)

_FQN = r"`[^`;]+`\.`[^`;]+`\.`[^`;]+`"
_TBLPROPS = rf"ALTER TABLE {_FQN} SET TBLPROPERTIES \((\s*'delta\.[A-Za-z.]+' = '[A-Za-z0-9 ]+'\s*,?)+\)"
COMMAND_PATTERNS: dict[ActionType, re.Pattern] = {
    ActionType.OPTIMIZE: re.compile(rf"^OPTIMIZE {_FQN}( WHERE [^;]+)?( ZORDER BY \([^;]+\))?$"),
    ActionType.VACUUM: re.compile(rf"^VACUUM {_FQN}( RETAIN \d+ HOURS)?$"),
    ActionType.SET_RETENTION: re.compile(rf"^{_TBLPROPS}$"),
    ActionType.ENABLE_AUTO_OPTIMIZE: re.compile(rf"^{_TBLPROPS}$"),
    ActionType.SET_TARGET_FILE_SIZE: re.compile(rf"^{_TBLPROPS}$"),
    ActionType.ENABLE_DELETION_VECTORS: re.compile(rf"^{_TBLPROPS}$"),
}


@dataclass
class ExecutionResult:
    rec_id: str
    status: str  # executed | dry_run | blocked | failed
    command: str | None
    message: str = ""


class ActionExecutor:
    def __init__(self, repo: RecommendationRepository, sql_runner: Callable[[str], Any] | None = None,
                 dry_run: bool = True, allowed_actions: list[str] | None = None, actor: str = "cco-executor") -> None:
        self.repo = repo
        self.sql_runner = sql_runner
        self.dry_run = dry_run
        self.allowed = {ActionType(a) for a in (allowed_actions or [a.value for a in COMMAND_PATTERNS])}
        self.actor = actor

    @classmethod
    def from_config(cls, repo: RecommendationRepository, cfg: dict | None, spark: Any = None) -> "ActionExecutor":
        cfg = cfg or {}
        runner = spark.sql if spark is not None else None
        return cls(repo, runner, bool(cfg.get("dry_run", True)), cfg.get("allowed"))

    def validate(self, rec: Recommendation) -> str | None:
        """Return a blocking reason, or None if the recommendation may run."""
        if rec.status != RecommendationStatus.APPROVED:
            return f"status is {rec.status.value}, not approved"
        if rec.action_type not in self.allowed:
            return f"action '{rec.action_type.value}' not in allow-list (manual action)"
        if not rec.command:
            return "no executable command (manual action)"
        pattern = COMMAND_PATTERNS.get(rec.action_type)
        if pattern is None or not pattern.match(rec.command.strip()):
            return "command failed safety validation"
        for dep in rec.depends_on:
            d = self.repo.get(dep)
            if d is None or d.status != RecommendationStatus.EXECUTED:
                return f"dependency {dep} not executed yet"
        return None

    def execute_approved(self) -> list[ExecutionResult]:
        results = []
        for rec in sorted(self.repo.list(RecommendationStatus.APPROVED), key=lambda r: len(r.depends_on)):
            results.append(self.execute(rec))
        return results

    def execute(self, rec: Recommendation) -> ExecutionResult:
        reason = self.validate(rec)
        if reason:
            self.repo.audit(rec.id, "execution_blocked", self.actor, {"reason": reason})
            return ExecutionResult(rec.id, "blocked", rec.command, reason)
        if self.dry_run or self.sql_runner is None:
            self.repo.audit(rec.id, "dry_run", self.actor, {"command": rec.command})
            log.info("[DRY RUN] %s", rec.command)
            return ExecutionResult(rec.id, "dry_run", rec.command, "dry run - not executed")
        started = utcnow()
        try:
            self.sql_runner(rec.command)
            rec.status = RecommendationStatus.EXECUTED
            rec.execution_result = {"ok": True, "started_at": started.isoformat(), "finished_at": utcnow().isoformat()}
            msg = "executed"
        except Exception as e:  # noqa: BLE001
            rec.status = RecommendationStatus.FAILED
            rec.execution_result = {"ok": False, "error": str(e)[:1000], "started_at": started.isoformat()}
            msg = str(e)[:300]
        self.repo.update(rec)
        self.repo.audit(rec.id, rec.status.value, self.actor, rec.execution_result)
        return ExecutionResult(rec.id, rec.status.value, rec.command, msg)
