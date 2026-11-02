"""Connector layer contract.

A connector only knows how to talk to ONE source system and yields raw payloads
(`RawRecord`). It must not interpret the data - that is the ETL layer's job.
To add a source, subclass `BaseConnector`, implement `collect_<entity>()` methods
that yield `(source_id, payload_dict)` tuples, and register it in `registry.py`.
"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Iterator

from cco.models.canonical import Provider
from cco.utils import utcnow

# Metadata entities understood by the ingestion layer
ENTITIES: tuple[str, ...] = (
    "workspaces",
    "accounts",
    "clusters",
    "jobs",
    "tasks",
    "pipelines",
    "schemas",
    "tables",
    "storage_stats",
    "compute_metrics",
    "access_logs",
    "costs",
)

Collected = Iterator[tuple[str, dict[str, Any]]]


class ConnectorError(RuntimeError):
    pass


@dataclass
class RawRecord:
    provider: str
    entity: str
    source_id: str
    payload: dict[str, Any]
    connector: str
    collected_at: datetime = field(default_factory=utcnow)

    def to_row(self, run_id: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "provider": self.provider,
            "connector": self.connector,
            "entity": self.entity,
            "source_id": self.source_id,
            "payload": json.dumps(self.payload, default=str),
            "collected_at": self.collected_at,
        }


class BaseConnector(ABC):
    name: ClassVar[str] = "base"
    provider: ClassVar[Provider]

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = dict(config or {})

    @abstractmethod
    def supported_entities(self) -> set[str]:
        """Subset of ENTITIES this connector can collect."""

    def test_connection(self) -> bool:
        return True

    def collect(self, entity: str, **kwargs: Any) -> Iterator[RawRecord]:
        if entity not in self.supported_entities():
            return
        method = getattr(self, f"collect_{entity}", None)
        if method is None:
            raise ConnectorError(f"{self.name} declares '{entity}' but has no collect_{entity}()")
        for source_id, payload in method(**kwargs):
            yield RawRecord(
                provider=self.provider.value,
                entity=entity,
                source_id=str(source_id),
                payload=payload,
                connector=self.name,
            )

    @staticmethod
    def _require(module: str, extra: str) -> Any:
        try:
            return __import__(module, fromlist=["_"])
        except ImportError as e:  # pragma: no cover
            raise ConnectorError(f"Missing dependency '{module}'. pip install 'cloud-cost-optimizer[{extra}]'") from e
