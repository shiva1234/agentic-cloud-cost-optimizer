"""Metadata ingestion layer: runs every enabled connector for every requested entity
and lands raw payloads in the raw metadata store. Failures are isolated per
(connector, entity) so one broken API never blocks the rest of the run.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from cco.connectors.base import ENTITIES, BaseConnector, RawRecord
from cco.storage.raw_store import RawMetadataStore
from cco.utils import utcnow

log = logging.getLogger(__name__)


@dataclass
class IngestionReport:
    run_id: str
    counts: dict[str, int] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    started_at: datetime = field(default_factory=utcnow)
    finished_at: datetime | None = None

    @property
    def total_records(self) -> int:
        return sum(self.counts.values())


class MetadataCollector:
    def __init__(
        self,
        connectors: Iterable[BaseConnector],
        raw_store: RawMetadataStore,
        entities: Iterable[str] | None = None,
        batch_size: int = 1000,
    ) -> None:
        self.connectors = list(connectors)
        self.raw_store = raw_store
        self.entities = list(entities) if entities else list(ENTITIES)
        self.batch_size = batch_size

    def run(self, run_id: str) -> IngestionReport:
        report = IngestionReport(run_id=run_id)
        for conn in self.connectors:
            try:
                conn.test_connection()
            except Exception as e:  # noqa: BLE001
                log.error("connector %s unavailable: %s", conn.name, e)
                report.errors.append({"connector": conn.name, "entity": "*", "error": str(e)[:500]})
                continue
            supported = conn.supported_entities()
            for entity in self.entities:
                if entity not in supported:
                    continue
                key = f"{conn.name}.{entity}"
                buf: list[RawRecord] = []
                n = 0
                try:
                    for rec in conn.collect(entity):
                        buf.append(rec)
                        if len(buf) >= self.batch_size:
                            self.raw_store.write(run_id, buf)
                            n += len(buf)
                            buf = []
                except Exception as e:  # noqa: BLE001
                    log.exception("collection failed for %s", key)
                    report.errors.append({"connector": conn.name, "entity": entity, "error": str(e)[:500]})
                finally:
                    if buf:
                        self.raw_store.write(run_id, buf)
                        n += len(buf)
                report.counts[key] = n
                log.info("collected %s records for %s", n, key)
        report.finished_at = utcnow()
        return report
