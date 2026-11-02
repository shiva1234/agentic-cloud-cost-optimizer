"""Raw metadata store: immutable landing zone for connector payloads (one row per record per run)."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any

from cco.connectors.base import RawRecord
from cco.utils import quote_fqn, sql_literal, to_dt

RAW_SCHEMA = "run_id STRING, provider STRING, connector STRING, entity STRING, source_id STRING, payload STRING, collected_at TIMESTAMP"


class RawMetadataStore(ABC):
    @abstractmethod
    def write(self, run_id: str, records: list[RawRecord]) -> None: ...

    @abstractmethod
    def read(self, run_id: str, provider: str | None = None, entity: str | None = None) -> list[RawRecord]: ...


class InMemoryRawStore(RawMetadataStore):
    def __init__(self) -> None:
        self._data: dict[str, list[RawRecord]] = defaultdict(list)

    def write(self, run_id: str, records: list[RawRecord]) -> None:
        self._data[run_id].extend(records)

    def read(self, run_id: str, provider: str | None = None, entity: str | None = None) -> list[RawRecord]:
        return [r for r in self._data.get(run_id, []) if (provider is None or r.provider == provider) and (entity is None or r.entity == entity)]


class DeltaRawStore(RawMetadataStore):
    """Appends to <catalog>.<schema>.raw_metadata (partition-friendly by run_id/entity)."""

    def __init__(self, spark: Any, catalog: str, schema: str, table: str = "raw_metadata") -> None:
        self.spark = spark
        self.fqn = f"{catalog}.{schema}.{table}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_fqn(catalog + '.' + schema)}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {quote_fqn(self.fqn)} ({RAW_SCHEMA}) CLUSTER BY (run_id, entity)")

    def write(self, run_id: str, records: list[RawRecord]) -> None:
        if not records:
            return
        rows = [r.to_row(run_id) for r in records]
        self.spark.createDataFrame(rows, schema=RAW_SCHEMA).write.mode("append").saveAsTable(self.fqn)

    def read(self, run_id: str, provider: str | None = None, entity: str | None = None) -> list[RawRecord]:
        where = [f"run_id = {sql_literal(run_id)}"]
        if provider:
            where.append(f"provider = {sql_literal(provider)}")
        if entity:
            where.append(f"entity = {sql_literal(entity)}")
        df = self.spark.sql(f"SELECT * FROM {quote_fqn(self.fqn)} WHERE {' AND '.join(where)}")
        return [
            RawRecord(provider=r.provider, entity=r.entity, source_id=r.source_id, payload=json.loads(r.payload),
                      connector=r.connector, collected_at=to_dt(r.collected_at))
            for r in df.collect()
        ]
