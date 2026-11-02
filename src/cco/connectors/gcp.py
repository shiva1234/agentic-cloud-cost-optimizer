"""GCP connector: Cloud Billing export in BigQuery (costs) + Cloud Storage buckets (storage)."""
from __future__ import annotations

from typing import Any

from cco.connectors.base import BaseConnector, Collected, ConnectorError
from cco.models.canonical import Provider


class GCPConnector(BaseConnector):
    name = "gcp"
    provider = Provider.GCP

    def __init__(self, config: dict | None = None, bq_client: Any = None, gcs_client: Any = None) -> None:
        super().__init__(config)
        self._bq = bq_client
        self._gcs = gcs_client

    def supported_entities(self) -> set[str]:
        return {"accounts", "costs", "storage_stats"}

    @property
    def bq(self) -> Any:
        if self._bq is None:
            bigquery = self._require("google.cloud.bigquery", "gcp")
            self._bq = bigquery.Client(project=self.config.get("project_id"))
        return self._bq

    @property
    def gcs(self) -> Any:
        if self._gcs is None:
            storage = self._require("google.cloud.storage", "gcp")
            self._gcs = storage.Client(project=self.config.get("project_id"))
        return self._gcs

    def collect_accounts(self) -> Collected:
        pid = self.config.get("project_id")
        if pid:
            yield pid, {"project_id": pid, "kind": "project"}

    def collect_costs(self) -> Collected:
        table = self.config.get("billing_export_table")
        if not table:
            raise ConnectorError("gcp.billing_export_table is required for costs")
        days = int(self.config.get("lookback_days", 30))
        sql = f"""
            SELECT service.description AS service, sku.description AS sku, project.id AS project_id,
                   currency, SUM(cost) AS amount, SUM(usage.amount) AS usage_quantity,
                   ANY_VALUE(usage.unit) AS usage_unit,
                   MIN(usage_start_time) AS period_start, MAX(usage_end_time) AS period_end
            FROM `{table}`
            WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            GROUP BY service, sku, project_id, currency"""
        for row in self.bq.query(sql).result():
            d = dict(row.items())
            yield f"{d['project_id']}|{d['service']}|{d['sku']}", d

    def collect_storage_stats(self) -> Collected:
        for b in self.gcs.list_buckets():
            yield b.name, {
                "bucket": b.name,
                "location": b.location,
                "storage_class": b.storage_class,
                "created": b.time_created,
                "lifecycle_rules": len(list(b.lifecycle_rules or [])),
            }
