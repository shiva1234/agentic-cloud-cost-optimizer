"""Azure connector: Cost Management (costs) + Storage accounts / Azure Monitor (storage)."""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from cco.connectors.base import BaseConnector, Collected, ConnectorError
from cco.models.canonical import Provider
from cco.utils import utcnow


class AzureConnector(BaseConnector):
    name = "azure"
    provider = Provider.AZURE

    def __init__(self, config: dict | None = None, credential: Any = None) -> None:
        super().__init__(config)
        self._credential = credential

    @property
    def subscription_id(self) -> str:
        sub = self.config.get("subscription_id")
        if not sub:
            raise ConnectorError("azure.subscription_id is required")
        return sub

    @property
    def credential(self) -> Any:
        if self._credential is None:
            identity = self._require("azure.identity", "azure")
            self._credential = identity.DefaultAzureCredential()
        return self._credential

    def supported_entities(self) -> set[str]:
        return {"accounts", "costs", "storage_stats"}

    def collect_accounts(self) -> Collected:
        yield self.subscription_id, {"subscription_id": self.subscription_id, "kind": "subscription"}

    def collect_costs(self) -> Collected:
        cm = self._require("azure.mgmt.costmanagement", "azure")
        models = self._require("azure.mgmt.costmanagement.models", "azure")
        client = cm.CostManagementClient(self.credential)
        end = utcnow()
        start = end - timedelta(days=int(self.config.get("lookback_days", 30)))
        query = models.QueryDefinition(
            type="ActualCost",
            timeframe="Custom",
            time_period=models.QueryTimePeriod(from_property=start, to=end),
            dataset=models.QueryDataset(
                granularity="None",
                aggregation={"totalCost": models.QueryAggregation(name="Cost", function="Sum")},
                grouping=[
                    models.QueryGrouping(type="Dimension", name="ServiceName"),
                    models.QueryGrouping(type="Dimension", name="ResourceId"),
                ],
            ),
        )
        result = client.query.usage(scope=f"/subscriptions/{self.subscription_id}", parameters=query)
        cols = [c.name for c in result.columns]
        for row in result.rows:
            rec = dict(zip(cols, row))
            rec.update(period_start=start.isoformat(), period_end=end.isoformat())
            yield f"{rec.get('ServiceName')}|{rec.get('ResourceId')}", rec

    def collect_storage_stats(self) -> Collected:
        storage = self._require("azure.mgmt.storage", "azure")
        monitor = self._require("azure.mgmt.monitor", "azure")
        sc = storage.StorageManagementClient(self.credential, self.subscription_id)
        mc = monitor.MonitorManagementClient(self.credential, self.subscription_id)
        for acct in sc.storage_accounts.list():
            payload: dict[str, Any] = {
                "id": acct.id, "name": acct.name, "location": acct.location,
                "kind": str(acct.kind), "access_tier": str(getattr(acct, "access_tier", None)),
            }
            try:
                m = mc.metrics.list(acct.id, metricnames="UsedCapacity", interval="PT1H", aggregation="Average")
                pts = [p.average for ts in m.value[0].timeseries for p in ts.data if p.average is not None]
                payload["size_bytes"] = pts[-1] if pts else None
            except Exception as e:  # noqa: BLE001
                payload["size_error"] = str(e)[:200]
            yield acct.id, payload
