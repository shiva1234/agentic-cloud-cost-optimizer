"""Snowflake connector: ACCOUNT_USAGE views for tables, storage, access history and credits."""
from __future__ import annotations

from typing import Any

from cco.config import resolve_secret
from cco.connectors.base import BaseConnector, Collected
from cco.models.canonical import Provider


class SnowflakeConnector(BaseConnector):
    name = "snowflake"
    provider = Provider.SNOWFLAKE

    def __init__(self, config: dict | None = None, connection: Any = None) -> None:
        super().__init__(config)
        self._conn = connection

    def supported_entities(self) -> set[str]:
        return {"accounts", "tables", "access_logs", "costs", "compute_metrics"}

    @property
    def conn(self) -> Any:
        if self._conn is None:
            sf = self._require("snowflake.connector", "snowflake")
            params = {k: self.config.get(k) for k in ("account", "user", "role", "warehouse", "authenticator") if self.config.get(k)}
            pwd = resolve_secret(self.config.get("password_secret"))
            if pwd:
                params["password"] = pwd
            self._conn = sf.connect(**params)
        return self._conn

    def _query(self, sql: str) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cols = [c[0].lower() for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            cur.close()

    @property
    def days(self) -> int:
        return int(self.config.get("lookback_days", 30))

    def collect_accounts(self) -> Collected:
        r = self._query("SELECT CURRENT_ACCOUNT() AS account, CURRENT_REGION() AS region")[0]
        yield r["account"], r

    def collect_tables(self) -> Collected:
        rows = self._query(
            """
            SELECT t.table_catalog, t.table_schema, t.table_name, t.table_type, t.row_count, t.bytes,
                   t.clustering_key, t.created, t.last_altered, t.retention_time,
                   m.active_bytes, m.time_travel_bytes, m.failsafe_bytes, m.retained_for_clone_bytes
            FROM snowflake.account_usage.tables t
            LEFT JOIN snowflake.account_usage.table_storage_metrics m
              ON m.id = t.table_id AND m.deleted = FALSE
            WHERE t.deleted IS NULL"""
        )
        for r in rows:
            yield f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}", r

    def collect_access_logs(self) -> Collected:
        rows = self._query(
            f"""
            SELECT obj.value:"objectName"::string AS table_full_name,
                   COUNT(*) AS reads, MAX(ah.query_start_time) AS last_read
            FROM snowflake.account_usage.access_history ah,
                 LATERAL FLATTEN(input => ah.base_objects_accessed) obj
            WHERE ah.query_start_time >= DATEADD(day, -{self.days}, CURRENT_TIMESTAMP())
            GROUP BY 1"""
        )
        for r in rows:
            r["window_days"] = self.days
            yield r["table_full_name"], r

    def collect_costs(self) -> Collected:
        price = float(self.config.get("credit_price_usd", 3.0))
        rows = self._query(
            f"""
            SELECT warehouse_name, SUM(credits_used) AS credits,
                   MIN(start_time) AS period_start, MAX(end_time) AS period_end
            FROM snowflake.account_usage.warehouse_metering_history
            WHERE start_time >= DATEADD(day, -{self.days}, CURRENT_TIMESTAMP())
            GROUP BY warehouse_name"""
        )
        for r in rows:
            r["amount"] = float(r["credits"] or 0) * price
            yield r["warehouse_name"], r

    def collect_compute_metrics(self) -> Collected:
        rows = self._query(
            f"""
            SELECT warehouse_name, AVG(avg_running) AS avg_running, AVG(avg_queued_load) AS avg_queued
            FROM snowflake.account_usage.warehouse_load_history
            WHERE start_time >= DATEADD(day, -{self.days}, CURRENT_TIMESTAMP())
            GROUP BY warehouse_name"""
        )
        for r in rows:
            yield r["warehouse_name"], r
