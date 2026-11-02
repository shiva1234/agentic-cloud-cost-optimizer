"""AWS connector: Cost Explorer (costs), S3 + CloudWatch (storage), EC2 (Databricks compute footprint)."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from cco.connectors.base import BaseConnector, Collected
from cco.models.canonical import Provider
from cco.utils import utcnow


class AWSConnector(BaseConnector):
    name = "aws"
    provider = Provider.AWS

    def __init__(self, config: dict | None = None, session: Any = None) -> None:
        super().__init__(config)
        self._session = session

    @property
    def session(self) -> Any:
        if self._session is None:
            boto3 = self._require("boto3", "aws")
            self._session = boto3.Session(profile_name=self.config.get("profile"), region_name=self.config.get("region"))
        return self._session

    def supported_entities(self) -> set[str]:
        return {"accounts", "costs", "storage_stats", "compute_metrics"}

    def test_connection(self) -> bool:
        self.session.client("sts").get_caller_identity()
        return True

    def collect_accounts(self) -> Collected:
        ident = self.session.client("sts").get_caller_identity()
        yield ident["Account"], {"account_id": ident["Account"], "arn": ident["Arn"]}

    def collect_costs(self) -> Collected:
        ce = self.session.client("ce")
        end = date.today()
        start = end - timedelta(days=int(self.config.get("lookback_days", 30)))
        group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}]
        if self.config.get("cost_allocation_tag"):
            group_by.append({"Type": "TAG", "Key": self.config["cost_allocation_tag"]})
        token = None
        while True:
            kwargs: dict[str, Any] = dict(
                TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
                Granularity="MONTHLY",
                Metrics=["UnblendedCost", "UsageQuantity"],
                GroupBy=group_by,
            )
            if token:
                kwargs["NextPageToken"] = token
            resp = ce.get_cost_and_usage(**kwargs)
            for period in resp.get("ResultsByTime", []):
                for g in period.get("Groups", []):
                    keys = g["Keys"]
                    yield "|".join([period["TimePeriod"]["Start"], *keys]), {
                        "service": keys[0],
                        "tag": keys[1] if len(keys) > 1 else None,
                        "period_start": period["TimePeriod"]["Start"],
                        "period_end": period["TimePeriod"]["End"],
                        "amount": float(g["Metrics"]["UnblendedCost"]["Amount"]),
                        "currency": g["Metrics"]["UnblendedCost"]["Unit"],
                        "usage_quantity": float(g["Metrics"]["UsageQuantity"]["Amount"]),
                    }
            token = resp.get("NextPageToken")
            if not token:
                break

    def collect_storage_stats(self) -> Collected:
        s3 = self.session.client("s3")
        cw = self.session.client("cloudwatch")
        now = utcnow()
        for b in s3.list_buckets().get("Buckets", []):
            name = b["Name"]
            payload: dict[str, Any] = {"bucket": name, "created": b.get("CreationDate")}
            for metric, stype, key in (("BucketSizeBytes", "StandardStorage", "size_bytes"), ("NumberOfObjects", "AllStorageTypes", "object_count")):
                try:
                    pts = cw.get_metric_statistics(
                        Namespace="AWS/S3", MetricName=metric,
                        Dimensions=[{"Name": "BucketName", "Value": name}, {"Name": "StorageType", "Value": stype}],
                        StartTime=now - timedelta(days=3), EndTime=now, Period=86400, Statistics=["Average"],
                    ).get("Datapoints", [])
                    payload[key] = max(pts, key=lambda p: p["Timestamp"])["Average"] if pts else None
                except Exception as e:  # noqa: BLE001
                    payload[f"{key}_error"] = str(e)[:200]
            yield name, payload

    def collect_compute_metrics(self) -> Collected:
        """EC2 instances launched by Databricks (tag Vendor=Databricks by default)."""
        ec2 = self.session.client("ec2")
        key = self.config.get("databricks_tag_key", "Vendor")
        val = self.config.get("databricks_tag_value", "Databricks")
        paginator = ec2.get_paginator("describe_instances")
        for page in paginator.paginate(Filters=[{"Name": f"tag:{key}", "Values": [val]}]):
            for res in page.get("Reservations", []):
                for inst in res.get("Instances", []):
                    tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
                    yield inst["InstanceId"], {
                        "instance_id": inst["InstanceId"],
                        "instance_type": inst.get("InstanceType"),
                        "state": (inst.get("State") or {}).get("Name"),
                        "launch_time": inst.get("LaunchTime"),
                        "lifecycle": inst.get("InstanceLifecycle", "on-demand"),
                        "cluster_id": tags.get("ClusterId"),
                        "tags": tags,
                    }
