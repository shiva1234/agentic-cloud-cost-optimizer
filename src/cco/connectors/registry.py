"""Connector registry: maps config keys to connector classes (plug-in point for new sources)."""
from __future__ import annotations

import logging
from typing import Any

from cco.connectors.aws import AWSConnector
from cco.connectors.azure import AzureConnector
from cco.connectors.base import BaseConnector
from cco.connectors.databricks import DatabricksConnector
from cco.connectors.gcp import GCPConnector
from cco.connectors.sample import SampleConnector
from cco.connectors.snowflake import SnowflakeConnector

log = logging.getLogger(__name__)

CONNECTORS: dict[str, type[BaseConnector]] = {
    "databricks": DatabricksConnector,
    "aws": AWSConnector,
    "azure": AzureConnector,
    "gcp": GCPConnector,
    "snowflake": SnowflakeConnector,
    "sample": SampleConnector,
}


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    CONNECTORS[cls.name] = cls
    return cls


def build_connectors(config: dict[str, Any], **init_kwargs: dict[str, Any]) -> list[BaseConnector]:
    """Instantiate enabled connectors. `init_kwargs[name]` injects clients (e.g. spark, workspace_client)."""
    out: list[BaseConnector] = []
    for name, ccfg in (config.get("connectors") or {}).items():
        if not (ccfg or {}).get("enabled"):
            continue
        cls = CONNECTORS.get(name)
        if cls is None:
            log.warning("Unknown connector '%s' in config - skipped", name)
            continue
        out.append(cls(ccfg, **(init_kwargs.get(name) or {})))
    return out
