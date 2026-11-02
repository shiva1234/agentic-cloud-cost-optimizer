"""Configuration loading (YAML/JSON) with deep-merge overrides and secret resolution."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

DEFAULTS: dict[str, Any] = {
    "project": {"name": "cloud-cost-optimizer", "environment": "dev"},
    "storage": {"mode": "memory", "catalog": "main", "schema": "cloud_cost_optimizer"},
    "connectors": {},
    "agents": {"max_workers": 4, "delta_table_optimization": {"enabled": True}},
    "scoring": {},
    "llm": {"enabled": False},
    "approval": {},
    "actions": {"dry_run": True},
    "alerts": {},
}


def deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_config(path: str | os.PathLike | None = None, overrides: dict | None = None) -> dict:
    """Load config file (yaml/json), merge onto DEFAULTS, then apply overrides."""
    cfg = copy.deepcopy(DEFAULTS)
    if path:
        text = Path(path).read_text()
        if str(path).endswith((".yaml", ".yml")):
            import yaml

            data = yaml.safe_load(text) or {}
        else:
            data = json.loads(text)
        cfg = deep_merge(cfg, data)
    if overrides:
        cfg = deep_merge(cfg, overrides)
    return cfg


def get_path(cfg: dict, dotted: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def resolve_secret(value: Any) -> str | None:
    """Resolve {scope, key} via Databricks secrets, {env: VAR} via env, else return the literal."""
    if value is None:
        return None
    if isinstance(value, dict):
        if value.get("env"):
            return os.environ.get(value["env"])
        if value.get("scope") and value.get("key"):
            from databricks.sdk import WorkspaceClient

            return WorkspaceClient().dbutils.secrets.get(scope=value["scope"], key=value["key"])
        return None
    return str(value)
