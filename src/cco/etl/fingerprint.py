"""Logic fingerprints used to detect the same transformation deployed more than once.

Databricks bundles deploy per user/target, e.g.
  /Workspace/Users/<id>/.bundle/dtc_sbs_de_etl/dev/files/code/gold/x  and job name "[dev alice] my_job".
These are normalized to a deployment-independent *logical* form before comparison.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_DEV_PREFIX = re.compile(r"^\s*\[[^\]]+\]\s*")
_BUNDLE = re.compile(r"^(?:/Workspace)?/(?:Users|Repos)/[^/]+/\.bundle/([^/]+)/[^/]+/files/")
_USER = re.compile(r"^(?:/Workspace)?/(?:Users|Repos)/[^/]+/")
_TEMPLATE = re.compile(r"\{\{[^}]*\}\}")


def logical_name(name: str | None) -> str:
    """'[dev alice] dtc_load_x_job' -> 'dtc_load_x_job'."""
    return _DEV_PREFIX.sub("", name or "").strip().lower()


def logical_path(path: str | None) -> str | None:
    if not path:
        return None
    p = str(path)
    m = _BUNDLE.match(p)
    if m:
        return f"bundle:{m.group(1)}/" + p[m.end():]
    p = _USER.sub("user:/", p)
    return p.removeprefix("/Workspace")


def task_source(task: dict[str, Any]) -> tuple[str | None, str | None, Any]:
    """(task_type, logical source, parameters) for a Jobs API task payload."""
    if "notebook_task" in task:
        t = task["notebook_task"] or {}
        return "notebook", logical_path(t.get("notebook_path")), t.get("base_parameters") or {}
    if "spark_python_task" in task:
        t = task["spark_python_task"] or {}
        return "python", logical_path(t.get("python_file")), {"args": t.get("parameters") or []}
    if "python_wheel_task" in task:
        t = task["python_wheel_task"] or {}
        return "wheel", f"{t.get('package_name')}:{t.get('entry_point')}", t.get("named_parameters") or {"args": t.get("parameters") or []}
    if "sql_task" in task:
        t = task["sql_task"] or {}
        ref = ((t.get("query") or {}).get("query_id") or logical_path((t.get("file") or {}).get("path"))
               or (t.get("dashboard") or {}).get("dashboard_id") or (t.get("alert") or {}).get("alert_id"))
        return "sql", ref, t.get("parameters") or {}
    if "pipeline_task" in task:
        return "pipeline", f"pipeline:{(task['pipeline_task'] or {}).get('pipeline_id')}", {}
    if "run_job_task" in task:
        t = task["run_job_task"] or {}
        return "run_job", f"job:{t.get('job_id')}", t.get("job_parameters") or {}
    if "dbt_task" in task:
        t = task["dbt_task"] or {}
        return "dbt", "|".join(t.get("commands") or []), {"project": logical_path(t.get("project_directory"))}
    if "spark_jar_task" in task:
        t = task["spark_jar_task"] or {}
        return "jar", t.get("main_class_name"), {"args": t.get("parameters") or []}
    if "for_each_task" in task:
        inner_type, inner_src, inner_params = task_source((task["for_each_task"] or {}).get("task") or {})
        return f"for_each:{inner_type}", inner_src, inner_params
    if "condition_task" in task:
        return "condition", None, {}
    return None, None, {}


def _clean_params(params: Any) -> Any:
    """Replace template spans ({{job.parameters.env}}) with a fixed marker so per-deployment wiring normalizes
    while distinguishing values that differ outside the template part (e.g. different table names).

    A value that is ENTIRELY a template (e.g. "{{job.parameters.env}}") becomes "{{}}", so true copies across
    dev/user deployments still match.  A value with a template embedded (e.g.
    "dt_{{job.parameters.env}}_gold.fct_web_cdi") keeps its distinguishing text so different tables stay distinct.
    """
    if isinstance(params, dict):
        return {k: _clean_params(v) for k, v in sorted(params.items())}
    if isinstance(params, list):
        return [_clean_params(v) for v in params]
    if isinstance(params, str):
        return _TEMPLATE.sub("{{}}", params)
    return params


def task_fingerprint(task: dict[str, Any]) -> str | None:
    ttype, src, params = task_source(task)
    if not src or ttype == "condition":
        return None
    blob = json.dumps([ttype, src, _clean_params(params)], sort_keys=True, default=str)
    return hashlib.sha1(blob.encode()).hexdigest()[:16]


def jaccard(a: Any, b: Any) -> float:
    a, b = set(a), set(b)
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def cosine(h1: dict, h2: dict) -> float:
    keys = set(h1) | set(h2)
    num = sum(float(h1.get(k, 0)) * float(h2.get(k, 0)) for k in keys)
    d1 = sum(float(v) ** 2 for v in h1.values()) ** 0.5
    d2 = sum(float(v) ** 2 for v in h2.values()) ** 0.5
    return num / (d1 * d2) if d1 and d2 else 0.0
