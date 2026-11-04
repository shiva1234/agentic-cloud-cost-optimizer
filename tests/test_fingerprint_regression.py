"""Regression tests for the false-positive duplicate-job fingerprint bug.

Bug: _clean_params dropped any parameter value containing a {{...}} template, so bundle jobs
that share one generic notebook but pass different templated table names (e.g.
  source_table = "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_web_cdi")
all collapsed to the same fingerprint and were wrongly flagged as duplicates.

Fix: replace template spans with a fixed marker instead of dropping the value, and preserve
task multiplicity in the normalizer (sorted list, not set) so a 2-task job can't match a 1-task job.
"""
from cco.etl.fingerprint import task_fingerprint


# Realistic bundle-deployed task payloads (same generic notebook, different table params)
_NB_PATH = "/Workspace/Users/sp-id/.bundle/dtc_sbs_de_etl/dev/files/code/gold/dwh_dbx/py/initial_loads/gold_table_initial_load"


def _nb_task(source_table: str, target_table: str) -> dict:
    return {
        "task_key": "initial_load",
        "notebook_task": {
            "notebook_path": _NB_PATH,
            "base_parameters": {"source_table": source_table, "target_table": target_table},
        },
    }


def test_different_templated_tables_get_different_fingerprints():
    """Two jobs using the same notebook but loading different tables must NOT match."""
    task_wheels = _nb_task(
        "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_product_analysis_wheels",
        "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_product_analysis_wheels",
    )
    task_cdi = _nb_task(
        "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_web_cdi",
        "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_web_cdi",
    )
    fp_wheels = task_fingerprint(task_wheels)
    fp_cdi = task_fingerprint(task_cdi)
    assert fp_wheels is not None and fp_cdi is not None
    assert fp_wheels != fp_cdi, "different tables must produce different fingerprints"


def test_same_job_across_bundle_users_gets_same_fingerprint():
    """The same job deployed under two bundle user dirs with only {{job.parameters.*}} differences
    (i.e. the table names are identical except for the template spans) must still match."""
    task_a = _nb_task(
        "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_sales",
        "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_sales",
    )
    task_b = _nb_task(
        "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_sales",
        "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_sales",
    )
    fp_a = task_fingerprint(task_a)
    fp_b = task_fingerprint(task_b)
    assert fp_a is not None and fp_b is not None
    assert fp_a == fp_b, "same logical params must produce same fingerprint across deployments"


def test_pure_template_param_normalizes():
    """A value that is entirely a template (e.g. '{{job.parameters.env}}') normalizes to the
    same marker regardless of the parameter name inside."""
    task_a = _nb_task("{{job.parameters.env}}", "{{job.parameters.target}}")
    task_b = _nb_task("{{job.parameters.other_env}}", "{{job.parameters.other_target}}")
    fp_a = task_fingerprint(task_a)
    fp_b = task_fingerprint(task_b)
    assert fp_a is not None and fp_b is not None
    assert fp_a == fp_b, "pure-template values should normalize to the same marker"


def test_two_task_job_preserves_multiplicity():
    """A job with 2 tasks must produce 2 fingerprints (even if identical), not 1 after set-dedup.

    This tests through the normalizer to verify the sorted-list change (not set).
    """
    from cco.etl.normalizer import _dbx_job
    from cco.connectors.base import RawRecord

    # Job A: 1 task loading fct_product_analysis_wheels
    job_a = {
        "job_id": 493091158595226,
        "settings": {
            "name": "[dev sp] dtc_sbs_gold_initial_load_fct_product_analysis_wheels",
            "tasks": [_nb_task(
                "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_product_analysis_wheels",
                "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_product_analysis_wheels",
            )],
            "parameters": [{"name": "env", "default": "dev"}, {"name": "source_env", "default": "prod"}],
        },
    }
    # Job B: 2 tasks loading fct_web_cdi and fct_web_cdi_daily
    job_b = {
        "job_id": 103007159217404,
        "settings": {
            "name": "[dev sp] dtc_sbs_gold_initial_load_fct_web_cdi",
            "tasks": [
                _nb_task(
                    "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_web_cdi",
                    "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_web_cdi",
                ),
                _nb_task(
                    "dt_{{job.parameters.source_env}}_gold.dwh_legacy.fct_web_cdi_daily",
                    "dt_{{job.parameters.env}}_gold.dwh_dbx.fct_web_cdi_daily",
                ),
            ],
            "parameters": [{"name": "env", "default": "dev"}, {"name": "source_env", "default": "prod"}],
        },
    }
    obj_a = list(_dbx_job(RawRecord(provider="databricks", entity="jobs", source_id="493091158595226", payload=job_a, connector="test")))[0]
    obj_b = list(_dbx_job(RawRecord(provider="databricks", entity="jobs", source_id="103007159217404", payload=job_b, connector="test")))[0]
    fps_a = obj_a.attributes["task_fingerprints"]
    fps_b = obj_b.attributes["task_fingerprints"]
    # Job A has 1 task -> 1 base fingerprint (+ jobparams suffix)
    # Job B has 2 tasks -> 2 base fingerprints (+ jobparams suffix)
    assert len(fps_b) > len(fps_a), "2-task job must have more fingerprints than 1-task job"
    assert len(fps_b) >= 3, "2-task job should have 2 base fps + 1 jobparams suffix = 3"
    # The two jobs must not share the same fingerprint tuple (different tables)
    assert fps_a != fps_b, "different-table jobs must not have identical fingerprint lists"


def test_duplicate_tasks_in_same_job_preserve_repeats():
    """If a job has 2 identical tasks (same notebook, same params), both fingerprints must
    appear in the list - not deduplicated to 1 by a set."""
    from cco.etl.normalizer import _dbx_job
    from cco.connectors.base import RawRecord

    job = {
        "job_id": 999,
        "settings": {
            "name": "two_identical_tasks",
            "tasks": [
                _nb_task("dt_{{job.parameters.source_env}}_gold.t1", "dt_{{job.parameters.env}}_gold.t1"),
                _nb_task("dt_{{job.parameters.source_env}}_gold.t1", "dt_{{job.parameters.env}}_gold.t1"),
            ],
        },
    }
    obj = list(_dbx_job(RawRecord(provider="databricks", entity="jobs", source_id="999", payload=job, connector="test")))[0]
    fps = obj.attributes["task_fingerprints"]
    # Both tasks produce the same fingerprint; both must be present (no set dedup)
    base_fps = [fp for fp in fps if not fp.startswith(("jobparams", "jobscoped"))]
    assert len(base_fps) == 2, f"expected 2 identical fingerprints preserved, got {len(base_fps)}"
    assert base_fps[0] == base_fps[1], "identical tasks must have identical fingerprints"
