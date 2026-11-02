"""REST API for the approval UI (deployable as a Databricks App). Requires the `api` extra.

    uvicorn cco.interfaces.api:app
"""
from __future__ import annotations

from typing import Any

from cco.approval.workflow import ApprovalError, ApprovalWorkflow
from cco.models.canonical import RecommendationStatus


def create_app(workflow: ApprovalWorkflow) -> Any:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    app = FastAPI(title="Cloud Cost Optimizer", version="0.1.0")

    class Decision(BaseModel):
        actor: str
        comment: str = ""

    @app.get("/recommendations")
    def list_recommendations(status: str | None = None) -> list[dict]:
        st = RecommendationStatus(status) if status else None
        return [r.to_dict() for r in workflow.repo.list(st)]

    @app.get("/recommendations/{rec_id}")
    def get_recommendation(rec_id: str) -> dict:
        rec = workflow.repo.get(rec_id)
        if rec is None:
            raise HTTPException(404, "not found")
        return rec.to_dict()

    @app.post("/recommendations/{rec_id}/approve")
    def approve(rec_id: str, d: Decision) -> dict:
        try:
            return workflow.approve(rec_id, d.actor, d.comment).to_dict()
        except ApprovalError as e:
            raise HTTPException(409, str(e)) from e

    @app.post("/recommendations/{rec_id}/reject")
    def reject(rec_id: str, d: Decision) -> dict:
        try:
            return workflow.reject(rec_id, d.actor, d.comment or "rejected").to_dict()
        except ApprovalError as e:
            raise HTTPException(409, str(e)) from e

    return app


def _default_app() -> Any:  # pragma: no cover - wiring for `uvicorn cco.interfaces.api:app`
    import os

    from cco.config import load_config
    from cco.pipeline import build_repository

    cfg = load_config(os.environ.get("CCO_CONFIG"))
    return create_app(ApprovalWorkflow(build_repository(cfg)))


def __getattr__(name: str) -> Any:
    if name == "app":
        return _default_app()
    raise AttributeError(name)
