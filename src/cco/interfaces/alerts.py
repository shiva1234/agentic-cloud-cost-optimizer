"""Alerting: notify approvers about new high-priority recommendations."""
from __future__ import annotations

import json
import logging
import urllib.request
from abc import ABC, abstractmethod

from cco.agents.coordinator import AnalysisReport

log = logging.getLogger(__name__)
PRIORITY_ORDER = ["P1", "P2", "P3", "P4"]


class Notifier(ABC):
    @abstractmethod
    def send(self, title: str, body: str) -> None: ...


class LogNotifier(Notifier):
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send(self, title: str, body: str) -> None:
        self.sent.append((title, body))
        log.info("ALERT %s\n%s", title, body)


class SlackWebhookNotifier(Notifier):
    def __init__(self, webhook_url: str, timeout: int = 10) -> None:
        self.url = webhook_url
        self.timeout = timeout

    def send(self, title: str, body: str) -> None:
        data = json.dumps({"text": f"*{title}*\n{body}"}).encode()
        req = urllib.request.Request(self.url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=self.timeout).read()


def build_notifier(cfg: dict | None) -> Notifier:
    cfg = cfg or {}
    return SlackWebhookNotifier(cfg["slack_webhook_url"]) if cfg.get("slack_webhook_url") else LogNotifier()


def notify_high_priority(report: AnalysisReport, notifier: Notifier, min_priority: str = "P2") -> int:
    cutoff = PRIORITY_ORDER.index(min_priority)
    hits = [r for r in report.recommendations if PRIORITY_ORDER.index(r.priority) <= cutoff]
    if not hits:
        return 0
    body = "\n".join(f"- [{r.priority}] {r.title} (${r.estimated_monthly_savings:,.0f}/mo, risk {r.risk.value}, id {r.id})" for r in hits[:15])
    try:
        notifier.send(f"Cost optimizer: {len(hits)} recommendation(s) awaiting approval", body)
    except Exception as e:  # noqa: BLE001
        log.warning("alert delivery failed: %s", e)
    return len(hits)
