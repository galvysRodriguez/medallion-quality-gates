"""Alert formatting and webhook delivery (local HTTP server, no network)."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from pipeline import alerts

BLOCKED = {
    "domain": "fintech", "batch_id": "2026-01-16", "status": "blocked", "failed_stage": "silver",
    "failed_rules": ["fintech_txn_amount_positive"], "failures": {"fintech_txn_amount_positive": 3},
    "warned_rules": [],
}
PUBLISHED_WITH_WARNINGS = {**BLOCKED, "status": "published", "failed_stage": None, "failed_rules": [],
                           "failures": {}, "warned_rules": ["fintech_txn_high_value_review"]}


@pytest.fixture
def webhook(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/hook")
    yield received
    server.shutdown()


def test_blocked_batch_is_sent_with_stage_and_rules(webhook):
    assert alerts.notify(BLOCKED) is True
    (payload,) = webhook
    assert "BLOCKED at silver" in payload["text"]
    assert "fintech_txn_amount_positive (3 rows)" in payload["text"]


def test_published_batch_is_silent_by_default(webhook):
    assert alerts.notify(PUBLISHED_WITH_WARNINGS) is False
    assert webhook == []


def test_warnings_can_be_opted_in(webhook, monkeypatch):
    monkeypatch.setenv("ALERT_ON_WARNINGS", "true")
    assert alerts.notify(PUBLISHED_WITH_WARNINGS) is True
    assert "fintech_txn_high_value_review" in webhook[0]["text"]


def test_without_webhook_alerts_are_only_logged(monkeypatch, caplog):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert alerts.notify(BLOCKED) is False
    assert "BLOCKED" in caplog.text


def test_unreachable_webhook_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/unreachable")
    assert alerts.notify(BLOCKED) is False
