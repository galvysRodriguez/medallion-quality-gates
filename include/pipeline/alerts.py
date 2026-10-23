"""Notify a chat webhook when a batch is blocked (or, optionally, when rules warn).

    ALERT_WEBHOOK_URL    incoming webhook (Slack, Mattermost, Google Chat: JSON {"text": ...})
    ALERT_ON_WARNINGS    "true" to also notify published batches that raised warn-severity rules

Without ALERT_WEBHOOK_URL the alert is only logged. Delivery failures are logged, never raised.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger(__name__)


def format_message(summary: dict) -> str | None:
    domain, batch = summary["domain"], summary["batch_id"]
    if summary["status"] == "not_arrived":
        return f":hourglass: [{domain}] batch {batch} did not land in time — nothing was processed."
    if summary["status"] != "published":
        rules = ", ".join(
            f"{rule} ({count} rows)" if count else rule
            for rule, count in summary.get("failures", {}).items()
        ) or ", ".join(summary["failed_rules"]) or "unknown"
        return (
            f":rotating_light: [{domain}] batch {batch} was BLOCKED at {summary['failed_stage']} — "
            f"gold was not updated. Failed rules: {rules}. "
            f"Failing rows are in dq_audit.<rule>."
        )
    if summary["warned_rules"] and os.environ.get("ALERT_ON_WARNINGS", "").lower() == "true":
        return f":warning: [{domain}] batch {batch} published with warnings: {', '.join(summary['warned_rules'])}"
    return None


def notify(summary: dict) -> bool:
    """Send the alert for this run summary, if any. Returns True when a webhook accepted it."""
    message = format_message(summary)
    if message is None:
        return False
    log.warning("ALERT %s", message)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return False
    request = urllib.request.Request(
        url, data=json.dumps({"text": message}).encode(), headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError) as exc:
        log.warning("could not deliver alert: %s", exc)
        return False
