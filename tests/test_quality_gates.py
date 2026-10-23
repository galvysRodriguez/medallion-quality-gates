"""End to end: every corruption scenario is stopped at the expected stage by the expected
rule, and consumers keep reading the last good gold data.

Needs the warehouse: `docker compose up -d warehouse` (or any Postgres 16 on WAREHOUSE_*).
"""

import pytest

from pipeline import orchestration
from pipeline.config import load_domains
from pipeline.dbt_runner import QualityGateError
from pipeline.generators import GENERATORS, generate

pytestmark = pytest.mark.integration

BASELINE = "2026-01-15"
DOMAINS = sorted(GENERATORS)


def gold_batches(conn, domain: str) -> set[str]:
    schema = load_domains()[domain].gold_schema
    tables = [r[0] for r in conn.execute(
        "select table_name from information_schema.tables where table_schema = %s", (schema,)
    )]
    assert tables, f"{schema} has no tables"
    batches = set()
    for table in tables:
        batches |= {r[0] for r in conn.execute(f'select distinct _batch_id from "{schema}"."{table}"')}
    return batches


def row_counts(conn, domain: str, batch_id: str) -> dict[str, int]:
    schema = load_domains()[domain].gold_schema
    tables = [r[0] for r in conn.execute(
        "select table_name from information_schema.tables where table_schema = %s", (schema,)
    )]
    return {
        t: conn.execute(f'select count(*) from "{schema}"."{t}" where _batch_id = %s', (batch_id,)).fetchone()[0]
        for t in tables
    }


@pytest.fixture(scope="module")
def published(warehouse):
    """Start from an empty gold history and publish a clean baseline batch for every domain."""
    for domain in DOMAINS:
        for suffix in ("gold", "gold_staging", "gold_previous"):
            warehouse.execute(f"drop schema if exists {domain}_{suffix} cascade")
        generate(domain, BASELINE)
        orchestration.run_pipeline(domain, BASELINE)
    return warehouse


@pytest.mark.parametrize("domain", DOMAINS)
def test_clean_batch_is_published(published, domain):
    assert gold_batches(published, domain) == {BASELINE}
    summary = orchestration.summarize(domain, BASELINE)
    assert summary["status"] == "published"
    assert set(summary["stages"].values()) == {"passed"}


@pytest.mark.parametrize(
    "domain,scenario",
    [(d, s) for d in DOMAINS for s in GENERATORS[d].scenarios()],
)
def test_corrupted_batch_is_blocked(published, domain, scenario):
    expected = GENERATORS[domain].scenarios()[scenario]
    batch_id = f"2026-01-16_{scenario}"
    generate(domain, batch_id, scenario)

    with pytest.raises(QualityGateError) as exc:
        orchestration.run_pipeline(domain, batch_id)

    result = exc.value.result
    failed = [r.name for r in result.failed]
    assert result.stage == expected.expected_stage, f"stopped at {result.stage} by {failed}"
    assert expected.expected_rule in failed
    # Write-Audit-Publish: the bad batch never reaches consumers.
    assert gold_batches(published, domain) == {BASELINE}

    summary = orchestration.summarize(domain, batch_id)
    assert summary["status"] == "blocked"
    assert summary["failed_stage"] == expected.expected_stage


def test_gold_keeps_history_and_republishing_is_idempotent(published):
    generate("fintech", "2026-01-17")
    orchestration.run_pipeline("fintech", "2026-01-17")
    assert gold_batches(published, "fintech") == {BASELINE, "2026-01-17"}
    first = row_counts(published, "fintech", "2026-01-17")
    assert all(first.values())

    orchestration.run_pipeline("fintech", "2026-01-17")  # reprocessing replaces the partition
    assert row_counts(published, "fintech", "2026-01-17") == first

    orchestration.rollback("fintech", "2026-01-17")
    assert gold_batches(published, "fintech") == {BASELINE}


def test_publish_refuses_a_stale_staging_schema(published):
    # Staging holds BASELINE's audited tables only if gold was last built for it; publishing
    # any other batch id from it must fail instead of mislabelling data.
    generate("ecommerce", BASELINE)
    orchestration.run_pipeline("ecommerce", BASELINE)
    with pytest.raises(QualityGateError, match="another batch"):
        orchestration.publish("ecommerce", "2026-01-18")
    assert gold_batches(published, "ecommerce") == {BASELINE}


def test_runs_and_rule_results_are_kept_in_dq_audit(published):
    generate("fintech", "2026-01-19_audit", "negative_amounts")
    with pytest.raises(QualityGateError):
        orchestration.run_pipeline("fintech", "2026-01-19_audit")
    summary = orchestration.finalize("fintech", "2026-01-19_audit")

    status, failed_stage, failed_rules = published.execute(
        "select status, failed_stage, failed_rules from dq_audit.pipeline_runs where run_id = %s",
        (summary["run_id"],),
    ).fetchone()
    assert (status, failed_stage) == ("blocked", "silver")
    assert "fintech_txn_amount_positive" in failed_rules

    rules = dict(published.execute(
        "select rule, status from dq_audit.rule_results where run_id = %s and stage = 'silver'",
        (summary["run_id"],),
    ).fetchall())
    assert rules["fintech_txn_amount_positive"] == "fail"
    assert rules["fintech_txn_id_unique"] == "pass"

    (failed,) = published.execute(
        "select failed from dq_audit.rule_health where domain = 'fintech' and rule = 'fintech_txn_amount_positive'"
    ).fetchone()
    assert failed >= 1
