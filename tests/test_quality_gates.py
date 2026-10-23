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


@pytest.fixture(scope="module")
def published(warehouse):
    """Publish a clean baseline batch for every domain."""
    for domain in DOMAINS:
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


def test_new_clean_batch_replaces_gold_and_keeps_previous(published):
    generate("fintech", "2026-01-17")
    orchestration.run_pipeline("fintech", "2026-01-17")
    assert gold_batches(published, "fintech") == {"2026-01-17"}
    (exists,) = published.execute(
        "select count(*) from pg_namespace where nspname = 'fintech_gold_previous'"
    ).fetchone()
    assert exists == 1
