"""Stage functions shared by the Airflow DAGs, the CLI and the tests.

    load -> bronze -> silver -> gold (built in <domain>_gold_staging) -> publish (upsert into <domain>_gold)

Each stage either returns a passing StageResult or raises QualityGateError, and writes
its verdict to data/reports/<domain>/<batch>/<stage>.json either way.
"""

from __future__ import annotations

import functools
import json
import logging
import shutil
from datetime import date, datetime
from pathlib import Path

from pipeline import dbt_runner
from pipeline.config import LANDING_DIR, LAYERS, REPORTS_DIR, load_domain, validate_batch_id
from pipeline.dbt_runner import QualityGateError, RuleResult, StageResult
from pipeline.loader import ContractError, load_batch

log = logging.getLogger(__name__)

STAGES = ("load", *LAYERS, "publish")


def resolve_batch(
    domain_name: str,
    requested: str | None = None,
    logical_date: date | datetime | None = None,
    landing_dir: Path | None = None,
) -> str:
    """Explicit batch id > the run's logical date (YYYY-MM-DD) > latest landed batch."""
    domain = load_domain(domain_name)
    root = (landing_dir or LANDING_DIR) / domain.name
    if requested:
        batch_id = validate_batch_id(requested)
    elif logical_date is not None:
        batch_id = logical_date.strftime("%Y-%m-%d")
    else:
        batches = sorted(p.name for p in root.glob("*") if p.is_dir())
        if not batches:
            raise FileNotFoundError(f"no landing batches under {root}")
        batch_id = batches[-1]
    if not (root / batch_id).is_dir():
        raise FileNotFoundError(f"landing batch {root / batch_id} does not exist")
    return batch_id


def _report_dir(domain: str, batch_id: str) -> Path:
    return REPORTS_DIR / domain / batch_id


def _write_report(result: StageResult) -> None:
    path = _report_dir(result.domain, result.batch_id) / f"{result.stage}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_dict(), indent=2, default=str))


def _reported(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs) -> StageResult:
        try:
            result = fn(*args, **kwargs)
        except QualityGateError as exc:
            _write_report(exc.result)
            raise
        _write_report(result)
        return result

    return wrapper


@_reported
def load(domain_name: str, batch_id: str, landing_dir: Path | None = None) -> StageResult:
    """Structural contract gate + load into <domain>_raw."""
    shutil.rmtree(_report_dir(domain_name, batch_id), ignore_errors=True)
    domain = load_domain(domain_name)
    try:
        counts = load_batch(domain, batch_id, landing_dir)
    except ContractError as exc:
        rule = RuleResult("landing_contract", "contract.landing", "contract", "error", message=str(exc))
        raise QualityGateError(StageResult(domain.name, "load", batch_id, False, [rule])) from exc
    results = [
        RuleResult(f"load_{entity}", f"load.{entity}", "load", "success", message=f"{n} rows")
        for entity, n in counts.items()
    ]
    return StageResult(domain.name, "load", batch_id, True, results)


@_reported
def build(domain_name: str, layer: str, batch_id: str) -> StageResult:
    """dbt build of one layer; any failing error-severity rule stops the pipeline."""
    return dbt_runner.build_layer(load_domain(domain_name).name, layer, batch_id)


@_reported
def publish(domain_name: str, batch_id: str) -> StageResult:
    """Add the audited batch to the consumer-facing gold history."""
    return dbt_runner.publish_gold(load_domain(domain_name).name, batch_id)


def rollback(domain_name: str, batch_id: str) -> StageResult:
    """Unpublish a batch from gold (e.g. a bad batch that slipped through a missing rule)."""
    return dbt_runner.rollback_gold(load_domain(domain_name).name, batch_id)


def run_pipeline(domain_name: str, batch_id: str, landing_dir: Path | None = None) -> list[StageResult]:
    """Run every stage in order, stopping at the first failing gate."""
    results = [load(domain_name, batch_id, landing_dir)]
    for layer in LAYERS:
        results.append(build(domain_name, layer, batch_id))
    results.append(publish(domain_name, batch_id))
    return results


def summarize(domain_name: str, batch_id: str) -> dict:
    """Combine the per-stage reports into quality_report.json."""
    report_dir = _report_dir(domain_name, batch_id)
    stages = {}
    for stage in STAGES:
        path = report_dir / f"{stage}.json"
        stages[stage] = json.loads(path.read_text()) if path.exists() else None

    failed_stage = next((s for s, r in stages.items() if r and not r["passed"]), None)
    summary = {
        "domain": domain_name,
        "batch_id": batch_id,
        "status": "published" if stages["publish"] and stages["publish"]["passed"] else "blocked",
        "failed_stage": failed_stage,
        "failed_rules": stages[failed_stage]["failed_rules"] if failed_stage else [],
        "warned_rules": sorted({w for r in stages.values() if r for w in r["warned_rules"]}),
        "stages": {s: (None if r is None else ("passed" if r["passed"] else "failed")) for s, r in stages.items()},
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "quality_report.json").write_text(json.dumps(summary, indent=2))
    return summary
