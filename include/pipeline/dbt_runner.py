"""Runs one medallion layer with `dbt build` and turns run_results.json into a gate verdict."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pipeline.config import DBT_PROJECT_DIR, LAYERS, validate_batch_id

log = logging.getLogger(__name__)

FAILED_STATUSES = {"fail", "error"}


@dataclass
class RuleResult:
    """Outcome of one dbt node (a rule when resource_type == 'test')."""

    name: str
    unique_id: str
    resource_type: str
    status: str
    failures: int | None = None
    message: str | None = None


@dataclass
class StageResult:
    domain: str
    stage: str
    batch_id: str
    passed: bool
    results: list[RuleResult] = field(default_factory=list)

    @property
    def failed(self) -> list[RuleResult]:
        return [r for r in self.results if r.status in FAILED_STATUSES]

    @property
    def warnings(self) -> list[RuleResult]:
        return [r for r in self.results if r.status == "warn"]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["failed_rules"] = [r.name for r in self.failed]
        data["warned_rules"] = [r.name for r in self.warnings]
        return data


class QualityGateError(Exception):
    """A stage rejected the batch; downstream stages must not run."""

    def __init__(self, result: StageResult):
        self.result = result
        details = "; ".join(
            f"{r.name} ({r.failures} failing rows)" if r.failures else f"{r.name}: {r.message}"
            for r in result.failed
        )
        super().__init__(f"[{result.domain}] {result.stage} gate failed for batch {result.batch_id}: {details}")


def dbt_executable() -> str:
    if os.environ.get("DBT_EXECUTABLE"):
        return os.environ["DBT_EXECUTABLE"]
    return shutil.which("dbt") or str(Path(sys.executable).with_name("dbt"))


def _invoke(args: list[str], target_path: Path) -> subprocess.CompletedProcess:
    profiles_dir = os.environ.get("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    cmd = [
        dbt_executable(), "--no-use-colors", *args,
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", profiles_dir,
        "--target-path", str(target_path),
        "--log-path", str(target_path / "logs"),
    ]
    log.info("running: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd, capture_output=True, text=True, cwd=DBT_PROJECT_DIR,
        env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
    )
    for line in proc.stdout.splitlines():
        log.info("dbt | %s", line)
    if proc.stderr:
        log.warning("dbt stderr | %s", proc.stderr.strip())
    return proc


def _parse_run_results(path: Path) -> list[RuleResult]:
    data = json.loads(path.read_text())
    results = []
    for r in data.get("results", []):
        unique_id = r["unique_id"]
        resource_type, _, name = unique_id.split(".")[:3]
        results.append(
            RuleResult(
                name=name,
                unique_id=unique_id,
                resource_type=resource_type,
                status=r["status"],
                failures=r.get("failures"),
                message=r.get("message"),
            )
        )
    return results


def build_layer(domain: str, layer: str, batch_id: str) -> StageResult:
    """`dbt build` one layer of one domain: models + their rules, nothing else."""
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer!r}")
    validate_batch_id(batch_id)
    target_path = DBT_PROJECT_DIR / "target" / domain / layer
    run_results = target_path / "run_results.json"
    run_results.unlink(missing_ok=True)

    proc = _invoke(
        [
            "build",
            "--select", f"path:models/{domain}/{layer}",
            # Run a cross-layer rule (e.g. gold reconciles with silver) only once every
            # model it reads has been built — i.e. with the downstream layer.
            "--indirect-selection", "buildable",
            "--vars", json.dumps({"batch_id": batch_id}),
        ],
        target_path,
    )

    if run_results.exists():
        results = _parse_run_results(run_results)
    else:
        results = [RuleResult("dbt", "dbt", "invocation", "error", message=proc.stdout[-2000:] or proc.stderr)]
    result = StageResult(domain, layer, batch_id, passed=proc.returncode == 0, results=results)
    if not result.passed:
        if not result.failed:  # dbt failed before producing node results
            result.results.append(RuleResult("dbt", "dbt", "invocation", "error", message=proc.stdout[-2000:]))
        raise QualityGateError(result)
    for warning in result.warnings:
        log.warning("rule %s raised a warning (%s rows) — not blocking", warning.name, warning.failures)
    return result


def _operation(domain: str, macro: str, batch_id: str) -> StageResult:
    validate_batch_id(batch_id)
    target_path = DBT_PROJECT_DIR / "target" / domain / "operations"
    args = json.dumps({"domain": domain, "batch_id": batch_id})
    proc = _invoke(["run-operation", macro, "--args", args], target_path)
    ok = proc.returncode == 0
    result = StageResult(
        domain, "publish", batch_id, passed=ok,
        results=[RuleResult(macro, f"operation.{macro}", "operation",
                            "success" if ok else "error", message=None if ok else proc.stdout[-2000:])],
    )
    if not ok:
        raise QualityGateError(result)
    return result


def publish_gold(domain: str, batch_id: str) -> StageResult:
    """Upsert the audited batch from `<domain>_gold_staging` into `<domain>_gold`."""
    return _operation(domain, "publish_gold", batch_id)


def rollback_gold(domain: str, batch_id: str) -> StageResult:
    """Remove a published batch from every `<domain>_gold` table."""
    return _operation(domain, "rollback_gold", batch_id)
