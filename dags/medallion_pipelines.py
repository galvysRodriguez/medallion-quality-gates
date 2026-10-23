"""One medallion DAG per data contract in include/domains/*.yml.

    resolve_batch -> load_raw -> bronze -> silver -> gold -> publish_gold -> quality_report

Every gate fails fast (no retries: bad data stays bad), so downstream layers never run on
a rejected batch and consumers keep reading the last published <domain>_gold.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowFailException

from pipeline import orchestration
from pipeline.config import LAYERS, Domain, load_domains
from pipeline.dbt_runner import QualityGateError

log = logging.getLogger(__name__)


def _gate(stage_fn, *args) -> dict:
    try:
        return stage_fn(*args).to_dict()
    except QualityGateError as exc:
        for rule in exc.result.failed:
            log.error("rule %s: %s", rule.name, rule.message)
        raise AirflowFailException(str(exc)) from exc


def build_dag(domain: Domain):
    @dag(
        dag_id=f"medallion_{domain.name}",
        description=domain.description,
        schedule=domain.schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 0},
        params={
            "batch_id": Param(
                None, type=["null", "string"],
                description="Landing batch (folder under data/landing/<domain>/). Empty: the run's date, else the latest.",
            ),
        },
        tags=["medallion", domain.name],
        doc_md=__doc__,
    )
    def medallion():
        @task
        def resolve_batch(params=None, logical_date=None) -> str:
            return orchestration.resolve_batch(domain.name, (params or {}).get("batch_id"), logical_date)

        @task
        def load_raw(batch_id: str) -> dict:
            return _gate(orchestration.load, domain.name, batch_id)

        @task
        def build_layer(layer: str, batch_id: str) -> dict:
            return _gate(orchestration.build, domain.name, layer, batch_id)

        @task
        def publish_gold(batch_id: str) -> dict:
            return _gate(orchestration.publish, domain.name, batch_id)

        @task(trigger_rule="all_done")
        def quality_report(batch_id: str) -> dict:
            summary = orchestration.finalize(domain.name, batch_id)  # report + dq_audit history + alert
            log.info("quality report: %s", summary)
            if summary["status"] != "published":
                raise AirflowFailException(
                    f"batch {batch_id} blocked at {summary['failed_stage']}: {', '.join(summary['failed_rules'])}"
                )
            return summary

        batch_id = resolve_batch()
        gates = [load_raw(batch_id)]
        for layer in LAYERS:
            gates.append(build_layer.override(task_id=layer)(layer, batch_id))
        gates.append(publish_gold(batch_id))
        for upstream, downstream in zip(gates, gates[1:]):
            upstream >> downstream
        gates >> quality_report(batch_id)

    return medallion()


for _domain in load_domains().values():
    globals()[f"medallion_{_domain.name}"] = build_dag(_domain)
