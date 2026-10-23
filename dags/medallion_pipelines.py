"""One medallion DAG per data contract in include/domains/*.yml.

    resolve_batch -> wait_for_batch -> load_raw -> bronze -> silver -> gold -> publish_gold
                                                                      quality_report (all_done)

wait_for_batch is a rescheduling sensor: it waits for the contract's completion marker
without holding a worker slot. Every gate fails fast (no retries: bad data stays bad), so
downstream layers never run on a rejected batch and consumers only see audited batches in
<domain>_gold. publish_gold updates the Asset medallion://<domain>/gold, so downstream DAGs
can be scheduled on fresh gold data (see gold_freshness.py).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import Asset, Param, PokeReturnValue, dag, task
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
                description=(
                    "Landing batch (folder under data/landing/<domain>/). Empty: scheduled runs take "
                    "their date minus batch_lag_days, manual runs the latest complete batch."
                ),
            ),
        },
        tags=["medallion", domain.name],
        doc_md=__doc__,
    )
    def medallion():
        @task
        def resolve_batch(params=None, dag_run=None, logical_date=None) -> str:
            scheduled = dag_run is not None and dag_run.run_type == "scheduled"
            return orchestration.resolve_batch(
                domain.name, (params or {}).get("batch_id"), logical_date if scheduled else None
            )

        @task.sensor(
            mode="reschedule",
            poke_interval=timedelta(minutes=domain.arrival.poke_minutes),
            timeout=timedelta(minutes=domain.arrival.timeout_minutes),
        )
        def wait_for_batch(batch_id: str) -> PokeReturnValue:
            ready = orchestration.batch_ready(domain.name, batch_id)
            if not ready:
                log.info("waiting for %s (marker: %s)", domain.batch_dir(batch_id), domain.arrival.marker)
            return PokeReturnValue(is_done=ready, xcom_value=batch_id)

        @task
        def load_raw(batch_id: str) -> dict:
            return _gate(orchestration.load, domain.name, batch_id)

        @task
        def build_layer(layer: str, batch_id: str) -> dict:
            return _gate(orchestration.build, domain.name, layer, batch_id)

        @task(outlets=[Asset(name=domain.gold_schema, uri=domain.gold_asset_uri)])
        def publish_gold(batch_id: str) -> dict:
            return _gate(orchestration.publish, domain.name, batch_id)

        @task(trigger_rule="all_done")
        def quality_report(batch_id: str) -> dict:
            summary = orchestration.finalize(domain.name, batch_id)  # report + dq_audit history + alert
            log.info("quality report: %s", summary)
            if summary["status"] == "not_arrived":
                raise AirflowFailException(f"batch {batch_id} never landed")
            if summary["status"] != "published":
                raise AirflowFailException(
                    f"batch {batch_id} blocked at {summary['failed_stage']}: {', '.join(summary['failed_rules'])}"
                )
            return summary

        batch_id = wait_for_batch(resolve_batch())
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
