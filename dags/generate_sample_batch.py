"""Writes a synthetic landing batch (clean or corrupted) and optionally triggers its medallion DAG.

Use it from the UI to watch a quality gate stop a pipeline: pick a domain and a scenario
(`python -m pipeline scenarios` lists which rule each scenario must trip).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from pipeline.generators import GENERATORS, generate

SCENARIOS = sorted({"clean", *(s for g in GENERATORS.values() for s in g.scenarios())})


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "sample-data"],
    params={
        "domain": Param("fintech", enum=sorted(GENERATORS)),
        "scenario": Param("clean", enum=SCENARIOS, description="Not every scenario exists for every domain."),
        "batch_id": Param(None, type=["null", "string"], description="Default: yesterday's date."),
        "rows": Param(200, type="integer", minimum=10, maximum=100_000),
        "trigger_pipeline": Param(True, type="boolean"),
    },
    doc_md=__doc__,
)
def generate_sample_batch():
    @task
    def write_batch(params=None) -> str:
        batch_id = params["batch_id"] or (date.today() - timedelta(days=1)).isoformat()
        path = generate(params["domain"], batch_id, params["scenario"], params["rows"])
        print(f"wrote {path}")
        return batch_id

    @task.short_circuit
    def should_trigger(params=None) -> bool:
        return params["trigger_pipeline"]

    batch_id = write_batch()
    trigger = TriggerDagRunOperator(
        task_id="trigger_pipeline",
        trigger_dag_id="medallion_{{ params.domain }}",
        conf={"batch_id": "{{ ti.xcom_pull(task_ids='write_batch') }}"},
    )
    batch_id >> should_trigger() >> trigger


generate_sample_batch()
