"""DAG integrity. Runs where Airflow is installed: `make test-dags` (inside the container)."""

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

from pipeline.config import PROJECT_ROOT, load_domains  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_one_medallion_dag_per_domain(dagbag):
    for name in load_domains():
        assert f"medallion_{name}" in dagbag.dag_ids


@pytest.mark.parametrize("domain", sorted(load_domains()))
def test_gates_run_in_medallion_order(dagbag, domain):
    dag = dagbag.dags[f"medallion_{domain}"]
    chain = ["resolve_batch", "load_raw", "bronze", "silver", "gold", "publish_gold"]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert dag.get_task("quality_report").trigger_rule == "all_done"
