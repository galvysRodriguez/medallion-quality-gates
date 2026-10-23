"""Example consumer: runs whenever any domain publishes gold (data-aware scheduling).

Replace the task with whatever should react to fresh gold data — refresh a BI extract,
export to a partner, train a model. It reports the latest published batch per gold table.
"""

from __future__ import annotations

import logging
from datetime import datetime
from functools import reduce

import psycopg
from airflow.sdk import Asset, dag, task

from pipeline.config import load_domains, warehouse_conninfo

log = logging.getLogger(__name__)
DOMAINS = load_domains()


@dag(
    schedule=reduce(lambda a, b: a | b, (Asset(name=d.gold_schema, uri=d.gold_asset_uri) for d in DOMAINS.values())),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "consumer"],
    doc_md=__doc__,
)
def gold_freshness():
    @task
    def latest_batches() -> dict:
        report = {}
        with psycopg.connect(warehouse_conninfo()) as conn:
            for domain in DOMAINS.values():
                tables = conn.execute(
                    "select table_name from information_schema.tables where table_schema = %s",
                    (domain.gold_schema,),
                ).fetchall()
                for (table,) in tables:
                    latest, batches = conn.execute(
                        f'select max(_batch_id), count(distinct _batch_id) from "{domain.gold_schema}"."{table}"'
                    ).fetchone()
                    report[f"{domain.gold_schema}.{table}"] = {"latest_batch": latest, "batches": batches}
                    log.info("%s.%s: latest %s (%s batches)", domain.gold_schema, table, latest, batches)
        return report

    latest_batches()


gold_freshness()
