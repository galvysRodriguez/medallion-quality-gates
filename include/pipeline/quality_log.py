"""Durable quality history in the warehouse.

dbt's store_failures tables (dq_audit.<rule>) only keep the latest failing rows. These
tables keep one row per pipeline run and per rule evaluation, so quality can be tracked
over time:

    dq_audit.pipeline_runs   one row per run (status, failed stage and rules)
    dq_audit.rule_results    one row per rule/node evaluated in a stage
    dq_audit.rule_health     view: runs, failures and last failure per rule

Recording is best effort: a logging problem must never change a gate's verdict.
"""

from __future__ import annotations

import logging

import psycopg
from psycopg.types.json import Jsonb

from pipeline.config import warehouse_conninfo
from pipeline.dbt_runner import StageResult

log = logging.getLogger(__name__)

DDL = """
create schema if not exists dq_audit;

create table if not exists dq_audit.pipeline_runs (
    run_id        uuid primary key,
    domain        text not null,
    batch_id      text not null,
    status        text not null,
    failed_stage  text,
    failed_rules  text[] not null default '{}',
    warned_rules  text[] not null default '{}',
    stages        jsonb,
    recorded_at   timestamptz not null default now()
);

create table if not exists dq_audit.rule_results (
    run_id        uuid not null,
    domain        text not null,
    batch_id      text not null,
    stage         text not null,
    rule          text not null,
    resource_type text not null,
    status        text not null,
    failures      bigint,
    message       text,
    recorded_at   timestamptz not null default now()
);
create index if not exists rule_results_domain_rule on dq_audit.rule_results (domain, rule, recorded_at);

create or replace view dq_audit.rule_health as
select
    domain,
    stage,
    rule,
    count(*) as evaluations,
    count(*) filter (where status in ('fail', 'error')) as failed,
    count(*) filter (where status = 'warn') as warned,
    round(100.0 * count(*) filter (where status in ('fail', 'error')) / count(*), 1) as failure_rate_pct,
    max(recorded_at) filter (where status in ('fail', 'error')) as last_failed_at,
    max(recorded_at) as last_evaluated_at
from dq_audit.rule_results
where resource_type in ('test', 'contract')
group by domain, stage, rule;
"""


def _execute(statements: list[tuple[str, tuple]], conninfo: str | None) -> None:
    try:
        with psycopg.connect(conninfo or warehouse_conninfo(), connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute(DDL)
            for sql, params in statements:
                cur.execute(sql, params)
    except psycopg.Error as exc:
        log.warning("could not record quality history: %s", exc)


def record_stage(result: StageResult, run_id: str, conninfo: str | None = None) -> None:
    _execute(
        [
            (
                "insert into dq_audit.rule_results "
                "(run_id, domain, batch_id, stage, rule, resource_type, status, failures, message) "
                "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (run_id, result.domain, result.batch_id, result.stage, r.name, r.resource_type,
                 r.status, r.failures, r.message),
            )
            for r in result.results
        ],
        conninfo,
    )


def record_run(summary: dict, run_id: str, conninfo: str | None = None) -> None:
    _execute(
        [(
            "insert into dq_audit.pipeline_runs "
            "(run_id, domain, batch_id, status, failed_stage, failed_rules, warned_rules, stages) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            "on conflict (run_id) do update set status = excluded.status, "
            "failed_stage = excluded.failed_stage, failed_rules = excluded.failed_rules, "
            "warned_rules = excluded.warned_rules, stages = excluded.stages, recorded_at = now()",
            (run_id, summary["domain"], summary["batch_id"], summary["status"], summary["failed_stage"],
             summary["failed_rules"], summary["warned_rules"], Jsonb(summary["stages"])),
        )],
        conninfo,
    )
