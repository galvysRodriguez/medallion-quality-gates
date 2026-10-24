# Medallion data quality pipeline — Airflow + dbt + Docker

A local, domain-agnostic pipeline that moves landing files through **Bronze → Silver → Gold**,
orchestrated by Airflow, and **stops automatically when data breaks a dbt quality rule**.
Consumers only ever read gold data that passed every rule (Write-Audit-Publish), and gold keeps
the full history of published batches.

It ships with three example domains — **fintech**, **ecommerce**, **streaming** — each with a
typed data contract, dbt models, business rules, and a sample-data generator that produces clean
batches or batches corrupted in a specific way.

| Component | Version | Where |
|---|---|---|
| Apache Airflow | 3.3.2 (Python 3.12, LocalExecutor) | http://localhost:8080 — `airflow` / `airflow` |
| dbt-core + dbt-postgres | 1.12.6 / 1.11.0 | isolated venv `/opt/airflow/dbt_venv` in the image |
| Postgres (Airflow metadata) | 16 | internal |
| Postgres (warehouse) | 16 | `localhost:5433` — `dbt`/`dbt` (read-only consumer: `analyst`/`analyst`) |

## How it works

```
data/landing/<domain>/<batch>/*.csv|json|jsonl + _SUCCESS
        │
        ▼  wait_for_batch  rescheduling sensor: waits for the completion marker (no worker held)
        ▼  load_raw        structural contract (include/domains/<domain>.yml): files exist,
        │                  parse, have the declared columns → <domain>_raw (all TEXT, append-only)
        ▼  bronze          views over the current batch; rules generated from the contract:
        │                  not empty, required columns present, typed columns castable
        ▼  silver          typed, trimmed, deduplicated tables; rules: uniqueness, accepted
        │                  values, ranges, referential integrity, row-level business rules
        ▼  gold            business tables built in <domain>_gold_staging; rules: aggregate
        │                  invariants and cross-layer reconciliation
        ▼  publish_gold    upsert the audited batch into <domain>_gold (one transaction),
        │                  update the Asset medallion://<domain>/gold
        ▼  quality_report  report + dq_audit history + alert when blocked
```

* Each layer is one `dbt build` (models **and** their rules). A failing `error`-severity rule
  fails the Airflow task (no retries), so the next layers never run.
* Gold is audited in a staging schema that holds only the current batch. Publishing replaces
  that batch's partition (`_batch_id`) in `<domain>_gold` atomically; re-processing a batch is
  idempotent and `make rollback` unpublishes one. A blocked batch never reaches `<domain>_gold`.
* The `analyst` login (role `gold_reader`) can read `<domain>_gold` and nothing else.
* Rules with `severity: warn` are reported but don't stop the pipeline.
* Scheduled runs process the previous day's batch (`arrival.batch_lag_days`); manual runs take
  the `batch_id` param or the latest complete batch.

## Example rules

| Domain | Bronze (generated) | Silver | Gold |
|---|---|---|---|
| fintech | amount numeric, ids present | unique ids, amount > 0 and ≤ 10,000, ISO currency matching the account, card payments carry an MCC, no posted movements on frozen/closed accounts, no future timestamps | no account beyond its credit limit, gold reconciles with silver |
| ecommerce | quantity integer, prices numeric | valid email, order lifecycle status, 1–1,000 units per line, discount ≤ line amount, order total = sum of lines, no discontinued products, every order has lines | daily average order value ≤ 5,000, revenue reconciles with silver |
| streaming | watch_seconds integer, user present | playhead within content duration, completion needs 90% played, free plan not on TV, no plays before signup | ≤ 24 h watched per user per day, watch hours reconcile |

`make scenarios` lists every corruption scenario and the exact rule expected to stop it:

```
fintech
  negative_amounts               silver  fintech_txn_amount_positive
  overdraft_beyond_limit         gold    fintech_no_overdraft_beyond_credit_limit
  missing_column                 load    landing_contract
  ...
```

## Getting started

```bash
make build && make up            # Airflow + both Postgres
```

In the UI, trigger **`generate_sample_batch`** with a domain and a scenario (`clean` or one of
the corruptions). It writes the files under `data/landing/` and triggers `medallion_<domain>`.
A clean batch ends in `publish_gold`; a corrupted one stops at the gate that owns the rule,
and `quality_report` fails listing the broken rules. `gold_freshness` runs after every publish.

Without the UI:

```bash
make demo DOMAIN=ecommerce                         # publish a clean batch, then get one blocked
make generate DOMAIN=streaming SCENARIO=free_user_on_tv BATCH=2026-10-22
make pipeline DOMAIN=streaming BATCH=2026-10-22
make rollback DOMAIN=streaming BATCH=2026-10-22    # unpublish a batch from gold
```

Consumers query gold only:

```bash
psql -h localhost -p 5433 -U analyst warehouse \
  -c 'select * from fintech_gold.gld_fintech__daily_summary order by position_date'
```

## Quality history and alerts

| Table / view | Contents |
|---|---|
| `dq_audit.pipeline_runs` | one row per run: status (`published`, `blocked`, `not_arrived`), failed stage and rules |
| `dq_audit.rule_results` | one row per rule evaluation, per stage and run |
| `dq_audit.rule_health` | per rule: evaluations, failures, failure rate, last failure |
| `dq_audit.<rule_name>` | failing rows of the rule's latest evaluation (dbt `store_failures`) |

```sql
select domain, rule, failed, failure_rate_pct, last_failed_at
from dq_audit.rule_health where failed > 0 order by last_failed_at desc;
```

Set `ALERT_WEBHOOK_URL` in `.env` to an incoming webhook (Slack, Mattermost, Google Chat) to be
notified when a batch is blocked or never lands; `ALERT_ON_WARNINGS=true` also reports published
batches with warnings. Without a webhook alerts are only logged.

## Making it yours

**New rule** — add it to the domain's `silver/_silver.yml` or `gold/_gold.yml`, using dbt's
built-in tests or the rule library in `dbt/macros/rules/` (`expression_is_true` accepts any SQL
predicate). Give it a `name` (its id in reports and in `dq_audit`) and a `description`.

**New domain** — nothing in the engine or the DAG code changes:

1. `include/domains/<domain>.yml` — entities, file, format (`csv`/`json`/`jsonl`), columns with
   their type (`text`, `integer`, `numeric`, `date`, `timestamptz`, `boolean`) and `required`,
   plus the `arrival` settings.
2. `make scaffold` — generates `_sources.yml`, the bronze views and the bronze rules.
3. Write `dbt/models/<domain>/silver/` and `gold/` models and their rules. Every gold model must
   carry `_batch_id`.
4. Optionally a generator in `include/pipeline/generators/` to produce test batches.

A `medallion_<domain>` DAG and its gold Asset appear automatically for every contract. Generated
files must not be edited by hand: change the contract and re-run `make scaffold` (CI checks it).

## Tests and CI

```bash
make venv                     # host .venv with dbt, psycopg, pytest
make test                     # unit: contracts, generators, loader, alerts, scaffold (no database)
make test-integration         # every scenario end to end; needs the warehouse on localhost:5433
make test-dags                # DAG integrity inside the Airflow image
```

The integration suite publishes a clean baseline per domain, then runs every corruption scenario
and asserts it was stopped **at the expected stage by the expected rule** and that gold still
holds only the audited batches. GitHub Actions (`.github/workflows/ci.yml`) runs the unit tests,
`scaffold --check` and `dbt parse`, the integration suite against Postgres 16 (including the
`analyst` grants), the DAG tests on Airflow 3.3.2, and the image build.

## Layout

```
include/domains/          typed data contracts, one YAML per domain
include/pipeline/         engine: loader, dbt runner, orchestration, scaffold, quality log,
                          alerts, CLI, generators
dags/                     medallion_pipelines.py (DAG factory), generate_sample_batch.py,
                          gold_freshness.py (Asset-scheduled consumer example)
dbt/macros/rules/         reusable rule library (generic tests)
dbt/macros/               schema convention, batch filter, publish/rollback
dbt/models/<domain>/      generated _sources + bronze; hand-written silver and gold
tests/                    pytest suite
.github/workflows/ci.yml  CI
docker/, docker-compose.yaml, Makefile
```

## Notes

- Fedora runs SELinux in Enforcing mode: the compose bind mounts use `:z`.
- Your user must belong to the `docker` group (or use `sudo docker ...`).
- `.env` contains the Fernet key and JWT secret; do not commit it. Start from `.env.example`.
- The warehouse init script (roles `gold_reader`, `analyst`) only runs on a fresh volume:
  `make clean` first if the warehouse was created before.
