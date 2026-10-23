# Medallion data quality pipeline — Airflow + dbt + Docker

A local, domain-agnostic pipeline that moves landing files through **Bronze → Silver → Gold**,
orchestrated by Airflow, and **stops automatically when data breaks a dbt quality rule**.
Consumers only ever read gold data that passed every rule (Write-Audit-Publish).

It ships with three example domains — **fintech**, **ecommerce**, **streaming** — each with a
data contract, dbt models, business rules, and a sample-data generator that can produce clean
batches or batches corrupted in a specific way.

| Component | Version | Where |
|---|---|---|
| Apache Airflow | 3.3.2 (Python 3.12, LocalExecutor) | http://localhost:8080 — `airflow` / `airflow` |
| dbt-core + dbt-postgres | 1.12.6 / 1.11.0 | isolated venv `/opt/airflow/dbt_venv` in the image |
| Postgres (Airflow metadata) | 16 | internal |
| Postgres (warehouse) | 16 | `localhost:5433` — `dbt`/`dbt` (read-only consumer: `analyst`/`analyst`) |

## How it works

```
data/landing/<domain>/<batch>/*.csv|json|jsonl
        │
        ▼  load_raw        structural contract (include/domains/<domain>.yml): files exist,
        │                  parse, have the declared columns → <domain>_raw (all TEXT, append-only)
        ▼  bronze          dbt views over the current batch; rules: not empty, keys present,
        │                  values castable to their types
        ▼  silver          typed, trimmed, deduplicated tables; rules: uniqueness, accepted
        │                  values, ranges, referential integrity, row-level business rules
        ▼  gold            business tables built in <domain>_gold_staging; rules: aggregate
        │                  invariants and cross-layer reconciliation
        ▼  publish_gold    atomic schema swap: <domain>_gold_staging → <domain>_gold
        ▼  quality_report  data/reports/<domain>/<batch>/quality_report.json
```

* Each layer is one `dbt build` (models **and** their rules). A failing `error`-severity rule
  fails the Airflow task (`AirflowFailException`, no retries), so the next layers never run.
* Gold is audited in a staging schema and swapped into `<domain>_gold` in a single transaction
  only when everything passed. A blocked batch leaves consumers on the last good version;
  `<domain>_gold_previous` keeps the version before that for rollback.
* Failing rows of every rule are stored in `dq_audit.<rule_name>` for investigation.
* Rules with `severity: warn` are reported but don't stop the pipeline.

## Example rules

| Domain | Bronze | Silver | Gold |
|---|---|---|---|
| fintech | amount is numeric, ids present | unique ids, amount > 0 and ≤ 10,000, ISO currency matching the account, card payments carry an MCC, no posted movements on frozen/closed accounts, no future timestamps | no account beyond its credit limit, gold reconciles with silver |
| ecommerce | quantity is an integer, prices numeric | valid email, order lifecycle status, 1–1,000 units per line, discount ≤ line amount, order total = sum of lines, no discontinued products, every order has lines | daily average order value ≤ 5,000, revenue reconciles with silver |
| streaming | watch_seconds is an integer, user present | playhead within content duration, completion needs 90% played, free plan not on TV, no plays before signup | ≤ 24 h watched per user per day, watch hours reconcile |

`python -m pipeline scenarios` (or `make scenarios`) lists every corruption scenario and the
exact rule expected to stop it:

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
and `quality_report` fails listing the broken rules.

Without the UI:

```bash
make demo DOMAIN=ecommerce                         # publish a clean batch, then get one blocked
make generate DOMAIN=streaming SCENARIO=free_user_on_tv BATCH=2026-10-22
make pipeline DOMAIN=streaming BATCH=2026-10-22
```

Consumers query gold only:

```bash
psql -h localhost -p 5433 -U analyst warehouse -c 'select * from fintech_gold.gld_fintech__daily_summary'
```

## Making it yours

**New rule** — add it to the domain's `_bronze.yml` / `_silver.yml` / `_gold.yml`, using dbt's
built-in tests or the rule library in `dbt/macros/rules/` (`expression_is_true` accepts any SQL
predicate). Give it a `name` (its id in reports and in `dq_audit`) and a `description`.

**New domain** — nothing in the engine or the DAG code changes:

1. `include/domains/<domain>.yml` — entities, file name, format (`csv`/`json`/`jsonl`), columns.
2. `dbt/models/<domain>/_sources.yml` plus `bronze/`, `silver/`, `gold/` models and rules.
   Schemas are derived from the folders (`macros/generate_schema_name.sql`).
3. Optionally a generator in `include/pipeline/generators/` to produce test batches.

A `medallion_<domain>` DAG appears automatically for every contract.

## Tests

```bash
make venv                     # host .venv with dbt, psycopg, pytest
make test                     # unit: contracts, generators, loader (no database)
make test-integration         # every scenario end to end; needs the warehouse on localhost:5433
make test-dags                # DAG integrity inside the Airflow image
```

The integration suite publishes a clean baseline per domain, then runs every corruption scenario
and asserts it was stopped **at the expected stage by the expected rule**, and that
`<domain>_gold` still contains only the baseline batch.

## Layout

```
include/domains/          data contracts, one YAML per domain
include/pipeline/         engine: loader, dbt runner, orchestration, CLI, generators
dags/                     medallion_pipelines.py (DAG factory), generate_sample_batch.py
dbt/macros/rules/         reusable rule library (generic tests)
dbt/macros/               schema convention, batch filter, publish/rollback
dbt/models/<domain>/      bronze / silver / gold models and their rules
tests/                    pytest suite
data/landing, data/reports
docker/, docker-compose.yaml, Makefile
```

## Notes

- Fedora runs SELinux in Enforcing mode: the compose bind mounts use `:z`.
- Your user must belong to the `docker` group (or use `sudo docker ...`).
- `.env` contains the Fernet key and JWT secret; do not commit it.
- The warehouse init script (roles `gold_reader`, `analyst`) only runs on a fresh volume:
  `make clean` first if the warehouse was created before.
