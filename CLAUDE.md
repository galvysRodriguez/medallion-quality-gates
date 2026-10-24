# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Domain-agnostic medallion pipeline (landing files → raw → bronze → silver → gold) orchestrated by Airflow 3.3.2 (Python 3.12, LocalExecutor) with dbt-core 1.12 / dbt-postgres on a Postgres 16 warehouse, via Docker Compose. dbt rules are quality gates: a failing rule stops the run, and a batch is only published into gold (which keeps every published batch) if everything passed. Example domains: fintech, ecommerce, streaming. All code, comments and docs are in English.

## Commands

```bash
make build / make up / make down / make ps / make logs
make clean                                   # down -v: destroys metadata + warehouse volumes

# Pipeline (inside the Airflow image; DOMAIN, BATCH, SCENARIO, ROWS are make variables)
make scenarios                               # every corruption scenario -> expected stage + rule
make generate DOMAIN=ecommerce SCENARIO=invalid_emails BATCH=2026-10-22
make pipeline DOMAIN=ecommerce BATCH=2026-10-22
make rollback DOMAIN=ecommerce BATCH=2026-10-22  # unpublish a batch from gold
make demo DOMAIN=fintech
make scaffold                                # regenerate _sources.yml + bronze from include/domains/*.yml (host .venv)
make docs                                    # regenerate docs/rules/ (one page per silver/gold rule + per-domain lineage) from the dbt YAML

# Host (.venv from `make venv`; pytest.ini puts include/ on sys.path)
make test                                    # unit tests, no database (includes scaffold drift check)
make test-integration                        # all scenarios end to end against localhost:5433 (~10 min)
.venv/bin/pytest tests/test_quality_gates.py -m integration -k "fintech and negative_amounts"
PYTHONPATH=include .venv/bin/python -m pipeline run fintech --batch 2026-10-22
make test-dags                               # DAG integrity, needs Airflow -> runs in the container

# dbt directly (dbt/ dir; --vars batch_id selects the batch, default = latest in raw)
docker compose run --rm dbt build -s path:models/fintech/silver --indirect-selection buildable --vars '{batch_id: 2026-10-22}'
cd dbt && ../.venv/bin/dbt parse --profiles-dir .
```

## Architecture

- **Single code path.** `include/pipeline/orchestration.py` (`resolve_batch`, `batch_ready`, `load`, `build`, `publish`, `rollback`, `run_pipeline`, `summarize`, `finalize`) is called by the DAGs, the CLI (`python -m pipeline`) and the tests. Every stage returns a `StageResult` or raises `QualityGateError` (stage + failing rules). Either way it writes `data/reports/<domain>/<batch>/<stage>.json` and `dq_audit.rule_results`. `load` starts a run (new `run_id` in `run.json`). `finalize` writes `quality_report.json` and `dq_audit.pipeline_runs`, and alerts via `alerts.py` (`ALERT_WEBHOOK_URL`). History writes are best effort and never change a verdict.
- **Domains are configuration.** `include/domains/<domain>.yml` is the contract. It lists entities (file, format csv/json/jsonl), columns with type (`text|integer|numeric|date|timestamptz|boolean`) and `required`, plus `arrival` (marker, timeout, poke, batch_lag_days). `scaffold.py` generates `models/<domain>/_sources.yml`, the bronze `.sql` and `bronze/_bronze.yml` from it. Those files carry a GENERATED header: never edit them, change the contract and run `make scaffold` (`test_contracts.py` and CI fail on drift). Bronze rule names are `<domain>_bronze_<entity>_<column>_{present,is_<type>}` and `<domain>_bronze_<entity>_not_empty`. `dags/medallion_pipelines.py` creates `medallion_<domain>` for each YAML. dbt schemas come from the folder layout via `dbt/macros/generate_schema_name.sql`: `models/<domain>/<layer>/` → `<domain>_<layer>`, with gold going to `<domain>_gold_staging`. Model names: `brz_/slv_/gld_<domain>__<entity>`.
- **Loader** (`loader.py`) only checks structure (files exist, parse, contract columns present). It writes every value as TEXT into `<domain>_raw.<entity>`, append-only with `_batch_id` (reloading a batch replaces it), so bad values reach dbt and the rules decide.
- **Batch scoping.** Each landing batch is a full snapshot. Bronze models are views from `select_current_batch()` filtered on `var('batch_id')`. Silver and gold are rebuilt from that batch only, so a bad batch never poisons later runs.
- **Gates.** `dbt_runner.build_layer` runs `dbt build --select path:models/<domain>/<layer> --indirect-selection buildable`. `buildable` makes cross-layer rules (e.g. gold `reconciles_with` silver) run with the downstream layer only. It parses `run_results.json`, and target paths are per domain/layer (`dbt/target/<domain>/<layer>`) so runs don't clash.
- **Write-Audit-Publish with history.** `<domain>_gold_staging` holds only the current batch. The `publish_gold(domain, batch_id)` macro (`dbt run-operation`) upserts it into `<domain>_gold` in one DO block. For each table it creates the live table `like` staging, adds new columns, deletes the batch's rows and inserts. It refuses staging rows from another batch and re-grants `gold_reader`. So every gold model must have `_batch_id`. `rollback_gold(domain, batch_id)` deletes one batch. `run_query` already commits — don't add `adapter.commit()`.
- **Rules.** Generic tests in `dbt/macros/rules/` (castable via `pg_input_is_valid` — PG16+, value_between, matches_pattern, not_in_future, expression_is_true, unique_combination, row_count_between, reconciles_with) plus dbt built-ins. Project-wide defaults: `severity: error` and `store_failures` into `dq_audit`. Every rule has an explicit `name` prefixed with its domain; that name is the rule id in reports, in `dq_audit`, and in scenario expectations. Test args use the `arguments:` syntax.
- **Generators** (`include/pipeline/generators/`): `generate_clean` must satisfy every rule. Each `Scenario` declares `expected_stage` + `expected_rule`. Generic format-level scenarios (missing_column/missing_file/malformed_record/empty_file) come from `generic_targets`. `tests/test_contracts.py` checks statically that every expected rule exists in the matching layer's YAML. `tests/test_quality_gates.py` checks end to end that the pipeline stops exactly there and that gold still holds the baseline batch.
- **Airflow tasks:** resolve_batch → wait_for_batch (`@task.sensor`, mode reschedule, waits for the contract marker `_SUCCESS`) → load_raw → bronze → silver → gold → publish_gold, plus `quality_report` (trigger_rule all_done). Scheduled runs process logical date minus `batch_lag_days`; manual runs use the `batch_id` param or the latest complete batch. `publish_gold` has outlet Asset `medallion://<domain>/gold` (`Domain.gold_asset_uri`); `gold_freshness` is scheduled on any of them. `quality_report` raises when the batch was blocked or `not_arrived`, so the DAG run ends failed. The `generate_sample_batch` DAG writes a batch (including the marker) from UI params and triggers the domain DAG with `conf.batch_id`.

## Adding things

- Rule: add it to the layer YAML with `name` + `description` (required for silver/gold, enforced by a test), then `make docs`. `docs/rules/` is generated by `rule_docs.py`, including the Mermaid lineage built from `ref()`/`source()` in the model SQL. Never edit it by hand; CI runs `docs --check`. `docs/architecture.md` holds hand-written Mermaid diagrams: update them when the flow changes. Inside Mermaid, write placeholders as `{domain}`, never `<domain>` or `&lt;`, because GitHub sanitizes them. If a generator scenario should trip it, add the `Scenario` and keep clean data passing.
- Domain: contract YAML → `make scaffold` → hand-write `silver/` and `gold/` (gold models must include `_batch_id`) + a generator registered in `generators/__init__.py`. `test_contracts.py` requires all three layers and scenarios covering load/bronze/silver/gold.
- CI: `.github/workflows/ci.yml` runs jobs unit (pytest + `scaffold --check` + `dbt parse`), integration (Postgres 16 service on 5433, applies `init.sql`, checks `analyst` can read gold), dags (pip-installed Airflow 3.3.2 with constraints) and image (docker build).

## Environment gotchas

- Fedora with SELinux Enforcing: every bind mount in `docker-compose.yaml` needs `:z`.
- The user was not in the `docker` group at creation. Rootless `podman` works without it; images are fully qualified (`docker.io/...`) for podman's short-name policy.
- In containers, `PYTHONPATH=/opt/airflow/include` and `PIPELINE_DATA_DIR=/opt/airflow/data`. Tests set `PIPELINE_DATA_DIR` to a temp dir in `tests/conftest.py` before importing `pipeline.config`, whose paths are module-level constants.
- `docker/warehouse/init.sql` (roles `gold_reader`, `analyst`) runs only on a fresh warehouse volume.
- Airflow 3.3 APIs: `DagBag` lives in `airflow.dag_processing.dagbag` (no `include_examples`), `AirflowFailException` in `airflow.sdk.exceptions`. Running the image as a non-default uid needs `HOME=/home/airflow`, because packages are in the airflow user site. The entrypoint only sets it when the uid has no passwd entry, and podman adds one, so `docker-compose.yaml` sets `HOME` explicitly.
- `max_active_runs=1` per medallion DAG is required: runs of a domain share the bronze, silver and gold_staging schemas. A scheduled run waiting in `wait_for_batch` blocks manual runs of that domain.
- Stay on dbt-core 1.x (ignore the "dbt v2 is available" banner).
