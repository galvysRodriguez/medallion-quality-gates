# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Domain-agnostic medallion pipeline (landing files → raw → bronze → silver → gold) orchestrated by Airflow 3.3.2 (Python 3.12, LocalExecutor) with dbt-core 1.12 / dbt-postgres on a Postgres 16 warehouse, via Docker Compose. dbt rules are quality gates: a failing rule stops the run and gold is only published if everything passed. Example domains: fintech, ecommerce, streaming. All code, comments and docs are in English.

## Commands

```bash
make build / make up / make down / make ps / make logs
make clean                                   # down -v: destroys metadata + warehouse volumes

# Pipeline (inside the Airflow image; DOMAIN, BATCH, SCENARIO, ROWS are make variables)
make scenarios                               # every corruption scenario -> expected stage + rule
make generate DOMAIN=ecommerce SCENARIO=invalid_emails BATCH=2026-10-22
make pipeline DOMAIN=ecommerce BATCH=2026-10-22
make demo DOMAIN=fintech

# Host (.venv from `make venv`; pytest.ini puts include/ on sys.path)
make test                                    # unit tests, no database
make test-integration                        # all scenarios end to end against localhost:5433 (~10 min)
.venv/bin/pytest tests/test_quality_gates.py -m integration -k "fintech and negative_amounts"
PYTHONPATH=include .venv/bin/python -m pipeline run fintech --batch 2026-10-22
make test-dags                               # DAG integrity, needs Airflow -> runs in the container

# dbt directly (dbt/ dir; --vars batch_id selects the batch, default = latest in raw)
docker compose run --rm dbt build -s path:models/fintech/silver --indirect-selection buildable --vars '{batch_id: 2026-10-22}'
cd dbt && ../.venv/bin/dbt parse --profiles-dir .
```

## Architecture

- **Single code path.** `include/pipeline/orchestration.py` (`load`, `build`, `publish`, `run_pipeline`, `summarize`) is called by the DAGs, the CLI (`python -m pipeline`) and the tests. Every stage returns a `StageResult` or raises `QualityGateError` (stage + failing rules), and writes `data/reports/<domain>/<batch>/<stage>.json`.
- **Domains are configuration.** `include/domains/<domain>.yml` is the structural contract (entities, file, format csv/json/jsonl, columns). `dags/medallion_pipelines.py` creates `medallion_<domain>` for each YAML. dbt schemas come from the folder layout via `dbt/macros/generate_schema_name.sql`: `models/<domain>/<layer>/` → `<domain>_<layer>`, with gold going to `<domain>_gold_staging`. Model names: `brz_/slv_/gld_<domain>__<entity>`.
- **Loader** (`loader.py`) only checks structure (files exist, parse, contract columns present). It writes every value as TEXT into `<domain>_raw.<entity>`, append-only with `_batch_id` (reloading a batch replaces it), so bad values reach dbt and the rules decide.
- **Batch scoping.** Each landing batch is a full snapshot. Bronze models are views from `select_current_batch()` filtered on `var('batch_id')`. Silver and gold are rebuilt from that batch only, so a bad batch never poisons later runs.
- **Gates.** `dbt_runner.build_layer` runs `dbt build --select path:models/<domain>/<layer> --indirect-selection buildable`. `buildable` makes cross-layer rules (e.g. gold `reconciles_with` silver) run with the downstream layer only. It parses `run_results.json`, and target paths are per domain/layer (`dbt/target/<domain>/<layer>`) so runs don't clash.
- **Write-Audit-Publish.** The `publish_gold` macro (`dbt run-operation`) swaps `<domain>_gold_staging` → `<domain>_gold` in one DO block, keeps `<domain>_gold_previous` (`rollback_gold` restores it), and re-grants `gold_reader`. `run_query` already commits — don't add `adapter.commit()`.
- **Rules.** Generic tests in `dbt/macros/rules/` (castable via `pg_input_is_valid` — PG16+, value_between, matches_pattern, not_in_future, expression_is_true, unique_combination, row_count_between, reconciles_with) plus dbt built-ins. Project-wide defaults: `severity: error` and `store_failures` into `dq_audit`. Every rule has an explicit `name` prefixed with its domain; that name is the rule id in reports, in `dq_audit`, and in scenario expectations. Test args use the `arguments:` syntax.
- **Generators** (`include/pipeline/generators/`): `generate_clean` must satisfy every rule. Each `Scenario` declares `expected_stage` + `expected_rule`. Generic format-level scenarios (missing_column/missing_file/malformed_record/empty_file) come from `generic_targets`. `tests/test_contracts.py` checks statically that every expected rule exists in the matching layer's YAML. `tests/test_quality_gates.py` checks end to end that the pipeline stops exactly there and that gold still holds the baseline batch.
- **Airflow tasks:** resolve_batch → load_raw → bronze → silver → gold → publish_gold, plus `quality_report` (trigger_rule all_done). `quality_report` raises when the batch was blocked, so the DAG run ends failed. `generate_sample_batch` DAG writes a batch from UI params and triggers the domain DAG with `conf.batch_id`.

## Adding things

- Rule: add it to the layer YAML with `name` + `description`. If a generator scenario should trip it, add the `Scenario` and keep clean data passing.
- Domain: contract YAML + `dbt/models/<domain>/{_sources.yml,bronze,silver,gold}` + generator registered in `generators/__init__.py`. `test_contracts.py` requires all three layers and scenarios covering load/bronze/silver/gold.

## Environment gotchas

- Fedora with SELinux Enforcing: every bind mount in `docker-compose.yaml` needs `:z`.
- The user was not in the `docker` group at creation. Rootless `podman` works without it; images are fully qualified (`docker.io/...`) for podman's short-name policy.
- In containers, `PYTHONPATH=/opt/airflow/include` and `PIPELINE_DATA_DIR=/opt/airflow/data`. Tests set `PIPELINE_DATA_DIR` to a temp dir in `tests/conftest.py` before importing `pipeline.config`, whose paths are module-level constants.
- `docker/warehouse/init.sql` (roles `gold_reader`, `analyst`) runs only on a fresh warehouse volume.
- Stay on dbt-core 1.x (ignore the "dbt v2 is available" banner).
