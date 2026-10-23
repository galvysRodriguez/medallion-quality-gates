.PHONY: build up down restart logs ps shell clean venv \
        domains scenarios generate pipeline demo \
        test test-integration test-dags dbt-debug dbt-docs

# Sample-data / pipeline parameters: make generate DOMAIN=ecommerce SCENARIO=invalid_emails
DOMAIN   ?= fintech
SCENARIO ?= clean
BATCH    ?= $(shell date -d yesterday +%F)
ROWS     ?= 200

PIPELINE = docker compose run --rm airflow-cli python -m pipeline
LOCAL    = PYTHONPATH=include .venv/bin/python

build:      ## Build the Airflow image with dbt
	docker compose build

up:         ## Start everything (UI at http://localhost:8080, user/password airflow/airflow)
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

shell:      ## Shell inside an Airflow container
	docker compose run --rm --entrypoint bash airflow-cli

clean:      ## Remove containers AND volumes (data is lost)
	docker compose down -v --remove-orphans

venv:       ## Create a local .venv with dbt, the pipeline package deps and pytest
	python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

domains:    ## List data contracts
	$(PIPELINE) domains

scenarios:  ## List corruption scenarios and the rule expected to stop each one
	$(PIPELINE) scenarios

generate:   ## Write a landing batch: DOMAIN, BATCH, SCENARIO, ROWS
	$(PIPELINE) generate $(DOMAIN) --batch $(BATCH) --scenario $(SCENARIO) --rows $(ROWS)

pipeline:   ## Run the medallion pipeline for DOMAIN/BATCH outside Airflow
	$(PIPELINE) run $(DOMAIN) --batch $(BATCH)

BAD_fintech   = negative_amounts
BAD_ecommerce = order_total_mismatch
BAD_streaming = impossible_watch_time

demo:       ## Publish a clean batch, then watch a corrupted one (SCENARIO or a default) get blocked
	$(MAKE) generate pipeline SCENARIO=clean
	$(MAKE) generate BATCH=$(BATCH)_bad SCENARIO=$(if $(filter clean,$(SCENARIO)),$(BAD_$(DOMAIN)),$(SCENARIO))
	-$(MAKE) pipeline BATCH=$(BATCH)_bad

test:       ## Unit tests (no database)
	.venv/bin/pytest -m "not integration"

test-integration: ## Every scenario end to end; needs the warehouse on localhost:5433
	.venv/bin/pytest -m integration

test-dags:  ## DAG integrity tests inside the Airflow image
	docker compose run --rm --entrypoint bash airflow-cli -c "cd /opt/airflow && pytest tests/test_dags.py"

dbt-debug:  ## Check dbt's connection to the warehouse
	docker compose run --rm dbt debug

dbt-docs:   ## Generate dbt docs (rule catalog with descriptions) into dbt/target
	docker compose run --rm dbt docs generate
