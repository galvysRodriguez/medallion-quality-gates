# Architecture

How the pipeline is put together, in seven diagrams. They are [Mermaid](https://mermaid.js.org/)
blocks, which GitHub renders in place. The per-domain lineage diagrams are generated from the dbt
models and live in the [rule catalog](rules/README.md) (one per domain).

1. [System](#1-system) — containers, volumes and who talks to whom
2. [Medallion flow and quality gates](#2-medallion-flow-and-quality-gates)
3. [Airflow DAG](#3-airflow-dag) — the tasks of `medallion_<domain>`
4. [Write-Audit-Publish](#4-write-audit-publish) — how gold is published
5. [Batch lifecycle](#5-batch-lifecycle) — every state a batch can end in
6. [Schemas and access](#6-schemas-and-access) — who can read what
7. [Adding a domain](#7-adding-a-domain) — what is configuration and what is code

## 1. System

```mermaid
flowchart LR
    producer["Producer<br/>(generate_sample_batch)"]
    landing[("data/landing<br/>files + _SUCCESS")]

    subgraph airflow["Airflow 3.3.2"]
        direction TB
        api["api-server · UI, REST"]
        sched["scheduler · LocalExecutor"]
        proc["dag-processor"]
        trig["triggerer"]
    end
    meta[("postgres<br/>Airflow metadata")]

    subgraph task["Each task"]
        direction TB
        pipeline["include/pipeline<br/>loader · runner · alerts"]
        dbt["dbt-core 1.12<br/>isolated venv"]
        pipeline --> dbt
    end

    subgraph wh["Warehouse :5433"]
        direction TB
        internal[("raw · bronze · silver<br/>gold_staging")]
        gold[("{domain}_gold")]
        audit[("dq_audit")]
    end

    consumers["Consumers<br/>analyst · BI · DBeaver"]
    alerts["Chat webhook"]

    producer --> landing --> task
    airflow --- meta
    airflow -- runs --> task
    task -- "load, build, test" --> internal
    task -- publish --> gold
    task -- history --> audit
    task -. "blocked batch" .-> alerts
    gold -- "read only" --> consumers
```

All Airflow services share one image (`docker/airflow/Dockerfile`): Airflow plus the pipeline
dependencies, with dbt in its own virtualenv so its dependencies never clash with Airflow's. Each
task also writes `data/reports/{domain}/{batch}/quality_report.json`.

## 2. Medallion flow and quality gates

Every gate is a separate Airflow task. A failing `error`-severity rule fails the task, and
everything after it is skipped, so a bad batch can never reach `<domain>_gold`.

```mermaid
flowchart TD
    land["Batch lands<br/>files + _SUCCESS marker"] --> wait{"wait_for_batch<br/>marker present?"}
    wait -- "timeout" --> na["not_arrived"]
    wait -- yes --> load{"load_raw<br/>files exist, parse,<br/>have contract columns?"}
    load -- no --> blocked["blocked<br/>gold unchanged, alert sent"]
    load -- yes --> raw[("{domain}_raw<br/>every value as text")]
    raw --> bronze{"bronze<br/>not empty · required present<br/>typed values castable"}
    bronze -- rule fails --> blocked
    bronze -- pass --> silver{"silver<br/>unique · accepted values · ranges<br/>references · business rules"}
    silver -- rule fails --> blocked
    silver -- pass --> goldgate{"gold, in gold_staging<br/>aggregate invariants<br/>reconciliation with silver"}
    goldgate -- rule fails --> blocked
    goldgate -- pass --> publish["publish_gold<br/>upsert batch into {domain}_gold"]
    publish --> published["published<br/>Asset event, consumers see it"]

    classDef bad fill:#fde2e1,stroke:#c0392b,color:#1f2933
    classDef good fill:#dff3e4,stroke:#2e8b57,color:#1f2933
    classDef warn fill:#fff3c4,stroke:#c9a227,color:#1f2933
    class blocked bad
    class published good
    class na warn
```

| Gate | Owns | Examples |
|---|---|---|
| load | the **structure** of the files (the contract) | file missing, JSON cut off, column missing |
| bronze | that the batch is **complete and typable** (generated from the contract) | empty file, null ids, `"12,50"` as an amount |
| silver | that each **row** is valid | duplicate ids, unknown currency, discount above the line amount |
| gold | that the **aggregates** make sense | overdraft beyond the credit limit, more than 24 h watched per day |

## 3. Airflow DAG

One `medallion_<domain>` DAG per contract in `include/domains/`, built by
`dags/medallion_pipelines.py`. Runs of a domain are serialized (`max_active_runs=1`).

```mermaid
flowchart LR
    resolve["resolve_batch<br/>param · schedule date − lag · latest"]
    wait["wait_for_batch<br/>sensor, mode=reschedule"]
    load["load_raw"]
    bronze["bronze<br/>dbt build"]
    silver["silver<br/>dbt build"]
    gold["gold<br/>dbt build"]
    publish["publish_gold<br/>outlet: medallion://{domain}/gold"]
    report["quality_report<br/>trigger_rule=all_done"]
    freshness["gold_freshness DAG<br/>(scheduled on the Asset)"]

    resolve --> wait --> load --> bronze --> silver --> gold --> publish
    load & bronze & silver & gold & publish --> report
    publish -. Asset event .-> freshness
```

`quality_report` always runs: it writes the report and the `dq_audit` history, sends the alert,
and fails when the batch was blocked, so a blocked run shows as failed in the UI.

## 4. Write-Audit-Publish

Gold is built and tested where consumers can't see it, and only then copied into the schema
they read, in one transaction.

```mermaid
sequenceDiagram
    autonumber
    participant AF as Airflow task
    participant DBT as dbt
    participant STG as {domain}_gold_staging
    participant LIVE as {domain}_gold
    participant C as Consumers

    AF->>DBT: dbt build path:models/{domain}/gold
    DBT->>STG: rebuild gold tables (current batch only)
    DBT->>STG: run gold rules (invariants, reconciliation)
    alt a rule fails
        DBT-->>AF: exit 1
        Note over AF,LIVE: task fails, publish_gold never runs,<br/>consumers keep the previous batches
    else every rule passes
        AF->>DBT: dbt run-operation publish_gold(domain, batch_id)
        DBT->>LIVE: BEGIN
        DBT->>LIVE: refuse if staging holds rows of another batch
        DBT->>LIVE: create missing tables / columns
        DBT->>LIVE: delete rows of this batch_id
        DBT->>LIVE: insert the audited rows
        DBT->>LIVE: COMMIT, grant select to gold_reader
        AF-->>C: Asset medallion://{domain}/gold updated
        C->>LIVE: select ... (new batch visible)
    end
```

Because publishing replaces the batch's partition, re-processing a batch is idempotent, and
`make rollback DOMAIN=<d> BATCH=<b>` removes it again.

## 5. Batch lifecycle

```mermaid
stateDiagram-v2
    [*] --> Landing: producer writes files
    Landing --> Waiting: run scheduled or triggered
    Waiting --> NotArrived: no _SUCCESS before timeout
    Waiting --> Loaded: marker present, contract OK
    Waiting --> Blocked: contract broken (load)
    Loaded --> Bronze
    Bronze --> Silver: rules pass
    Silver --> Gold: rules pass
    Gold --> Published: rules pass + publish
    Bronze --> Blocked: rule fails
    Silver --> Blocked: rule fails
    Gold --> Blocked: rule fails
    Published --> Waiting: re-processed (partition replaced)
    Published --> RolledBack: make rollback
    Blocked --> Waiting: fixed batch re-delivered
    NotArrived --> [*]
    RolledBack --> [*]
```

The final state of every run is recorded in `dq_audit.pipeline_runs` (`published`, `blocked` or
`not_arrived`), and each rule evaluation in `dq_audit.rule_results`.

## 6. Schemas and access

```mermaid
flowchart LR
    subgraph roles["Warehouse roles"]
        dbtrole["dbt<br/>(pipeline)"]
        reader["analyst<br/>member of gold_reader"]
    end

    subgraph internal["Internal: pipeline only"]
        raw[("{domain}_raw")]
        bronze[("{domain}_bronze")]
        silver[("{domain}_silver")]
        staging[("{domain}_gold_staging")]
        audit[("dq_audit")]
    end

    gold[("{domain}_gold<br/>one partition per published batch")]

    dbtrole -- read / write --> internal
    dbtrole -- publish --> gold
    reader -- select --> gold
    reader -. "permission denied" .-x internal
```

`publish_gold` re-grants `select` to `gold_reader` on every publish, so new gold tables are
readable right away. Create more consumer logins with `CREATE ROLE x LOGIN IN ROLE gold_reader`.

## 7. Adding a domain

```mermaid
flowchart LR
    contract["include/domains/{domain}.yml<br/>entities · types · required · arrival"]
    scaffold{{"make scaffold"}}
    gen["generated<br/>_sources.yml · bronze views · bronze rules"]
    hand["hand-written<br/>silver/ and gold/ models + business rules"]
    docs{{"make docs"}}
    catalog["docs/rules/{domain}/<br/>one page per rule + lineage"]
    dag["medallion_{domain} DAG<br/>+ gold Asset (automatic)"]
    ci["CI: scaffold --check · docs --check<br/>unit · integration · DAG tests"]

    contract --> scaffold --> gen
    contract --> dag
    gen --> hand
    hand --> docs --> catalog
    gen & hand & catalog --> ci
```

Only the contract and the silver/gold logic are written by hand; the engine, the DAG and the
documentation adapt to the new domain on their own.
