# Rule library

Reusable, domain-agnostic generic tests. A domain's business rules are just these
building blocks (plus dbt's built-in `unique`, `not_null`, `accepted_values`,
`relationships`) configured in its `_bronze.yml` / `_silver.yml` / `_gold.yml`.

Each rule returns the rows that violate it; any row fails the rule. Give every rule a
`name` (it becomes the rule id in reports and the `dq_audit` table name) and a
`description` (the business reason). Set `config: {severity: warn}` to report without
stopping the pipeline.

| Rule | Level | Checks |
|---|---|---|
| `castable` | column | non-null text value parses as `to` (numeric, integer, date, timestamptz, boolean, …) |
| `value_between` | column | `min_value <= value <= max_value` (either bound optional) |
| `matches_pattern` | column | value matches a POSIX regex |
| `not_in_future` | column | timestamp/date not after now (+ tolerance) |
| `expression_is_true` | model | arbitrary row-level SQL predicate — the escape hatch for any business rule |
| `unique_combination` | model | combination of columns is unique |
| `row_count_between` | model | number of rows in range (catches empty or truncated loads) |
| `reconciles_with` | model | an aggregate equals the same aggregate in another model (cross-layer reconciliation) |

Postgres-specific: `castable` relies on `pg_input_is_valid` (PostgreSQL 16+).
