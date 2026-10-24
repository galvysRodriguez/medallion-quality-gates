"""Generate the contract-derived dbt files of a domain from include/domains/<domain>.yml.

    dbt/models/<domain>/_sources.yml            the <domain>_raw tables the loader writes
    dbt/models/<domain>/bronze/brz_*.sql        one view per entity over the current batch
    dbt/models/<domain>/bronze/_bronze.yml      structural rules: not empty, required columns
                                                present, typed columns castable

Silver and gold are business logic and stay hand-written. Generated files carry a header
and must not be edited: change the contract and run `python -m pipeline scaffold`.
`--check` exits non-zero when the files on disk are out of date (used by tests and CI).
"""

from __future__ import annotations

from pathlib import Path

import yaml

from pipeline.config import DBT_PROJECT_DIR, Domain

HEADER = "# GENERATED from include/domains/{domain}.yml by `python -m pipeline scaffold` — do not edit.\n"


def _dump(data: dict, domain: str) -> str:
    return HEADER.format(domain=domain) + "\n" + yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)


def bronze_rule_name(domain: str, entity: str, column: str | None, check: str) -> str:
    return f"{domain}_bronze_{entity}_{check}" if column is None else f"{domain}_bronze_{entity}_{column}_{check}"


def render(domain: Domain) -> dict[Path, str]:
    """Generated path (relative to dbt/) -> file content."""
    base = Path("models") / domain.name
    files: dict[Path, str] = {}

    files[base / "_sources.yml"] = _dump({
        "version": 2,
        "sources": [{
            "name": domain.raw_schema,
            "description": "Landing batches loaded as text by include/pipeline/loader.py (append-only, one _batch_id per load).",
            "schema": domain.raw_schema,
            "tables": [{"name": e.name} for e in domain.entities.values()],
        }],
    }, domain.name)

    models = []
    for entity in domain.entities.values():
        model = f"brz_{domain.name}__{entity.name}"
        files[base / "bronze" / f"{model}.sql"] = (
            "{{ config(materialized='view') }}\n"
            f"{{{{ select_current_batch(source('{domain.raw_schema}', '{entity.name}')) }}}}\n"
        )
        columns = []
        for column in entity.columns:
            tests = []
            if column in entity.required:
                tests.append({"not_null": {"name": bronze_rule_name(domain.name, entity.name, column, "present")}})
            col_type = entity.type_of(column)
            if col_type != "text":
                tests.append({"castable": {
                    "name": bronze_rule_name(domain.name, entity.name, column, f"is_{col_type}"),
                    "arguments": {"to": col_type},
                }})
            if tests:
                columns.append({"name": column, "data_tests": tests})
        models.append({
            "name": model,
            "description": f"Current landing batch of {entity.name} ({entity.file}), raw text. {entity.description}".strip(),
            "data_tests": [{"row_count_between": {
                "name": bronze_rule_name(domain.name, entity.name, None, "not_empty"),
                "description": f"A batch without {entity.name} means the extract failed upstream.",
                "arguments": {"min_value": 1},
            }}],
            "columns": columns,
        })
    files[base / "bronze" / "_bronze.yml"] = _dump({"version": 2, "models": models}, domain.name)
    return files


def stale_files(domain: Domain, dbt_dir: Path | None = None) -> list[Path]:
    dbt_dir = dbt_dir or DBT_PROJECT_DIR
    stale = [p for p, content in render(domain).items()
             if not (dbt_dir / p).is_file() or (dbt_dir / p).read_text() != content]
    # Bronze models whose entity was removed from the contract.
    expected = {p.name for p in render(domain) if p.suffix == ".sql"}
    bronze_dir = dbt_dir / "models" / domain.name / "bronze"
    stale += [p.relative_to(dbt_dir) for p in bronze_dir.glob("brz_*.sql") if p.name not in expected]
    return stale


def write(domain: Domain, dbt_dir: Path | None = None) -> list[Path]:
    """Write the generated files (and remove orphaned bronze models). Returns what changed."""
    dbt_dir = dbt_dir or DBT_PROJECT_DIR
    changed = stale_files(domain, dbt_dir)
    rendered = render(domain)
    for path in changed:
        target = dbt_dir / path
        if path in rendered:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(rendered[path])
        else:
            target.unlink()
    for layer in ("silver", "gold"):
        (dbt_dir / "models" / domain.name / layer).mkdir(parents=True, exist_ok=True)
    return changed
