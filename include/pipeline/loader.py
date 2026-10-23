"""Landing files -> `<domain>_raw` tables.

The loader only enforces the *structural* contract (files exist, parse, and carry the
declared columns). Every value is stored as TEXT so bad data reaches the warehouse
intact and the dbt rules — not a Python exception — decide whether it is acceptable.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg import sql

from pipeline.config import Domain, Entity, validate_batch_id, warehouse_conninfo

log = logging.getLogger(__name__)

METADATA_COLUMNS = ("_batch_id", "_source_file", "_row_number")


class ContractError(Exception):
    """A landing batch violates the structural data contract."""


@dataclass
class ParsedFile:
    entity: Entity
    path: Path
    rows: list[dict[str, str | None]]
    extra_columns: list[str] = field(default_factory=list)


def _to_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    text = str(value)
    return text if text.strip() != "" else None


def _read_records(path: Path, fmt: str) -> tuple[list[str], list[dict]]:
    if fmt == "csv":
        with path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            return list(reader.fieldnames or []), list(reader)

    if fmt == "jsonl":
        records = []
        with path.open(encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ContractError(f"{path.name}: malformed JSON on line {line_no}: {exc.msg}") from exc
                if not isinstance(record, dict):
                    raise ContractError(f"{path.name}: line {line_no} is not a JSON object")
                records.append(record)
    elif fmt == "json":
        try:
            records = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ContractError(f"{path.name}: malformed JSON: {exc.msg} (line {exc.lineno})") from exc
        if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
            raise ContractError(f"{path.name}: expected a JSON array of objects")
    else:
        raise ContractError(f"{path.name}: unsupported format {fmt!r}")

    header: dict[str, None] = {}
    for record in records:
        header.update(dict.fromkeys(record))
    return list(header), records


def parse_file(entity: Entity, path: Path) -> ParsedFile:
    if not path.is_file():
        raise ContractError(f"missing landing file for entity {entity.name!r}: {path}")
    header, records = _read_records(path, entity.format)
    # A completely empty file has no header to check; the bronze row-count rule handles it.
    if header or records:
        missing = [c for c in entity.columns if c not in header]
        if missing:
            raise ContractError(f"{path.name}: missing contract column(s) {', '.join(missing)}")
    extra = [c for c in header if c not in entity.columns]
    if extra:
        log.warning("%s: ignoring columns not in the contract: %s", path.name, ", ".join(extra))
    rows = [{c: _to_text(r.get(c)) for c in entity.columns} for r in records]
    return ParsedFile(entity, path, rows, extra)


def parse_batch(domain: Domain, batch_id: str, landing_dir: Path | None = None) -> list[ParsedFile]:
    """Parse and validate every entity before anything is written (all-or-nothing)."""
    batch_dir = domain.batch_dir(batch_id, landing_dir)
    if not batch_dir.is_dir():
        raise ContractError(f"landing batch not found: {batch_dir}")
    return [parse_file(entity, batch_dir / entity.file) for entity in domain.entities.values()]


def load_batch(
    domain: Domain,
    batch_id: str,
    landing_dir: Path | None = None,
    conninfo: str | None = None,
) -> dict[str, int]:
    """Load one landing batch into `<domain>_raw`. Re-loading a batch replaces it."""
    validate_batch_id(batch_id)
    parsed = parse_batch(domain, batch_id, landing_dir)
    schema = sql.Identifier(domain.raw_schema)

    with psycopg.connect(conninfo or warehouse_conninfo()) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("create schema if not exists {}").format(schema))
        counts = {}
        for pf in parsed:
            table = sql.Identifier(domain.raw_schema, pf.entity.name)
            cur.execute(
                sql.SQL(
                    "create table if not exists {} ("
                    "_batch_id text not null, _source_file text, _row_number integer, "
                    "_loaded_at timestamptz not null default now())"
                ).format(table)
            )
            # Contract evolution: new columns are added, old ones are kept for history.
            for column in pf.entity.columns:
                cur.execute(
                    sql.SQL("alter table {} add column if not exists {} text").format(
                        table, sql.Identifier(column)
                    )
                )
            cur.execute(sql.SQL("delete from {} where _batch_id = %s").format(table), (batch_id,))

            columns = METADATA_COLUMNS + pf.entity.columns
            copy_sql = sql.SQL("copy {} ({}) from stdin").format(
                table, sql.SQL(", ").join(map(sql.Identifier, columns))
            )
            with cur.copy(copy_sql) as copy:
                for row_number, row in enumerate(pf.rows, start=1):
                    copy.write_row((batch_id, pf.path.name, row_number, *row.values()))
            counts[pf.entity.name] = len(pf.rows)
            log.info("loaded %s rows into %s.%s", len(pf.rows), domain.raw_schema, pf.entity.name)
    return counts
