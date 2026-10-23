"""Synthetic landing batches: a clean dataset per domain plus named corruption scenarios.

A scenario mutates a clean batch so that exactly one rule should stop the pipeline,
and records which stage/rule that is — the tests assert the pipeline agrees.
"""

from __future__ import annotations

import csv
import io
import json
import random
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

from pipeline.config import LANDING_DIR, Domain, load_domain, validate_batch_id

Rows = dict[str, list[dict]]


@dataclass
class GeneratedBatch:
    day: date
    rows: Rows
    dropped_columns: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    text_mutations: dict[str, Callable[[str], str]] = field(default_factory=dict)
    missing_files: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    expected_stage: str  # load | bronze | silver | gold
    expected_rule: str  # dbt test name, or "landing_contract" for the loader
    apply: Callable[[GeneratedBatch, random.Random], None]


def money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def utc_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def time_window(day: date) -> tuple[datetime, datetime]:
    """[start, end] of `day` in UTC, capped a few minutes before now (no future events)."""
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = min(start + timedelta(days=1) - timedelta(seconds=1), datetime.now(timezone.utc) - timedelta(minutes=10))
    return start, max(start, end)


def random_ts(rng: random.Random, day: date) -> datetime:
    start, end = time_window(day)
    return start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))


class DomainGenerator:
    domain: str
    # Entities used by the generic, format-level scenarios.
    generic_targets: dict[str, object] = {}

    def generate_clean(self, day: date, rows: int, rng: random.Random) -> Rows:
        raise NotImplementedError

    def domain_scenarios(self) -> list[Scenario]:
        raise NotImplementedError

    @property
    def contract(self) -> Domain:
        return load_domain(self.domain)

    def scenarios(self) -> dict[str, Scenario]:
        found = {s.name: s for s in [*self._generic_scenarios(), *self.domain_scenarios()]}
        return dict(sorted(found.items()))

    def _generic_scenarios(self) -> list[Scenario]:
        t = self.generic_targets
        out = []
        if "missing_column" in t:
            entity, column = t["missing_column"]

            def drop(b, rng, entity=entity, column=column):
                b.dropped_columns[entity].add(column)

            out.append(Scenario("missing_column", f"{entity} file arrives without the '{column}' column",
                                "load", "landing_contract", drop))
        if "missing_file" in t:
            entity = t["missing_file"]
            out.append(Scenario("missing_file", f"{entity} file never lands",
                                "load", "landing_contract", lambda b, rng, e=entity: b.missing_files.add(e)))
        if "malformed_record" in t:
            entity = t["malformed_record"]
            fmt = self.contract.entities[entity].format
            mutate = (lambda text: text + '{"truncated": ') if fmt == "jsonl" else (lambda text: text[: len(text) // 2])
            out.append(Scenario("malformed_record", f"{entity} ({fmt}) is cut off mid-record",
                                "load", "landing_contract",
                                lambda b, rng, e=entity, m=mutate: b.text_mutations.__setitem__(e, m)))
        if "empty_file" in t:
            entity = t["empty_file"]

            def empty(b, rng, entity=entity):
                b.rows[entity] = []

            out.append(Scenario("empty_file", f"{entity} file lands with no records",
                                "bronze", f"{self.domain}_bronze_{entity}_not_empty", empty))
        return out

    def build(self, batch_id: str, rows: int = 200, scenario: str = "clean", seed: int | None = None) -> GeneratedBatch:
        day = batch_day(batch_id)
        rng = random.Random(seed if seed is not None else zlib.crc32(f"{self.domain}/{batch_id}".encode()))
        batch = GeneratedBatch(day, self.generate_clean(day, rows, rng))
        if scenario != "clean":
            scenarios = self.scenarios()
            if scenario not in scenarios:
                raise KeyError(f"unknown scenario {scenario!r} for {self.domain}; options: clean, {', '.join(scenarios)}")
            scenarios[scenario].apply(batch, rng)
        return batch


def batch_day(batch_id: str) -> date:
    """Batches named after a date (e.g. 2026-10-22 or 2026-10-22_retry) generate data for that day."""
    try:
        return date.fromisoformat(batch_id[:10])
    except ValueError:
        return date.today() - timedelta(days=1)


def _plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return utc_ts(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _csv_cell(value) -> str:
    value = _plain(value)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def render(entity_format: str, columns: list[str], rows: list[dict]) -> str:
    records = [{c: _plain(r.get(c)) for c in columns} for r in rows]
    if entity_format == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows([[_csv_cell(r.get(c)) for c in columns] for r in rows])
        return buf.getvalue()
    if entity_format == "jsonl":
        return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records)
    if entity_format == "json":
        return json.dumps(records, indent=2, ensure_ascii=False) + "\n"
    raise ValueError(entity_format)


def write_batch(domain: Domain, batch_id: str, batch: GeneratedBatch, landing_dir: Path | None = None) -> Path:
    out_dir = domain.batch_dir(validate_batch_id(batch_id), landing_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.iterdir():
        old.unlink()
    for name, entity in domain.entities.items():
        if name in batch.missing_files:
            continue
        columns = [c for c in entity.columns if c not in batch.dropped_columns.get(name, set())]
        text = render(entity.format, columns, batch.rows.get(name, []))
        if name in batch.text_mutations:
            text = batch.text_mutations[name](text)
        (out_dir / entity.file).write_text(text, encoding="utf-8")
    return out_dir
