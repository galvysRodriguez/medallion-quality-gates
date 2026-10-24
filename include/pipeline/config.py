"""Paths, warehouse connection settings and domain data contracts (include/domains/*.yml)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# include/pipeline/config.py -> project root (works on the host and at /opt/airflow in Docker)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DOMAINS_DIR = Path(os.environ.get("DOMAINS_DIR", PROJECT_ROOT / "include" / "domains"))
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
LANDING_DIR = DATA_DIR / "landing"
REPORTS_DIR = DATA_DIR / "reports"
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", PROJECT_ROOT / "dbt"))

SUPPORTED_FORMATS = ("csv", "json", "jsonl")
# Contract column types; anything but text gets a bronze `castable` rule (Postgres type names).
COLUMN_TYPES = ("text", "integer", "numeric", "date", "timestamptz", "boolean")
LAYERS = ("bronze", "silver", "gold")

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
# Batch ids end up inside SQL (dbt var) and file paths, so keep them boring.
_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Entity:
    name: str
    file: str
    format: str
    columns: tuple[str, ...]
    column_types: dict[str, str] = field(default_factory=dict)  # missing = text
    required: frozenset[str] = frozenset()  # columns that must be non-null
    description: str = ""

    def type_of(self, column: str) -> str:
        return self.column_types.get(column, "text")


@dataclass(frozen=True)
class Arrival:
    """How to tell that a landing batch is complete, and how long to wait for it."""

    marker: str | None = None  # file the producer writes last (e.g. _SUCCESS); None = folder exists
    timeout_minutes: int = 360
    poke_minutes: int = 5
    batch_lag_days: int = 1  # a scheduled run on day D processes batch D - lag


@dataclass(frozen=True)
class Domain:
    name: str
    description: str
    schedule: str | None
    entities: dict[str, Entity]
    arrival: Arrival = Arrival()

    @property
    def raw_schema(self) -> str:
        return f"{self.name}_raw"

    @property
    def gold_schema(self) -> str:
        """Schema consumers read from: one partition per published batch (column _batch_id)."""
        return f"{self.name}_gold"

    @property
    def gold_asset_uri(self) -> str:
        """Airflow Asset updated by every successful publish of this domain."""
        return f"medallion://{self.name}/gold"

    @property
    def gold_staging_schema(self) -> str:
        return f"{self.name}_gold_staging"

    def batch_dir(self, batch_id: str, landing_dir: Path | None = None) -> Path:
        return (landing_dir or LANDING_DIR) / self.name / validate_batch_id(batch_id)


def validate_batch_id(batch_id: str) -> str:
    if not isinstance(batch_id, str) or not _BATCH_ID.match(batch_id):
        raise ConfigError(f"invalid batch id {batch_id!r}: use letters, digits, '_', '-', '.'")
    return batch_id


def _identifier(value: str, what: str, source: Path) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(f"{source.name}: invalid {what} {value!r} (lowercase snake_case expected)")
    return value


def _parse_columns(spec, where: str) -> tuple[tuple[str, ...], dict[str, str], frozenset[str]]:
    """Columns as a list of names (all optional text) or a mapping:

        columns:
          account_id: {type: text, required: true}
          credit_limit: numeric
          customer_name: text
    """
    if isinstance(spec, list):
        spec = {name: "text" for name in spec}
    if not isinstance(spec, dict) or not spec:
        raise ConfigError(f"{where}: declares no columns")
    types, required = {}, set()
    for name, column in spec.items():
        if not isinstance(name, str) or not _IDENTIFIER.match(name):
            raise ConfigError(f"{where}: invalid column {name!r} (lowercase snake_case expected)")
        if isinstance(column, str):
            column = {"type": column}
        column = column or {}
        unknown = set(column) - {"type", "required"}
        if unknown:
            raise ConfigError(f"{where}.{name}: unknown setting(s) {', '.join(sorted(unknown))}")
        col_type = column.get("type", "text")
        if col_type not in COLUMN_TYPES:
            raise ConfigError(f"{where}.{name}: unsupported type {col_type!r}; use one of {', '.join(COLUMN_TYPES)}")
        types[name] = col_type
        if column.get("required"):
            required.add(name)
    return tuple(spec), types, frozenset(required)


def load_domain_file(path: Path) -> Domain:
    spec = yaml.safe_load(path.read_text()) or {}
    name = _identifier(spec.get("domain", path.stem), "domain", path)
    entities = {}
    for entity_name, entity in (spec.get("entities") or {}).items():
        _identifier(entity_name, "entity", path)
        fmt = entity.get("format")
        if fmt not in SUPPORTED_FORMATS:
            raise ConfigError(f"{path.name}: entity {entity_name!r} has unsupported format {fmt!r}")
        columns, types, required = _parse_columns(entity.get("columns"), f"{path.name}: {entity_name}")
        entities[entity_name] = Entity(
            entity_name, entity["file"], fmt, columns, types, required, entity.get("description", "")
        )
    if not entities:
        raise ConfigError(f"{path.name}: no entities declared")
    arrival_spec = spec.get("arrival") or {}
    unknown = set(arrival_spec) - set(Arrival.__dataclass_fields__)
    if unknown:
        raise ConfigError(f"{path.name}: unknown arrival setting(s) {', '.join(sorted(unknown))}")
    return Domain(name, spec.get("description", ""), spec.get("schedule"), entities, Arrival(**arrival_spec))


def load_domains(domains_dir: Path | None = None) -> dict[str, Domain]:
    domains = {}
    for path in sorted((domains_dir or DOMAINS_DIR).glob("*.yml")):
        domain = load_domain_file(path)
        domains[domain.name] = domain
    return domains


def load_domain(name: str, domains_dir: Path | None = None) -> Domain:
    domains = load_domains(domains_dir)
    if name not in domains:
        raise ConfigError(f"unknown domain {name!r}; available: {', '.join(domains) or 'none'}")
    return domains[name]


def warehouse_conninfo() -> str:
    """Same WAREHOUSE_* variables (and host defaults) as dbt/profiles.yml."""
    env = os.environ.get
    return (
        f"host={env('WAREHOUSE_HOST', 'localhost')} port={env('WAREHOUSE_PORT', '5433')} "
        f"user={env('WAREHOUSE_USER', 'dbt')} password={env('WAREHOUSE_PASSWORD', 'dbt')} "
        f"dbname={env('WAREHOUSE_DB', 'warehouse')}"
    )
