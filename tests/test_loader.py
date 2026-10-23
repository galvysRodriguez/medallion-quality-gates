"""Structural contract checks (no database) and raw loading (integration)."""

import json

import pytest

from pipeline.config import Domain, Entity, load_domain
from pipeline.generators import generate
from pipeline.loader import ContractError, load_batch, parse_file

ENTITY = Entity("things", "things.csv", "csv", ("id", "amount", "active"))


def test_csv_values_are_kept_as_text(tmp_path):
    path = tmp_path / "things.csv"
    path.write_text("id,amount,active\n1,12.50,true\n2,,false\n")
    pf = parse_file(ENTITY, path)
    assert pf.rows == [
        {"id": "1", "amount": "12.50", "active": "true"},
        {"id": "2", "amount": None, "active": "false"},
    ]


def test_json_types_are_serialised_to_text(tmp_path):
    entity = Entity("things", "things.json", "json", ("id", "amount", "active"))
    path = tmp_path / "things.json"
    path.write_text(json.dumps([{"id": 1, "amount": 9.99, "active": True}]))
    assert parse_file(entity, path).rows == [{"id": "1", "amount": "9.99", "active": "true"}]


def test_malformed_values_reach_the_warehouse_untouched(tmp_path):
    entity = Entity("things", "things.jsonl", "jsonl", ("id", "amount", "active"))
    path = tmp_path / "things.jsonl"
    path.write_text('{"id": "1", "amount": "12,50", "active": "maybe"}\n')
    assert parse_file(entity, path).rows[0]["amount"] == "12,50"


def test_missing_contract_column_is_rejected(tmp_path):
    path = tmp_path / "things.csv"
    path.write_text("id,active\n1,true\n")
    with pytest.raises(ContractError, match="missing contract column"):
        parse_file(ENTITY, path)


def test_extra_columns_are_ignored(tmp_path):
    path = tmp_path / "things.csv"
    path.write_text("id,amount,active,surprise\n1,2,true,x\n")
    pf = parse_file(ENTITY, path)
    assert pf.extra_columns == ["surprise"] and "surprise" not in pf.rows[0]


def test_malformed_jsonl_reports_the_line(tmp_path):
    entity = Entity("things", "things.jsonl", "jsonl", ("id",))
    path = tmp_path / "things.jsonl"
    path.write_text('{"id": 1}\n{"id": \n')
    with pytest.raises(ContractError, match="line 2"):
        parse_file(entity, path)


def test_missing_file_is_rejected(tmp_path):
    with pytest.raises(ContractError, match="missing landing file"):
        parse_file(ENTITY, tmp_path / "things.csv")


def test_empty_file_passes_the_loader(tmp_path):
    # Emptiness is a data-quality decision, made by the bronze row-count rule.
    path = tmp_path / "things.csv"
    path.write_text("id,amount,active\n")
    assert parse_file(ENTITY, path).rows == []


@pytest.mark.integration
def test_reloading_a_batch_replaces_it(warehouse, tmp_path):
    domain: Domain = load_domain("fintech")
    generate("fintech", "2026-01-10", rows=50, landing_dir=tmp_path)
    first = load_batch(domain, "2026-01-10", tmp_path)
    second = load_batch(domain, "2026-01-10", tmp_path)
    assert first == second
    (count,) = warehouse.execute(
        "select count(*) from fintech_raw.transactions where _batch_id = '2026-01-10'"
    ).fetchone()
    assert count == first["transactions"]
