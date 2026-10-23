"""Generated landing files: formats, determinism, and that each scenario changes the data."""

import csv
import json

import pytest

from pipeline.config import load_domains
from pipeline.generators import GENERATORS, generate
from pipeline.loader import ContractError, parse_batch

DOMAINS = load_domains()


def read_all(path):
    return {p.name: p.read_bytes() for p in sorted(path.iterdir())}


@pytest.mark.parametrize("domain", sorted(GENERATORS))
def test_clean_batch_writes_every_file_in_its_format(tmp_path, domain):
    out = generate(domain, "2026-01-15", landing_dir=tmp_path)
    for entity in DOMAINS[domain].entities.values():
        path = out / entity.file
        assert path.is_file()
        if entity.format == "csv":
            header = next(csv.reader(path.open()))
            assert header == list(entity.columns)
        elif entity.format == "jsonl":
            records = [json.loads(line) for line in path.read_text().splitlines()]
            assert records and set(records[0]) == set(entity.columns)
        else:
            records = json.loads(path.read_text())
            assert isinstance(records, list) and set(records[0]) == set(entity.columns)
    parsed = parse_batch(DOMAINS[domain], "2026-01-15", tmp_path)
    assert all(pf.rows for pf in parsed)


@pytest.mark.parametrize("domain", sorted(GENERATORS))
def test_generation_is_deterministic(tmp_path, domain):
    a = read_all(generate(domain, "2026-01-15", landing_dir=tmp_path / "a"))
    b = read_all(generate(domain, "2026-01-15", landing_dir=tmp_path / "b"))
    assert a == b


@pytest.mark.parametrize("rows", [10, 1000])
def test_row_count_scales(tmp_path, rows):
    out = generate("fintech", "2026-01-15", rows=rows, landing_dir=tmp_path)
    assert len((out / "transactions.jsonl").read_text().splitlines()) >= rows


@pytest.mark.parametrize(
    "domain,scenario",
    [(d, s) for d, g in sorted(GENERATORS.items()) for s in g.scenarios()],
)
def test_scenario_changes_the_landed_files(tmp_path, domain, scenario):
    clean = read_all(generate(domain, "2026-01-15", landing_dir=tmp_path / "clean"))
    corrupted = read_all(generate(domain, "2026-01-15", scenario, landing_dir=tmp_path / "bad"))
    assert corrupted != clean


@pytest.mark.parametrize(
    "domain,scenario",
    [(d, s) for d, g in sorted(GENERATORS.items()) for s, sc in g.scenarios().items() if sc.expected_stage == "load"],
)
def test_load_stage_scenarios_break_the_contract(tmp_path, domain, scenario):
    generate(domain, "2026-01-15", scenario, landing_dir=tmp_path)
    with pytest.raises(ContractError):
        parse_batch(DOMAINS[domain], "2026-01-15", tmp_path)


def test_unknown_scenario_is_rejected(tmp_path):
    with pytest.raises(KeyError, match="unknown scenario"):
        generate("fintech", "2026-01-15", "does_not_exist", landing_dir=tmp_path)
