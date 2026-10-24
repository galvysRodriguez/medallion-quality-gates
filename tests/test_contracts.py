"""Static checks: contracts, generators and dbt rules agree with each other."""

from pathlib import Path

import pytest
import yaml

from pipeline import scaffold
from pipeline.config import DBT_PROJECT_DIR, LAYERS, ConfigError, load_domain_file, load_domains
from pipeline.generators import GENERATORS

DOMAINS = load_domains()


def dbt_rule_names() -> dict[str, str]:
    """rule name -> layer, from every models/<domain>/<layer>/*.yml."""
    rules = {}

    def collect(tests, layer):
        for test in tests or []:
            if isinstance(test, dict):
                (spec,) = test.values()
                if isinstance(spec, dict) and spec.get("name"):
                    rules[spec["name"]] = layer

    for path in (DBT_PROJECT_DIR / "models").glob("*/*/*.yml"):
        layer = path.parent.name
        for model in (yaml.safe_load(path.read_text()) or {}).get("models", []):
            collect(model.get("data_tests"), layer)
            for column in model.get("columns", []):
                collect(column.get("data_tests"), layer)
    return rules


RULES = dbt_rule_names()


def test_every_domain_has_a_generator_and_models():
    for name in DOMAINS:
        assert name in GENERATORS, f"no sample-data generator for {name}"
        for layer in LAYERS:
            assert list((DBT_PROJECT_DIR / "models" / name / layer).glob("*.sql")), f"{name} has no {layer} models"


def test_rule_names_are_prefixed_with_their_domain():
    for rule in RULES:
        assert rule.split("_")[0] in DOMAINS, rule


@pytest.mark.parametrize("domain", sorted(GENERATORS))
def test_clean_rows_match_the_contract(domain):
    gen = GENERATORS[domain]
    rows = gen.build("2026-01-15").rows
    for name, entity in DOMAINS[domain].entities.items():
        assert rows[name], f"{domain}.{name} generated no rows"
        assert set(rows[name][0]) == set(entity.columns), f"{domain}.{name} columns drift from the contract"


@pytest.mark.parametrize(
    "domain,scenario",
    [(d, s) for d, g in sorted(GENERATORS.items()) for s in g.scenarios()],
)
def test_scenario_expects_an_existing_rule_in_the_right_layer(domain, scenario):
    s = GENERATORS[domain].scenarios()[scenario]
    if s.expected_stage == "load":
        assert s.expected_rule == "landing_contract"
    else:
        assert s.expected_rule in RULES, f"rule {s.expected_rule} is not defined in dbt"
        assert RULES[s.expected_rule] == s.expected_stage


def test_each_domain_covers_every_stage():
    for domain, gen in GENERATORS.items():
        stages = {s.expected_stage for s in gen.scenarios().values()}
        assert stages == {"load", "bronze", "silver", "gold"}, f"{domain} only exercises {stages}"


@pytest.mark.parametrize("domain", sorted(DOMAINS))
def test_generated_bronze_matches_the_contract(domain):
    stale = scaffold.stale_files(DOMAINS[domain])
    assert not stale, f"out of date: {stale} — run `python -m pipeline scaffold`"


def test_scaffold_derives_bronze_rules_from_column_types(tmp_path):
    path = tmp_path / "shop.yml"
    path.write_text(
        "domain: shop\nentities:\n  sales:\n    file: sales.csv\n    format: csv\n    columns:\n"
        "      sale_id: {type: text, required: true}\n      amount: numeric\n      note: text\n"
    )
    domain = load_domain_file(path)
    scaffold.write(domain, tmp_path / "dbt")
    bronze = yaml.safe_load((tmp_path / "dbt/models/shop/bronze/_bronze.yml").read_text())
    (model,) = bronze["models"]
    assert model["name"] == "brz_shop__sales"
    rules = {next(iter(t.values()))["name"] for c in model["columns"] for t in c["data_tests"]}
    assert rules == {"shop_bronze_sales_sale_id_present", "shop_bronze_sales_amount_is_numeric"}
    assert (tmp_path / "dbt/models/shop/bronze/brz_shop__sales.sql").is_file()
    assert (tmp_path / "dbt/models/shop/silver").is_dir()
    assert scaffold.stale_files(domain, tmp_path / "dbt") == []


def test_unknown_column_type_is_rejected(tmp_path):
    path = tmp_path / "x.yml"
    path.write_text("domain: x\nentities:\n  a: {file: a.csv, format: csv, columns: {id: uuid}}\n")
    with pytest.raises(ConfigError, match="unsupported type"):
        load_domain_file(path)


def test_rule_docs_are_up_to_date():
    from pipeline import rule_docs

    stale = rule_docs.stale_files()
    assert not stale, f"docs/rules out of date: {stale[:5]} — run `python -m pipeline docs`"


def test_every_business_rule_explains_why():
    from pipeline import rule_docs

    missing = [r.name for r in rule_docs.collect_rules() if r.layer in ("silver", "gold") and not r.description]
    assert not missing, f"add a description to: {missing}"
