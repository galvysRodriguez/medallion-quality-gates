"""Which batch a run processes, and when a landing batch counts as complete."""

from datetime import datetime

import pytest

from pipeline import orchestration
from pipeline.generators import generate


def test_generator_writes_the_completion_marker_last(tmp_path):
    out = generate("fintech", "2026-01-15", landing_dir=tmp_path)
    assert (out / "_SUCCESS").is_file()
    assert orchestration.batch_ready("fintech", "2026-01-15", tmp_path)


def test_batch_without_marker_is_not_ready(tmp_path):
    out = generate("fintech", "2026-01-15", landing_dir=tmp_path)
    (out / "_SUCCESS").unlink()
    assert not orchestration.batch_ready("fintech", "2026-01-15", tmp_path)
    assert not orchestration.batch_ready("fintech", "2026-01-20", tmp_path)


def test_explicit_batch_wins(tmp_path):
    assert orchestration.resolve_batch("fintech", "2026-03-01", datetime(2026, 5, 5), tmp_path) == "2026-03-01"


def test_scheduled_run_processes_the_previous_day_even_before_it_lands(tmp_path):
    assert orchestration.resolve_batch("fintech", scheduled_for=datetime(2026, 5, 5), landing_dir=tmp_path) == "2026-05-04"


def test_manual_run_takes_the_latest_complete_batch(tmp_path):
    generate("fintech", "2026-01-15", landing_dir=tmp_path)
    late = generate("fintech", "2026-01-16", landing_dir=tmp_path)
    (late / "_SUCCESS").unlink()  # still being written by the producer
    assert orchestration.resolve_batch("fintech", landing_dir=tmp_path) == "2026-01-15"


def test_no_complete_batch_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        orchestration.resolve_batch("fintech", landing_dir=tmp_path)


def test_arrival_settings_are_validated(tmp_path):
    from pipeline.config import ConfigError, load_domain_file

    path = tmp_path / "x.yml"
    path.write_text("domain: x\narrival: {marker: _OK, retries: 3}\nentities:\n  a: {file: a.csv, format: csv, columns: [id]}\n")
    with pytest.raises(ConfigError, match="retries"):
        load_domain_file(path)
