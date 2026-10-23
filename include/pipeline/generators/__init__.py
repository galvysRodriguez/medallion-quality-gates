"""Registry of sample-data generators. Add a domain by subclassing DomainGenerator and registering it here."""

from __future__ import annotations

from pathlib import Path

from pipeline.generators.base import DomainGenerator, Scenario, write_batch
from pipeline.generators.ecommerce import EcommerceGenerator
from pipeline.generators.fintech import FintechGenerator
from pipeline.generators.streaming import StreamingGenerator

GENERATORS: dict[str, DomainGenerator] = {
    g.domain: g for g in (FintechGenerator(), EcommerceGenerator(), StreamingGenerator())
}


def get_generator(domain: str) -> DomainGenerator:
    if domain not in GENERATORS:
        raise KeyError(f"no sample-data generator for domain {domain!r}; available: {', '.join(GENERATORS)}")
    return GENERATORS[domain]


def generate(
    domain: str,
    batch_id: str,
    scenario: str = "clean",
    rows: int = 200,
    seed: int | None = None,
    landing_dir: Path | None = None,
) -> Path:
    """Write a landing batch to data/landing/<domain>/<batch_id>/ and return that directory."""
    generator = get_generator(domain)
    batch = generator.build(batch_id, rows=rows, scenario=scenario, seed=seed)
    return write_batch(generator.contract, batch_id, batch, landing_dir)


__all__ = ["GENERATORS", "DomainGenerator", "Scenario", "generate", "get_generator"]
