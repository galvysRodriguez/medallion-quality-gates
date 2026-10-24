"""CLI: python -m pipeline {domains,scaffold,docs,scenarios,generate,run,rollback} ...

Run from the project root with include/ on PYTHONPATH (the Makefile does this).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from pipeline import orchestration, rule_docs, scaffold
from pipeline.config import load_domains
from pipeline.dbt_runner import QualityGateError
from pipeline.generators import GENERATORS, generate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("domains", help="list configured domains and their entities")

    p = sub.add_parser("scaffold", help="generate sources + bronze models/rules from the data contracts")
    p.add_argument("domain", nargs="?", help="default: every domain")
    p.add_argument("--check", action="store_true", help="only report out-of-date files (exit 1 if any)")

    p = sub.add_parser("docs", help="generate the rule catalog in docs/rules/ from the dbt YAML")
    p.add_argument("--check", action="store_true", help="only report out-of-date pages (exit 1 if any)")

    p = sub.add_parser("scenarios", help="list corruption scenarios and the rule that must catch them")
    p.add_argument("domain", nargs="?")

    p = sub.add_parser("generate", help="write a sample landing batch")
    p.add_argument("domain")
    p.add_argument("--batch", required=True, help="batch id, e.g. 2026-10-22")
    p.add_argument("--scenario", default="clean")
    p.add_argument("--rows", type=int, default=200)
    p.add_argument("--seed", type=int)

    p = sub.add_parser("run", help="run load -> bronze -> silver -> gold -> publish for one batch")
    p.add_argument("domain")
    p.add_argument("--batch", help="defaults to the latest landed batch")

    p = sub.add_parser("rollback", help="remove a published batch from <domain>_gold")
    p.add_argument("domain")
    p.add_argument("--batch", required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if args.command == "domains":
        for domain in load_domains().values():
            print(f"{domain.name}: {domain.description}")
            for e in domain.entities.values():
                cols = ", ".join(f"{c}:{e.type_of(c)}{'!' if c in e.required else ''}" for c in e.columns)
                print(f"  - {e.name} ({e.file}, {e.format}): {cols}")
    elif args.command == "scaffold":
        domains = [d for d in load_domains().values() if not args.domain or d.name == args.domain]
        if not domains:
            parser.error(f"unknown domain {args.domain!r}")
        stale = {d.name: (scaffold.stale_files(d) if args.check else scaffold.write(d)) for d in domains}
        for name, paths in stale.items():
            for path in paths:
                print(f"{'out of date' if args.check else 'wrote'}: dbt/{path}")
        if args.check and any(stale.values()):
            print("run `python -m pipeline scaffold` to regenerate")
            return 1
    elif args.command == "docs":
        changed = rule_docs.stale_files() if args.check else rule_docs.write()
        for path in changed:
            print(f"{'out of date' if args.check else 'wrote'}: docs/rules/{path}")
        if args.check and changed:
            print("run `python -m pipeline docs` to regenerate")
            return 1
        if not args.check:
            print(f"{len(rule_docs.render())} pages in docs/rules/ ({len(changed)} changed)")
    elif args.command == "scenarios":
        for name, gen in GENERATORS.items():
            if args.domain and name != args.domain:
                continue
            print(name)
            for s in gen.scenarios().values():
                print(f"  {s.name:<30} {s.expected_stage:<7} {s.expected_rule}")
    elif args.command == "generate":
        print(generate(args.domain, args.batch, args.scenario, args.rows, args.seed))
    elif args.command == "run":
        batch_id = orchestration.resolve_batch(args.domain, args.batch)
        if not orchestration.batch_ready(args.domain, batch_id):
            logging.warning("batch %s has not fully landed (missing folder or completion marker)", batch_id)
        try:
            orchestration.run_pipeline(args.domain, batch_id)
        except QualityGateError as exc:
            logging.error("%s", exc)
            return 1
        finally:
            print(json.dumps(orchestration.finalize(args.domain, batch_id), indent=2))
    elif args.command == "rollback":
        try:
            orchestration.rollback(args.domain, args.batch)
        except QualityGateError as exc:
            logging.error("%s", exc)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
