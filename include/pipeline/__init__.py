"""Domain-agnostic medallion pipeline: landing files -> raw -> bronze -> silver -> gold.

Airflow DAGs, the CLI (`python -m pipeline`) and the test suite all drive the
pipeline through `pipeline.orchestration`, so they exercise exactly the same code.
"""
