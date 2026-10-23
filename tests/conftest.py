import os
import sys
import tempfile
from pathlib import Path

import pytest

# Must be set before pipeline.config is imported: generated batches and reports go to a
# throwaway directory instead of the project's data/.
os.environ.setdefault("PIPELINE_DATA_DIR", tempfile.mkdtemp(prefix="pipeline-tests-"))
_local_dbt = Path(sys.executable).with_name("dbt")
if "DBT_EXECUTABLE" not in os.environ and _local_dbt.exists():
    os.environ["DBT_EXECUTABLE"] = str(_local_dbt)

from pipeline.config import warehouse_conninfo  # noqa: E402


@pytest.fixture(scope="session")
def warehouse():
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(warehouse_conninfo(), connect_timeout=3, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse not reachable ({exc}); start it with `docker compose up -d warehouse`")
    yield conn
    conn.close()
