"""Real binary HS2 security profiles; run through the dedicated Compose entry."""

import json
import os
import secrets
import time
import uuid
from pathlib import Path
from typing import Callable, TypeVar, cast

import pyarrow as pa
import pytest

import ray
from ray.data.context import DataContext
from ray.data.tests.datasource._hive_test_utils import (
    _case_directory,
    _connection_options,
    _expiry,
    _make_auth_factory,
    _native,
    _read_events,
    _temporary_table,
    _tls_condition,
)

PROFILE = os.environ["HIVE_AUTH_PROFILE"]
CASE_FILE = Path(__file__).resolve().parents[5] / "ci/ray_ci/data/hive_auth/cases.json"
CASES = json.loads(CASE_FILE.read_text())[PROFILE]
SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string())])
ROWS = 2049
pytestmark = pytest.mark.timeout(180)
_Result = TypeVar("_Result")


@pytest.fixture(scope="module")
def environment():
    ray.init(address="auto")
    previous = DataContext.get_current().copy()
    DataContext.get_current().max_errored_blocks = 0
    os.environ["HIVE_TEST_PASSWORD"] = secrets.token_urlsafe(16)
    os.environ["RAY_HIVE_TEST_USE_SSL"] = str(PROFILE in {"T", "GT"}).lower()
    auth = (
        "GSSAPI"
        if PROFILE in {"G", "GT"}
        else ("NOSASL" if PROFILE == "N" else "PLAIN")
    )
    options = _connection_options(os.environ["RAY_HIVE_TEST_HOST"], auth)
    try:
        yield options
    finally:
        DataContext._set_current(previous)
        ray.shutdown()


@pytest.fixture(scope="module")
def table(environment):
    # repeat supplies enough elements whether split preserves trailing empties.
    insert = (
        "SELECT CAST(id AS BIGINT), CASE WHEN id % 7 = 0 THEN CAST(NULL AS STRING) "
        "ELSE concat('row-', CAST(id AS STRING)) END "
        "FROM (SELECT split(repeat('x,', 2049), ',') AS tokens) src "
        "LATERAL VIEW posexplode(src.tokens) items AS id, item "
        "WHERE id < 2049"
    )
    secure = PROFILE in {"G", "GT"}
    storage = (
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE"
        if secure
        else ""
    )
    with _temporary_table(
        environment,
        "id BIGINT, name STRING",
        insert,
        storage_clause=storage,
        local_data_path="/security/test-data.tsv" if secure else None,
        timeout=30,
    ) as (
        name,
        cursor,
    ):
        if secure:
            cursor.execute(f"SELECT id, name FROM `{name}`")
            assert len(cursor.fetchall()) == ROWS
        else:
            cursor.execute(f"SELECT COUNT(*) FROM `{name}`")
            assert cursor.fetchone()[0] == ROWS
        yield name


def _worker(function: Callable[..., _Result], *args) -> _Result:
    return cast(
        _Result,
        ray.get(
            ray.remote(resources={"hive_auth_worker": 1.0})(function).remote(*args),
            timeout=30,
        ),
    )


def _read(table, options, case_id, *, query, worker_env):
    kwargs = {
        "connection_factory": _make_auth_factory(options, case_id),
        "resources": {"hive_auth_worker": 1.0},
        "runtime_env": {"env_vars": worker_env},
    }
    if query:
        dataset = ray.data.read_hive(
            query=f"SELECT id AS id, name AS name FROM `{table}`",
            schema=SCHEMA,
            **kwargs,
        )
    else:
        dataset = ray.data.read_hive(table, **kwargs)
    # Exhaust one action. Calling a second action would be a new legal execution.
    batches: list[pa.Table] = []
    for batch in dataset.iter_batches(batch_format="pyarrow", batch_size=None):
        assert isinstance(batch, pa.Table)
        assert batch.schema == SCHEMA
        batches.append(batch)
    assert batches
    result = pa.concat_tables(batches)
    assert result.num_rows == ROWS
    values = result.to_pylist()
    assert len({row["id"] for row in values}) == ROWS
    assert sorted(values, key=lambda row: row["id"]) == [
        {"id": i, "name": None if i % 7 == 0 else f"row-{i}"} for i in range(ROWS)
    ]


@pytest.mark.parametrize("scenario", CASES, ids=CASES)
def test_hive_auth(scenario, environment, table, monkeypatch):
    # A real successful control precedes each changed-condition read. These
    # readiness SQLs use uninstrumented clients and are not target data RPCs.
    driver_node = _native(environment)
    worker_node = _worker(_native, environment)
    assert worker_node != driver_node
    positive = scenario.startswith("positive_")
    driver_failure = scenario.endswith("_driver")
    query = scenario.endswith("_query")
    condition = scenario.split("_", 1)[0]
    roles = (
        ["driver"]
        if driver_failure
        else (["worker"] if query else ["driver", "worker"])
    )
    overrides = {}
    if not positive:
        overrides = {
            "missing": {"KRB5CCNAME": "FILE:/auth/missing.ccache"},
            "invalid": {"KRB5CCNAME": "FILE:/auth/invalid.ccache"},
            "service": {"HIVE_AUTH_SERVICE_OVERRIDE": "nonexistent-service"},
            "expired": {"KRB5CCNAME": "FILE:/auth/expired.ccache"},
            "ca": {"HIVE_AUTH_CA_OVERRIDE": "/ca/wrong-ca.pem"},
            "host": {"HIVE_AUTH_HOST_OVERRIDE": "wrong.hive.auth.test"},
            "file": {"HIVE_AUTH_CA_OVERRIDE": "/ca/missing.pem"},
        }[condition]
        if condition in {"ca", "host", "file"}:
            if driver_failure:
                _tls_condition(condition)
            else:
                _worker(_tls_condition, condition)
        if condition == "expired":
            info = _worker(_expiry)
            boundary = info["end"] + info["clockskew"] + info["margin"]
            deadline = time.monotonic() + 30
            while time.time() <= boundary:
                if time.monotonic() >= deadline:
                    pytest.fail(
                        "Expired-cache fixture did not cross the verified tolerance window"
                    )
                time.sleep(0.2)

    case_id = uuid.uuid4().hex
    directory = _case_directory(case_id)
    directory.mkdir(parents=True)
    manifest = {
        "roles": roles,
        "terminal": None,
        "run_id": os.environ["HIVE_AUTH_RUN_ID"],
        "case_id": case_id,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest))

    def action():
        try:
            _read(
                table,
                environment,
                case_id,
                query=query,
                worker_env={} if driver_failure else overrides,
            )
        except Exception:
            manifest["terminal"] = "error"
            raise
        else:
            manifest["terminal"] = "success"
        finally:
            (directory / "manifest.json").write_text(json.dumps(manifest))

    with monkeypatch.context() as scoped:
        if driver_failure:
            for key, value in overrides.items():
                scoped.setenv(key, value)
        if positive:
            action()
        else:
            with pytest.raises(Exception, match="HiveServer2"):
                action()

    records = _read_events(directory, roles=roles)
    for record in records:
        assert record["source"].startswith("/ray/python/ray/data/")
        assert record["node"] == (
            driver_node if record["role"] == "driver" else worker_node
        )
    rpc = [r for r in records if r["event"] == "rpc_attempt" and r["kind"] == "data"]
    assert len(rpc) == (1 if positive else 0)
    if positive:
        fetched = [
            r["rows"] for r in records if r["event"] == "fetch" and r["rows"] > 0
        ]
        assert len(fetched) >= 3 and sum(fetched) == ROWS
        for role in roles:
            events = {r["event"] for r in records if r["role"] == role}
            assert {"cursor_close", "connection_close"} <= events
    else:
        failed_role = "driver" if driver_failure else "worker"
        assert any(
            r["event"] == "error" and r["stage"] in {"connect", "session"}
            for r in records
            if r["role"] == failed_role
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
