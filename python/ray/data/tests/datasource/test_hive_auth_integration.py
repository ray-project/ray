"""Real binary HS2 security profiles; run through the dedicated Compose entry."""

import json
import os
import secrets
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
    _make_auth_factory,
    _native,
    _read_summaries,
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
    options = _connection_options(PROFILE)
    try:
        yield options
    finally:
        DataContext._set_current(previous)
        ray.shutdown()


@pytest.fixture(scope="module")
def table(environment):
    with _temporary_table(environment) as name:
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
            "ca": {"HIVE_AUTH_CA_OVERRIDE": "/ca/wrong-ca.pem"},
            "host": {"HIVE_AUTH_HOST_OVERRIDE": "wrong.hive.auth.test"},
            "file": {"HIVE_AUTH_CA_OVERRIDE": "/ca/missing.pem"},
        }[condition]
        if condition in {"ca", "host", "file"}:
            if driver_failure:
                _tls_condition(condition)
            else:
                _worker(_tls_condition, condition)

    case_id = uuid.uuid4().hex
    directory = _case_directory(case_id)
    directory.mkdir(parents=True)

    def action():
        _read(
            table,
            environment,
            case_id,
            query=query,
            worker_env={} if driver_failure else overrides,
        )

    with monkeypatch.context() as scoped:
        if driver_failure:
            for key, value in overrides.items():
                scoped.setenv(key, value)
        if positive:
            action()
        else:
            with pytest.raises(Exception, match="HiveServer2"):
                action()

    records = _read_summaries(
        directory,
        {role: driver_node if role == "driver" else worker_node for role in roles},
    )
    assert sum(r["data_rpc"] for r in records) == (1 if positive else 0)
    if positive:
        assert sum(r["batches"] for r in records) >= 3
        assert sum(r["rows"] for r in records) == ROWS
        assert all(r["cursor_closed"] and r["connection_closed"] for r in records)
    else:
        failed_role = "driver" if driver_failure else "worker"
        assert any(
            r["error"] in {"connect", "session"}
            for r in records
            if r["role"] == failed_role
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
