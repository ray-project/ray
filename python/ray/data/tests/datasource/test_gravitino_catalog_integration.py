"""Manual acceptance tests against disposable Gravitino REST and storage.

Required RAY_GRAVITINO_TEST settings: _GRAVITINO_URI, _METALAKE,
_ICEBERG_REST_URI, _ICEBERG_READ_TABLE, _ICEBERG_WRITE_TABLE,
_STORAGE_READ_DENIED_TABLE, _STORAGE_WRITE_DENIED_TABLE,
_READ_DENIED_REST_KWARGS, _READ_ONLY_REST_KWARGS and _DENIED_REST_KWARGS.
REST kwargs are JSON objects with PyIceberg authentication/storage properties.
_ICEBERG_REST_KWARGS configures the allowed identity; optional
_STORAGE_DENIED_REST_KWARGS supplies denied-policy storage settings.

The read table must contain at least four physical files. The write table must
be disposable and contain existing rows. Storage-denied identities must read
metadata/manifests while data-file GET or PUT is denied. The read-denied REST
identity must authenticate successfully but lack table-read privileges. The
read-only identity must read successfully but lack table-write privileges.

Use auxiliary Iceberg REST mode with Gravitino authorization enabled and the
dynamic catalog configuration provider. Provision identities, policies and
tables before invocation. Missing configuration fails this manual target.
Install PyIceberg and normal Ray test dependencies; no Gravitino SDK is needed.
Only the fixture owner cleans up this run's disposable resources.
"""

import json
import os
import re
import uuid

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    ForbiddenError,
    NoSuchTableError,
    RESTError,
    UnauthorizedError,
)
from pyiceberg.io.pyarrow import schema_to_pyarrow
from requests.exceptions import HTTPError

import ray
from ray.data.catalog import GravitinoCatalog, ReaderFormat
from ray.data.tests.conftest import *  # noqa: F401,F403
from ray.exceptions import RayTaskError

_PREFIX = "RAY_GRAVITINO_TEST"


def _required_env(suffix: str) -> str:
    name = f"{_PREFIX}_{suffix}"
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"Set {name} to run the Gravitino Iceberg integration tests.")
    return value


def _settings(suffix: str) -> dict:
    value = os.environ.get(f"{_PREFIX}_{suffix}", "{}")
    settings = json.loads(value)
    if not isinstance(settings, dict):
        pytest.fail(f"{_PREFIX}_{suffix} must be a JSON object.")
    return settings


def _identity_settings(suffix: str) -> dict:
    settings = _settings(suffix)
    if not settings:
        pytest.fail(f"Set {_PREFIX}_{suffix} to run REST identity denial.")
    return settings


def _make_catalog(*, rest_kwargs=None) -> GravitinoCatalog:
    return GravitinoCatalog(
        uri=_required_env("GRAVITINO_URI"),
        metalake=_required_env("METALAKE"),
        iceberg_rest_uri=_required_env("ICEBERG_REST_URI"),
        iceberg_rest_catalog_kwargs=(
            _settings("ICEBERG_REST_KWARGS") if rest_kwargs is None else rest_kwargs
        ),
    )


def _independent_table(identifier: str):
    """Load via PyIceberg directly, without using GravitinoCatalog.resolve."""
    catalog_name, schema_name, table_name = identifier.split(".")
    properties = _settings("ICEBERG_REST_KWARGS")
    properties.update(
        {
            "type": "rest",
            "uri": _required_env("ICEBERG_REST_URI"),
            "warehouse": catalog_name,
        }
    )
    if not any(key in properties for key in ("auth", "credential", "token")):
        properties["auth"] = {"type": "noop"}
    return load_catalog("independent", **properties).load_table(
        (schema_name, table_name)
    )


@pytest.fixture(scope="module")
def gravitino_catalog():
    return _make_catalog()


def test_iceberg_multitask_read(ray_start_regular_shared, gravitino_catalog):
    identifier = _required_env("ICEBERG_READ_TABLE")
    table = _independent_table(identifier)
    assert len(list(table.scan().plan_files())) >= 4
    expected = table.scan().to_arrow().to_pylist()
    dataset = ray.data.read_iceberg(
        table_identifier=identifier, catalog=gravitino_catalog, override_num_blocks=4
    ).materialize()
    actual = dataset.take_all()
    assert sorted(actual, key=lambda row: row["id"]) == sorted(
        expected, key=lambda row: row["id"]
    )
    assert dataset._raw_stats().extra_metrics["num_tasks_finished"] >= 2


def test_iceberg_append_and_independent_readback(
    ray_start_regular_shared, gravitino_catalog
):
    identifier = _required_env("ICEBERG_WRITE_TABLE")
    table = _independent_table(identifier)
    before_schema = table.schema()
    before_snapshot = table.current_snapshot()
    before_rows = table.scan().to_arrow().to_pylist()
    assert before_rows, "The append fixture must already contain rows."
    markers = [f"ray-gravitino-{uuid.uuid4().hex}" for _ in range(4)]
    schema = schema_to_pyarrow(before_schema)
    batches = [
        pa.Table.from_pylist([{"id": marker}], schema=schema) for marker in markers
    ]
    ray.data.from_arrow(batches).write_iceberg(identifier, catalog=gravitino_catalog)
    after = _independent_table(identifier)
    assert after.schema() == before_schema
    assert after.current_snapshot() != before_snapshot
    rows = after.scan().to_arrow().to_pylist()
    assert len(rows) == len(before_rows) + len(markers)
    assert all(sum(row["id"] == marker for row in rows) == 1 for marker in markers)
    assert {row["id"] for row in before_rows}.issubset({row["id"] for row in rows})


def test_iceberg_rest_read_denial(ray_start_regular_shared):
    catalog = _make_catalog(rest_kwargs=_identity_settings("READ_DENIED_REST_KWARGS"))
    identifier = _required_env("ICEBERG_READ_TABLE")
    catalog.resolve(identifier, reader=ReaderFormat.ICEBERG)
    with pytest.raises(ForbiddenError):
        ray.data.read_iceberg(table_identifier=identifier, catalog=catalog).take_all()


def test_iceberg_rest_write_privilege_is_distinct(ray_start_regular_shared):
    catalog = _make_catalog(rest_kwargs=_identity_settings("READ_ONLY_REST_KWARGS"))
    identifier = _required_env("ICEBERG_WRITE_TABLE")
    table = _independent_table(identifier)
    before = table.current_snapshot()
    before_rows = table.scan().to_arrow().to_pylist()
    assert ray.data.read_iceberg(
        table_identifier=identifier, catalog=catalog
    ).count() == len(before_rows)
    batch = pa.Table.from_pylist(
        [{"id": f"rest-denied-{uuid.uuid4().hex}"}],
        schema=schema_to_pyarrow(table.schema()),
    )
    with pytest.raises(ForbiddenError):
        ray.data.from_arrow(batch).write_iceberg(identifier, catalog=catalog)
    table.refresh()
    assert table.current_snapshot() == before
    assert table.scan().to_arrow().to_pylist() == before_rows


def test_iceberg_rest_auth_denial(ray_start_regular_shared):
    catalog = _make_catalog(rest_kwargs=_identity_settings("DENIED_REST_KWARGS"))
    identifier = _required_env("ICEBERG_READ_TABLE")
    catalog.resolve(identifier, reader=ReaderFormat.ICEBERG)
    with pytest.raises((UnauthorizedError, ForbiddenError)):
        ray.data.read_iceberg(table_identifier=identifier, catalog=catalog).take_all()


@pytest.mark.parametrize("missing", ["catalog", "table"])
def test_missing_iceberg_object(ray_start_regular_shared, gravitino_catalog, missing):
    catalog_name, schema_name, _ = _required_env("ICEBERG_READ_TABLE").split(".")
    identifier = (
        f"missing_catalog.{schema_name}.table"
        if missing == "catalog"
        else f"{catalog_name}.{schema_name}.missing_table"
    )
    with pytest.raises((RESTError, NoSuchTableError)) as caught:
        ray.data.read_iceberg(
            table_identifier=identifier, catalog=gravitino_catalog
        ).take_all()
    cause = caught.value.__cause__
    assert isinstance(cause, HTTPError)
    assert cause.response is not None
    assert cause.response.status_code == 404


def _assert_storage_denial(error):
    assert re.search(
        r"AccessDenied|access.denied|forbidden|permission|403",
        str(error.cause),
        re.IGNORECASE,
    ), "The worker must fail due to the configured object-store access policy."


def test_iceberg_worker_storage_read_denial(ray_start_regular_shared):
    identifier = _required_env("STORAGE_READ_DENIED_TABLE")
    catalog = _make_catalog(
        rest_kwargs={
            **_settings("ICEBERG_REST_KWARGS"),
            **_settings("STORAGE_DENIED_REST_KWARGS"),
        }
    )
    resolved = catalog.resolve(identifier, reader=ReaderFormat.ICEBERG)
    table = load_catalog("denied", **resolved.catalog_kwargs).load_table(
        resolved.table_identifier
    )
    assert list(table.scan().plan_files()), "Metadata and manifest reads must succeed."
    with pytest.raises(RayTaskError) as caught:
        ray.data.read_iceberg(table_identifier=identifier, catalog=catalog).take_all()
    _assert_storage_denial(caught.value)


def test_iceberg_worker_storage_write_denial(ray_start_regular_shared):
    identifier = _required_env("STORAGE_WRITE_DENIED_TABLE")
    catalog = _make_catalog(
        rest_kwargs={
            **_settings("ICEBERG_REST_KWARGS"),
            **_settings("STORAGE_DENIED_REST_KWARGS"),
        }
    )
    resolved = catalog.resolve(identifier, reader=ReaderFormat.ICEBERG)
    table = load_catalog("denied", **resolved.catalog_kwargs).load_table(
        resolved.table_identifier
    )
    before = table.current_snapshot()
    batch = pa.Table.from_pylist(
        [{"id": f"denied-{uuid.uuid4().hex}"}], schema=schema_to_pyarrow(table.schema())
    )
    with pytest.raises(RayTaskError) as caught:
        ray.data.from_arrow(batch).write_iceberg(identifier, catalog=catalog)
    _assert_storage_denial(caught.value)
    table.refresh()
    assert table.current_snapshot() == before


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
