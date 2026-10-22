from dataclasses import FrozenInstanceError
from typing import Any, cast

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveReadSpec,
)


def test_table_and_query_modes():
    def connection_factory():
        return object()

    assert HiveReadSpec(connection_factory, table="events").table_identifier == (
        "default",
        "events",
    )
    assert HiveReadSpec(
        connection_factory, table="analytics.events", limit=0
    ).table_identifier == (
        "analytics",
        "events",
    )

    schema = pa.schema([("id", pa.int64())])
    query = HiveReadSpec(
        connection_factory, query="SELECT id FROM events", schema=schema
    )
    assert query.table_identifier is None
    assert query.schema == schema
    with pytest.raises(FrozenInstanceError):
        cast(Any, query).limit = 1


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({}, "exactly one"),
        ({"table": "events", "query": "SELECT 1"}, "exactly one"),
        ({"table": ""}, "table must be"),
        ({"table": 123}, "table must be"),
        ({"table": "analytics.events.extra"}, "table must be"),
        ({"table": "analytics.bad-name"}, "table must be"),
        ({"table": "events", "schema": pa.schema([("id", pa.int64())])}, "schema"),
        ({"table": "events", "limit": -1}, "limit"),
        ({"table": "events", "limit": True}, "limit"),
        ({"query": "SELECT 1"}, "pyarrow.Schema"),
        ({"query": "  ", "schema": pa.schema([("id", pa.int64())])}, "query"),
        ({"query": "SELECT 1", "schema": pa.schema([])}, "pyarrow.Schema"),
        (
            {
                "query": "SELECT 1",
                "schema": pa.schema([("id", pa.int64()), ("id", pa.string())]),
            },
            "unique column names",
        ),
        (
            {
                "query": "SELECT 1",
                "schema": pa.schema([("ID", pa.int64()), ("id", pa.string())]),
            },
            "unique column names",
        ),
        (
            {
                "query": "SELECT 1",
                "schema": pa.schema([("id", pa.int64())]),
                "limit": 1,
            },
            "only supported for table",
        ),
    ],
)
def test_read_spec_rejects_unsupported_inputs(kwargs, message):
    with pytest.raises(ValueError, match=message):
        HiveReadSpec(
            lambda: object(),
            **kwargs,
        )


@pytest.mark.parametrize("connection_factory", [None, "hs2", 1, object()])
def test_read_spec_requires_callable_connection_factory(connection_factory):
    with pytest.raises(TypeError, match="connection_factory must be callable"):
        HiveReadSpec(connection_factory, table="events")


@pytest.mark.parametrize("user", ["", " reader ", 123, True])
def test_read_spec_rejects_invalid_session_user(user):
    with pytest.raises(ValueError, match="user"):
        HiveReadSpec(lambda: object(), user=user, table="events")


def test_factory_is_not_called_during_input_validation():
    def connection_factory():
        pytest.fail("Input validation must not open a connection")

    spec = HiveReadSpec(connection_factory, table="events", user="session-user")
    assert spec.connection_factory is connection_factory
    assert spec.user == "session-user"


def test_callable_object_and_query_are_hidden_from_repr():
    class ConnectionFactory:
        def __call__(self):
            return object()

        def __repr__(self):
            return "private-factory-state"

    factory = ConnectionFactory()
    query_text = "SELECT * FROM events WHERE token = 'private-literal'"
    spec = HiveReadSpec(
        factory, query=query_text, schema=pa.schema([("id", pa.int64())])
    )
    assert spec.connection_factory is factory
    assert "private-factory-state" not in repr(spec)
    assert "private-literal" not in repr(spec)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
