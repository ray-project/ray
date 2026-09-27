from dataclasses import FrozenInstanceError

import pyarrow as pa
import pytest

from ray.data._internal.datasource.hive_contract import (
    HiveConnectionOptions,
    HiveReadSpec,
)


def test_table_and_query_modes():
    connection = HiveConnectionOptions(host="hive.example.com", auth_mechanism="NOSASL")
    assert HiveReadSpec(connection, table="events").table_identifier == (
        "default",
        "events",
    )
    assert HiveReadSpec(
        connection, table="analytics.events", limit=0
    ).table_identifier == (
        "analytics",
        "events",
    )

    schema = pa.schema([("id", pa.int64())])
    query = HiveReadSpec(connection, query="SELECT id FROM events", schema=schema)
    assert query.table_identifier is None
    assert query.schema == schema
    with pytest.raises(FrozenInstanceError):
        query.limit = 1


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
            HiveConnectionOptions(host="hive.example.com", auth_mechanism="NOSASL"),
            **kwargs,
        )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"host": ""}, "host"),
        ({"host": " hive.example.com "}, "host"),
        ({"host": "hive\n.example.com"}, "host"),
        ({"host": "hive.example.com", "port": True}, "port"),
        ({"host": "hive.example.com", "port": 0}, "port"),
        ({"host": "hive.example.com", "auth_mechanism": "NONE"}, "auth_mechanism"),
        ({"host": "hive.example.com", "auth_mechanism": "PLAIN"}, "user and password"),
        ({"host": "hive.example.com", "password": "secret"}, "only supported"),
        (
            {
                "host": "hive.example.com",
                "auth_mechanism": "GSSAPI",
                "password": "secret",
            },
            "only supported",
        ),
        ({"host": "hive.example.com", "user": ""}, "user"),
        ({"host": "hive.example.com", "ca_cert": "ca.pem"}, "use_ssl"),
        ({"host": "hive.example.com", "use_ssl": "yes"}, "use_ssl"),
        ({"host": "hive.example.com", "timeout": float("inf")}, "timeout"),
        ({"host": "hive.example.com", "timeout": 0}, "timeout"),
        ({"host": "hive.example.com", "timeout": -1}, "timeout"),
    ],
)
def test_connection_options_reject_unsupported_inputs(kwargs, message):
    with pytest.raises(ValueError, match=message):
        HiveConnectionOptions(**{"auth_mechanism": "NOSASL", **kwargs})


def test_authentication_selection_is_required():
    with pytest.raises(TypeError, match="auth_mechanism"):
        HiveConnectionOptions(host="hive.example.com")


def test_read_spec_requires_connection_options():
    with pytest.raises(TypeError, match="connection"):
        HiveReadSpec("hive.example.com", table="events")


def test_supported_auth_profiles_and_secret_redaction():
    plain = HiveConnectionOptions(
        host="hive.example.com",
        auth_mechanism="PLAIN",
        user="reader",
        password="private-password",
        use_ssl=True,
        ca_cert="ca.pem",
    )
    kerberos = HiveConnectionOptions(host="hive.example.com", auth_mechanism="GSSAPI")
    assert plain.auth_mechanism == "PLAIN"
    assert kerberos.kerberos_service_name == "hive"
    assert "private-password" not in repr(plain)

    query_text = "SELECT * FROM events WHERE token = 'private-literal'"
    spec = HiveReadSpec(plain, query=query_text, schema=pa.schema([("id", pa.int64())]))
    assert "private-password" not in repr(spec)
    assert "private-literal" not in repr(spec)
