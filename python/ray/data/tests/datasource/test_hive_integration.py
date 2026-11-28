"""Real HiveServer2 tests, enabled with RAY_HIVE_TEST_HOST."""

import os
import socket
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Optional, TypedDict, cast

import pyarrow as pa
import pytest

import ray
import ray.data
from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import (
    _connect as connect_read_hive,
)

_HOST = os.environ.get("RAY_HIVE_TEST_HOST")
_AUTH = os.environ.get("RAY_HIVE_TEST_AUTH", "PLAIN").upper()

pytestmark = pytest.mark.skipif(
    not _HOST,
    reason="Set RAY_HIVE_TEST_HOST to run against a real HiveServer2 instance",
)


class _ConnectionOptions(TypedDict):
    """Non-secret connection settings captured by the test factory."""

    host: str
    port: int
    auth_mechanism: Literal["NOSASL", "PLAIN", "GSSAPI"]
    user: str
    kerberos_service_name: str
    use_ssl: bool
    ca_cert: Optional[str]


def _connection_options() -> _ConnectionOptions:
    if _AUTH not in ("NOSASL", "PLAIN", "GSSAPI"):
        pytest.fail("RAY_HIVE_TEST_AUTH must be NOSASL, PLAIN, or GSSAPI")
    password = os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get(
        "RAY_HIVE_TEST_PASSWORD"
    )
    if _AUTH == "PLAIN" and not password:
        pytest.fail("PLAIN integration requires HIVE_TEST_PASSWORD")
    ssl_setting = os.environ.get("RAY_HIVE_TEST_USE_SSL", "false").lower()
    if ssl_setting not in ("true", "false", "1", "0"):
        pytest.fail("RAY_HIVE_TEST_USE_SSL must be true or false")
    options: _ConnectionOptions = {
        "host": cast(str, _HOST),
        "port": int(os.environ.get("RAY_HIVE_TEST_PORT", "10000")),
        "auth_mechanism": cast(Literal["NOSASL", "PLAIN", "GSSAPI"], _AUTH),
        "user": os.environ.get("RAY_HIVE_TEST_USER", "hive"),
        "kerberos_service_name": os.environ.get(
            "RAY_HIVE_TEST_KERBEROS_SERVICE", "hive"
        ),
        "use_ssl": ssl_setting in ("true", "1"),
        "ca_cert": os.environ.get("RAY_HIVE_TEST_CA_CERT"),
    }
    return options


def _make_connection_factory(options: _ConnectionOptions):
    def connection_factory():
        from impala.dbapi import connect

        password = None
        if options["auth_mechanism"] == "PLAIN":
            password = os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get(
                "RAY_HIVE_TEST_PASSWORD"
            )
            if not password:
                raise ValueError("PLAIN integration requires HIVE_TEST_PASSWORD")
        return connect(
            **options, password=password, verify_cert=options["use_ssl"], retries=1
        )

    return connection_factory


def _connect(options: _ConnectionOptions):
    return connect_read_hive(_make_connection_factory(options))


@contextmanager
def _temporary_table(options, columns, insert_clause, storage_clause=""):
    connection = _connect(options)
    cursor = None
    table = f"ray_hive_it_{uuid.uuid4().hex[:12]}"
    created = False
    try:
        cursor = connection.cursor(user=options["user"])
        cursor.execute(f"CREATE TABLE {table} ({columns}) {storage_clause}")
        created = True
        cursor.execute(f"INSERT INTO TABLE {table} {insert_clause}")
        yield table, cursor
    finally:
        try:
            if created and cursor is not None:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            try:
                if cursor is not None:
                    cursor.close()
            finally:
                connection.close()


def test_hive_table_query_limit_and_ignored_block_hint(tmp_path, shutdown_only):
    options = _connection_options()
    connection_factory = _make_connection_factory(options)
    with _temporary_table(
        options,
        "id BIGINT, name STRING",
        "VALUES (1, 'a'), (2, 'b')",
    ) as (table, cursor):
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count_row = cursor.fetchone()
        assert count_row is not None
        assert count_row[0] == 2

        ray.init(num_cpus=2, include_dashboard=False)
        try:
            rows = [
                dict(row)
                for row in ray.data.read_hive(
                    table, connection_factory=connection_factory, user=options["user"]
                ).take_all()
            ]
            assert sorted(rows, key=lambda row: row["id"]) == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
            ]

            query = ray.data.read_hive(
                query=f"SELECT id FROM {table}",
                schema=pa.schema([("id", pa.int64())]),
                override_num_blocks=2,
                connection_factory=connection_factory,
                user=options["user"],
            )
            assert sorted(row["id"] for row in query.take_all()) == [1, 2]

            assert (
                ray.data.read_hive(
                    table,
                    limit=0,
                    connection_factory=connection_factory,
                    user=options["user"],
                ).count()
                == 0
            )
            assert (
                ray.data.read_hive(
                    table,
                    limit=1,
                    connection_factory=connection_factory,
                    user=options["user"],
                ).count()
                == 1
            )

            trace = tmp_path / "statements"
            table_sql = f"`default`.`{table}`"

            def source(limit=None):
                trace.write_text("")
                return ray.data.read_hive(
                    table,
                    limit=limit,
                    connection_factory=_traced_factory(options, trace),
                )

            rows = source(2).select_columns(["name"]).limit(1).take_all()
            assert len(rows) == 1 and set(rows[0]) == {"name"}
            assert trace.read_text() == f"SELECT `name` FROM {table_sql} LIMIT 1\n"

            result = source().select_columns([]).materialize()
            assert result.count() == 2 and result.schema().names == []
            assert (
                trace.read_text() == f"SELECT 1 AS `__ray_hive_row` FROM {table_sql}\n"
            )
            assert source().limit(0).take_all() == []
            assert trace.read_text() == ""

            with _temporary_table(
                options,
                "`a+b` BIGINT, ab BIGINT, `类别` BIGINT, `a``b` BIGINT",
                "SELECT 11, 22, 33, 44",
                storage_clause="STORED AS ORC",
            ) as (special_table, _):
                for mode in ("column", "none"):
                    trace.write_text("")
                    columns = ["a+b", "类别", "a`b"]
                    dataset = (
                        ray.data.read_hive(
                            special_table,
                            connection_factory=_traced_factory(options, trace, mode),
                        )
                        .select_columns(columns)
                        .limit(1)
                    )
                    assert [dict(row) for row in dataset.take_all()] == [
                        {"a+b": 11, "类别": 33, "a`b": 44}
                    ]
                    assert dataset.schema().names == columns
                    assert (
                        trace.read_text()
                        == f"SELECT * FROM `default`.`{special_table}` LIMIT 1\n"
                    )
        finally:
            ray.shutdown()


def test_hive_scalar_types_over_real_hs2():
    if _AUTH != "PLAIN":
        pytest.skip("Run the scalar type matrix once with RAY_HIVE_TEST_AUTH=PLAIN")
    options = _connection_options()
    connection_factory = _make_connection_factory(options)
    columns = (
        "flag BOOLEAN, tiny TINYINT, small SMALLINT, id INT, big BIGINT, "
        "ratio FLOAT, score DOUBLE, name STRING, payload BINARY, "
        "event_date DATE, amount DECIMAL(12,2)"
    )
    insert_clause = (
        "SELECT TRUE, CAST(7 AS TINYINT), CAST(123 AS SMALLINT), "
        "CAST(42 AS INT), CAST(1234567890123 AS BIGINT), "
        "CAST(1.5 AS FLOAT), CAST(2.25 AS DOUBLE), 'name', "
        "binary('ab'), CAST('2024-01-02' AS DATE), "
        "CAST(12.34 AS DECIMAL(12,2))"
    )
    with _temporary_table(
        options, columns, insert_clause, storage_clause="STORED AS ORC"
    ) as (table, _):
        ray.init(num_cpus=2, include_dashboard=False)
        try:
            rows = ray.data.read_hive(
                table, connection_factory=connection_factory, user=options["user"]
            ).take_all()
            assert len(rows) == 1
            assert dict(rows[0]) == {
                "flag": True,
                "tiny": 7,
                "small": 123,
                "id": 42,
                "big": 1234567890123,
                "ratio": 1.5,
                "score": 2.25,
                "name": "name",
                "payload": b"ab",
                "event_date": date(2024, 1, 2),
                "amount": Decimal("12.34"),
            }
        finally:
            ray.shutdown()


def test_hive_timestamps_over_real_hs2():
    options = _connection_options()
    connection_factory = _make_connection_factory(options)
    schema = pa.schema([("id", pa.int64()), ("created_at", pa.timestamp("us"))])
    insert_clause = (
        "SELECT CAST(1 AS BIGINT), "
        "CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) "
        "UNION ALL SELECT CAST(2 AS BIGINT), CAST(NULL AS TIMESTAMP) "
        "UNION ALL SELECT CAST(3 AS BIGINT), "
        "CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP) "
        "UNION ALL SELECT CAST(4 AS BIGINT), "
        "CAST('2024-01-02 03:04:05' AS TIMESTAMP)"
    )
    expected = [
        {"id": 1, "created_at": datetime(2024, 1, 2, 3, 4, 5, 123456)},
        {"id": 2, "created_at": None},
        {"id": 3, "created_at": datetime(1969, 12, 31, 23, 59, 59, 999999)},
        {"id": 4, "created_at": datetime(2024, 1, 2, 3, 4, 5)},
    ]
    with _temporary_table(
        options, "id BIGINT, created_at TIMESTAMP", insert_clause, "STORED AS TEXTFILE"
    ) as (table, cursor):
        # Verify Hive retained nanoseconds before Impyla decodes the TIMESTAMP.
        cursor.execute(
            f"SELECT id, CAST(created_at AS STRING) FROM {table} ORDER BY id"
        )
        stored = cursor.fetchall()
        assert len(stored) == 4
        assert stored[0] == (1, "2024-01-02 03:04:05.123456789")
        assert stored[1] == (2, None)
        assert stored[2] == (3, "1969-12-31 23:59:59.999999999")

        ray.init(num_cpus=2, include_dashboard=False)
        try:
            for read_mode in ("table", "query"):
                if read_mode == "table":
                    dataset = ray.data.read_hive(
                        table,
                        connection_factory=connection_factory,
                        user=options["user"],
                    )
                else:
                    dataset = ray.data.read_hive(
                        query=f"SELECT id AS id, created_at AS created_at FROM {table}",
                        schema=schema,
                        connection_factory=connection_factory,
                        user=options["user"],
                    )
                result = dataset.materialize()
                assert result.schema().base_schema == schema
                assert sorted(result.take_all(), key=lambda row: row["id"]) == expected
        finally:
            ray.shutdown()


def test_hive_tls_rejects_untrusted_or_mismatched_server():
    """TLS negative tests against a real HiveServer2.

    Assumes the test CA is untrusted by the system CA bundle and that the
    server certificate has no SAN for RAY_HIVE_TEST_WRONG_HOST (default
    127.0.0.1). The positive control and the TCP probe below ensure the
    negative assertions fail at TLS certificate validation rather than at
    connection setup.
    """
    options = _connection_options()
    if not options["use_ssl"] or not options["ca_cert"]:
        pytest.skip("Run against a TLS-enabled HiveServer2 with a test CA certificate")

    # Positive control: the configured endpoint trusts the test CA.
    _connect(options).close()

    wrong_host = os.environ.get("RAY_HIVE_TEST_WRONG_HOST", "127.0.0.1")
    if wrong_host == options["host"]:
        pytest.fail("RAY_HIVE_TEST_WRONG_HOST must differ from RAY_HIVE_TEST_HOST")
    try:
        socket.create_connection((wrong_host, options["port"]), timeout=10).close()
    except OSError:
        pytest.fail(
            "RAY_HIVE_TEST_WRONG_HOST must route to the same TLS-enabled HiveServer2"
        )

    mismatched_host_options = options.copy()
    mismatched_host_options["host"] = wrong_host
    with pytest.raises(RuntimeError, match="HiveServer2 connection failed"):
        _connect(mismatched_host_options)

    untrusted_ca_options = options.copy()
    untrusted_ca_options["ca_cert"] = None
    with pytest.raises(RuntimeError, match="HiveServer2 connection failed"):
        _connect(untrusted_ca_options)


def _traced_factory(options, trace_path, quoted_identifiers=None):
    base_factory = _make_connection_factory(options)

    def factory():
        class Proxy:
            def __init__(self, obj):
                self.obj = obj

            def __getattr__(self, name):
                return getattr(self.obj, name)

        class Cursor(Proxy):
            def execute(self, statement):
                with trace_path.open("a") as stream:
                    stream.write(statement + "\n")
                return self.obj.execute(statement)

        class Connection(Proxy):
            def cursor(self, *, user):
                cursor = self.obj.cursor(user=user)
                if quoted_identifiers is not None:
                    cursor.execute(
                        "SET hive.support.quoted.identifiers=" + quoted_identifiers
                    )
                return Cursor(cursor)

        return Connection(base_factory())

    return factory


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
