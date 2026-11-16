"""Real HiveServer2 tests, enabled with RAY_HIVE_TEST_HOST."""

import json
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
from ray.data.expressions import col
from ray.tests.conftest import _ray_start

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


def _traced_factory(options, trace_path, fail_fetch=False):
    base_factory = _make_connection_factory(options)
    path = str(trace_path)

    def factory():
        class Cursor:
            def __init__(self, cursor):
                self.cursor = cursor
                self.fetches = 0

            def __getattr__(self, name):
                return getattr(self.cursor, name)

            def record(self, kind, value):
                with open(path, "a") as stream:
                    stream.write(json.dumps([kind, value]) + "\n")

            def execute(self, statement):
                self.record("execute", statement)
                return self.cursor.execute(statement)

            def fetchmany(self, size):
                self.fetches += 1
                if fail_fetch and self.fetches == 2:
                    raise RuntimeError("injected fetch failure after real HS2 output")
                return self.cursor.fetchmany(size)

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def cursor(self, *, user):
                return Cursor(self.connection.cursor(user=user))

            def close(self):
                return self.connection.close()

        return Connection(base_factory())

    return factory


def _data_statements(trace_path):
    if not trace_path.exists():
        return []
    return [
        value
        for kind, value in map(json.loads, trace_path.read_text().splitlines())
        if kind == "execute" and value.startswith("SELECT")
    ]


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


def test_hive_table_query_limit_and_ignored_block_hint():
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


class TestHiveTablePushdown:
    @pytest.fixture(scope="class", autouse=True)
    def shared_ray(self):
        with _ray_start(num_cpus=2, include_dashboard=False):
            yield

    def test_hive_projection_limit_and_filter_order_over_real_hs2(self, tmp_path):
        options = _connection_options()
        values = "SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c' UNION ALL SELECT 4, 'd' UNION ALL SELECT 5, 'e'"
        with _temporary_table(options, "id BIGINT, name STRING", values) as (table, _):

            def source(trace, limit=None):
                return ray.data.read_hive(
                    table,
                    limit=limit,
                    connection_factory=_traced_factory(options, trace),
                )

            trace = tmp_path / "projection.jsonl"
            rows = source(trace).select_columns(["name"]).limit(2).take_all()
            assert len(rows) == 2 and all(set(row) == {"name"} for row in rows)
            sql = _data_statements(trace)
            assert len(sql) == 1 and sql[0].startswith("SELECT `name`")
            assert sql[0].endswith(" LIMIT 2")

            trace = tmp_path / "branches.jsonl"
            dataset = source(trace)
            ids = dataset.select_columns(["id"])
            names = dataset.select_columns(["name"])
            assert sorted(row["id"] for row in ids.take_all()) == [1, 2, 3, 4, 5]
            assert sorted(row["name"] for row in names.take_all()) == [
                "a",
                "b",
                "c",
                "d",
                "e",
            ]
            assert sorted(row["id"] for row in ids.take_all()) == [1, 2, 3, 4, 5]
            statements = _data_statements(trace)
            assert len(statements) == 3
            assert [statement.split(" FROM ")[0] for statement in statements] == [
                "SELECT `id`",
                "SELECT `name`",
                "SELECT `id`",
            ]

            trace = tmp_path / "query.jsonl"
            query = f"SELECT id, name FROM {table}"
            rows = (
                ray.data.read_hive(
                    query=query,
                    schema=pa.schema([("id", pa.int64()), ("name", pa.string())]),
                    connection_factory=_traced_factory(options, trace),
                )
                .filter(expr=col("id") >= 3)
                .select_columns(["name"])
                .limit(2)
                .take_all()
            )
            assert len(rows) == 2 and all(
                row["name"] in ("c", "d", "e") for row in rows
            )
            assert _data_statements(trace) == [query]

            trace = tmp_path / "source-filter-limit.jsonl"
            rows = (
                source(trace, limit=5).filter(expr=col("id") >= 3).limit(2).take_all()
            )
            assert len(rows) == 2 and all(row["id"] >= 3 for row in rows)
            sql = _data_statements(trace)
            assert len(sql) == 1 and sql[0].endswith(" LIMIT 5")
            assert " WHERE " not in sql[0]

            trace = tmp_path / "filter-limit.jsonl"
            rows = source(trace).filter(expr=col("id") >= 3).limit(2).take_all()
            assert len(rows) == 2 and all(row["id"] >= 3 for row in rows)
            sql = _data_statements(trace)
            assert len(sql) == 1 and " WHERE " not in sql[0] and " LIMIT " not in sql[0]

            trace = tmp_path / "limit-filter.jsonl"
            rows = source(trace).limit(5).filter(expr=col("id") >= 3).take_all()
            assert sorted(row["id"] for row in rows) == [3, 4, 5]
            sql = _data_statements(trace)
            assert len(sql) == 1 and sql[0].endswith(" LIMIT 5")
            assert " WHERE " not in sql[0]

            trace = tmp_path / "empty.jsonl"
            result = source(trace).select_columns([]).materialize()
            assert result.count() == 5 and result.schema().names == []
            sql = _data_statements(trace)
            assert len(sql) == 1 and "1 AS `__ray_hive_row`" in sql[0]

            trace = tmp_path / "filter-empty.jsonl"
            result = (
                source(trace)
                .filter(expr=col("id") >= 3)
                .select_columns([])
                .materialize()
            )
            assert result.count() == 3 and result.schema().names == []
            sql = _data_statements(trace)
            assert len(sql) == 1 and " WHERE " not in sql[0]

            trace = tmp_path / "zero.jsonl"
            assert source(trace).limit(0).take_all() == []
            assert _data_statements(trace) == []

            trace = tmp_path / "fetch-failure.jsonl"
            failing = ray.data.read_hive(
                table,
                connection_factory=_traced_factory(options, trace, fail_fetch=True),
            ).select_columns(["id"])
            with pytest.raises(Exception):
                failing.take_all()
            assert len(_data_statements(trace)) == 1

    def test_hive_dataset_boolean_limit_over_real_hs2(self, tmp_path):
        options = _connection_options()
        values = "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
        with _temporary_table(options, "id BIGINT", values) as (table, _):
            for limit in (True, False):
                trace = tmp_path / f"boolean-limit-{limit}.jsonl"
                dataset = ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                ).limit(limit)
                rows = dataset.take_all()
                assert len(rows) == int(limit)
                assert all(row["id"] in (1, 2, 3) for row in rows)
                statements = _data_statements(trace)
                if limit:
                    assert len(statements) == 1 and " LIMIT " not in statements[0]
                else:
                    assert statements == []

            trace = tmp_path / "source-limit-boolean.jsonl"
            dataset = ray.data.read_hive(
                table, limit=2, connection_factory=_traced_factory(options, trace)
            ).limit(True)
            assert len(dataset.take_all()) == 1
            statements = _data_statements(trace)
            assert len(statements) == 1 and statements[0].endswith(" LIMIT 2")

    def test_hive_dataset_limit_integer_range_over_real_hs2(self, tmp_path):
        options = _connection_options()
        values = "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
        with _temporary_table(options, "id BIGINT", values) as (table, _):
            for limit in (2**31 - 1, 2**31, 2**63):
                trace = tmp_path / f"limit-{limit}.jsonl"
                dataset = ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                ).limit(limit)
                assert sorted(row["id"] for row in dataset.take_all()) == [1, 2, 3]
                statements = _data_statements(trace)
                assert len(statements) == 1
                if limit == 2**31 - 1:
                    assert statements[0].endswith(f" LIMIT {limit}")
                else:
                    assert " LIMIT " not in statements[0]

            trace = tmp_path / "smaller-limit.jsonl"
            dataset = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                )
                .limit(2**31)
                .limit(1)
            )
            rows = dataset.take_all()
            assert len(rows) == 1 and rows[0]["id"] in (1, 2, 3)
            statements = _data_statements(trace)
            assert len(statements) == 1 and statements[0].endswith(" LIMIT 1")

            trace = tmp_path / "filter-after-large-limit.jsonl"
            dataset = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                )
                .limit(2**31)
                .filter(expr=col("id") > 1)
                .limit(1)
            )
            rows = dataset.take_all()
            assert len(rows) == 1 and rows[0]["id"] in (2, 3)
            statements = _data_statements(trace)
            assert len(statements) == 1
            assert " WHERE " not in statements[0] and " LIMIT " not in statements[0]

    def test_hive_projection_quoted_columns_over_real_hs2(self, tmp_path):
        options = _connection_options()
        value = "O'Reilly\\line\n\t\x00中文"
        encoded = value.encode("utf-8").hex()
        values = f"SELECT 1, decode(unhex('{encoded}'), 'UTF-8'), '标签', 'tick'"
        with _temporary_table(
            options,
            "id BIGINT, `select` STRING, `类别` STRING, `a``b` STRING",
            values,
            storage_clause="STORED AS ORC",
        ) as (table, _):
            for index, (column, expected) in enumerate(
                (("select", value), ("类别", "标签"), ("a`b", "tick"))
            ):
                trace = tmp_path / f"quoted-{index}.jsonl"
                dataset = ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                ).select_columns([column, "id"])
                assert [dict(row) for row in dataset.take_all()] == [
                    {column: expected, "id": 1}
                ]
                assert dataset.schema().names == [column, "id"]
                statements = _data_statements(trace)
                assert len(statements) == 1 and " WHERE " not in statements[0]
                assert statements[0].startswith(
                    "SELECT `" + column.replace("`", "``") + "`, `id`"
                )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
