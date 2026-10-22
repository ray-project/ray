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
from ray.data.expressions import col, lit

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
                rows = self.cursor.fetchmany(size)
                self.record("fetch", [len(rows), len(self.cursor.description)])
                return rows

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


def test_hive_table_pushdown_types_and_combinations(tmp_path, restore_data_context):
    options = _connection_options()
    columns = "id BIGINT, name STRING, score DOUBLE, amount DECIMAL(12,2), day DATE, flag BOOLEAN, padded CHAR(8), code VARCHAR(8)"
    values = (
        "SELECT 1, 'a', 1.5, CAST(12.34 AS DECIMAL(12,2)), DATE '2024-01-02', TRUE, 'a ', 'x ' "
        "UNION ALL SELECT 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL "
        "UNION ALL SELECT 3, 'b', 3.5, CAST(-0.01 AS DECIMAL(12,2)), DATE '2025-01-02', FALSE, 'b', 'y'"
    )
    with _temporary_table(options, columns, values) as (table, _):
        ray.init(num_cpus=2, include_dashboard=False)
        try:
            context = ray.data.DataContext.get_current()
            context.enable_progressbars = False
            context.target_max_block_size = 1
            baseline = ray.data.read_hive(
                table, connection_factory=_make_connection_factory(options)
            )
            schema = baseline.schema().base_schema
            reference = pa.Table.from_pylist(
                [dict(row) for row in baseline.take_all()], schema=schema
            )
            cases = [
                (col("id") >= 2, True),
                (col("flag") == lit(True), True),
                (col("amount") >= Decimal("0.00"), True),
                (col("day") < date(2025, 1, 1), True),
                (col("name") == "a", True),
                (col("code") == "x ", True),
                (col("code") == "x value longer than varchar length", True),
                (col("name").is_null(), True),
                (col("amount").is_not_null(), True),
                ((col("id") == 1) | (col("id") == 3), True),
                (~(col("id") == 1), True),
                (
                    (col("amount") >= Decimal("0.00")) | (col("flag") == lit(False)),
                    True,
                ),
                (~(col("amount") >= Decimal("0.00")), True),
                ((col("amount") >= Decimal("0.00")) & (col("flag") == lit(True)), True),
                (col("score") > 2.0, False),
                (col("padded") == "a ", False),
            ]
            for index, (predicate, pushed) in enumerate(cases):
                trace = tmp_path / f"type-{index}.jsonl"
                expected = (
                    ray.data.from_arrow(reference)
                    .filter(expr=predicate)
                    .select_columns(["id"])
                    .take_all()
                )
                actual = (
                    ray.data.read_hive(
                        table, connection_factory=_traced_factory(options, trace)
                    )
                    .filter(expr=predicate)
                    .select_columns(["id"])
                    .take_all()
                )
                assert sorted(
                    (dict(row) for row in actual), key=lambda row: row["id"]
                ) == sorted((dict(row) for row in expected), key=lambda row: row["id"])
                statements = _data_statements(trace)
                assert len(statements) == 1
                assert (" WHERE " in statements[0]) == pushed
                if pushed:
                    assert statements[0].startswith("SELECT `id` FROM")
                    fetched = [
                        value
                        for kind, value in map(
                            json.loads, trace.read_text().splitlines()
                        )
                        if kind == "fetch"
                    ]
                    assert sum(count for count, width in fetched) == len(actual)
                    assert all(width == 1 for count, width in fetched)

            trace = tmp_path / "mixed.jsonl"
            mixed = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                )
                .filter(expr=(col("id") >= 1) & (col("score") > 3.0))
                .select_columns(["id"])
                .limit(1)
            )
            assert [dict(row) for row in mixed.take_all()] == [{"id": 3}]
            assert len(_data_statements(trace)) == 1
            assert _data_statements(trace)[0].startswith("SELECT `id`, `score`")
            assert " LIMIT " not in _data_statements(trace)[0]

            trace = tmp_path / "limit.jsonl"
            limited = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                )
                .filter(expr=col("id") >= 2)
                .select_columns(["id"])
                .limit(1)
            )
            rows = limited.take_all()
            assert len(rows) == 1 and rows[0]["id"] in (2, 3)
            assert _data_statements(trace)[0].endswith("LIMIT 1")

            trace = tmp_path / "source-limit.jsonl"
            source_limited = (
                ray.data.read_hive(
                    table, limit=2, connection_factory=_traced_factory(options, trace)
                )
                .filter(expr=col("id") > 2)
                .limit(1)
            )
            rows = source_limited.take_all()
            assert len(rows) <= 1 and all(row["id"] > 2 for row in rows)
            assert len(_data_statements(trace)) == 1
            assert " WHERE " not in _data_statements(trace)[0]
            assert _data_statements(trace)[0].endswith("LIMIT 2")

            trace = tmp_path / "empty.jsonl"
            empty_projection = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace)
                )
                .filter(expr=col("id") >= 2)
                .select_columns([])
                .materialize()
            )
            assert empty_projection.count() == 2
            assert (
                empty_projection.schema().names == empty_projection.schema().types == []
            )
            assert len(_data_statements(trace)) == 1
            assert "1 AS `__ray_hive_row`" in _data_statements(trace)[0]

            trace = tmp_path / "zero.jsonl"
            zero = ray.data.read_hive(
                table, connection_factory=_traced_factory(options, trace)
            ).limit(0)
            assert zero.take_all() == []
            assert zero.schema().base_schema == schema
            assert _data_statements(trace) == []

            trace = tmp_path / "fetch-failure.jsonl"
            failing = ray.data.read_hive(
                table,
                connection_factory=_traced_factory(options, trace, fail_fetch=True),
            ).select_columns(["id"])
            with pytest.raises(Exception):
                failing.take_all()
            assert len(_data_statements(trace)) == 1
        finally:
            ray.shutdown()


def test_hive_dataset_limit_integer_range_over_real_hs2(tmp_path):
    options = _connection_options()
    values = "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
    with _temporary_table(options, "id BIGINT", values) as (table, _):
        ray.init(num_cpus=2, include_dashboard=False)
        try:
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
        finally:
            ray.shutdown()


def test_hive_pushdown_quoted_columns_and_literals(tmp_path):
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
        ray.init(num_cpus=2, include_dashboard=False)
        try:
            for index, (column, literal) in enumerate(
                (("select", value), ("类别", "标签"), ("a`b", "tick"))
            ):
                trace = tmp_path / f"quoted-{index}.jsonl"
                dataset = (
                    ray.data.read_hive(
                        table, connection_factory=_traced_factory(options, trace)
                    )
                    .filter(expr=col(column) == literal)
                    .select_columns([column, "id"])
                )
                assert [dict(row) for row in dataset.take_all()] == [
                    {column: literal, "id": 1}
                ]
                assert dataset.schema().names == [column, "id"]
                assert len(_data_statements(trace)) == 1
                assert " WHERE " in _data_statements(trace)[0]
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
