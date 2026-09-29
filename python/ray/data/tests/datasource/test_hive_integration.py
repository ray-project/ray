"""Real HiveServer2 tests, enabled with RAY_HIVE_TEST_HOST."""

import os
import uuid
from contextlib import contextmanager
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

import ray
import ray.data
from ray.data._internal.datasource.hive_contract import HiveConnectionOptions
from ray.data._internal.datasource.hive_hs2 import _connect as connect_read_hive

_HOST = os.environ.get("RAY_HIVE_TEST_HOST")
_AUTH = os.environ.get("RAY_HIVE_TEST_AUTH", "PLAIN").upper()

pytestmark = pytest.mark.skipif(
    not _HOST,
    reason="Set RAY_HIVE_TEST_HOST to run against a real HiveServer2 instance",
)


def _connection_options():
    if _AUTH not in ("NOSASL", "PLAIN", "GSSAPI"):
        pytest.fail("RAY_HIVE_TEST_AUTH must be NOSASL, PLAIN, or GSSAPI")
    password = os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get(
        "RAY_HIVE_TEST_PASSWORD"
    )
    if _AUTH == "PLAIN" and not password:
        pytest.fail("PLAIN integration requires HIVE_TEST_PASSWORD")
    if _AUTH != "PLAIN":
        password = None

    ssl_setting = os.environ.get("RAY_HIVE_TEST_USE_SSL", "false").lower()
    if ssl_setting not in ("true", "false", "1", "0"):
        pytest.fail("RAY_HIVE_TEST_USE_SSL must be true or false")
    options = dict(
        host=_HOST,
        port=int(os.environ.get("RAY_HIVE_TEST_PORT", "10000")),
        auth_mechanism=_AUTH,
        user=os.environ.get("RAY_HIVE_TEST_USER", "hive"),
        password=password,
        kerberos_service_name=os.environ.get("RAY_HIVE_TEST_KERBEROS_SERVICE", "hive"),
        use_ssl=ssl_setting in ("true", "1"),
        ca_cert=os.environ.get("RAY_HIVE_TEST_CA_CERT"),
    )
    HiveConnectionOptions(**options)
    return options


def _connect(options):
    from impala.dbapi import connect

    return connect(**options, verify_cert=options["use_ssl"], retries=1)


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


def test_hive_table_query_limit_and_repartition():
    options = _connection_options()
    with _temporary_table(
        options,
        "id BIGINT, name STRING",
        "VALUES (1, 'a'), (2, 'b')",
    ) as (table, cursor):
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        assert cursor.fetchone()[0] == 2

        ray.init(num_cpus=2, include_dashboard=False)
        try:
            rows = [
                dict(row) for row in ray.data.read_hive(table, **options).take_all()
            ]
            assert sorted(rows, key=lambda row: row["id"]) == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
            ]

            query = ray.data.read_hive(
                query=f"SELECT id FROM {table}",
                schema=pa.schema([("id", pa.int64())]),
                override_num_blocks=2,
                **options,
            )
            assert sorted(row["id"] for row in query.take_all()) == [1, 2]

            assert ray.data.read_hive(table, limit=0, **options).count() == 0
            assert ray.data.read_hive(table, limit=1, **options).count() == 1
        finally:
            ray.shutdown()


def test_hive_scalar_types_over_real_hs2():
    if _AUTH != "PLAIN":
        pytest.skip("Run the scalar type matrix once against the PLAIN/NONE profile")
    options = _connection_options()
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
            rows = ray.data.read_hive(table, **options).take_all()
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


def test_hive_tls_rejects_untrusted_or_mismatched_server():
    options = _connection_options()
    if not options["use_ssl"] or not options["ca_cert"]:
        pytest.skip("Run against a TLS-enabled HiveServer2 with a test CA certificate")

    wrong_host = os.environ.get("RAY_HIVE_TEST_WRONG_HOST", "127.0.0.1")
    if wrong_host == options["host"]:
        pytest.fail("RAY_HIVE_TEST_WRONG_HOST must differ from RAY_HIVE_TEST_HOST")
    with pytest.raises(RuntimeError, match="HiveServer2 connection failed"):
        connect_read_hive(HiveConnectionOptions(**{**options, "host": wrong_host}))
    with pytest.raises(RuntimeError, match="HiveServer2 connection failed"):
        connect_read_hive(HiveConnectionOptions(**{**options, "ca_cert": None}))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
