"""Real HiveServer2 smoke test, enabled with RAY_HIVE_TEST_HOST."""

import os
import uuid

import pyarrow as pa
import pytest

import ray
import ray.data

pytestmark = pytest.mark.skipif(
    not os.environ.get("RAY_HIVE_TEST_HOST"),
    reason="Set RAY_HIVE_TEST_HOST to run against a real HiveServer2 instance",
)


def test_hive_table_query_limit_and_repartition():
    from impala.dbapi import connect

    host = os.environ["RAY_HIVE_TEST_HOST"]
    port = int(os.environ.get("RAY_HIVE_TEST_PORT", "10000"))
    password = os.environ["RAY_HIVE_TEST_PASSWORD"]
    table = f"ray_hive_it_{uuid.uuid4().hex[:12]}"
    connection = connect(
        host=host,
        port=port,
        auth_mechanism="PLAIN",
        user="hive",
        password=password,
        kerberos_service_name="hive",
        retries=1,
    )
    cursor = connection.cursor(user="hive")
    try:
        cursor.execute(f"CREATE TABLE {table} (id BIGINT, name STRING)")
        cursor.execute(f"INSERT INTO TABLE {table} VALUES (1, 'a'), (2, 'b')")

        ray.init(num_cpus=2, include_dashboard=False)
        options = dict(
            host=host,
            port=port,
            auth_mechanism="PLAIN",
            user="hive",
            password=password,
        )
        rows = [dict(row) for row in ray.data.read_hive(table, **options).take_all()]
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
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
