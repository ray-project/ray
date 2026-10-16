"""Opt-in real Hive, native storage, and Ray worker tests for direct reads."""

import json
import os
import re
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.data import DataContext
from ray.data.tests.datasource.test_hive_integration import (
    _connection_options,
)

pytestmark = pytest.mark.skipif(
    not all(
        os.environ.get(key)
        for key in (
            "RAY_HIVE_TEST_HOST",
            "RAY_HIVE_DIRECT_HDFS_URI",
            "RAY_HIVE_DIRECT_S3_URI",
        )
    ),
    reason="Set real HS2, HDFS and S3 locations to run native Hive integration",
)


@pytest.fixture(scope="module")
def direct_runtime():
    with patch.dict(
        os.environ,
        {
            "RAY_DATA_PARQUET_FOOTER_NUM_ACTORS": "1",
            "RAY_DATA_PARQUET_BIN_PACKING_BYTES": "1",
        },
    ):
        ray.init(
            num_cpus=4, include_dashboard=False, object_store_memory=512 * 1024**2
        )
        original = DataContext.get_current()
        context = original.copy()
        context.use_datasource_v2 = True
        context.target_min_block_size = 1
        context.target_max_block_size = 16 * 1024
        DataContext._set_current(context)
        try:
            yield
        finally:
            ray.shutdown()
            DataContext._set_current(original)


def _storage(backend):
    uri = os.environ[f"RAY_HIVE_DIRECT_{backend.upper()}_URI"].rstrip("/")
    if backend == "hdfs":
        filesystem, base = pafs.FileSystem.from_uri(uri)
    else:
        parsed = urlparse(uri)
        filesystem = pafs.S3FileSystem(
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            session_token=os.environ.get("AWS_SESSION_TOKEN"),
            endpoint_override=os.environ.get("RAY_HIVE_DIRECT_S3_ENDPOINT"),
            scheme=os.environ.get("RAY_HIVE_DIRECT_S3_SCHEME", "https"),
            region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        base = parsed.netloc + parsed.path
    suffix = "native_" + uuid.uuid4().hex[:12]
    path = base + "/" + suffix
    filesystem.create_dir(path, recursive=True)
    location = uri + "/" + suffix
    if backend == "s3":
        # Hive uses Hadoop's S3A URI; the native Arrow client uses S3.
        location = "s3a:" + location.split(":", 1)[1]
    return filesystem, path, location


def _native_connection_factory():
    options = _connection_options()
    if options["auth_mechanism"] != "PLAIN" or options["use_ssl"]:
        pytest.fail(
            "Native storage integration requires the PLAIN/NONE non-TLS profile"
        )

    def factory():
        from impala.dbapi import connect

        password = os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get(
            "RAY_HIVE_TEST_PASSWORD"
        )
        return connect(**options, password=password, retries=1)

    return factory


def _reference_rows(cursor, table, complex_columns=()):
    """Read every logical column through Hive, independently of native readers."""
    cursor.execute(f"SELECT * FROM {table}")
    names = [column[0].split(".")[-1] for column in cursor.description]
    rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    for row in rows:
        for name in complex_columns:
            if isinstance(row[name], (str, bytes)):
                row[name] = json.loads(row[name])
    return sorted(rows, key=lambda row: row["id"])


def _metadata_only_factory(factory, statements):
    driver_pid = os.getpid()

    class Connection:
        def __init__(self):
            assert os.getpid() == driver_pid
            self.connection = factory()

        def cursor(self, **kwargs):
            cursor = self.connection.cursor(**kwargs)

            class Cursor:
                def execute(self, statement):
                    assert statement.startswith("DESCRIBE FORMATTED ")
                    statements.append(statement)
                    return cursor.execute(statement)

                def fetchall(self):
                    return cursor.fetchall()

                def close(self):
                    cursor.close()

            return Cursor()

        def close(self):
            self.connection.close()

    return Connection


@pytest.mark.parametrize("backend", ["hdfs", "s3"])
@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_nonpartitioned_native_read(direct_runtime, backend, file_format):
    filesystem, path, location = _storage(backend)
    connection_factory = _native_connection_factory()
    connection = connection_factory()
    cursor = connection.cursor()
    table_name = "ray_direct_" + uuid.uuid4().hex[:12]
    columns = (
        "id BIGINT, flag BOOLEAN, tiny TINYINT, small SMALLINT, n INT, "
        "ratio FLOAT, score DOUBLE, name STRING, payload BINARY, d DATE, "
        "amount DECIMAL(12,2), ts TIMESTAMP, items ARRAY<INT>, "
        "labels MAP<STRING,STRING>, nested STRUCT<name:STRING,value:INT>, "
        "code CHAR(4), tag VARCHAR(8)"
    )
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("flag", pa.bool_()),
            ("tiny", pa.int8()),
            ("small", pa.int16()),
            ("n", pa.int32()),
            ("ratio", pa.float32()),
            ("score", pa.float64()),
            ("name", pa.string()),
            ("payload", pa.binary()),
            ("d", pa.date32()),
            ("amount", pa.decimal128(12, 2)),
            ("ts", pa.timestamp("us")),
            ("items", pa.list_(pa.int32())),
            ("labels", pa.map_(pa.string(), pa.string())),
            ("nested", pa.struct([("name", pa.string()), ("value", pa.int32())])),
            ("code", pa.string()),
            ("tag", pa.string()),
        ]
    )
    rows = [
        {
            "id": i,
            "flag": i % 2 == 0,
            "tiny": i,
            "small": i * 100,
            "n": i,
            "ratio": 1.5,
            "score": 2.25,
            "name": None if i == 0 else str(i),
            "payload": b"data",
            "d": date(2026, 1, 2),
            "amount": Decimal("12.34"),
            "ts": None if i == 0 else datetime(2026, 1, 2, 3, 4, 5, 123456),
            "items": None if i == 0 else ([] if i == 1 else [i, None]),
            "labels": [] if i == 0 else [("key", str(i))],
            "nested": None if i == 0 else {"name": "nested", "value": i},
            "code": None
            if i == 0
            else ("    " if i == 1 else ("中文😀a" if i == 2 else "ab  ")),
            "tag": "abcdefgh" if i == 2 else "tag",
        }
        for i in range(32)
    ]
    expected = pa.Table.from_pylist(rows, schema=schema)
    physical = (
        expected.set_column(
            schema.get_field_index("ts"),
            "ts",
            pa.array([None] + [1767323045123456789] * 31, type=pa.timestamp("ns")),
        )
        .set_column(
            schema.get_field_index("code"),
            "code",
            pa.array([None, "", "中文😀abc"] + ["ab"] * 29, type=pa.string()),
        )
        .set_column(
            schema.get_field_index("tag"),
            "tag",
            pa.array(["tag", "tag", "abcdefghijkl"] + ["tag"] * 29),
        )
    )
    created = False
    observer = None
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table_name} ({columns}) "
            f"STORED AS {file_format.upper()} LOCATION '{location}'"
        )
        created = True
        for index in range(4):
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                batch = physical.slice(index * 8, 8)
                if file_format == "parquet":
                    pq.write_table(batch, stream, use_deprecated_int96_timestamps=True)
                else:
                    orc.write_table(batch, stream)

        driver_pid = os.getpid()

        statements = []
        metadata_only_factory = _metadata_only_factory(connection_factory, statements)

        @ray.remote(num_cpus=0)
        class ReadObserver:
            def __init__(self):
                self.pids = set()

            def record(self, pid):
                self.pids.add(pid)
                return len(self.pids)

        observer = ReadObserver.remote()

        def _tag_read_worker(batch):
            # Defined locally so worker deserialization doesn't depend on
            # pytest's bare test-module import name. It must fuse with ReadFiles.
            deadline = time.monotonic() + 20
            while ray.get(observer.record.remote(os.getpid())) < 2:
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        "Two native reading workers did not participate"
                    )
                time.sleep(0.05)
            return batch.append_column(
                "_read_pid",
                pa.repeat(pa.scalar(os.getpid(), type=pa.int64()), len(batch)),
            )

        dataset = ray.data.read_hive(
            table_name,
            connection_factory=metadata_only_factory,
            read_mode="direct",
            filesystem=filesystem,
            override_num_blocks=4,
        )
        materialized = dataset.map_batches(
            _tag_read_worker, batch_format="pyarrow"
        ).materialize()
        result = materialized.take_all()
        pids = {row.pop("_read_pid") for row in result}
        print(materialized.stats())
        assert len(pids) >= 2
        assert driver_pid not in pids
        assert re.search(
            r"ReadFilesHiveDirect(?:->[^\n:]*)?->MapBatches", materialized.stats()
        )
        assert sorted(result, key=lambda row: row["id"]) == expected.to_pylist()
        assert dataset.schema().base_schema == schema

        limited = ray.data.read_hive(
            table_name,
            connection_factory=metadata_only_factory,
            read_mode="direct",
            filesystem=filesystem,
            limit=3,
        )
        assert len(limited.take_all()) == 3
        zero = ray.data.read_hive(
            table_name,
            connection_factory=metadata_only_factory,
            read_mode="direct",
            filesystem=filesystem,
            limit=0,
        )
        assert zero.take_all() == []
        assert zero.schema().base_schema == schema

        normalized = sorted(result, key=lambda row: row["id"])
        for row in normalized:
            row["labels"] = dict(row["labels"]) if row["labels"] is not None else None
        assert normalized == _reference_rows(
            cursor, table_name, ("items", "labels", "nested")
        )
        assert len(statements) == 3  # One metadata query per direct Dataset.

        from ray.data.expressions import col

        converted_filter = dataset.filter(
            expr=(col("code") == "ab  ")
            & (col("ts") == datetime(2026, 1, 2, 3, 4, 5, 123456))
        ).select_columns(["id"])
        assert sorted(row["id"] for row in converted_filter.take_all()) == list(
            range(3, 32)
        )
    finally:
        if observer is not None:
            ray.kill(observer)
        if created:
            cursor.execute(f"DROP TABLE {table_name}")
        cursor.close()
        connection.close()
        filesystem.delete_dir(path)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_empty_native_table(direct_runtime, file_format):
    filesystem, path, location = _storage("s3")
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_empty_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id BIGINT, name STRING) "
            f"STORED AS {file_format.upper()} LOCATION '{location}'"
        )
        created = True
        dataset = ray.data.read_hive(
            table,
            connection_factory=factory,
            read_mode="direct",
            filesystem=filesystem,
            merge_schema=file_format == "parquet",
        )
        assert dataset.take_all() == []
        assert dataset.schema().base_schema == pa.schema(
            [("id", pa.int64()), ("name", pa.string())]
        )
    finally:
        if created:
            cursor.execute(f"DROP TABLE {table}")
        cursor.close()
        connection.close()
        filesystem.delete_dir(path)


def test_default_hs2_table_and_explicit_query_still_read(direct_runtime):
    filesystem, path, location = _storage("s3")
    with filesystem.open_output_stream(path + "/000000_0") as stream:
        pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), stream)
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_hs2_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id BIGINT, name STRING) "
            f"STORED AS PARQUET LOCATION '{location}'"
        )
        created = True
        result = ray.data.read_hive(table, connection_factory=factory).take_all()
        assert sorted(row["id"] for row in result) == [1, 2]
        result = ray.data.read_hive(
            query=f"SELECT id AS event_id FROM {table}",
            schema=pa.schema([("event_id", pa.int64())]),
            connection_factory=factory,
        ).take_all()
        assert sorted(row["event_id"] for row in result) == [1, 2]
    finally:
        if created:
            cursor.execute(f"DROP TABLE {table}")
        cursor.close()
        connection.close()
        filesystem.delete_dir(path)


def test_parquet_merge_schema_retains_nonempty_file_set(direct_runtime):
    filesystem, path, location = _storage("s3")
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_merge_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id BIGINT, late STRING) "
            f"STORED AS PARQUET LOCATION '{location}'"
        )
        created = True
        for index in range(17):
            values = {"id": [index]}
            if index == 16:
                values["late"] = ["present"]
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                pq.write_table(pa.table(values), stream)
        dataset = ray.data.read_hive(
            table,
            connection_factory=factory,
            read_mode="direct",
            filesystem=filesystem,
            merge_schema=True,
        )
        # Reuse native full-schema file-list retention, rather than discovering
        # a wider set of files when the Hive dataset is executed later.
        with filesystem.open_output_stream(path + "/published_later") as stream:
            pq.write_table(pa.table({"id": [99], "late": ["not planned"]}), stream)
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        assert result == [
            {"id": index, "late": "present" if index == 16 else None}
            for index in range(17)
        ]
    finally:
        if created:
            cursor.execute(f"DROP TABLE {table}")
        cursor.close()
        connection.close()
        filesystem.delete_dir(path)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_written_file_values(direct_runtime, file_format):
    filesystem, path, location = _storage("hdfs")
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_written_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id BIGINT, code CHAR(4), tag VARCHAR(3), "
            "ts TIMESTAMP, items ARRAY<INT>, labels MAP<STRING,STRING>, "
            "nested STRUCT<name:STRING,value:INT>) "
            f"STORED AS {file_format.upper()} LOCATION '{location}'"
        )
        created = True
        cursor.execute(
            f"INSERT INTO {table} SELECT CAST(1 AS BIGINT), CAST('ab' AS CHAR(4)), "
            "CAST('abcdef' AS VARCHAR(3)), CAST('2026-01-02 03:04:05.123456789' AS TIMESTAMP), "
            "array(1, CAST(NULL AS INT)), map('key', 'value'), "
            "named_struct('name', 'nested', 'value', 7)"
        )
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        result = dataset.take_all()
        for row in result:
            row["labels"] = dict(row["labels"])
        assert result == _reference_rows(cursor, table, ("items", "labels", "nested"))
        assert result[0]["code"] == "ab  "
        assert result[0]["tag"] == "abc"
        assert result[0]["ts"] == datetime(2026, 1, 2, 3, 4, 5, 123456)
        assert len(statements) == 1
    finally:
        try:
            if created:
                cursor.execute(f"DROP TABLE {table}")
        finally:
            cursor.close()
            connection.close()
            filesystem.delete_dir(path)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_written_epoch_timestamps(direct_runtime, file_format):
    from ray.data.expressions import col

    filesystem, path, location = _storage("hdfs")
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_epoch_" + uuid.uuid4().hex[:12]
    literals = [
        "1969-12-31 23:59:59.999998999",
        "1969-12-31 23:59:59.999999000",
        "1969-12-31 23:59:59.999999001",
        "1969-12-31 23:59:59.999999999",
        "1970-01-01 00:00:00.000000000",
        "1970-01-01 00:00:00.000000001",
        "1970-01-01 00:00:00.000000999",
        "1970-01-01 00:00:00.000001000",
        "1970-01-01 00:00:00.000001001",
        None,
    ]
    micros = [-2, -1, -1, -1, 0, 0, 0, 1, 1, None]
    if file_format == "orc":
        # The tested Hive ORC writer shifts fractions in the last pre-epoch
        # second on its own readback. Use earlier negative values for this
        # writer comparison; native file/unit tests still cover -1 ns in ORC.
        literals = [
            value.replace("23:59:59.", "23:59:58.") if value is not None else None
            for value in literals
        ]
        micros = [
            value - 1_000_000 if value is not None and value < 0 else value
            for value in micros
        ]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id INT, ts TIMESTAMP, "
            "nested STRUCT<ts:TIMESTAMP>, items ARRAY<TIMESTAMP>, "
            f"labels MAP<STRING,TIMESTAMP>) STORED AS {file_format.upper()} "
            f"LOCATION '{location}'"
        )
        created = True
        selects = []
        for index, literal in enumerate(literals):
            timestamp = (
                "CAST(NULL AS TIMESTAMP)"
                if literal is None
                else f"CAST('{literal}' AS TIMESTAMP)"
            )
            selects.append(
                f"SELECT {index}, {timestamp}, named_struct('ts', {timestamp}), "
                f"array({timestamp}, CAST(NULL AS TIMESTAMP)), map('key', {timestamp})"
            )
        cursor.execute(f"INSERT INTO {table} " + " UNION ALL ".join(selects))
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        timestamps = pa.array(micros, type=pa.timestamp("us")).to_pylist()
        assert result == [
            {
                "id": index,
                "ts": value,
                "nested": {"ts": value},
                "items": [value, None],
                "labels": [("key", value)],
            }
            for index, value in enumerate(timestamps)
        ]
        assert dataset.schema().base_schema == pa.schema(
            [
                ("id", pa.int32()),
                ("ts", pa.timestamp("us")),
                ("nested", pa.struct([("ts", pa.timestamp("us"))])),
                ("items", pa.list_(pa.timestamp("us"))),
                ("labels", pa.map_(pa.string(), pa.timestamp("us"))),
            ]
        )
        cursor.execute(
            f"SELECT id, ts, nested.ts, items[0], items[1], labels['key'] FROM {table}"
        )
        assert sorted(cursor.fetchall()) == [
            (index, value, value, value, None, value)
            for index, value in enumerate(timestamps)
        ]
        assert sorted(
            row["id"]
            for row in dataset.filter(expr=col("ts") < datetime(1970, 1, 1))
            .select_columns(["id"])
            .take_all()
        ) == [0, 1, 2, 3]
        assert sorted(
            row["id"]
            for row in dataset.filter(expr=col("ts") == datetime(1970, 1, 1))
            .select_columns(["id"])
            .take_all()
        ) == [4, 5, 6]
        assert len(statements) == 1
    finally:
        try:
            if created:
                cursor.execute(f"DROP TABLE {table}")
        finally:
            cursor.close()
            connection.close()
            filesystem.delete_dir(path)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_late_file_schema_failure_does_not_replay_hs2(direct_runtime, file_format):
    filesystem, path, location = _storage("s3")
    factory = _native_connection_factory()
    connection = factory()
    cursor = connection.cursor()
    table = "ray_failure_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} (id BIGINT) STORED AS {file_format.upper()} "
            f"LOCATION '{location}'"
        )
        created = True
        for index in range(17):
            data = (
                pa.table({"id": [index]})
                if index < 16
                else pa.table({"id": ["bad-hive-value"]})
            )
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                if file_format == "parquet":
                    pq.write_table(data, stream)
                else:
                    orc.write_table(data, stream)
        statements = []
        metadata_factory = _metadata_only_factory(factory, statements)
        dataset = ray.data.read_hive(
            table,
            connection_factory=metadata_factory,
            read_mode="direct",
            filesystem=filesystem,
        )
        assert len(statements) == 1
        with pytest.raises(Exception, match="bad-hive-value"):
            dataset.take_all()
        assert len(statements) == 1
        if file_format == "parquet":
            # The opt-in full mode checks the incompatible 17th schema while
            # planning; bounded mode above leaves it to the native read path.
            with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
                ray.data.read_hive(
                    table,
                    connection_factory=metadata_factory,
                    read_mode="direct",
                    filesystem=filesystem,
                    merge_schema=True,
                )
            assert len(statements) == 2
    finally:
        try:
            if created:
                cursor.execute(f"DROP TABLE {table}")
        finally:
            cursor.close()
            connection.close()
            filesystem.delete_dir(path)


def test_native_factory_accepts_existing_password_alias(monkeypatch):
    from unittest.mock import Mock

    from impala import dbapi

    connect = Mock()
    monkeypatch.setattr(dbapi, "connect", connect)
    monkeypatch.delenv("HIVE_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("RAY_HIVE_TEST_PASSWORD", "alias-password")
    _native_connection_factory()()
    assert connect.call_args.kwargs["password"] == "alias-password"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
