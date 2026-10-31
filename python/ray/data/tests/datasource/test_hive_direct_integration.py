"""Opt-in real Hive, native storage, and Ray worker tests for direct reads."""

import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.actor import ActorHandle
from ray.data import DataContext
from ray.data.tests.datasource.test_hive_integration import (
    _connection_options,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("RAY_HIVE_TEST_HOST"),
    reason="Set RAY_HIVE_TEST_HOST to run real Hive direct-read integration",
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
    uri = os.environ.get(f"RAY_HIVE_DIRECT_{backend.upper()}_URI")
    if not uri:
        pytest.skip(f"Set RAY_HIVE_DIRECT_{backend.upper()}_URI for this storage test")
    uri = uri.rstrip("/")
    if backend in ("hdfs", "local"):
        fs, base = pafs.FileSystem.from_uri(uri)
    else:
        parsed = urlparse(uri)
        fs = pafs.S3FileSystem(
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
    fs.create_dir(path, recursive=True)
    location = uri + "/" + suffix
    if backend == "s3":
        # Hive uses Hadoop's S3A URI; the native Arrow client uses S3.
        location = "s3a:" + location.split(":", 1)[1]
    return fs, path, location


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
                    assert statement.startswith("DESCRIBE FORMATTED ") or re.fullmatch(
                        r"SET [a-z.]+", statement
                    )
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


def _metadata_lookup_count(statements):
    return sum(statement.startswith("DESCRIBE FORMATTED ") for statement in statements)


def _orc_session_factory(factory, positional, settings=None):
    class Connection:
        def __init__(self):
            self.connection = factory()

        def cursor(self, **kwargs):
            cursor = self.connection.cursor(**kwargs)
            # Each Impyla cursor opens a session; SET must apply to this one.
            if positional is not None:
                cursor.execute(
                    "SET orc.force.positional.evolution=" + str(positional).lower()
                )
            for option, value in (settings or {}).items():
                cursor.execute(f"SET {option}={value}")
            return cursor

        def close(self):
            self.connection.close()

    return Connection


@contextmanager
def _hive_table(columns, file_format, positional=None, settings=None, *, backend=None):
    # A shared local directory also permits real HS2 semantic tests without
    # requiring the independent HDFS/S3 infrastructure matrix.
    if backend is None:
        backend = "local" if os.environ.get("RAY_HIVE_DIRECT_LOCAL_URI") else "hdfs"
    fs, path, location = _storage(backend)
    factory = _orc_session_factory(_native_connection_factory(), positional, settings)
    connection = factory()
    cursor = connection.cursor()
    table = "ray_evolution_" + uuid.uuid4().hex[:12]
    created = False
    try:
        cursor.execute(
            f"CREATE EXTERNAL TABLE {table} ({columns}) "
            f"STORED AS {file_format.upper()} LOCATION '{location}'"
        )
        created = True
        yield fs, path, table, cursor, factory
    finally:
        try:
            if created:
                cursor.execute(f"DROP TABLE {table}")
        finally:
            cursor.close()
            connection.close()
            fs.delete_dir(path)


def _read_direct(table, factory, fs, **options):
    return ray.data.read_hive(
        table, connection_factory=factory, filesystem=fs, read_mode="direct", **options
    )


def _write(fs, path, file_format, data, **options):
    with fs.open_output_stream(path) as stream:
        (pq.write_table if file_format == "parquet" else orc.write_table)(
            data, stream, **options
        )


def _sample_paths(monkeypatch):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    original = hive_direct.sample_files
    paths = []

    def sample(*args, **kwargs):
        manifest = original(*args, **kwargs)
        paths.extend(manifest.paths.tolist())
        return manifest

    monkeypatch.setattr(hive_direct, "sample_files", sample)
    return paths


@pytest.mark.parametrize("namespace,positional", [("table", True), ("serde", False)])
def test_orc_rename_and_session_precedence(direct_runtime, namespace, positional):
    with _hive_table("id INT, old_value BIGINT", "orc", positional) as (
        fs,
        path,
        table,
        cursor,
        factory,
    ):
        cursor.execute(f"INSERT INTO {table} VALUES (0,42),(1,-7),(2,NULL)")
        cursor.execute(f"ALTER TABLE {table} CHANGE COLUMN old_value new_value BIGINT")
        clause = "TBLPROPERTIES" if namespace == "table" else "SERDEPROPERTIES"
        cursor.execute(
            f"ALTER TABLE {table} SET {clause} ('orc.force.positional.evolution'='{str(not positional).lower()}')"
        )
        hs2 = ray.data.read_hive(table, connection_factory=factory).materialize()
        reference = sorted(hs2.take_all(), key=lambda row: row["id"])
        assert [row["new_value"] for row in reference] == (
            [42, -7, None] if positional else [None] * 3
        )
        direct = _read_direct(table, factory, fs)
        assert sorted(direct.take_all(), key=lambda row: row["id"]) == reference
        assert (
            direct.schema().base_schema
            == hs2.schema().base_schema
            == pa.schema([("id", pa.int32()), ("new_value", pa.int64())])
        )


@pytest.mark.parametrize(
    "tolerate,difference", [(True, "name"), (False, "name"), (False, "type")]
)
def test_legacy_orc_policy_matches_real_hs2(direct_runtime, tolerate, difference):
    logical = "_col0" if difference == "type" else "id"
    with _hive_table(
        f"`{logical}` BIGINT",
        "orc",
        False,
        {"orc.tolerate.missing.schema": str(tolerate).lower()},
    ) as (fs, path, table, cursor, factory):
        data = pa.table(
            {
                "_col0": pa.array(
                    [42], type=pa.int32() if difference == "type" else pa.int64()
                )
            }
        )
        _write(fs, path + "/part", "orc", data)
        if tolerate:
            direct = _read_direct(table, factory, fs)
            assert (
                direct.take_all() == _reference_rows(cursor, table) == [{logical: 42}]
            )
            assert direct.schema().base_schema == pa.schema([(logical, pa.int64())])
        else:
            with pytest.raises(Exception, match="[Mm]issing|schema|metadata"):
                cursor.execute(f"SELECT * FROM {table}")
                cursor.fetchall()
            with pytest.raises(ValueError, match="tolerance is disabled"):
                _read_direct(table, factory, fs)


def test_parquet_positional_setting_is_rejected_before_files(
    direct_runtime, monkeypatch
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    with _hive_table(
        "id BIGINT", "parquet", settings={"parquet.column.index.access": "true"}
    ) as (fs, path, table, cursor, factory):
        _write(fs, path + "/part", "parquet", pa.table({"old_id": [42]}))
        assert _reference_rows(cursor, table) == [{"id": 42}]
        monkeypatch.setattr(
            hive_direct,
            "HiveDirectDatasource",
            lambda *a, **kw: pytest.fail("Unexpected file planning"),
        )
        with pytest.raises(ValueError, match="Parquet positional"):
            _read_direct(table, factory, fs)


def test_legacy_orc_timestamp_schema_matches_real_hs2(direct_runtime, monkeypatch):
    from ray.data.expressions import col

    def schema(unit):
        return pa.schema(
            [
                ("_col0", pa.timestamp(unit)),
                ("_col1", pa.struct([("ts", pa.timestamp(unit))])),
            ]
        )

    physical, declared = schema("ns"), schema("us")
    with _hive_table(
        "`_col0` TIMESTAMP, `_col1` STRUCT<ts:TIMESTAMP>",
        "orc",
        False,
        {"orc.tolerate.missing.schema": "false"},
    ) as (fs, path, table, cursor, factory):
        expected = []
        for index in range(17):
            tick = 86400123456789 + index * 1000
            data = pa.Table.from_pylist(
                [
                    {"_col0": tick, "_col1": {"ts": tick}},
                    {"_col0": None, "_col1": None},
                ],
                schema=physical,
            )
            expected.extend(data.cast(declared, safe=False).to_pylist())
            if index < 16:
                data = data.append_column(
                    "marker", pa.array([index, None], type=pa.int32())
                )
            _write(fs, f"{path}/{index:06d}_0", "orc", data)
        sampled = _sample_paths(monkeypatch)
        dataset = _read_direct(table, factory, fs)
        assert len(sampled) == 16 and path + "/000016_0" not in sampled

        def key(row):
            return row["_col0"] is None, row["_col0"] or datetime.min

        result = sorted(dataset.take_all(), key=key)
        assert result == sorted(expected, key=key)
        assert dataset.schema().base_schema == declared
        cursor.execute(f"SELECT `_col0`, `_col1`.ts FROM {table}")
        reference = sorted(
            cursor.fetchall(), key=lambda row: (row[0] is None, row[0] or datetime.min)
        )
        assert [
            (row["_col0"], (row["_col1"] or {}).get("ts")) for row in result
        ] == reference
        assert sorted(
            str(row["_col1"])
            for row in dataset.filter(expr=col("_col0") > datetime(1970, 1, 2))
            .select_columns(["_col1"])
            .take_all()
        ) == sorted(str(row["_col1"]) for row in result if row["_col0"] is not None)


@pytest.mark.parametrize(
    "file_format,merge_schema", [("parquet", False), ("orc", False), ("orc", True)]
)
def test_hive_schema_evolution_matches_real_hs2(
    direct_runtime, file_format, merge_schema, monkeypatch
):
    from ray.data.expressions import col

    prefix = "old_" if file_format == "orc" else ""
    columns = (
        f"id INT, {prefix}n INT, {prefix}payload STRUCT<n:INT,f:FLOAT,d:DECIMAL(9,2),ts:TIMESTAMP>, "
        f"{prefix}items ARRAY<INT>, {prefix}labels MAP<STRING,INT>"
    )

    def payload(integer, floating, precision, unit):
        return pa.struct(
            [
                ("n", integer),
                ("f", floating),
                ("d", pa.decimal128(precision, 2)),
                ("ts", pa.timestamp(unit)),
            ]
        )

    wide_payload = payload(pa.int64(), pa.float64(), 18, "ns")
    wide_schema = pa.schema(
        [
            ("id", pa.int32()),
            ("n", pa.int64()),
            ("payload", wide_payload),
            ("items", pa.list_(pa.int64())),
            ("labels", pa.map_(pa.string(), pa.int64())),
        ]
    )
    declared = wide_schema.set(
        2, pa.field("payload", payload(pa.int64(), pa.float64(), 18, "us"))
    )
    with _hive_table(columns, file_format, True if file_format == "orc" else None) as (
        fs,
        path,
        table,
        cursor,
        factory,
    ):
        # Keep a genuine Hive-written old-schema file, then make the wider file
        # the only unsampled file. NULL parents retain their physical types.
        cursor.execute(
            f"INSERT INTO {table} SELECT row_id, if(row_id=1,42,NULL), "
            "if(row_id=1,named_struct('n',42,'f',CAST(0.1 AS FLOAT),'d',CAST(amount AS DECIMAL(9,2)),"
            "'ts',CAST('1969-12-31 23:59:58.123456789' AS TIMESTAMP)),NULL),"
            "if(row_id=1,array(7,CAST(NULL AS INT)),NULL),if(row_id=1,map('k',8),NULL)"
            " FROM (SELECT STACK(2,1,'-123.45',2,NULL) AS (row_id,amount)) source"
        )
        files = [
            info.path
            for info in fs.get_file_info(pafs.FileSelector(path, recursive=True))
            if info.type == pafs.FileType.File
            and not info.path.rsplit("/", 1)[-1].startswith((".", "_"))
        ]
        assert len(files) == 1
        fs.move(files[0], path + "/part-00")
        for index in range(1, 16):
            fs.copy_file(path + "/part-00", f"{path}/part-{index:02d}")
        changes = {
            "n": "BIGINT",
            "payload": "STRUCT<n:BIGINT,f:DOUBLE,d:DECIMAL(18,2),ts:TIMESTAMP>",
            "items": "ARRAY<BIGINT>",
            "labels": "MAP<STRING,BIGINT>",
        }
        for name, type_name in changes.items():
            cursor.execute(
                f"ALTER TABLE {table} CHANGE COLUMN {prefix}{name} {name} {type_name}"
            )
        wide = {
            "id": 17,
            "n": 2**40,
            "payload": {
                "n": 2**40,
                "f": 1e100,
                "d": Decimal("12345678901.23"),
                "ts": 1001,
            },
            "items": [2**40, None],
            "labels": [("k", 2**40)],
        }
        _write(
            fs,
            f"{path}/zz_late",
            file_format,
            pa.Table.from_pylist([wide], schema=wide_schema),
        )
        sampled = _sample_paths(monkeypatch)
        dataset = _read_direct(table, factory, fs, merge_schema=merge_schema)
        assert len(sampled) == (17 if merge_schema else 16)
        assert (f"{path}/zz_late" in sampled) == merge_schema
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        assert dataset.schema().base_schema == declared
        cursor.execute(
            f"SELECT id,n,payload.n,payload.f,payload.d,payload.ts,items[0],items[1],labels['k'] FROM {table}"
        )
        flattened = []
        for row in result:
            value = row["payload"] or dict.fromkeys(("n", "f", "d", "ts"))
            items = row["items"] or [None, None]
            flattened.append(
                (
                    row["id"],
                    row["n"],
                    value["n"],
                    value["f"],
                    value["d"],
                    value["ts"],
                    *items,
                    dict(row["labels"] or []).get("k"),
                )
            )
        assert flattened == sorted(cursor.fetchall())
        assert len(result) == 33
        assert [row for row in result if row["id"] == 2] == [
            dict.fromkeys(declared.names, None) | {"id": 2}
        ] * 16
        assert result[0]["payload"]["ts"] == datetime(1969, 12, 31, 23, 59, 58, 123456)
        assert result[-1]["payload"]["ts"] == datetime(1970, 1, 1, 0, 0, 0, 1)
        assert dataset.filter(expr=col("id") > 16).select_columns(
            ["items"]
        ).take_all() == [{"items": [2**40, None]}]
        assert dataset.select_columns([]).count() == 33
        assert dataset.filter(expr=col("n") > 2**32).select_columns(
            ["id"]
        ).take_all() == [{"id": 17}]


@pytest.mark.parametrize("backend", ["hdfs", "s3"])
@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_nonpartitioned_native_read(direct_runtime, backend, file_format):
    columns = "id BIGINT, items ARRAY<INT>, labels MAP<STRING,STRING>, nested STRUCT<name:STRING,value:INT>"
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("items", pa.list_(pa.int32())),
            ("labels", pa.map_(pa.string(), pa.string())),
            ("nested", pa.struct([("name", pa.string()), ("value", pa.int32())])),
        ]
    )
    rows = [
        {
            "id": i,
            "items": None if i == 0 else [] if i == 1 else [i, None],
            "labels": [] if i == 0 else [("key", str(i))],
            "nested": None if i == 0 else {"name": "nested", "value": i},
        }
        for i in range(32)
    ]
    expected = pa.Table.from_pylist(rows, schema=schema)
    physical = expected
    observer = None
    with _hive_table(columns, file_format, backend=backend) as (
        fs,
        path,
        table_name,
        cursor,
        connection_factory,
    ):
        try:
            for index in range(4):
                options = (
                    {"use_deprecated_int96_timestamps": True}
                    if file_format == "parquet"
                    else {}
                )
                _write(
                    fs,
                    f"{path}/{index:06d}_0",
                    file_format,
                    physical.slice(index * 8, 8),
                    **options,
                )

            driver_pid = os.getpid()

            statements = []
            metadata_only_factory = _metadata_only_factory(
                connection_factory, statements
            )

            class ReadObserver:
                def __init__(self):
                    self.pids: set[int] = set()

                def record(self, pid: int) -> int:
                    self.pids.add(pid)
                    return len(self.pids)

            observer_handle = cast(
                ActorHandle[Any],
                ray.remote(ReadObserver).options(num_cpus=0).remote(),
            )
            observer = observer_handle

            def _tag_read_worker(batch):
                # Defined locally so worker deserialization doesn't depend on
                # pytest's bare test-module import name. It must fuse with ReadFiles.
                deadline = time.monotonic() + 20
                while (
                    cast(int, ray.get(observer_handle.record.remote(os.getpid()))) < 2
                ):
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
                filesystem=fs,
                override_num_blocks=4,
                merge_schema=backend == "s3",
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

            for limit in (0, 3):
                limited = ray.data.read_hive(
                    table_name,
                    connection_factory=metadata_only_factory,
                    read_mode="direct",
                    filesystem=fs,
                    limit=limit,
                )
                assert len(limited.take_all()) == limit
                assert limited.schema().base_schema == schema

            normalized = sorted(result, key=lambda row: row["id"])
            for row in normalized:
                row["labels"] = (
                    dict(row["labels"]) if row["labels"] is not None else None
                )
            assert normalized == _reference_rows(
                cursor, table_name, ("items", "labels", "nested")
            )
            assert _metadata_lookup_count(statements) == 3

        finally:
            if observer is not None:
                ray.kill(observer)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_written_epoch_timestamps(direct_runtime, file_format):
    from ray.data.expressions import col

    literals = [
        "1969-12-31 23:59:59.999998999",
        "1969-12-31 23:59:59.999999999",
        "1970-01-01 00:00:00.000000000",
        "1970-01-01 00:00:00.000000001",
        "1970-01-01 00:00:00.000001001",
        None,
    ]
    micros = [-2, -1, 0, 0, 1, None]
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
    with _hive_table(
        "id INT, ts TIMESTAMP, nested STRUCT<ts:TIMESTAMP>, "
        "items ARRAY<TIMESTAMP>, labels MAP<STRING,TIMESTAMP>, code CHAR(4), tag VARCHAR(3)",
        file_format,
        backend="hdfs",
    ) as (fs, path, table, cursor, factory):
        selects = []
        for index, literal in enumerate(literals):
            timestamp = (
                "CAST(NULL AS TIMESTAMP)"
                if literal is None
                else f"CAST('{literal}' AS TIMESTAMP)"
            )
            selects.append(
                f"SELECT {index}, {timestamp}, named_struct('ts', {timestamp}), "
                f"array({timestamp}, CAST(NULL AS TIMESTAMP)), map('key', {timestamp}), "
                "CAST('ab' AS CHAR(4)), CAST('abcdef' AS VARCHAR(3))"
            )
        cursor.execute(f"INSERT INTO {table} " + " UNION ALL ".join(selects))
        dataset = _read_direct(table, factory, fs)
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        timestamps = pa.array(micros, type=pa.timestamp("us")).to_pylist()
        assert result == [
            {
                "id": index,
                "ts": value,
                "nested": {"ts": value},
                "items": [value, None],
                "labels": [("key", value)],
                "code": "ab  ",
                "tag": "abc",
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
                ("code", pa.string()),
                ("tag", pa.string()),
            ]
        )
        cursor.execute(
            f"SELECT id, ts, nested.ts, items[0], items[1], labels['key'],code,tag FROM {table}"
        )
        assert sorted(cursor.fetchall()) == [
            (index, value, value, value, None, value, "ab  ", "abc")
            for index, value in enumerate(timestamps)
        ]
        assert sorted(
            row["id"]
            for row in dataset.filter(expr=col("ts") < datetime(1970, 1, 1))
            .select_columns(["id"])
            .take_all()
        ) == [0, 1]
        assert sorted(
            row["id"]
            for row in dataset.filter(
                expr=(col("ts") == datetime(1970, 1, 1)) & (col("code") == "ab  ")
            )
            .select_columns(["id"])
            .take_all()
        ) == [2, 3]


@pytest.mark.parametrize(
    "file_format,case_sensitive", [("parquet", False), ("orc", False), ("orc", True)]
)
def test_name_mapping_matches_real_hs2_beyond_sampling(
    direct_runtime, monkeypatch, file_format, case_sensitive
):
    from ray.data.expressions import col

    settings = (
        {"parquet.column.index.access": "false"}
        if file_format == "parquet"
        else {"orc.schema.evolution.case.sensitive": str(case_sensitive).lower()}
    )
    names = ["id", "key", "ss"] + ([" value", "tail "] if file_format == "orc" else [])
    with _hive_table(
        ", ".join(f"`{name}` BIGINT" for name in names),
        file_format,
        False if file_format == "orc" else None,
        settings,
    ) as (fs, path, table, cursor, factory):
        for index in range(17):
            key = "KEY" if file_format == "orc" and index == 16 else "key"
            values = {
                "id": [index],
                key: [index - 1 if file_format == "parquet" else index + 10],
                "ß": [999],
            }
            if file_format == "orc":
                values.update({" value": [index + 20], "tail ": [index + 30]})
            if index < 16:
                values["SS" if file_format == "parquet" else "ss"] = [index + 100]
            _write(fs, f"{path}/{index:06d}_0", file_format, pa.table(values))
        reference = _reference_rows(cursor, table)
        assert reference[-1]["ss"] is None
        assert reference[-1]["key"] == (
            None if case_sensitive else (15 if file_format == "parquet" else 26)
        )
        sampled = _sample_paths(monkeypatch)
        dataset = _read_direct(table, factory, fs)
        assert len(sampled) == 16 and path + "/000016_0" not in sampled
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == reference
        assert dataset.schema().base_schema == pa.schema(
            [(name, pa.int64()) for name in names]
        )
        if file_format == "parquet":
            selected = dataset.filter(expr=col("key") > 0).select_columns(["id"])
            expected = [row["id"] for row in reference if row["key"] > 0]
            assert sorted(row["id"] for row in selected.take_all()) == expected
        else:
            selected = dataset.filter(
                expr=(col(" value") > 20) & (col("key") > 0)
            ).select_columns(["tail "])
            assert sorted(row["tail "] for row in selected.take_all()) == [
                row["tail "]
                for row in reference
                if row[" value"] > 20 and row["key"] is not None
            ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
