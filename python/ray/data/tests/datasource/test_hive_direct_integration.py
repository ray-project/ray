"""Opt-in real Hive, native storage, and Ray worker tests for direct reads."""

import json
import os
import re
import time
import uuid
from contextlib import contextmanager
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
                    assert statement.startswith("DESCRIBE FORMATTED ") or statement in (
                        "SET parquet.column.index.access",
                        "SET orc.force.positional.evolution",
                        "SET orc.force.positional.evolution.level",
                        "SET hive.exec.schema.evolution",
                        "SET orc.tolerate.missing.schema",
                        "SET hive.exec.orc.tolerate.missing.schema",
                        "SET orc.schema.evolution.case.sensitive",
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
def _schema_evolution_table(
    columns, file_format, positional=None, settings=None, *, backend=None
):
    # A shared local directory also permits real HS2 semantic tests without
    # requiring the independent HDFS/S3 infrastructure matrix.
    if backend is None:
        backend = "local" if os.environ.get("RAY_HIVE_DIRECT_LOCAL_URI") else "hdfs"
    filesystem, path, location = _storage(backend)
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
        yield filesystem, path, table, cursor, factory
    finally:
        try:
            if created:
                cursor.execute(f"DROP TABLE {table}")
        finally:
            cursor.close()
            connection.close()
            filesystem.delete_dir(path)


@pytest.mark.parametrize("positional", [None, True, False])
def test_orc_rename_matches_real_hs2_mapping_boundary(direct_runtime, positional):
    with _schema_evolution_table("id INT, old_value BIGINT", "orc", positional) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        cursor.execute(f"INSERT INTO {table} VALUES (0,42), (1,-7), (2,NULL)")
        cursor.execute("SET orc.force.positional.evolution")
        actual_mode = cursor.fetchall()[0][0].split("=", 1)[1].strip().lower()
        baseline = ray.data.read_hive(
            table, connection_factory=factory, read_mode="direct", filesystem=filesystem
        ).take_all()
        assert sorted(baseline, key=lambda row: row["id"]) == _reference_rows(
            cursor, table
        )
        cursor.execute(f"ALTER TABLE {table} CHANGE COLUMN old_value new_value BIGINT")
        reference = _reference_rows(cursor, table)
        assert [row["new_value"] for row in reference] == (
            [42, -7, None] if actual_mode == "true" else [None] * 3
        )
        # Check the public HS2 path as well as the independent SQL reference.
        assert (
            sorted(
                ray.data.read_hive(table, connection_factory=factory).take_all(),
                key=lambda row: row["id"],
            )
            == reference
        )
        statements = []
        kwargs = dict(
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        dataset = ray.data.read_hive(table, **kwargs)
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == reference
        assert dataset.schema().base_schema == pa.schema(
            [("id", pa.int32()), ("new_value", pa.int64())]
        )
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("positional", [False, True])
@pytest.mark.parametrize("merge_schema", [False, True])
def test_orc_column_name_whitespace_matches_real_hs2(
    direct_runtime, positional, merge_schema
):
    from ray.data.expressions import col

    names = ["id", "value", " value", "trailing ", " both "]
    expected = [
        dict(zip(names, (1, 10, 11, 12, 13))),
        dict(zip(names, (2, 20, None, 22, None))),
    ]
    schema = pa.schema([(name, pa.int64()) for name in names])
    with _schema_evolution_table(
        ", ".join(f"`{name}` BIGINT" for name in names), "orc", positional
    ) as (filesystem, path, table, cursor, factory):
        cursor.execute(
            f"INSERT INTO {table} VALUES (1,10,11,12,13),(2,20,NULL,22,NULL)"
        )
        reference = _reference_rows(cursor, table)
        assert reference == expected
        hs2 = ray.data.read_hive(table, connection_factory=factory).materialize()
        assert sorted(hs2.take_all(), key=lambda row: row["id"]) == reference
        assert hs2.schema().base_schema == schema

        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
            merge_schema=merge_schema,
        )
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == reference
        assert dataset.schema().base_schema == hs2.schema().base_schema
        projected = dataset.select_columns(["id", " both "])
        assert sorted(projected.take_all(), key=lambda row: row["id"]) == [
            {"id": row["id"], " both ": row[" both "]} for row in reference
        ]
        assert projected.schema().base_schema == pa.schema(
            [("id", pa.int64()), (" both ", pa.int64())]
        )
        filtered = dataset.filter(expr=col(" value") > 10).select_columns(
            ["id", "trailing "]
        )
        assert filtered.take_all() == [{"id": 1, "trailing ": 12}]
        assert filtered.schema().base_schema == pa.schema(
            [("id", pa.int64()), ("trailing ", pa.int64())]
        )
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("required", ["all", "projection", "filter_only"])
def test_unsampled_orc_rename_matches_hs2_with_projection_and_filter(
    direct_runtime, monkeypatch, required
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    with _schema_evolution_table("id BIGINT, value BIGINT", "orc", True) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        for index in range(17):
            data = pa.table(
                {"id" if index < 16 else "old_id": [index], "value": [index + 100]}
            )
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                orc.write_table(data, stream)
        original_sample = hive_direct.sample_files
        sampled = []

        def record_sample(*args, **kwargs):
            manifest = original_sample(*args, **kwargs)
            sampled.extend(manifest.paths.tolist())
            return manifest

        monkeypatch.setattr(hive_direct, "sample_files", record_sample)
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        assert len(sampled) == 16 and f"{path}/000016_0" not in sampled
        reference = _reference_rows(cursor, table)
        if required == "projection":
            dataset = dataset.select_columns(["id"])
            reference = [{"id": row["id"]} for row in reference]
        elif required == "filter_only":
            dataset = dataset.filter(expr=col("id") > 15).select_columns(["value"])
            reference = [{"value": row["value"]} for row in reference if row["id"] > 15]
        result = dataset.take_all()
        assert sorted(result, key=lambda row: tuple(row.values())) == reference
        assert dataset.schema().base_schema.names == list(reference[0])
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_safe_widening_with_nested_and_unsampled_files(
    direct_runtime, file_format, monkeypatch
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    old_value = pa.struct(
        [("n", pa.int32()), ("f", pa.float32()), ("d", pa.decimal128(9, 2))]
    )
    new_value = pa.struct(
        [("n", pa.int64()), ("f", pa.float64()), ("d", pa.decimal128(18, 2))]
    )
    physical = pa.schema(
        [
            ("id", pa.int64()),
            ("tiny", pa.int8()),
            ("small", pa.int16()),
            ("n", pa.int32()),
            ("f", pa.float32()),
            ("d", pa.decimal128(9, 2)),
            ("items", pa.list_(pa.int32())),
            ("labels", pa.map_(pa.string(), pa.int32())),
            ("nested", old_value),
        ]
    )
    declared = pa.schema(
        [
            ("id", pa.int64()),
            ("tiny", pa.int16()),
            ("small", pa.int32()),
            ("n", pa.int64()),
            ("f", pa.float64()),
            ("d", pa.decimal128(18, 2)),
            ("items", pa.list_(pa.int64())),
            ("labels", pa.map_(pa.string(), pa.int64())),
            ("nested", new_value),
        ]
    )
    columns = (
        "id BIGINT, tiny TINYINT, small SMALLINT, n INT, f FLOAT, d DECIMAL(9,2), "
        "items ARRAY<INT>, labels MAP<STRING,INT>, nested STRUCT<n:INT,f:FLOAT,d:DECIMAL(9,2)>"
    )
    with _schema_evolution_table(columns, file_format) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        expected = []
        for index in range(16):
            value = (
                None
                if index == 0
                else {"n": -2147483648, "f": 0.1, "d": Decimal("-123.45")}
            )
            data = pa.Table.from_pylist(
                [
                    {
                        "id": index,
                        "tiny": -128,
                        "small": -32768,
                        "n": 2147483647 if index else None,
                        "f": 0.1 if index else None,
                        "d": Decimal("9999999.99") if index else None,
                        "items": [-2147483648, None] if index else [],
                        "labels": [("k", 2147483647)] if index else [],
                        "nested": value,
                    }
                ],
                schema=physical,
            )
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                (pq.write_table if file_format == "parquet" else orc.write_table)(
                    data, stream
                )
            expected.extend(data.cast(declared).to_pylist())
        changes = {
            "tiny": "SMALLINT",
            "small": "INT",
            "n": "BIGINT",
            "f": "DOUBLE",
            "d": "DECIMAL(18,2)",
            "items": "ARRAY<BIGINT>",
            "labels": "MAP<STRING,BIGINT>",
            "nested": "STRUCT<n:BIGINT,f:DOUBLE,d:DECIMAL(18,2)>",
        }
        for name, type_name in changes.items():
            cursor.execute(
                f"ALTER TABLE {table} CHANGE COLUMN {name} {name} {type_name}"
            )
        wide = pa.Table.from_pylist(
            [
                {
                    "id": 99,
                    "tiny": 128,
                    "small": 32768,
                    "n": 2**40,
                    "f": 1e100,
                    "d": Decimal("12345678901.23"),
                    "items": [2**40, None],
                    "labels": [("k", 2**40)],
                    "nested": {
                        "n": 2**40,
                        "f": 1e100,
                        "d": Decimal("12345678901.23"),
                    },
                }
            ],
            schema=declared,
        )
        with filesystem.open_output_stream(f"{path}/000099_0") as stream:
            (pq.write_table if file_format == "parquet" else orc.write_table)(
                wide, stream
            )
        expected.extend(wide.to_pylist())
        original_sample = hive_direct.sample_files
        sampled = []

        def record_sample(*args, **kwargs):
            manifest = original_sample(*args, **kwargs)
            sampled.extend(manifest.paths.tolist())
            return manifest

        monkeypatch.setattr(hive_direct, "sample_files", record_sample)
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        assert len(sampled) == 16 and f"{path}/000099_0" not in sampled
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        assert result == expected
        assert dataset.schema().base_schema == declared
        # Flatten in Hive SQL to preserve typed DECIMAL/FLOAT values instead
        # of relying on JSON's representation of complex result columns.
        cursor.execute(
            f"SELECT id,tiny,small,n,f,d,items[0],items[1],labels['k'],"
            f"nested.n,nested.f,nested.d FROM {table}"
        )
        reference = sorted(cursor.fetchall())
        flattened = []
        for row in result:
            nested = row["nested"] or {"n": None, "f": None, "d": None}
            flattened.append(
                tuple(row[k] for k in ("id", "tiny", "small", "n", "f", "d"))
                + (
                    row["items"][0] if row["items"] else None,
                    None,
                    dict(row["labels"]).get("k"),
                    nested["n"],
                    nested["f"],
                    nested["d"],
                )
            )
        assert flattened == reference
        assert [
            row["id"]
            for row in dataset.filter(expr=col("n") > 2**32)
            .select_columns(["id"])
            .take_all()
        ] == [99]
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("namespace", ["table", "serde"])
@pytest.mark.parametrize("positional", [False, True])
def test_orc_mapping_uses_real_session_over_table_properties(
    direct_runtime, namespace, positional
):
    with _schema_evolution_table("id BIGINT", "orc", positional) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        with filesystem.open_output_stream(f"{path}/000000_0") as stream:
            orc.write_table(pa.table({"old_id": [42]}), stream)
        properties = "TBLPROPERTIES" if namespace == "table" else "SERDEPROPERTIES"
        cursor.execute(
            f"ALTER TABLE {table} SET {properties} "
            f"('orc.force.positional.evolution'='{str(not positional).lower()}')"
        )
        reference = _reference_rows(cursor, table)
        assert reference == [{"id": 42 if positional else None}]
        dataset = ray.data.read_hive(
            table, connection_factory=factory, read_mode="direct", filesystem=filesystem
        )
        assert dataset.take_all() == reference
        assert dataset.schema().base_schema == pa.schema([("id", pa.int64())])


@pytest.mark.parametrize(
    "positional,tolerate", [(False, True), (False, False), (True, False)]
)
def test_legacy_orc_mapping_matches_real_hs2(direct_runtime, positional, tolerate):
    settings = {"orc.tolerate.missing.schema": str(tolerate).lower()}
    with _schema_evolution_table("id BIGINT", "orc", positional, settings) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        with filesystem.open_output_stream(f"{path}/000000_0") as stream:
            orc.write_table(pa.table({"_col0": [42]}), stream)
        if not positional and not tolerate:
            with pytest.raises(Exception, match="[Mm]issing|schema|metadata"):
                _reference_rows(cursor, table)
            with pytest.raises(ValueError, match="tolerance is disabled"):
                ray.data.read_hive(
                    table,
                    connection_factory=factory,
                    read_mode="direct",
                    filesystem=filesystem,
                )
        else:
            reference = _reference_rows(cursor, table)
            assert reference == [{"id": 42}]
            dataset = ray.data.read_hive(
                table,
                connection_factory=factory,
                read_mode="direct",
                filesystem=filesystem,
            )
            assert dataset.take_all() == reference
            assert dataset.schema().base_schema == pa.schema([("id", pa.int64())])


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_orc_name_mode_matches_hs2_case_setting_for_unsampled_file(
    direct_runtime, case_sensitive, monkeypatch
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    settings = {"orc.schema.evolution.case.sensitive": str(case_sensitive).lower()}
    with _schema_evolution_table("id BIGINT", "orc", False, settings) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        for index in range(17):
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                orc.write_table(
                    pa.table({"id" if index < 16 else "ID": [index]}), stream
                )
        original_sample = hive_direct.sample_files
        sampled = []

        def record_sample(*args, **kwargs):
            manifest = original_sample(*args, **kwargs)
            sampled.extend(manifest.paths.tolist())
            return manifest

        monkeypatch.setattr(hive_direct, "sample_files", record_sample)
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        assert len(sampled) == 16 and f"{path}/000016_0" not in sampled
        cursor.execute(f"SELECT id FROM {table}")
        reference = sorted(
            cursor.fetchall(), key=lambda row: (row[0] is not None, row[0])
        )
        result = sorted(
            (row["id"] for row in dataset.take_all()),
            key=lambda value: (value is not None, value),
        )
        assert result == [row[0] for row in reference]
        assert result == (
            [None] + list(range(16)) if case_sensitive else list(range(17))
        )
        assert dataset.schema().base_schema == pa.schema([("id", pa.int64())])
        assert sorted(
            row["id"] for row in dataset.filter(expr=col("id") >= 15).take_all()
        ) == ([15] if case_sensitive else [15, 16])
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("index_access", [False, True])
def test_parquet_session_positional_mapping_is_rejected_before_files(
    direct_runtime, monkeypatch, index_access
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    settings = {"parquet.column.index.access": str(index_access).lower()}
    with _schema_evolution_table("id BIGINT", "parquet", settings=settings) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        with filesystem.open_output_stream(f"{path}/000000_0") as stream:
            pq.write_table(
                pa.table({"old_id": pa.array([42, None], type=pa.int64())}), stream
            )
        cursor.execute(f"SELECT id FROM {table}")
        reference = [row[0] for row in cursor.fetchall()]
        assert reference == ([42, None] if index_access else [None, None])
        hs2 = ray.data.read_hive(table, connection_factory=factory).materialize()
        assert [row["id"] for row in hs2.take_all()] == reference
        assert hs2.schema().base_schema == pa.schema([("id", pa.int64())])
        if index_access:
            monkeypatch.setattr(
                hive_direct,
                "HiveDirectDatasource",
                lambda *a, **kw: pytest.fail("Unexpected file planning"),
            )
            with pytest.raises(ValueError, match="Parquet positional"):
                ray.data.read_hive(
                    table,
                    connection_factory=factory,
                    read_mode="direct",
                    filesystem=filesystem,
                )
        else:
            statements = []
            direct = ray.data.read_hive(
                table,
                connection_factory=_metadata_only_factory(factory, statements),
                read_mode="direct",
                filesystem=filesystem,
            ).materialize()
            assert [row["id"] for row in direct.take_all()] == reference
            assert direct.schema().base_schema == hs2.schema().base_schema
            assert statements == [
                f"DESCRIBE FORMATTED `default`.`{table}`",
                "SET parquet.column.index.access",
            ]


@pytest.mark.parametrize("logical", ["ss", "\u00df"])
def test_parquet_sampled_unicode_name_matches_real_hs2(direct_runtime, logical):
    settings = {"parquet.column.index.access": "false"}
    with _schema_evolution_table(
        f"id BIGINT, `{logical}` BIGINT", "parquet", settings=settings
    ) as (filesystem, path, table, cursor, factory):
        with filesystem.open_output_stream(f"{path}/000000_0") as stream:
            pq.write_table(
                pa.table(
                    {"id": [0, 1], "\u00df": pa.array([42, None], type=pa.int64())}
                ),
                stream,
            )
        reference = _reference_rows(cursor, table)
        assert reference == [
            {"id": 0, logical: 42 if logical == "\u00df" else None},
            {"id": 1, logical: None},
        ]
        hs2 = ray.data.read_hive(table, connection_factory=factory).materialize()
        statements = []
        direct = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        ).materialize()
        assert sorted(hs2.take_all(), key=lambda row: row["id"]) == reference
        assert sorted(direct.take_all(), key=lambda row: row["id"]) == reference
        assert (
            direct.schema().base_schema
            == hs2.schema().base_schema
            == pa.schema([("id", pa.int64()), (logical, pa.int64())])
        )
        assert statements == [
            f"DESCRIBE FORMATTED `default`.`{table}`",
            "SET parquet.column.index.access",
        ]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("required", ["all", "projection", "filter_only"])
def test_unicode_name_mapping_matches_hs2_beyond_sampling(
    direct_runtime, monkeypatch, required, file_format
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    settings = (
        {"parquet.column.index.access": "false"}
        if file_format == "parquet"
        else {"orc.schema.evolution.case.sensitive": "false"}
    )
    with _schema_evolution_table(
        "id BIGINT, ss BIGINT",
        file_format,
        False if file_format == "orc" else None,
        settings,
    ) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        for index in range(17):
            values = {"id": [index], "\u00df": [42 if index == 16 else index + 200]}
            if index < 16:
                values["SS" if file_format == "parquet" else "ss"] = [index + 100]
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                (pq.write_table if file_format == "parquet" else orc.write_table)(
                    pa.table(values), stream
                )
        original_sample = hive_direct.sample_files
        sampled = []

        def record_sample(*args, **kwargs):
            manifest = original_sample(*args, **kwargs)
            sampled.extend(manifest.paths.tolist())
            return manifest

        monkeypatch.setattr(hive_direct, "sample_files", record_sample)
        reference = _reference_rows(cursor, table)
        assert reference[-1] == {"id": 16, "ss": None}
        hs2 = ray.data.read_hive(table, connection_factory=factory).materialize()
        assert sorted(hs2.take_all(), key=lambda row: row["id"]) == reference
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        assert len(sampled) == 16 and f"{path}/000016_0" not in sampled
        if required == "projection":
            dataset = dataset.select_columns(["ss"])
            expected = [{"ss": row["ss"]} for row in reference]
        elif required == "filter_only":
            dataset = dataset.filter(expr=col("ss") > 0).select_columns(["id"])
            expected = [{"id": row["id"]} for row in reference if row["ss"] is not None]
        else:
            expected = reference
        materialized = dataset.materialize()

        def key(row):
            value = next(iter(row.values()))
            return value is None, value

        assert sorted(materialized.take_all(), key=key) == sorted(expected, key=key)
        assert materialized.schema().base_schema == pa.schema(
            [hs2.schema().base_schema.field(name) for name in expected[0]]
        )
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("late", [False, True])
def test_legacy_orc_timestamp_schema_matches_real_hs2(
    direct_runtime, monkeypatch, late
):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    def schema(unit):
        ts = pa.timestamp(unit)
        return pa.schema(
            [
                ("_col0", ts),
                ("_col1", pa.struct([("ts", ts)])),
                ("_col2", pa.list_(ts)),
                ("_col3", pa.map_(pa.string(), ts)),
            ]
        )

    physical, declared = schema("ns"), schema("us")
    settings = {"orc.tolerate.missing.schema": "false"}
    columns = (
        "`_col0` TIMESTAMP, `_col1` STRUCT<ts:TIMESTAMP>, "
        "`_col2` ARRAY<TIMESTAMP>, `_col3` MAP<STRING,TIMESTAMP>"
    )
    with _schema_evolution_table(columns, "orc", False, settings) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        expected = []
        for i in range(17 if late else 1):
            tick = 86400123456789 + i * 1000
            data = pa.Table.from_pylist(
                [
                    {
                        "_col0": tick,
                        "_col1": {"ts": tick},
                        "_col2": [tick, None],
                        "_col3": [("k", tick)],
                    },
                    dict.fromkeys(physical.names),
                ],
                schema=physical,
            )
            expected.extend(data.cast(declared, safe=False).to_pylist())
            if late and i < 16:
                data = data.append_column(
                    "marker", pa.array([i, None], type=pa.int32())
                )
            with filesystem.open_output_stream(f"{path}/{i:06d}_0") as stream:
                orc.write_table(data, stream)
        original_sample = hive_direct.sample_files
        sampled = []

        def record_sample(*args, **kwargs):
            manifest = original_sample(*args, **kwargs)
            sampled.extend(manifest.paths.tolist())
            return manifest

        monkeypatch.setattr(hive_direct, "sample_files", record_sample)
        dataset = ray.data.read_hive(
            table, connection_factory=factory, read_mode="direct", filesystem=filesystem
        )
        if late:
            assert len(sampled) == 16 and f"{path}/000016_0" not in sampled

        def key(row):
            return row["_col0"] is None, row["_col0"] or datetime.min

        result = sorted(dataset.take_all(), key=key)
        assert result == sorted(expected, key=key)
        assert dataset.schema().base_schema == declared
        # Compare typed HS2 fields, retaining the existing HS2 complex-decoding
        # boundary instead of adding a second row decoder to these tests.
        query = f"SELECT `_col0` AS ts, `_col1`.ts AS nested, `_col2`[0] AS item, `_col3`['k'] AS label FROM {table}"
        cursor.execute(query)
        reference = sorted(
            cursor.fetchall(), key=lambda row: (row[0] is None, row[0] or datetime.min)
        )
        flat = [
            (
                row["_col0"],
                (row["_col1"] or {}).get("ts"),
                row["_col2"][0] if row["_col2"] else None,
                dict(row["_col3"] or []).get("k"),
            )
            for row in result
        ]
        assert flat == reference
        flat_schema = pa.schema(
            [(name, pa.timestamp("us")) for name in ("ts", "nested", "item", "label")]
        )
        hs2 = ray.data.read_hive(
            query=query, schema=flat_schema, connection_factory=factory
        ).materialize()
        assert (
            sorted(
                [tuple(row.values()) for row in hs2.take_all()],
                key=lambda row: (row[0] is None, row[0] or datetime.min),
            )
            == reference
        )
        assert hs2.schema().base_schema == flat_schema
        projected = dataset.select_columns(["_col1"]).take_all()
        assert sorted((str(row["_col1"]) for row in projected)) == sorted(
            str(row["_col1"]) for row in result
        )
        filtered = dataset.filter(
            expr=col("_col0") > datetime(1970, 1, 2)
        ).select_columns(["_col3"])
        assert sorted(str(row["_col3"]) for row in filtered.take_all()) == sorted(
            str(row["_col3"]) for row in result if row["_col0"] is not None
        )


@pytest.mark.parametrize("difference", ["name", "type"])
def test_legacy_orc_real_schema_changes_still_require_tolerance(
    direct_runtime, difference
):
    settings = {"orc.tolerate.missing.schema": "false"}
    with _schema_evolution_table("`_col0` BIGINT", "orc", False, settings) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        data = pa.table(
            {
                "_col1"
                if difference == "name"
                else "_col0": pa.array(
                    [42], type=pa.int32() if difference == "type" else pa.int64()
                )
            }
        )
        with filesystem.open_output_stream(f"{path}/000000_0") as stream:
            orc.write_table(data, stream)
        with pytest.raises(Exception, match="[Mm]issing|schema|metadata"):
            cursor.execute(f"SELECT * FROM {table}")
            cursor.fetchall()
        with pytest.raises(ValueError, match="tolerance is disabled"):
            ray.data.read_hive(
                table,
                connection_factory=factory,
                read_mode="direct",
                filesystem=filesystem,
            )


def test_orc_positional_missing_fields_count_projection_and_limit(direct_runtime):
    from ray.data.expressions import col

    with _schema_evolution_table("id BIGINT, added BIGINT", "orc", True) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        for name, data in [
            ("000000_0", pa.table({"old_id": [1, 1, 2]})),
            (
                "000001_0",
                pa.table({"id": [3, 3], "added": [30, 30], "extra": [99, 99]}),
            ),
        ]:
            with filesystem.open_output_stream(f"{path}/{name}") as stream:
                orc.write_table(data, stream)
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        assert result == _reference_rows(cursor, table)
        assert (
            result
            == [{"id": 1, "added": None}] * 2
            + [{"id": 2, "added": None}]
            + [{"id": 3, "added": 30}] * 2
        )
        assert dataset.schema().base_schema == pa.schema(
            [("id", pa.int64()), ("added", pa.int64())]
        )
        missing = dataset.filter(expr=col("id") < 3).select_columns(["added"])
        assert missing.take_all() == [{"added": None}] * 3
        assert dataset.filter(expr=col("id") >= 3).select_columns(["added"]).limit(
            1
        ).take_all() == [{"added": 30}]
        assert dataset.count() == 5
        assert dataset.select_columns([]).count() == 5
        assert _metadata_lookup_count(statements) == 1


def test_orc_renamed_complex_columns_match_real_hs2(direct_runtime):
    from ray.data.expressions import col

    columns = (
        "id INT, old_payload STRUCT<n:INT,f:FLOAT,d:DECIMAL(9,2),ts:TIMESTAMP>, "
        "old_items ARRAY<INT>, old_labels MAP<STRING,INT>, old_code CHAR(4), old_tag VARCHAR(3)"
    )
    with _schema_evolution_table(columns, "orc", True) as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        cursor.execute(
            f"INSERT INTO {table} SELECT row_id, "
            "named_struct('n',42,'f',CAST(0.1 AS FLOAT),'d',CAST(amount AS DECIMAL(9,2)),"
            "'ts',CAST('1969-12-31 23:59:58.123456789' AS TIMESTAMP)),"
            "array(7,CAST(NULL AS INT)),map('k',8),CAST('ab' AS CHAR(4)),CAST('abcdef' AS VARCHAR(3)) "
            "FROM (SELECT STACK(1,1,'-123.45') AS (row_id,amount)) source"
        )
        null_schema = pa.schema(
            [
                ("id", pa.int32()),
                (
                    "old_payload",
                    pa.struct(
                        [
                            ("n", pa.int32()),
                            ("f", pa.float32()),
                            ("d", pa.decimal128(9, 2)),
                            ("ts", pa.timestamp("ns")),
                        ]
                    ),
                ),
                ("old_items", pa.list_(pa.int32())),
                ("old_labels", pa.map_(pa.string(), pa.int32())),
                ("old_code", pa.string()),
                ("old_tag", pa.string()),
            ]
        )
        # Include NULL complex parents with their physical types intact.
        null_row = dict.fromkeys(null_schema.names)
        null_row["id"] = 2
        with filesystem.open_output_stream(f"{path}/nullable_fixture.orc") as stream:
            orc.write_table(
                pa.Table.from_pylist([null_row], schema=null_schema), stream
            )
        changes = {
            "old_payload": (
                "payload",
                "STRUCT<n:BIGINT,f:DOUBLE,d:DECIMAL(18,2),ts:TIMESTAMP>",
            ),
            "old_items": ("items", "ARRAY<BIGINT>"),
            "old_labels": ("labels", "MAP<STRING,BIGINT>"),
            "old_code": ("code", "CHAR(4)"),
            "old_tag": ("tag", "VARCHAR(3)"),
        }
        for old_name, (new_name, type_name) in changes.items():
            cursor.execute(
                f"ALTER TABLE {table} CHANGE COLUMN {old_name} {new_name} {type_name}"
            )
        cursor.execute(
            f"INSERT INTO {table} SELECT row_id, "
            "named_struct('n',CAST(1099511627776 AS BIGINT),'f',CAST(1e100 AS DOUBLE),"
            "'d',CAST(amount AS DECIMAL(18,2)),"
            "'ts',CAST('1970-01-01 00:00:00.000001001' AS TIMESTAMP)),"
            "array(CAST(1099511627776 AS BIGINT),CAST(NULL AS BIGINT)),"
            "map('k',CAST(1099511627776 AS BIGINT)),CAST('ab' AS CHAR(4)),CAST('abcdef' AS VARCHAR(3)) "
            "FROM (SELECT STACK(1,3,'12345678901.23') AS (row_id,amount)) source"
        )
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
        )
        result = sorted(dataset.take_all(), key=lambda row: row["id"])
        cursor.execute(
            f"SELECT id,payload.n,payload.f,payload.d,payload.ts,items[0],items[1],"
            f"labels['k'],code,tag FROM {table}"
        )
        reference = sorted(cursor.fetchall())
        flattened = []
        for row in result:
            payload = row["payload"] or dict.fromkeys(("n", "f", "d", "ts"))
            items = row["items"] or [None, None]
            flattened.append(
                (
                    row["id"],
                    payload["n"],
                    payload["f"],
                    payload["d"],
                    payload["ts"],
                    items[0],
                    items[1],
                    dict(row["labels"] or []).get("k"),
                    row["code"],
                    row["tag"],
                )
            )
        assert flattened == reference
        assert result[1] == {
            "id": 2,
            "payload": None,
            "items": None,
            "labels": None,
            "code": None,
            "tag": None,
        }
        assert result[0]["payload"]["ts"] == datetime(1969, 12, 31, 23, 59, 58, 123456)
        assert result[2]["payload"]["ts"] == datetime(1970, 1, 1, 0, 0, 0, 1)
        assert result[0]["code"] == result[2]["code"] == "ab  "
        assert result[0]["tag"] == result[2]["tag"] == "abc"
        schema = pa.schema(
            [
                ("id", pa.int32()),
                (
                    "payload",
                    pa.struct(
                        [
                            ("n", pa.int64()),
                            ("f", pa.float64()),
                            ("d", pa.decimal128(18, 2)),
                            ("ts", pa.timestamp("us")),
                        ]
                    ),
                ),
                ("items", pa.list_(pa.int64())),
                ("labels", pa.map_(pa.string(), pa.int64())),
                ("code", pa.string()),
                ("tag", pa.string()),
            ]
        )
        assert dataset.schema().base_schema == schema
        selected = (
            dataset.filter(expr=col("id") > 1).select_columns(["code"]).take_all()
        )
        assert sorted(selected, key=lambda row: row["code"] or "") == [
            {"code": None},
            {"code": "ab  "},
        ]
        assert sorted(
            row["id"]
            for row in dataset.filter(expr=col("code") == "ab  ")
            .select_columns(["id"])
            .take_all()
        ) == [1, 3]
        assert dataset.select_columns([]).count() == 3
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("backend", ["hdfs", "s3"])
@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_nonpartitioned_native_read(direct_runtime, backend, file_format):
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
    observer = None
    with _schema_evolution_table(columns, file_format, backend=backend) as (
        filesystem,
        path,
        table_name,
        cursor,
        connection_factory,
    ):
        try:
            for index in range(4):
                with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                    batch = physical.slice(index * 8, 8)
                    if file_format == "parquet":
                        pq.write_table(
                            batch, stream, use_deprecated_int96_timestamps=True
                        )
                    else:
                        orc.write_table(batch, stream)

            driver_pid = os.getpid()

            statements = []
            metadata_only_factory = _metadata_only_factory(
                connection_factory, statements
            )

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
                row["labels"] = (
                    dict(row["labels"]) if row["labels"] is not None else None
                )
            assert normalized == _reference_rows(
                cursor, table_name, ("items", "labels", "nested")
            )
            assert _metadata_lookup_count(statements) == 3

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


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_empty_native_table(direct_runtime, file_format):
    with _schema_evolution_table(
        "id BIGINT, name STRING", file_format, backend="s3"
    ) as (filesystem, path, table, cursor, factory):
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


def test_default_hs2_table_and_explicit_query_still_read(direct_runtime):
    with _schema_evolution_table("id BIGINT, name STRING", "parquet", backend="s3") as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
        with filesystem.open_output_stream(path + "/000000_0") as stream:
            pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), stream)
        result = ray.data.read_hive(table, connection_factory=factory).take_all()
        assert sorted(row["id"] for row in result) == [1, 2]
        result = ray.data.read_hive(
            query=f"SELECT id AS event_id FROM {table}",
            schema=pa.schema([("event_id", pa.int64())]),
            connection_factory=factory,
        ).take_all()
        assert sorted(row["event_id"] for row in result) == [1, 2]


@pytest.mark.parametrize("positional", [False, True])
def test_orc_merge_schema_matches_hs2_and_retains_files(direct_runtime, positional):
    from ray.data.expressions import col

    with _schema_evolution_table(
        "id BIGINT, late STRING",
        "orc",
        positional,
        {"orc.schema.evolution.case.sensitive": "false"},
    ) as (filesystem, path, table, cursor, factory):
        for index in range(17):
            values = {"old_id" if positional else "id": [index]}
            if index == 16:
                values = {
                    "new_id" if positional else "ID": [index],
                    "new_late" if positional else "late": ["present"],
                }
            with filesystem.open_output_stream(f"{path}/{index:06d}_0") as stream:
                orc.write_table(pa.table(values), stream)
        reference = _reference_rows(cursor, table)
        assert reference == [
            {"id": index, "late": "present" if index == 16 else None}
            for index in range(17)
        ]
        statements = []
        dataset = ray.data.read_hive(
            table,
            connection_factory=_metadata_only_factory(factory, statements),
            read_mode="direct",
            filesystem=filesystem,
            merge_schema=True,
        )
        assert dataset.schema().base_schema == pa.schema(
            [("id", pa.int64()), ("late", pa.string())]
        )
        with filesystem.open_output_stream(path + "/published_later") as stream:
            orc.write_table(pa.table({"id": [99], "late": ["not planned"]}), stream)
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == reference
        restored = ray.data.Dataset.deserialize_lineage(dataset.serialize_lineage())
        projected = (
            restored.filter(expr=col("id") >= 16).select_columns(["late"]).limit(2)
        )
        assert projected.take_all() == [{"late": "present"}]
        assert _metadata_lookup_count(statements) == 1


def test_parquet_merge_schema_retains_nonempty_file_set(direct_runtime):
    with _schema_evolution_table("id BIGINT, late STRING", "parquet", backend="s3") as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
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


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_written_file_values(direct_runtime, file_format):
    with _schema_evolution_table(
        "id BIGINT, code CHAR(4), tag VARCHAR(3), "
        "ts TIMESTAMP, items ARRAY<INT>, labels MAP<STRING,STRING>, "
        "nested STRUCT<name:STRING,value:INT>",
        file_format,
        backend="hdfs",
    ) as (filesystem, path, table, cursor, factory):
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
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_hive_written_epoch_timestamps(direct_runtime, file_format):
    from ray.data.expressions import col

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
    with _schema_evolution_table(
        "id INT, ts TIMESTAMP, nested STRUCT<ts:TIMESTAMP>, "
        "items ARRAY<TIMESTAMP>, labels MAP<STRING,TIMESTAMP>",
        file_format,
        backend="hdfs",
    ) as (filesystem, path, table, cursor, factory):
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
        assert _metadata_lookup_count(statements) == 1


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_late_file_schema_failure_does_not_replay_hs2(direct_runtime, file_format):
    with _schema_evolution_table("id BIGINT", file_format, backend="s3") as (
        filesystem,
        path,
        table,
        cursor,
        factory,
    ):
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
        assert _metadata_lookup_count(statements) == 1
        with pytest.raises(Exception, match="type mismatch"):
            dataset.take_all()
        assert _metadata_lookup_count(statements) == 1
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
            assert _metadata_lookup_count(statements) == 2


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
