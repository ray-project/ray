"""Hive metadata contracts and composition with native file readers."""

from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

from ray.data._internal.datasource_v2.formats.hive import hive_direct
from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_direct import (
    HiveDirectDatasource,
    HiveFileTable,
    _cast_hive_timestamps,
    _parse_file_table,
    _read_schema,
    read_file_table_metadata,
)
from ray.data._internal.datasource_v2.formats.hive.hive_schema import hive_type_to_arrow
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest


def _metadata(file_format="parquet", table_type="EXTERNAL_TABLE", properties=None):
    formats = {
        "parquet": (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        ),
        "orc": (
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        ),
    }
    input_format, serde = formats[file_format]
    return [
        ("# col_name", "data_type", "comment"),
        ("id", "bigint", None),
        ("payload", "struct<items:array<int>,labels:map<string,string>>", None),
        ("", "", ""),
        ("# Detailed Table Information", None, None),
        ("Table Type:", table_type, None),
        ("Location:", "hdfs://namenode:8020/warehouse/events", None),
        ("Table Parameters:", None, None),
        *[("", key, value) for key, value in (properties or {}).items()],
        ("# Storage Information", None, None),
        ("SerDe Library:", serde, None),
        ("InputFormat:", input_format, None),
    ]


def _metadata_session(file_format="orc", settings=None):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    values = {
        "parquet.column.index.access": "false",
        "orc.force.positional.evolution": "false",
        "orc.force.positional.evolution.level": None,
        "hive.exec.schema.evolution": "true",
        "orc.tolerate.missing.schema": None,
        "hive.exec.orc.tolerate.missing.schema": None,
        "orc.schema.evolution.case.sensitive": None,
    }
    values.update(settings or {})

    def fetch():
        statement = cursor.execute.call_args.args[0]
        if statement.startswith("DESCRIBE"):
            return _metadata(file_format)
        key = statement[4:]
        value = values[key]
        if isinstance(value, list):
            return value
        return [(f"{key} is undefined" if value is None else f"{key}={value}",)]

    cursor.fetchall.side_effect = fetch
    return connection, cursor


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("table_type", ["MANAGED_TABLE", "EXTERNAL_TABLE"])
def test_metadata_accepts_supported_file_tables(file_format, table_type):
    table = _parse_file_table(_metadata(file_format, table_type))
    assert table.file_format == file_format
    assert table.schema.names == ["id", "payload"]
    assert pa.types.is_struct(table.schema.field("payload").type)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_metadata_preserves_column_name_whitespace(file_format):
    names = ["id", " id", "id ", " id "]
    rows = _metadata(file_format, properties={" transactional ": " false "})
    rows[1:3] = [(name, " bigint ", None) for name in names]
    rows[0] = (" # col_name ", " data_type ", "comment")
    rows = [
        (key, value, comment) if key in names else (f" {key} ", value, comment)
        for key, value, comment in rows
    ]
    table = _parse_file_table(rows)
    assert table.schema.names == names
    assert all(field.type == pa.int64() for field in table.schema)
    assert all(field.metadata == {b"hive_type": b"bigint"} for field in table.schema)


def test_serde_transactional_key_is_not_a_table_transaction_flag():
    rows = _metadata()
    rows.extend([("Storage Desc Params:", None, None), ("", "transactional", "true")])
    assert _parse_file_table(rows).file_format == "parquet"


@pytest.mark.parametrize("section", ["table", "serde"])
def test_positional_options_are_checked_in_both_namespaces(section):
    rows = _metadata(
        properties={"parquet.column.index.access": str(section == "table")}
    )
    rows.extend(
        [
            ("Storage Desc Params:", None, None),
            ("", "parquet.column.index.access", str(section == "serde")),
        ]
    )
    with pytest.raises(ValueError, match="positional"):
        _parse_file_table(rows)


@pytest.mark.parametrize(
    "file_format,value",
    [("parquet", None), ("parquet", "false"), ("orc", "false"), ("orc", "true")],
)
def test_metadata_session_settings_and_cleanup(file_format, value):
    key = (
        "parquet.column.index.access"
        if file_format == "parquet"
        else "orc.force.positional.evolution"
    )
    connection, cursor = _metadata_session(file_format, {key: value})
    result = read_file_table_metadata(
        HiveReadSpec(
            connection_factory=lambda: connection, table="events", user="reader"
        )
    )
    assert result.file_format == file_format
    assert result.schema.field("id").type == pa.int64()
    assert result.force_positional_evolution == (value == "true")
    if file_format == "orc":
        assert result.tolerate_missing_schema and result.schema_evolution_case_sensitive
    statements = [c.args[0] for c in cursor.execute.call_args_list]
    assert statements[0] == "DESCRIBE FORMATTED `default`.`events`"
    assert f"SET {key}" in statements
    assert len(statements) == (2 if file_format == "parquet" else 7)
    assert all(statement.startswith("SET ") for statement in statements[1:])
    connection.cursor.assert_called_once_with(user="reader")
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_metadata_uses_lowercase_for_name_ambiguity(file_format):
    rows = _metadata(file_format)
    rows[1:3] = [("ss", "bigint", None), ("\u00df", "bigint", None)]
    assert _parse_file_table(rows).schema.names == ["ss", "\u00df"]
    if file_format == "parquet":
        rows[2] = ("SS", "bigint", None)
        with pytest.raises(ValueError, match="unambiguous"):
            _parse_file_table(rows)


def test_lookup_preserves_error_and_closes_on_failure():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("metadata failed")
    cursor.close.side_effect = RuntimeError("cleanup failed")
    with pytest.raises(RuntimeError, match="metadata failed"):
        read_file_table_metadata(
            HiveReadSpec(connection_factory=lambda: connection, table="events")
        )
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "file_format,setting,value",
    [
        ("parquet", "parquet.column.index.access", "true"),
        ("parquet", "parquet.column.index.access", "unknown"),
        *[
            ("parquet", "parquet.column.index.access", rows)
            for rows in [
                [],
                [("other=false",)],
                [("parquet.column.index.access=false", "extra")],
            ]
        ],
        *[
            ("orc", "orc.force.positional.evolution", rows)
            for rows in [
                [],
                [("undefined",)],
                [("orc.force.positional.evolution=unknown",)],
                [("orc.force.positional.evolution=true", "extra")],
            ]
        ],
        ("orc", "orc.force.positional.evolution.level", "2"),
        ("orc", "hive.exec.schema.evolution", "false"),
        ("orc", "orc.tolerate.missing.schema", "unknown"),
        ("orc", "orc.schema.evolution.case.sensitive", "unknown"),
    ],
)
def test_invalid_session_settings_precede_file_planning(
    monkeypatch, file_format, setting, value
):
    from ray.data import read_api

    connection, cursor = _metadata_session(file_format, {setting: value})
    monkeypatch.setattr(
        hive_direct,
        "HiveDirectDatasource",
        lambda *a, **kw: pytest.fail("Unexpected file planning"),
    )
    with pytest.raises(ValueError):
        read_api.read_hive(
            "events", connection_factory=lambda: connection, read_mode="direct"
        )
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "rows",
    [
        *[
            _metadata(properties=properties)
            for properties in [
                {"transactional": "true"},
                {"transactional_properties": "insert_only"},
                {"table_type": "ICEBERG"},
                {
                    "storage_handler": "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"
                },
                {"spark.sql.sources.provider": "delta"},
                {"parquet.column.index.access": "true"},
                {"orc.force.positional.evolution": "true"},
            ]
        ],
        _metadata()[:3] + [("# Partition Information", None, None)],
        _metadata(table_type="VIRTUAL_VIEW"),
        _metadata()[:-1],
        [row for row in _metadata() if row[0] != "Location:"],
        *[
            [
                (key, location if key == "Location:" else value, comment)
                for key, value, comment in _metadata()
            ]
            for location in ["https://example.test/data", "s3://user@bucket/data"]
        ],
        [_metadata()[0], ("#unexpected", "bigint", None), *_metadata()[2:]],
        [*_metadata()[:2], ("payload", "struct<code:char(4)>", None), *_metadata()[3:]],
        [
            *_metadata(properties={"transactional": "true"}),
            ("Storage Desc Params:", None, None),
            ("", "transactional", "false"),
        ],
    ],
)
def test_public_boundary_rejection_precedes_any_file_plan(monkeypatch, rows):
    from ray.data import read_api

    connection = MagicMock()
    connection.cursor.return_value.fetchall.return_value = rows

    def unexpected(*args, **kwargs):
        pytest.fail("Unsupported metadata must fail before file planning")

    monkeypatch.setattr(hive_direct, "HiveDirectDatasource", unexpected)
    monkeypatch.setattr(read_api, "_read_datasource_v2", unexpected)
    with pytest.raises(ValueError):
        read_api.read_hive(
            "events", connection_factory=lambda: connection, read_mode="direct"
        )
    connection.cursor.return_value.execute.assert_called_once_with(
        "DESCRIBE FORMATTED `default`.`events`"
    )


@pytest.mark.parametrize(
    "description,expected",
    [
        ("boolean", pa.bool_()),
        ("tinyint", pa.int8()),
        ("smallint", pa.int16()),
        ("int", pa.int32()),
        ("bigint", pa.int64()),
        ("float", pa.float32()),
        ("double", pa.float64()),
        ("binary", pa.binary()),
        ("date", pa.date32()),
        ("char(4)", pa.string()),
        ("varchar(12)", pa.string()),
        ("timestamp", pa.timestamp("us")),
        ("decimal(12, 2)", pa.decimal128(12, 2)),
        ("array<int>", pa.list_(pa.int32())),
        ("map<string,array<bigint>>", pa.map_(pa.string(), pa.list_(pa.int64()))),
        (
            "struct<`a b`:int, nested:array<string>>",
            pa.struct([("a b", pa.int32()), ("nested", pa.list_(pa.string()))]),
        ),
    ],
)
def test_hive_type_metadata(description, expected):
    assert hive_type_to_arrow(description) == expected


@pytest.mark.parametrize(
    "description",
    [
        "",
        "array<int",
        "map<string>",
        "decimal(39,0)",
        "decimal(3,4)",
        "uniontype<int,string>",
        "struct<a:int,A:string>",
        "varchar(0)",
        "char(256)",
        "varchar(65536)",
        "array<char(4)>",
        "map<string,varchar(4)>",
        "struct<code:char(4)>",
        "int junk",
    ],
)
def test_bad_type_metadata_fails_early(description):
    with pytest.raises(ValueError):
        hive_type_to_arrow(description)


def test_schema_name_mapping_and_compatibility():
    declared = pa.schema([("id", pa.int64()), ("missing", pa.string())])
    schema, renames = _read_schema(declared, pa.schema([("ID", pa.int64())]))
    assert schema.names == ["ID", "missing"]
    assert renames == {"ID": "id"}
    with pytest.raises(ValueError, match="ambiguous"):
        _read_schema(declared, pa.schema([("id", pa.int64()), ("ID", pa.int64())]))
    with pytest.raises(ValueError, match="type mismatch"):
        _read_schema(declared, pa.schema([("id", pa.string())]))


@pytest.mark.parametrize(
    "actual,expected",
    [
        (actual, expected)
        for actual in (pa.int8(), pa.int16(), pa.int32())
        for expected in (pa.int16(), pa.int32(), pa.int64())
        if actual.bit_width < expected.bit_width
    ]
    + [(pa.float32(), pa.float64()), (pa.decimal128(9, 2), pa.decimal128(18, 2))],
)
def test_safe_widening_uses_the_declared_target_schema(actual, expected):
    schema, renames = _read_schema(
        pa.schema([("v", expected)]), pa.schema([("v", actual)])
    )
    assert schema == pa.schema([("v", expected)])
    assert renames == {}


@pytest.mark.parametrize(
    "actual,expected",
    [
        (pa.int64(), pa.int32()),
        (pa.float64(), pa.float32()),
        (pa.int32(), pa.float64()),
        (pa.uint32(), pa.int64()),
        (pa.string(), pa.int64()),
        (pa.decimal128(18, 2), pa.decimal128(9, 2)),
        (pa.decimal128(9, 2), pa.decimal128(18, 3)),
    ],
)
def test_other_type_changes_remain_unsupported(actual, expected):
    with pytest.raises(ValueError, match="type mismatch"):
        _read_schema(pa.schema([("v", expected)]), pa.schema([("v", actual)]))


@pytest.mark.parametrize("chunked", [False, True])
def test_timestamp_cast_preserves_sliced_nested_containers(chunked):
    def dtype(unit):
        ts = pa.timestamp(unit)
        return pa.struct(
            [
                ("n", pa.int32()),
                ("ts", ts),
                ("items", pa.list_(ts)),
                ("values", pa.map_(pa.string(), ts)),
                ("keys", pa.map_(ts, pa.string())),
                ("nested", pa.struct([("groups", pa.list_(pa.map_(pa.string(), ts)))])),
            ]
        )

    def row(value):
        return {
            "n": 42,
            "ts": value,
            "items": [value, None],
            "values": [("k", value)],
            "keys": [(value, "v")],
            "nested": {"groups": [None, [], [("k", value)]]},
        }

    nulls = dict.fromkeys(field.name for field in dtype("ns"))
    empty = dict(nulls, items=[], values=[], keys=[], nested={"groups": []})
    ticks = [-1001, -1000, -999, -1, 0, 1, 999, 1000, 1001]
    micros = [-2, -1, -1, -1, 0, 0, 0, 1, 1]
    data = [None, nulls, empty] + [row(value) for value in ticks]
    expected = [None, nulls, empty] + [row(value) for value in micros]
    values = pa.array([row(9999)] + data + [row(9999)], type=dtype("ns")).slice(
        1, len(data)
    )
    assert values.offset == 1
    if chunked:
        values = pa.chunked_array(
            [values.slice(0, 3), values.slice(3, 0), values.slice(3)]
        )
    result = _cast_hive_timestamps(values, dtype("us"))
    if chunked:
        assert result.num_chunks == 3
        result = result.combine_chunks()
    assert result.equals(pa.array(expected, type=dtype("us")))


def test_timestamp_cast_avoids_nanosecond_floor_overflow():
    values = pa.array(
        [-9223372036854775807, 9223372036854775807], type=pa.timestamp("ns")
    )
    result = _cast_hive_timestamps(values, pa.timestamp("us"))
    assert result.cast(pa.int64()).to_pylist() == [-9223372036854776, 9223372036854775]


def _write_table_file(path, file_format, table):
    if file_format == "parquet":
        pq.write_table(table, path)
    else:
        orc.write_table(table, path)
    return path


@pytest.mark.parametrize(
    "actual,expected",
    [
        (pa.int32(), pa.int64()),
        (pa.timestamp("ms"), pa.timestamp("us")),
        (pa.timestamp("ns", tz="UTC"), pa.timestamp("us")),
        (
            pa.struct([("old", pa.timestamp("ns"))]),
            pa.struct([("new", pa.timestamp("us"))]),
        ),
        (
            pa.struct([("a", pa.int64()), ("b", pa.int64())]),
            pa.struct([("b", pa.int64()), ("a", pa.int64())]),
        ),
        (pa.list_(pa.int32()), pa.list_(pa.int64())),
        (pa.map_(pa.string(), pa.int32()), pa.map_(pa.string(), pa.int64())),
    ],
)
def test_legacy_orc_schema_equality_is_stricter_than_safe_casts(actual, expected):
    with pytest.raises(ValueError, match="tolerance is disabled"):
        hive_direct._orc_uses_positions(
            pa.schema([("_col0", expected)]),
            pa.schema([("_col0", actual)]),
            False,
            False,
        )


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_file_schema_metadata_read_uses_configured_io_retry(
    tmp_path, file_format, monkeypatch
):
    import ray.data._internal.util as ray_util

    path = tmp_path / "part"
    expected = pa.schema([("id", pa.int64())])
    read = MagicMock(side_effect=[OSError("temporary schema I/O error"), expected])
    retry = ray_util.iterate_with_retry
    monkeypatch.setattr(hive_direct, "_read_physical_schema", read)
    monkeypatch.setattr(
        hive_direct,
        "iterate_with_retry",
        lambda factory, description, **kwargs: retry(
            factory, description, max_backoff_s=0, **kwargs
        ),
    )
    monkeypatch.setattr(ray_util.time, "sleep", lambda _: None)
    assert (
        hive_direct._read_physical_schema_with_retry(
            str(path), file_format, None, ["temporary schema I/O error"]
        )
        == expected
    )
    assert read.call_count == 2


@pytest.mark.parametrize("use_filesystem", [False, True])
@pytest.mark.parametrize("fail_schema", [False, True])
def test_parquet_schema_read_closes_file(
    tmp_path, monkeypatch, use_filesystem, fail_schema
):
    from pyarrow.fs import LocalFileSystem

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [42]}), path)
    opened = []

    def schema(parquet):
        opened.append(parquet)
        assert not parquet.closed
        if fail_schema:
            raise OSError("schema lookup failed")
        return pa.schema([("id", pa.int64())])

    monkeypatch.setattr(pq.ParquetFile, "schema_arrow", property(schema))
    if fail_schema:
        with pytest.raises(OSError, match="schema lookup failed"):
            hive_direct._read_physical_schema(
                str(path), "parquet", LocalFileSystem() if use_filesystem else None
            )
    else:
        assert hive_direct._read_physical_schema(
            str(path), "parquet", LocalFileSystem() if use_filesystem else None
        ) == pa.schema([("id", pa.int64())])
    assert len(opened) == 1 and opened[0].closed


@pytest.mark.parametrize("nested", [False, True])
def test_orc_rejects_decoded_pickle_before_filter(tmp_path, monkeypatch, nested):
    import pickle
    from types import SimpleNamespace

    from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
    from ray.data.expressions import col

    monkeypatch.delenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", raising=False)
    marker = tmp_path / "unpickled"

    class Payload:
        def __reduce__(self):
            return type(marker).touch, (marker,)

    storage = pa.array([pickle.dumps(Payload())], type=pa.large_binary())
    values = pa.ExtensionArray.from_storage(ArrowPythonObjectType(), storage)
    declared_type = pa.binary()
    if nested:
        values = pa.StructArray.from_arrays([values], names=["value"])
        declared_type = pa.struct([("value", pa.binary())])
    declared = pa.schema([("id", pa.int64()), ("payload", declared_type)])
    batch = pa.record_batch([pa.array([1]), values], names=declared.names)
    scanner = hive_direct._HiveOrcScanner(
        schema=declared,
        hive_schema=declared,
        sampled_names=tuple((name, name) for name in declared.names),
    )
    scanner, residual = scanner.push_filters(col("id") > 100)
    assert residual is None
    check_pickle = MagicMock(wraps=hive_direct.raise_on_pickle_object_columns)
    monkeypatch.setattr(hive_direct, "raise_on_pickle_object_columns", check_pickle)

    # Inject a decoded Arrow batch at the reader boundary. This does not
    # claim that a real ORC file can persist this extension type.
    fragment = SimpleNamespace(
        physical_schema=declared,
        scanner=lambda **kwargs: SimpleNamespace(
            scan_batches=lambda: iter([SimpleNamespace(record_batch=batch)])
        ),
    )
    reader = scanner.create_reader()
    assert isinstance(reader, hive_direct._HiveOrcFileReader)
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(reader._iter_fragment_tables(fragment, {}))
    check_pickle.assert_called_once()
    # An after-filter check would see zero rows and miss the intended order.
    assert check_pickle.call_args.args[0].num_rows == 1
    assert not marker.exists()


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_scanner_options_preserve_native_behavior(tmp_path, monkeypatch, file_format):
    schema = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, schema))
    native = MagicMock(wraps=source._source.create_scanner)
    monkeypatch.setattr(source._source, "create_scanner", native)
    prefixes = ["_custom"]
    scanner = source.create_scanner(schema, partitioning=None, ignore_prefixes=prefixes)
    native.assert_called_once_with(
        schema,
        filesystem=source._source.filesystem,
        partitioning=None,
        ignore_prefixes=prefixes,
    )
    reader = scanner.create_reader()
    assert scanner.schema == schema and reader._partition_parser is None
    assert reader._ignore_prefixes == (prefixes if file_format == "parquet" else None)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize(
    "merge_schema,file_count,should_warn",
    [(False, 16, False), (True, 10_000, False), (True, 10_001, True)],
)
def test_full_schema_warning_precedes_schema_reads(
    tmp_path, monkeypatch, file_format, merge_schema, file_count, should_warn
):
    from ray.data import read_api
    from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
        ParquetDatasourceV2,
    )

    assert (
        hive_direct._FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT
        == read_api._FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT
        == 10_000
    )
    declared = pa.schema([("id", pa.int64())])
    sample = FileManifest.construct_manifest(
        paths=[str(tmp_path / f"part-{i}") for i in range(file_count)],
        sizes=[1] * file_count,
        chunk_metadatas=[None] * file_count,
    )
    sampler, warning = MagicMock(return_value=sample), MagicMock()

    def infer(_source, candidate_files, *_):
        assert warning.call_count == int(should_warn) and candidate_files is sample
        return declared

    monkeypatch.setattr(hive_direct, "sample_files", sampler)
    monkeypatch.setattr(hive_direct.logger, "warning", warning)
    monkeypatch.setattr(ParquetDatasourceV2, "infer_schema", infer)
    monkeypatch.setattr(
        hive_direct,
        "_infer_orc_read_schema",
        lambda *args: (infer(*args), {"id": "id"}),
    )
    result = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), file_format, declared), merge_schema=merge_schema
    )
    assert result.infer_schema(None) == declared and result._retained is (
        sample if merge_schema else None
    )
    assert sampler.call_count == 1 and sampler.call_args.kwargs["max_files"] == (
        None if merge_schema else 16
    )
    assert warning.call_count == int(should_warn)
    if should_warn:
        message, count, threshold = warning.call_args.args
        assert (count, threshold) == (file_count, 10_000)
        assert "before returning the Dataset" in message


def test_orc_full_schema_validates_late_types_during_planning(tmp_path):
    declared = pa.schema([("id", pa.int32())])
    for index in range(17):
        _write_table_file(
            tmp_path / f"part-{index:02d}",
            "orc",
            pa.table(
                {
                    "id": pa.array(
                        [index], type=pa.int64() if index == 16 else pa.int32()
                    )
                }
            ),
        )
    table = HiveFileTable(str(tmp_path), "orc", declared)
    assert HiveDirectDatasource(table).infer_schema(None) == declared
    with pytest.raises(ValueError, match="type mismatch"):
        HiveDirectDatasource(table, merge_schema=True)


@pytest.mark.parametrize(
    "physical,required,error",
    [
        (["id", "name"], None, None),
        (["name"], None, None),
        (["ID", "name"], None, "column name changes"),
        (["id", "ID", "name"], {"id"}, "ambiguous"),
        (["ID", "name"], {"name"}, None),
        (["id", "ID", "name"], {"name"}, None),
    ],
)
def test_per_file_name_validation_only_checks_required_fields(
    physical, required, error
):
    declared = pa.schema([(name, pa.int64()) for name in ("id", "name")])
    actual = pa.schema([(name, pa.int64()) for name in physical])

    def validate():
        hive_direct._validate_file_schema(
            declared, actual, {"id": "id", "name": "name"}, required
        )

    if error:
        with pytest.raises(ValueError, match=error):
            validate()
    else:
        validate()


@pytest.mark.parametrize(
    "logical,physical,force,sensitive,tolerate,expected",
    [
        (["id", "value"], ["value", "id"], True, False, True, [0, 1]),
        (["id", "value"], ["value", "id"], False, False, True, [1, 0]),
        (["id", "value"], ["old", "value"], True, False, True, [0, 1]),
        (["id", "value"], ["id"], True, False, True, [0, -1]),
        (["id"], ["id", "extra"], True, False, True, [0]),
        (["id", "_col0"], ["_col0", "_col1"], False, False, True, [0, 1]),
        (["id", "_col0"], ["_col0"], False, False, True, [0, -1]),
        (["id"], ["_col0", "id"], False, False, True, [1]),
        (["_column"], ["_color", "_column"], False, False, True, [1]),
        (["_column"], ["_color"], False, False, True, [-1]),
        (["ss", "ß"], ["ß", "ss"], False, False, True, [1, 0]),
        (["ss"], ["ß"], False, False, True, [-1]),
        (["id"], ["ID"], False, True, True, [-1]),
        (["id"], ["ID"], False, False, True, [0]),
        (["_col0"], ["_col0"], False, False, False, [0]),
        (["id"], ["_col0"], True, False, False, [0]),
    ],
)
def test_orc_mapping_rules(logical, physical, force, sensitive, tolerate, expected):
    def schema(names):
        return pa.schema([(name, pa.int64()) for name in names])

    assert (
        hive_direct._orc_column_indices(
            schema(logical), schema(physical), force, tolerate, sensitive
        )
        == expected
    )


@pytest.mark.parametrize("shape", ["scalar", "struct", "array", "map_value", "map_key"])
def test_legacy_timestamp_equality_ignores_only_ns_us(shape):
    def dtype(unit):
        ts = pa.timestamp(unit)
        return {
            "scalar": ts,
            "struct": pa.struct([("ts", ts)]),
            "array": pa.list_(ts),
            "map_value": pa.map_(pa.string(), ts),
            "map_key": pa.map_(ts, pa.string()),
        }[shape]

    assert not hive_direct._orc_uses_positions(
        pa.schema([("_col0", dtype("us"))]),
        pa.schema([("_col0", dtype("ns"))]),
        False,
        False,
    )


@pytest.mark.parametrize("positional", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_orc_scan_requests_keep_only_output_and_filter_columns(
    tmp_path, positional, filtered
):
    from types import SimpleNamespace

    import pyarrow.dataset as pds

    from ray.data.expressions import col

    names = ["unused", "id", "value", "payload"]
    data = pa.table(dict(zip(names, [[99, 99], [0, 1], [41, 42], ["wide", "wide"]])))
    physical = (
        data.rename_columns(["old_" + name for name in names])
        if positional
        else data.select(["payload", "value", "id", "unused"])
    )
    path = _write_table_file(tmp_path / "part", "orc", physical)
    source = HiveDirectDatasource(
        HiveFileTable(
            str(tmp_path), "orc", data.schema, force_positional_evolution=positional
        )
    )
    scanner = source.create_scanner(source.infer_schema(None)).prune_columns(["value"])
    if filtered:
        scanner, residual = scanner.push_filters(col("id") > 0)
        assert residual is None
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())
    scan = MagicMock(wraps=fragment.scanner)
    proxy = SimpleNamespace(physical_schema=fragment.physical_schema, scanner=scan)
    reader = scanner.create_reader()
    assert isinstance(reader, hive_direct._HiveOrcFileReader)
    result = pa.concat_tables(list(reader._iter_fragment_tables(proxy, {})))
    assert result.select(["value"]).to_pylist() == [
        {"value": value} for value in ([42] if filtered else [41, 42])
    ]
    indices = {"id": 1, "value": 2} if positional else {"id": 2, "value": 1}
    required = ["id", "value"] if filtered else ["value"]
    assert {
        name: str(expr) for name, expr in scan.call_args.kwargs["columns"].items()
    } == {name: str(pds.field(indices[name])) for name in required}
    assert scan.call_args.kwargs["filter"] is None


@pytest.mark.parametrize("filtered", [False, True])
def test_legacy_orc_projection_keeps_full_hive_positions(tmp_path, filtered):
    import pyarrow.dataset as pds

    from ray.data.expressions import col

    path = _write_table_file(tmp_path / "part", "orc", pa.table({"_col0": [42]}))
    schema = pa.schema([("id", pa.int64()), ("_col0", pa.int64())])
    scanner = hive_direct._HiveOrcScanner(
        schema=schema,
        hive_schema=schema,
        sampled_names=tuple((name, name) for name in schema.names),
    ).prune_columns(["_col0"])
    if filtered:
        scanner, residual = scanner.push_filters(col("_col0") > 0)
        assert residual is None
    reader = scanner.create_reader()
    assert isinstance(reader, hive_direct._HiveOrcFileReader)
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())
    result = pa.concat_tables(list(reader._iter_fragment_tables(fragment, {})))
    assert result.to_pylist() == ([] if filtered else [{"_col0": None}])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
