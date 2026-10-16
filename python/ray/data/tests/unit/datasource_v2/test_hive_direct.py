"""Hive metadata contracts and composition with native file readers."""

from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

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


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("table_type", ["MANAGED_TABLE", "EXTERNAL_TABLE"])
def test_metadata_accepts_supported_file_tables(file_format, table_type):
    table = _parse_file_table(_metadata(file_format, table_type))
    assert table.file_format == file_format
    assert table.schema.names == ["id", "payload"]
    assert pa.types.is_struct(table.schema.field("payload").type)


@pytest.mark.parametrize(
    "properties",
    [
        {"transactional": "true"},
        {"transactional_properties": "insert_only"},
        {"table_type": "ICEBERG"},
        {"storage_handler": "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"},
        {"spark.sql.sources.provider": "delta"},
        {"parquet.column.index.access": "true"},
        {"orc.force.positional.evolution": "true"},
    ],
)
def test_metadata_rejects_unsupported_properties(properties):
    with pytest.raises(ValueError):
        _parse_file_table(_metadata(properties=properties))


def test_serde_properties_cannot_mask_table_transactionality():
    rows = _metadata(
        file_format="orc",
        table_type="MANAGED_TABLE",
        properties={"transactional": "true", "transactional_properties": "default"},
    )
    rows.extend(
        [
            ("Storage Desc Params:", None, None),
            ("", "transactional", "false"),
            ("", "transactional_properties", ""),
        ]
    )
    with pytest.raises(ValueError, match="ACID"):
        _parse_file_table(rows)


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


def test_metadata_rejects_partitions_views_and_incomplete_storage():
    with pytest.raises(ValueError, match="partitioned"):
        _parse_file_table(_metadata()[:3] + [("# Partition Information", None, None)])
    with pytest.raises(ValueError, match="managed or external"):
        _parse_file_table(_metadata(table_type="VIRTUAL_VIEW"))
    with pytest.raises(ValueError, match="supported Parquet or ORC"):
        _parse_file_table(_metadata()[:-1])
    with pytest.raises(ValueError, match="location"):
        _parse_file_table([r for r in _metadata() if r[0] != "Location:"])


@pytest.mark.parametrize(
    "location", ["https://example.test/data", "s3://user@bucket/data"]
)
def test_metadata_rejects_unsupported_or_credentialed_location(location):
    rows = [
        (key, location if key == "Location:" else value, comment)
        for key, value, comment in _metadata()
    ]
    with pytest.raises(ValueError):
        _parse_file_table(rows)


def test_metadata_does_not_silently_drop_header_like_column_names():
    rows = _metadata()
    rows[1] = ("#unexpected", "bigint", None)
    with pytest.raises(ValueError, match="column metadata"):
        _parse_file_table(rows)


def test_lookup_uses_only_metadata_and_closes_resources():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = _metadata()
    spec = HiveReadSpec(connection_factory=lambda: connection, table="analytics.events")
    result = read_file_table_metadata(spec)
    assert result.schema.field("id").type == pa.int64()
    cursor.execute.assert_called_once_with("DESCRIBE FORMATTED `analytics`.`events`")
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


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
    "unsupported", ["acid", "shadowed_acid", "partition", "serde", "nested_bounded"]
)
def test_public_boundary_rejection_precedes_any_file_plan(monkeypatch, unsupported):
    from ray.data import read_api
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    rows = _metadata(properties={"transactional": "true"})
    if unsupported == "partition":
        rows = _metadata()[:3] + [("# Partition Information", None, None)]
    elif unsupported == "serde":
        rows = _metadata()[:-1]
    elif unsupported == "shadowed_acid":
        rows.extend(
            [("Storage Desc Params:", None, None), ("", "transactional", "false")]
        )
    elif unsupported == "nested_bounded":
        rows = _metadata()
        rows[2] = ("payload", "struct<code:char(4)>", None)
    connection = MagicMock()
    connection.cursor.return_value.fetchall.return_value = rows

    def unexpected_file_plan(*args, **kwargs):
        pytest.fail("Unsupported Hive metadata must fail before file planning")

    monkeypatch.setattr(hive_direct, "HiveDirectDatasource", unexpected_file_plan)
    monkeypatch.setattr(read_api, "_read_datasource_v2", unexpected_file_plan)
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
        ("char(4)", pa.string()),
        ("varchar(12)", pa.string()),
        ("binary", pa.binary()),
        ("date", pa.date32()),
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


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_timestamp_units_use_native_cast_after_file_read(tmp_path, file_format):
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )

    timestamp = pa.timestamp("ns")
    physical = pa.schema(
        [
            ("ts", timestamp),
            ("nested", pa.struct([("ts", timestamp)])),
            ("items", pa.list_(timestamp)),
            ("labels", pa.map_(pa.string(), timestamp)),
        ]
    )
    logical = pa.schema(
        [
            ("ts", pa.timestamp("us")),
            ("nested", pa.struct([("ts", pa.timestamp("us"))])),
            ("items", pa.list_(pa.timestamp("us"))),
            ("labels", pa.map_(pa.string(), pa.timestamp("us"))),
        ]
    )
    ticks = [-1001, -1000, -999, -1, 0, 1, 999, 1000, 1001, 1767323045123456789]
    micros = [-2, -1, -1, -1, 0, 0, 0, 1, 1, 1767323045123456]
    values = pa.Table.from_pylist(
        [
            {
                "ts": tick,
                "nested": {"ts": tick},
                "items": [tick, None],
                "labels": [("key", tick)],
            }
            for tick in ticks
        ]
        + [{"ts": None, "nested": None, "items": [], "labels": []}],
        schema=physical,
    )
    path = tmp_path / "000000_0"
    if file_format == "parquet":
        pq.write_table(values, path, use_deprecated_int96_timestamps=True)
    else:
        orc.write_table(values, path)
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, logical))
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    read = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(manifest)
        )
    )
    expressions = source.column_expressions()
    expected_columns = {
        "ts": micros + [None],
        "nested": [{"ts": value} for value in micros] + [None],
        "items": [[value, None] for value in micros] + [[]],
        "labels": [[("key", value)] for value in micros] + [[]],
    }
    for name, expected_values in expected_columns.items():
        result = eval_expr(expressions[name], read)
        assert result.type == logical.field(name).type
        expected = pa.array(expected_values, type=logical.field(name).type)
        assert result.combine_chunks().equals(expected)


@pytest.mark.parametrize(
    "shape", ["timestamp", "struct", "array", "map_values", "map_keys", "nested"]
)
@pytest.mark.parametrize("chunked", [False, True])
def test_timestamp_cast_preserves_sliced_nested_containers(shape, chunked):
    ticks = [-1001, -1000, -999, -1, 0, 1, 999, 1000, 1001]
    micros = [-2, -1, -1, -1, 0, 0, 0, 1, 1]
    source_type = pa.timestamp("ns")
    target_type = pa.timestamp("us")
    if shape == "timestamp":
        data, expected = [None] + ticks, [None] + micros
        prefix = suffix = 9999
    elif shape == "struct":
        source_type = pa.struct([("ts", source_type), ("id", pa.int32())])
        target_type = pa.struct([("ts", target_type), ("id", pa.int32())])
        data = [None] + [{"ts": tick, "id": i} for i, tick in enumerate(ticks)]
        expected = [None] + [{"ts": value, "id": i} for i, value in enumerate(micros)]
        prefix = suffix = {"ts": 9999, "id": 99}
    elif shape == "array":
        source_type, target_type = pa.list_(source_type), pa.list_(target_type)
        data = [None, []] + [[tick, None] for tick in ticks]
        expected = [None, []] + [[value, None] for value in micros]
        prefix = suffix = [9999]
    elif shape == "map_values":
        source_type = pa.map_(pa.string(), source_type)
        target_type = pa.map_(pa.string(), target_type)
        data = [None, []] + [[("key", tick)] for tick in ticks]
        expected = [None, []] + [[("key", value)] for value in micros]
        prefix = suffix = [("key", 9999)]
    elif shape == "map_keys":
        source_type = pa.map_(source_type, pa.string())
        target_type = pa.map_(target_type, pa.string())
        data = [None, []] + [[(tick, "value")] for tick in ticks]
        expected = [None, []] + [[(value, "value")] for value in micros]
        prefix = suffix = [(9999, "value")]
    else:
        source_type = pa.struct(
            [("groups", pa.list_(pa.map_(pa.string(), source_type)))]
        )
        target_type = pa.struct(
            [("groups", pa.list_(pa.map_(pa.string(), target_type)))]
        )
        data = [None, {"groups": None}, {"groups": []}] + [
            {"groups": [None, [], [("key", tick)]]} for tick in ticks
        ]
        expected = [None, {"groups": None}, {"groups": []}] + [
            {"groups": [None, [], [("key", value)]]} for value in micros
        ]
        prefix = suffix = {"groups": [[("key", 9999)]]}
    values = pa.array([prefix] + data + [suffix], type=source_type).slice(1, len(data))
    assert values.offset == 1
    if chunked:
        values = pa.chunked_array(
            [values.slice(0, 3), values.slice(3, 0), values.slice(3)]
        )
    result = _cast_hive_timestamps(values, target_type)
    if chunked:
        assert result.num_chunks == 3
        result = result.combine_chunks()
    assert result.equals(pa.array(expected, type=target_type))


def test_timestamp_cast_avoids_nanosecond_floor_overflow():
    values = pa.array(
        [-9223372036854775807, 9223372036854775807], type=pa.timestamp("ns")
    )
    result = _cast_hive_timestamps(values, pa.timestamp("us"))
    assert result.cast(pa.int64()).to_pylist() == [-9223372036854776, 9223372036854775]


def test_bounded_string_projection_reuses_unicode_kernels(tmp_path):
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )

    schema = pa.schema(
        [
            pa.field("code", pa.string(), metadata={b"hive_type": b"char(4)"}),
            pa.field("tag", pa.string(), metadata={b"hive_type": b"varchar(3)"}),
        ]
    )
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "parquet", schema))
    values = pa.table(
        {"code": ["ab", "中文😀abc", "", None], "tag": ["abcdef", "中文😀abc", "", None]}
    )
    expressions = source.column_expressions()
    assert eval_expr(expressions["code"], values).to_pylist() == [
        "ab  ",
        "中文😀a",
        "    ",
        None,
    ]
    assert eval_expr(expressions["tag"], values).to_pylist() == ["abc", "中文😀", "", None]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_native_reader_keeps_schema_nested_values_and_null_fill(tmp_path, file_format):
    path = tmp_path / "000000_0"  # Hive files need not have an extension.
    payload_type = pa.struct([("items", pa.list_(pa.int32()))])
    expected = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "payload": pa.array([{"items": [1, 2]}, None], type=payload_type),
        }
    )
    if file_format == "parquet":
        pq.write_table(expected, path)
    else:
        orc.write_table(expected, path)
    declared = expected.schema.append(pa.field("missing", pa.string()))
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    scanner = source.create_scanner(source.infer_schema(None))
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
    assert result.schema.names == ["id", "payload", "missing"]
    assert (
        result.column("payload").to_pylist() == expected.column("payload").to_pylist()
    )
    assert result.column("missing").to_pylist() == [None, None]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_empty_table_keeps_declared_schema(tmp_path, file_format):
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    assert source.infer_schema(None) == declared


def test_orc_merge_option_rejected_before_file_access(tmp_path):
    with pytest.raises(ValueError, match="only supported for Parquet"):
        HiveDirectDatasource(
            HiveFileTable(
                str(tmp_path / "absent"), "orc", pa.schema([("id", pa.int64())])
            ),
            merge_schema=True,
        )


def test_parquet_full_schema_mode_retains_the_hive_file_set(tmp_path):
    schema = pa.schema([("id", pa.int64())])
    for index in range(17):
        pq.write_table(pa.table({"id": [index]}), tmp_path / f"{index:06d}_0")
    table = HiveFileTable(str(tmp_path), "parquet", schema)
    sampled = HiveDirectDatasource(table)
    assert sampled._retained is None
    full = HiveDirectDatasource(table, merge_schema=True)
    assert len(full._retained) == 17
    pq.write_table(pa.table({"id": [99]}), tmp_path / "published_later")
    infos = list(full._get_file_indexer().list_file_infos(pa.array(full.paths)))
    assert len(infos) == 17
    assert not any(info.path.endswith("published_later") for info in infos)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
