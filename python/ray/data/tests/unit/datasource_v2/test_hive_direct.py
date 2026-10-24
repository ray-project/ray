"""Hive metadata contracts and composition with native file readers."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

from ray import cloudpickle
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
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    SupportsMetadata,
)


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
    cursor.fetchall.side_effect = [
        _metadata(),
        [("parquet.column.index.access is undefined",)],
    ]
    spec = HiveReadSpec(connection_factory=lambda: connection, table="analytics.events")
    result = read_file_table_metadata(spec)
    assert result.schema.field("id").type == pa.int64()
    assert cursor.execute.call_args_list == [
        call("DESCRIBE FORMATTED `analytics`.`events`"),
        call("SET parquet.column.index.access"),
    ]
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "rows,accepted",
    [
        ([("parquet.column.index.access=false",)], True),
        ([("parquet.column.index.access is undefined",)], True),
        ([("parquet.column.index.access=true",)], False),
        ([("parquet.column.index.access=unknown",)], False),
        ([("other=false",)], False),
        ([("parquet.column.index.access=false", "extra")], False),
        ([], False),
    ],
)
def test_parquet_session_mapping_is_checked_before_file_access(
    monkeypatch, rows, accepted
):
    from ray.data import read_api

    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.side_effect = [_metadata(), rows]
    spec = HiveReadSpec(
        connection_factory=lambda: connection, table="events", user="reader"
    )
    if accepted:
        assert read_file_table_metadata(spec).file_format == "parquet"
    else:
        monkeypatch.setattr(
            hive_direct,
            "HiveDirectDatasource",
            lambda *a, **kw: pytest.fail("Unexpected file planning"),
        )
        with pytest.raises(ValueError, match="Hive|positional"):
            read_api.read_hive(
                "events",
                connection_factory=spec.connection_factory,
                user="reader",
                read_mode="direct",
            )
    connection.cursor.assert_called_once_with(user="reader")
    assert cursor.execute.call_args_list == [
        call("DESCRIBE FORMATTED `default`.`events`"),
        call("SET parquet.column.index.access"),
    ]
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


@pytest.mark.parametrize("value", ["true", "false"])
def test_orc_lookup_reads_mapping_mode_on_the_metadata_session(value):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.side_effect = [
        _metadata("orc"),
        [(f"orc.force.positional.evolution={value}",)],
        [("orc.force.positional.evolution.level is undefined",)],
        [("hive.exec.schema.evolution=true",)],
        [("orc.tolerate.missing.schema is undefined",)],
        [("hive.exec.orc.tolerate.missing.schema is undefined",)],
        [("orc.schema.evolution.case.sensitive is undefined",)],
    ]
    result = read_file_table_metadata(
        HiveReadSpec(
            connection_factory=lambda: connection, table="events", user="reader"
        )
    )
    assert result.force_positional_evolution == (value == "true")
    assert result.tolerate_missing_schema
    assert result.schema_evolution_case_sensitive
    connection.cursor.assert_called_once_with(user="reader")
    assert cursor.execute.call_args_list == [
        call("DESCRIBE FORMATTED `default`.`events`"),
        call("SET orc.force.positional.evolution"),
        call("SET orc.force.positional.evolution.level"),
        call("SET hive.exec.schema.evolution"),
        call("SET orc.tolerate.missing.schema"),
        call("SET hive.exec.orc.tolerate.missing.schema"),
        call("SET orc.schema.evolution.case.sensitive"),
    ]
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "setting,value",
    [
        ("orc.force.positional.evolution.level", "2"),
        ("hive.exec.schema.evolution", "false"),
        ("orc.tolerate.missing.schema", "unknown"),
        ("orc.schema.evolution.case.sensitive", "unknown"),
    ],
)
def test_unsupported_orc_mapping_settings_fail_before_file_access(
    monkeypatch, setting, value
):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    settings = {
        "orc.force.positional.evolution": "true",
        "orc.force.positional.evolution.level": "1",
        "hive.exec.schema.evolution": "true",
        "orc.tolerate.missing.schema": "true",
        "orc.schema.evolution.case.sensitive": "true",
    }
    settings[setting] = value

    def fetch():
        statement = cursor.execute.call_args.args[0]
        if statement.startswith("DESCRIBE"):
            return _metadata("orc")
        key = statement[4:]
        return [(key + "=" + settings[key],)]

    cursor.fetchall.side_effect = fetch
    monkeypatch.setattr(
        hive_direct,
        "sample_files",
        lambda *a, **kw: pytest.fail("Unexpected file read"),
    )
    with pytest.raises(ValueError):
        read_file_table_metadata(
            HiveReadSpec(connection_factory=lambda: connection, table="events")
        )
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [("undefined",)],
        [("orc.force.positional.evolution=unknown",)],
        [("orc.force.positional.evolution=true", "extra")],
    ],
)
def test_orc_unknown_mapping_mode_fails_before_file_planning(monkeypatch, rows):
    from ray.data import read_api

    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.side_effect = [_metadata("orc"), rows]
    monkeypatch.setattr(
        hive_direct,
        "sample_files",
        lambda *a, **kw: pytest.fail("Unexpected file access"),
    )
    with pytest.raises(ValueError, match="Cannot determine Hive"):
        read_api.read_hive(
            "events", connection_factory=lambda: connection, read_mode="direct"
        )
    cursor.close.assert_called_once()
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


@pytest.mark.parametrize(
    "physical,logical,expected",
    [
        ({"\u00df": [42, None]}, "ss", [None, None]),
        ({"\u00df": [42, None]}, "\u00df", [42, None]),
        ({"ss": [43, None], "\u00df": [42, None]}, "ss", [43, None]),
        ({"ss": [43, None], "\u00df": [42, None]}, "\u00df", [42, None]),
    ],
)
def test_parquet_sampled_name_mapping_does_not_use_casefold(
    tmp_path, physical, logical, expected
):
    path = _write_table_file(tmp_path / "part-00", "parquet", pa.table(physical))
    declared = pa.schema([(logical, pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "parquet", declared))
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    assert source.column_renames == {}
    assert result.schema == declared
    assert result.column(logical).to_pylist() == expected


@pytest.mark.parametrize(
    "late,required", [(False, "all"), (True, "projection"), (True, "filter_only")]
)
def test_parquet_lowercase_ambiguity_is_rejected_for_required_fields(
    tmp_path, monkeypatch, late, required
):
    from ray.data.expressions import col

    paths = [
        _write_table_file(
            tmp_path / f"part-{i:02d}",
            "parquet",
            pa.table({"id": [i], "ss": [i + 100]}),
        )
        for i in range(16 if late else 0)
    ]
    ambiguous = _write_table_file(
        tmp_path / "part-99", "parquet", pa.table({"id": [16], "ss": [42], "SS": [43]})
    )
    sampled = _track_sampled_paths(monkeypatch)
    table = HiveFileTable(
        str(tmp_path), "parquet", pa.schema([("id", pa.int64()), ("ss", pa.int64())])
    )
    if not late:
        with pytest.raises(ValueError, match="ambiguous"):
            HiveDirectDatasource(table)
        return
    source = HiveDirectDatasource(table)
    assert len(sampled) == 16 and str(ambiguous) not in sampled
    scanner = source.create_scanner(source.infer_schema(None))
    if required == "projection":
        scanner = scanner.prune_columns(["ss"])
    else:
        scanner = scanner.prune_columns(["id"])
        scanner, residual = scanner.push_filters(col("ss") > 0)
        assert residual is None
    with pytest.raises(ValueError, match="ambiguous"):
        list(scanner.create_reader().read(_manifest_for_paths([*paths, ambiguous])))
    # An ambiguous field unused by output and filters does not affect id.
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .prune_columns(["id"])
            .create_reader()
            .read(_manifest_for_paths([ambiguous]))
        )
    )
    assert result.to_pylist() == [{"id": 16}]


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
    data: List[Any]
    expected: List[Any]
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
        {
            "code": ["ab", "中文😀abc", "", None],
            "tag": ["abcdef", "中文😀abc", "", None],
        }
    )
    expressions = source.column_expressions()
    assert eval_expr(expressions["code"], values).to_pylist() == [
        "ab  ",
        "中文😀a",
        "    ",
        None,
    ]
    assert eval_expr(expressions["tag"], values).to_pylist() == [
        "abc",
        "中文😀",
        "",
        None,
    ]


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


def test_native_parquet_preserves_metadata_column_name_whitespace(tmp_path):
    expected = pa.table(
        {"id": [1, 2], " id": [11, None], "id ": [12, 22], " id ": [13, None]}
    )
    path = tmp_path / "000000_0"
    pq.write_table(expected, path)
    rows = _metadata()
    rows[1:3] = [(name, "bigint", None) for name in expected.column_names]
    rows = [
        (key, str(tmp_path) if key == "Location:" else value, comment)
        for key, value, comment in rows
    ]
    source = HiveDirectDatasource(_parse_file_table(rows))
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    assert result.schema.equals(expected.schema, check_metadata=False)
    assert result.to_pylist() == expected.to_pylist()


def _write_column_file(path, file_format, name, value):
    table = pa.table({name: pa.array([value], type=pa.int64())})
    return _write_table_file(path, file_format, table)


def _write_table_file(path, file_format, table):
    if file_format == "parquet":
        pq.write_table(table, path)
    else:
        orc.write_table(table, path)
    return path


def _manifest_for_paths(paths):
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def _track_sampled_paths(monkeypatch):
    original_sample_files = hive_direct.sample_files
    observed_sample_paths = []

    def track_sample(indexer, paths, filesystem, pruners=None, max_files=16):
        sample = original_sample_files(
            indexer,
            paths,
            filesystem,
            pruners=pruners,
            max_files=max_files,
        )
        observed_sample_paths.extend(sample.paths.tolist())
        return sample

    monkeypatch.setattr(hive_direct, "sample_files", track_sample)
    return observed_sample_paths


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_native_safe_widening_preserves_nested_values(tmp_path, file_format):
    old_value = pa.struct(
        [
            ("n", pa.int32()),
            ("f", pa.float32()),
            ("d", pa.decimal128(9, 2)),
        ]
    )
    new_value = pa.struct(
        [
            ("n", pa.int64()),
            ("f", pa.float64()),
            ("d", pa.decimal128(18, 2)),
        ]
    )
    physical = pa.schema(
        [
            ("v", old_value),
            ("items", pa.list_(old_value)),
            ("labels", pa.map_(pa.string(), old_value)),
            ("keys", pa.map_(pa.int32(), pa.string())),
        ]
    )
    declared = pa.schema(
        [
            ("v", new_value),
            ("items", pa.list_(new_value)),
            ("labels", pa.map_(pa.string(), new_value)),
            ("keys", pa.map_(pa.int64(), pa.string())),
        ]
    )
    value = {"n": -2147483648, "f": 0.1, "d": Decimal("-123.45")}
    data = pa.Table.from_pylist(
        [
            {
                "v": value,
                "items": [value, None],
                "labels": [("k", value)],
                "keys": [(-2147483648, "value")],
            },
            {"v": None, "items": [], "labels": [], "keys": []},
            {"v": None, "items": None, "labels": None, "keys": None},
        ],
        schema=physical,
    )
    old = _write_table_file(tmp_path / "part-00", file_format, data)
    new = _write_table_file(tmp_path / "part-01", file_format, data.cast(declared))
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([old, new]))
        )
    )
    assert result.equals(pa.concat_tables([data.cast(declared)] * 2))
    assert source.column_expressions() == {}


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("late_wider", [False, True])
def test_unsampled_file_type_uses_the_same_compatibility_rules(
    tmp_path, file_format, late_wider, monkeypatch
):
    declared = pa.schema([("id", pa.int64() if late_wider else pa.int32())])
    paths = [
        _write_table_file(
            tmp_path / f"part-{i:02d}",
            file_format,
            pa.table({"id": pa.array([i], type=pa.int32())}),
        )
        for i in range(16)
    ]
    late = _write_table_file(
        tmp_path / "part-99",
        file_format,
        pa.table({"id": pa.array([42], type=pa.int64())}),
    )
    sampled = _track_sampled_paths(monkeypatch)
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    assert len(sampled) == 16 and str(late) not in sampled
    reader = source.create_scanner(source.infer_schema(None)).create_reader()
    if late_wider:
        result = pa.concat_tables(
            list(reader.read(_manifest_for_paths([*paths, late])))
        )
        assert result.schema == declared
        assert result.column("id").to_pylist() == list(range(16)) + [42]
    else:
        # The value fits int32, but narrowing is not an allowed Hive conversion.
        with pytest.raises(ValueError, match="type mismatch"):
            list(reader.read(_manifest_for_paths([*paths, late])))


@pytest.mark.parametrize("physical", ["renamed", "reordered", "mixed"])
def test_sampled_orc_positional_mapping_uses_original_positions(tmp_path, physical):
    values = {
        "renamed": {"old_id": [42], "value": [43]},
        "reordered": {"value": [43], "id": [42]},
        "mixed": {"_col0": [42], "id": [43]},
    }[physical]
    path = _write_table_file(tmp_path / "part-00", "orc", pa.table(values))
    declared = pa.schema([("id", pa.int64()), ("value", pa.int64())])
    source = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), "orc", declared, force_positional_evolution=True)
    )
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    assert result.to_pylist() == [
        dict(zip(declared.names, [v[0] for v in values.values()]))
    ]


@pytest.mark.parametrize("required", ["all", "projection", "filter_only"])
def test_unsampled_orc_rename_checks_full_hive_positions(
    tmp_path, monkeypatch, required
):
    from ray.data.expressions import col

    paths = [
        _write_table_file(
            tmp_path / f"part-{i:02d}", "orc", pa.table({"id": [i], "value": [i + 100]})
        )
        for i in range(16)
    ]
    late = _write_table_file(
        tmp_path / "part-99", "orc", pa.table({"old_id": [42], "value": [142]})
    )
    sampled = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64()), ("value", pa.int64())])
    source = HiveDirectDatasource(
        HiveFileTable(
            str(tmp_path),
            "orc",
            declared,
            force_positional_evolution=True,
        )
    )
    assert len(sampled) == 16 and str(late) not in sampled
    scanner = source.create_scanner(source.infer_schema(None))
    if required == "projection":
        scanner = scanner.prune_columns(["id"])
    elif required == "filter_only":
        scanner = scanner.prune_columns(["value"])
        scanner, residual = scanner.push_filters(col("id") > 0)
        assert residual is None
    reader = cloudpickle.loads(cloudpickle.dumps(scanner.create_reader()))
    result = pa.concat_tables(list(reader.read(_manifest_for_paths([*paths, late]))))
    expected = [{"id": i, "value": i + 100} for i in range(16)] + [
        {"id": 42, "value": 142}
    ]
    if required == "projection":
        expected = [{"id": row["id"]} for row in expected]
    elif required == "filter_only":
        expected = [{"value": row["value"]} for row in expected if row["id"] > 0]
    assert result.to_pylist() == expected
    # A rename in an unread, unfiltered column does not affect the valid field.
    if required != "filter_only":
        safe_reader = source.create_scanner(source.infer_schema(None)).prune_columns(
            ["value"]
        )
        result = pa.concat_tables(
            list(safe_reader.create_reader().read(_manifest_for_paths([late])))
        )
        assert result.column("value").to_pylist() == [142]


def test_orc_positional_mode_preserves_trailing_missing_and_extra_columns(tmp_path):
    paths = [
        _write_table_file(tmp_path / "part-00", "orc", pa.table({"id": [42]})),
        _write_table_file(
            tmp_path / "part-01",
            "orc",
            pa.table({"id": [43], "added": [44], "extra": [45]}),
        ),
    ]
    declared = pa.schema([("id", pa.int64()), ("added", pa.int64())])
    source = HiveDirectDatasource(
        HiveFileTable(
            str(tmp_path),
            "orc",
            declared,
            force_positional_evolution=True,
        )
    )
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths(paths))
        )
    )
    assert result.to_pylist() == [{"id": 42, "added": None}, {"id": 43, "added": 44}]


@pytest.mark.parametrize(
    "logical_name,tolerate", [("id", False), ("id", True), ("_col0", True)]
)
def test_legacy_orc_honors_missing_schema_tolerance(tmp_path, logical_name, tolerate):
    path = _write_column_file(tmp_path / "part-00", "orc", "_col0", 42)
    declared = pa.schema([(logical_name, pa.int64())])
    table = HiveFileTable(
        str(tmp_path), "orc", declared, tolerate_missing_schema=tolerate
    )
    if logical_name != "_col0" and not tolerate:
        with pytest.raises(ValueError, match="tolerance is disabled"):
            HiveDirectDatasource(table)
    else:
        source = HiveDirectDatasource(table)
        result = pa.concat_tables(
            list(
                source.create_scanner(source.infer_schema(None))
                .create_reader()
                .read(_manifest_for_paths([path]))
            )
        )
        assert result.to_pylist() == [{logical_name: 42}]


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_orc_name_mode_keeps_casefold_distinct_names(tmp_path, case_sensitive):
    data = pa.table({"ss": [43, None], "\u00df": [42, None]})
    path = _write_table_file(tmp_path / "part-00", "orc", data)
    source = HiveDirectDatasource(
        HiveFileTable(
            str(tmp_path),
            "orc",
            data.schema,
            schema_evolution_case_sensitive=case_sensitive,
        )
    )
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    assert result.equals(data)


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("required", ["all", "projection", "filter_only"])
def test_unsampled_unicode_name_is_not_a_casefold_alias(
    tmp_path, monkeypatch, required, file_format
):
    from ray.data.expressions import col

    paths = [
        _write_table_file(
            tmp_path / f"part-{i:02d}",
            file_format,
            pa.table({"id": [i], "ss": [i + 100]}),
        )
        for i in range(16)
    ]
    late = _write_table_file(
        tmp_path / "part-99", file_format, pa.table({"id": [16], "\u00df": [42]})
    )
    sampled = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64()), ("ss", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    assert len(sampled) == 16 and str(late) not in sampled
    scanner = source.create_scanner(source.infer_schema(None))
    expected = [{"id": i, "ss": i + 100} for i in range(16)] + [{"id": 16, "ss": None}]
    if required == "projection":
        scanner = scanner.prune_columns(["ss"])
        expected = [{"ss": row["ss"]} for row in expected]
    elif required == "filter_only":
        scanner = scanner.prune_columns(["id"])
        scanner, residual = scanner.push_filters(col("ss") > 0)
        assert residual is None
        expected = [{"id": i} for i in range(16)]
    if file_format == "parquet":
        # ReadFiles serializes the scanner and creates its reader on the worker.
        reader = cloudpickle.loads(cloudpickle.dumps(scanner)).create_reader()
    else:
        reader = cloudpickle.loads(cloudpickle.dumps(scanner.create_reader()))
    result = pa.concat_tables(list(reader.read(_manifest_for_paths([*paths, late]))))
    assert result.to_pylist() == expected


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


@pytest.mark.parametrize(
    "late,required",
    [(False, "all"), (True, "all"), (True, "projection"), (True, "filter_only")],
)
def test_legacy_orc_timestamp_units_reuse_existing_nested_project(
    tmp_path, monkeypatch, late, required
):
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )
    from ray.data.expressions import col

    def schema(unit):
        ts = pa.timestamp(unit)
        return pa.schema(
            [
                ("_col0", ts),
                ("_col1", pa.struct([("ts", ts), ("n", pa.int32())])),
                ("_col2", pa.list_(ts)),
                ("_col3", pa.map_(pa.string(), ts)),
                ("_col4", pa.map_(ts, pa.string())),
            ]
        )

    physical, declared = schema("ns"), schema("us")
    paths, expected = [], []
    for i in range(17 if late else 1):
        tick = 86400123456789 + i * 1000
        data = pa.Table.from_pylist(
            [
                {
                    "_col0": tick,
                    "_col1": {"ts": tick, "n": i},
                    "_col2": [tick, None],
                    "_col3": [("k", tick)],
                    "_col4": [(tick, "v")],
                },
                dict.fromkeys(physical.names),
            ],
            schema=physical,
        )
        expected.append(data.cast(declared, safe=False))
        # Only the last file has an all-_colN layout. The earlier files must
        # not exercise the legacy equality check during schema sampling.
        if late and i < 16:
            data = data.append_column("marker", pa.array([i, None], type=pa.int32()))
        paths.append(_write_table_file(tmp_path / f"part-{i:02d}", "orc", data))
    sampled = _track_sampled_paths(monkeypatch)
    source = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), "orc", declared, tolerate_missing_schema=False)
    )
    if late:
        assert len(sampled) == 16 and str(paths[-1]) not in sampled
    scanner = source.create_scanner(source.infer_schema(None))
    expected = pa.concat_tables(expected)
    if required == "projection":
        scanner = scanner.prune_columns(["_col1"])
        expected = expected.select(["_col1"])
    elif required == "filter_only":
        scanner = scanner.prune_columns(["_col1"])
        scanner, residual = scanner.push_filters(col("_col0") > datetime(1970, 1, 2))
        assert residual is None
        expected = expected.filter(expected.column("_col0").is_valid()).select(
            ["_col1"]
        )
    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest_for_paths(paths)))
    )
    expressions = source.column_expressions()
    result = pa.table(
        {
            name: eval_expr(expressions[name], result)
            if name in expressions
            else result[name]
            for name in result.column_names
        }
    )
    assert result.equals(expected)


def test_orc_positional_mixed_names_missing_projection_and_limit(tmp_path):
    paths = [
        _write_table_file(
            tmp_path / "part-00",
            "orc",
            pa.table({"old_id": [1, 2, 3, 4], "old_value": [10, 20, 30, 40]}),
        ),
        _write_table_file(
            tmp_path / "part-01", "orc", pa.table({"id": [5, 6], "value": [50, 60]})
        ),
    ]
    declared = pa.schema(
        [("id", pa.int64()), ("value", pa.int64()), ("missing", pa.int64())]
    )
    source = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), "orc", declared, force_positional_evolution=True)
    )
    from ray.data.expressions import col

    scanner = source.create_scanner(source.infer_schema(None)).prune_columns(
        ["missing"]
    )
    scanner, residual = scanner.push_filters(col("value") >= 30)
    assert residual is None
    scanner = scanner.push_limit(3)
    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest_for_paths(paths)))
    )
    missing_row: Dict[str, Optional[int]] = {"missing": None}
    assert result.to_pylist() == [missing_row] * 3
    # Empty projection must preserve rows across multiple batches and files.
    scanner = source.create_scanner(source.infer_schema(None)).prune_columns([])
    from dataclasses import replace

    reader = replace(scanner, batch_size=2).create_reader()
    assert sum(table.num_rows for table in reader.read(_manifest_for_paths(paths))) == 6


def test_orc_positional_renamed_complex_column_reuses_timestamp_project(tmp_path):
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )

    actual = pa.struct(
        [
            ("n", pa.int32()),
            ("ts", pa.timestamp("ns")),
            ("items", pa.list_(pa.int32())),
            ("labels", pa.map_(pa.string(), pa.int32())),
        ]
    )
    target = pa.struct(
        [
            ("n", pa.int64()),
            ("ts", pa.timestamp("us")),
            ("items", pa.list_(pa.int64())),
            ("labels", pa.map_(pa.string(), pa.int64())),
        ]
    )
    data = pa.table(
        {
            "old": pa.array(
                [
                    {"n": 42, "ts": -1, "items": [7, None], "labels": [("k", 8)]},
                    None,
                    {"n": None, "ts": 1001, "items": [], "labels": []},
                ],
                type=actual,
            )
        }
    )
    path = _write_table_file(tmp_path / "part-00", "orc", data)
    source = HiveDirectDatasource(
        HiveFileTable(
            str(tmp_path),
            "orc",
            pa.schema([("new", target)]),
            force_positional_evolution=True,
        )
    )
    read = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    result = eval_expr(source.column_expressions()["new"], read).combine_chunks()
    assert result.type == target
    assert result.field("n").to_pylist() == [42, None, None]
    assert result.field("ts").cast(pa.int64()).to_pylist() == [-1, None, 1]
    assert result.field("items").to_pylist() == [[7, None], None, []]
    assert result.field("labels").to_pylist() == [[("k", 8)], None, []]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_actual_file_uses_sampled_case_mapping(tmp_path, file_format):
    path = _write_column_file(tmp_path / "part-000", file_format, "ID", 42)
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    scanner = source.create_scanner(source.infer_schema(None))
    reader = scanner.create_reader()
    if file_format == "parquet":
        assert isinstance(reader, SupportsMetadata)
        restored_reader = cloudpickle.loads(cloudpickle.dumps(reader))
        assert isinstance(restored_reader, SupportsMetadata)
        assert [
            metadata.num_rows
            for metadata in restored_reader.read_metadata(_manifest_for_paths([path]))
        ] == [1]
    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest_for_paths([path])))
    )

    assert source.column_renames == ({"ID": "id"} if file_format == "parquet" else {})
    assert result.column("ID" if file_format == "parquet" else "id").to_pylist() == [42]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_unsampled_file_case_change_fails_before_data_read(
    tmp_path, file_format, monkeypatch
):
    sampled_paths = [
        _write_column_file(tmp_path / f"part-{index:02d}", file_format, "id", index)
        for index in range(16)
    ]
    late_path = _write_column_file(tmp_path / "part-99", file_format, "ID", 99)
    observed_sample_paths = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    assert len(observed_sample_paths) == 16
    assert str(late_path) not in observed_sample_paths

    scanner = source.create_scanner(source.infer_schema(None))
    manifest = _manifest_for_paths([*sampled_paths, late_path])
    if file_format == "parquet":
        with pytest.raises(ValueError, match="per-file column name changes"):
            list(scanner.create_reader().read(manifest))
    else:
        result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
        assert result.column("id").to_pylist() == list(range(16)) + [99]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_unprojected_unsampled_case_change_does_not_block_read(
    tmp_path, file_format, monkeypatch
):
    def write(path, id_name, value):
        return _write_table_file(
            path,
            file_format,
            pa.table({id_name: [value], "name": [f"name-{value}"]}),
        )

    sampled_paths = [
        write(tmp_path / f"part-{index:02d}", "id", index) for index in range(16)
    ]
    late_path = write(tmp_path / "part-99", "ID", 99)
    observed_sample_paths = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64()), ("name", pa.string())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, declared))
    assert len(observed_sample_paths) == 16
    assert str(late_path) not in observed_sample_paths
    scanner = source.create_scanner(source.infer_schema(None)).prune_columns(["name"])

    result = pa.concat_tables(
        list(
            scanner.create_reader().read(
                _manifest_for_paths([*sampled_paths, late_path])
            )
        )
    )

    assert result.column("name").to_pylist() == [
        f"name-{index}" for index in range(16)
    ] + ["name-99"]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_file_schema_metadata_read_uses_configured_io_retry(
    tmp_path, file_format, monkeypatch
):
    import ray.data._internal.util as ray_util

    path = _write_column_file(tmp_path / "part-000", file_format, "id", 42)
    original_read = hive_direct._read_physical_schema
    calls = 0

    def fail_once(path, file_format, filesystem):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("temporary schema I/O error")
        return original_read(path, file_format, filesystem)

    def no_backoff(factory, description, **kwargs):
        return ray_util.iterate_with_retry(
            factory, description, max_backoff_s=0, **kwargs
        )

    monkeypatch.setattr(hive_direct, "_read_physical_schema", fail_once)
    monkeypatch.setattr(hive_direct, "iterate_with_retry", no_backoff)
    monkeypatch.setattr(ray_util.time, "sleep", lambda _: None)

    schema = hive_direct._read_physical_schema_with_retry(
        str(path), file_format, None, ["temporary schema I/O error"]
    )

    assert schema.names == ["id"]
    assert calls == 2


@pytest.mark.parametrize("use_filesystem", [False, True])
@pytest.mark.parametrize("fail_schema", [False, True])
def test_parquet_schema_read_closes_file(
    tmp_path, monkeypatch, use_filesystem, fail_schema
):
    from pyarrow.fs import LocalFileSystem

    path = tmp_path / "data.parquet"
    expected = pa.table({"id": [42]})
    pq.write_table(expected, path)
    schema_getter = pq.ParquetFile.schema_arrow.fget
    opened = []

    def read_schema(parquet_file):
        # Retain the real reader so GC cannot hide a missing explicit close.
        opened.append(parquet_file)
        assert not parquet_file.closed
        if fail_schema:
            raise OSError("schema lookup failed")
        assert schema_getter is not None
        return schema_getter(parquet_file)

    monkeypatch.setattr(pq.ParquetFile, "schema_arrow", property(read_schema))
    filesystem = LocalFileSystem() if use_filesystem else None
    if fail_schema:
        with pytest.raises(OSError, match="schema lookup failed"):
            hive_direct._read_physical_schema(str(path), "parquet", filesystem)
    else:
        schema = hive_direct._read_physical_schema(str(path), "parquet", filesystem)
        assert schema == expected.schema
    assert len(opened) == 1
    assert opened[0].closed


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


def test_orc_coln_layout_uses_position_even_when_names_conflict(tmp_path):
    path = tmp_path / "part-000"
    physical = pa.Table.from_arrays(
        [pa.array([42], type=pa.int64()), pa.array([7], type=pa.int64())],
        names=["_col0", "_col1"],
    )
    orc.write_table(physical, path)
    declared = pa.schema([("id", pa.int64()), ("_col0", pa.int64())])

    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "orc", declared))
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths([path]))
        )
    )
    assert result.to_pylist() == [{"id": 42, "_col0": 7}]


@pytest.mark.parametrize("push_filter", [False, True])
def test_unsampled_legacy_orc_keeps_full_positions_under_projection(
    tmp_path, monkeypatch, push_filter
):
    sampled_paths = [
        _write_table_file(
            tmp_path / f"part-{index:02d}",
            "orc",
            pa.table({"id": [index], "_col0": [index + 100]}),
        )
        for index in range(16)
    ]
    late_path = _write_table_file(
        tmp_path / "part-99", "orc", pa.table({"_col0": [42]})
    )
    observed_sample_paths = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64()), ("_col0", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "orc", declared))
    assert len(observed_sample_paths) == 16
    assert str(late_path) not in observed_sample_paths

    scanner = source.create_scanner(source.infer_schema(None)).prune_columns(["_col0"])
    if push_filter:
        from ray.data.expressions import col

        scanner, residual = scanner.push_filters(col("_col0") > 0)
        assert residual is None

    manifest = _manifest_for_paths([*sampled_paths, late_path])
    result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
    assert result.column("_col0").to_pylist() == list(range(100, 116)) + (
        [] if push_filter else [None]
    )


def test_orc_mixed_coln_and_named_columns_use_name_mapping(tmp_path):
    path = tmp_path / "part-000"
    physical = pa.Table.from_arrays(
        [pa.array([42], type=pa.int64()), pa.array([43], type=pa.int64())],
        names=["_col0", "id"],
    )
    orc.write_table(physical, path)
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "orc", declared))
    scanner = source.create_scanner(source.infer_schema(None))

    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest_for_paths([path])))
    )

    assert result.column("id").to_pylist() == [43]


@pytest.mark.parametrize(
    "physical,expected",
    [({"_color": [42], "_column": [43]}, [43]), ({"_color": [42]}, [None])],
)
def test_orc_col_prefix_names_use_name_mapping(tmp_path, physical, expected):
    path = _write_table_file(
        tmp_path / "part-000",
        "orc",
        pa.table(physical),
    )
    declared = pa.schema([("_column", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "orc", declared))
    scanner = source.create_scanner(source.infer_schema(None))

    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest_for_paths([path])))
    )

    assert result.column("_column").to_pylist() == expected


def test_unsampled_legacy_orc_is_read_by_position(tmp_path, monkeypatch):
    sampled_paths = [
        _write_column_file(tmp_path / f"part-{index:02d}", "orc", "id", index)
        for index in range(16)
    ]
    late_path = _write_column_file(tmp_path / "part-99", "orc", "_col0", 99)
    observed_sample_paths = _track_sampled_paths(monkeypatch)
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), "orc", declared))
    assert len(observed_sample_paths) == 16
    assert str(late_path) not in observed_sample_paths

    scanner = source.create_scanner(source.infer_schema(None))
    manifest = _manifest_for_paths([*sampled_paths, late_path])
    result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
    assert result.column("id").to_pylist() == list(range(16)) + [99]


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_scanner_options_preserve_native_behavior(tmp_path, monkeypatch, file_format):
    schema = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(HiveFileTable(str(tmp_path), file_format, schema))
    native_factory = MagicMock(wraps=source._source.create_scanner)
    monkeypatch.setattr(source._source, "create_scanner", native_factory)
    ignore_prefixes = ["_custom"]

    scanner = source.create_scanner(
        schema, partitioning=None, ignore_prefixes=ignore_prefixes
    )

    native_factory.assert_called_once_with(
        schema,
        filesystem=source._source.filesystem,
        partitioning=None,
        ignore_prefixes=ignore_prefixes,
    )
    assert scanner.schema == schema
    assert scanner.partitioning is None
    reader = scanner.create_reader()
    assert isinstance(
        reader,
        (
            hive_direct._HiveParquetSchemaValidatingReader,
            hive_direct._HiveOrcFileReader,
        ),
    )
    assert reader._partition_parser is None
    # Parquet consumes this option; ORC retains its native factory's default.
    expected_prefixes = ignore_prefixes if file_format == "parquet" else None
    assert scanner.ignore_prefixes == expected_prefixes
    assert reader._ignore_prefixes == expected_prefixes


@pytest.mark.parametrize(
    "file_format,merge_schema", [("parquet", False), ("orc", False), ("orc", True)]
)
def test_empty_table_keeps_declared_schema(tmp_path, file_format, merge_schema):
    declared = pa.schema([("id", pa.int64())])
    source = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), file_format, declared), merge_schema=merge_schema
    )
    assert source.infer_schema(None) == declared
    if merge_schema:
        assert (
            list(
                source._get_file_indexer().list_file_infos(
                    pa.array(source.paths), filesystem=None
                )
            )
            == []
        )


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
    sampler = MagicMock(return_value=sample)
    warning = MagicMock()
    inferred = []

    def infer_schema(_source, candidate_files, *_):
        assert warning.call_count == int(should_warn)
        inferred.append(candidate_files)
        return declared

    monkeypatch.setattr(hive_direct, "sample_files", sampler)
    monkeypatch.setattr(hive_direct.logger, "warning", warning)
    monkeypatch.setattr(ParquetDatasourceV2, "infer_schema", infer_schema)
    monkeypatch.setattr(
        hive_direct,
        "_infer_orc_read_schema",
        lambda *args: (infer_schema(*args), {"id": "id"}),
    )
    source = HiveDirectDatasource(
        HiveFileTable(str(tmp_path), file_format, declared), merge_schema=merge_schema
    )
    assert source.infer_schema(None) == declared
    assert len(inferred) == 1 and inferred[0] is sample
    assert sampler.call_count == 1
    assert sampler.call_args.kwargs["max_files"] == (None if merge_schema else 16)
    assert source._retained is (sample if merge_schema else None)
    assert warning.call_count == int(should_warn)
    if should_warn:
        message, count, threshold = warning.call_args.args
        assert (count, threshold) == (file_count, 10_000)
        assert "before returning the Dataset" in message


@pytest.mark.parametrize("mapping", ["name", "position", "legacy"])
def test_orc_full_schema_normalizes_before_shared_merge(tmp_path, monkeypatch, mapping):
    declared = pa.schema(
        [
            ("id", pa.int64()),
            ("payload", pa.struct([("values", pa.list_(pa.int64()))])),
            ("late", pa.string()),
        ]
    )
    paths = []
    for index in range(17):
        if mapping == "name":
            names = ["id", "payload", "late"]
        elif mapping == "legacy":
            names = [f"_col{i}" for i in range(3)]
        else:
            prefix = "new" if index == 16 else "old"
            names = [f"{prefix}_{i}" for i in range(3)]
        values = {
            names[0]: pa.array([index], type=pa.int32()),
            names[1]: pa.array(
                [{"values": [index, None]}],
                type=pa.struct([("values", pa.list_(pa.int32()))]),
            ),
        }
        if index == 16:
            values[names[2]] = pa.array(["present"])
            if mapping == "name":
                values = {
                    "late": values["late"],
                    "payload": values["payload"],
                    "ID": values["id"],
                }
        paths.append(
            _write_table_file(tmp_path / f"part-{index:02d}", "orc", pa.table(values))
        )

    calls = []
    original_infer = hive_direct.infer_schema_from_files

    def infer(paths, read_schema, **kwargs):
        calls.append(len(paths))

        def read_normalized(path):
            schema = read_schema(path)
            assert schema == declared
            return schema

        return original_infer(paths, read_normalized, **kwargs)

    monkeypatch.setattr(hive_direct, "infer_schema_from_files", infer)
    table = HiveFileTable(
        str(tmp_path), "orc", declared, force_positional_evolution=mapping == "position"
    )
    HiveDirectDatasource(table)
    source = HiveDirectDatasource(table, merge_schema=True)
    assert calls == [16, 17]
    assert source.infer_schema(None) == declared
    result = pa.concat_tables(
        list(
            source.create_scanner(source.infer_schema(None))
            .create_reader()
            .read(_manifest_for_paths(paths))
        )
    )
    assert result.schema == declared
    assert result.to_pylist() == [
        {
            "id": i,
            "payload": {"values": [i, None]},
            "late": "present" if i == 16 else None,
        }
        for i in range(17)
    ]


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


@pytest.mark.parametrize("file_format", ["parquet", "orc"])
def test_full_schema_mode_retains_the_hive_file_set(tmp_path, file_format):
    schema = pa.schema([("id", pa.int64())])
    for index in range(17):
        _write_table_file(
            tmp_path / f"{index:06d}_0", file_format, pa.table({"id": [index]})
        )
    table = HiveFileTable(str(tmp_path), file_format, schema)
    sampled = HiveDirectDatasource(table)
    assert sampled._retained is None
    full = HiveDirectDatasource(table, merge_schema=True)
    assert full._retained is not None
    assert len(full._retained) == 17
    _write_table_file(tmp_path / "published_later", file_format, pa.table({"id": [99]}))
    infos = list(
        full._get_file_indexer().list_file_infos(pa.array(full.paths), filesystem=None)
    )
    assert len(infos) == 17
    assert not any(info.path.endswith("published_later") for info in infos)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
