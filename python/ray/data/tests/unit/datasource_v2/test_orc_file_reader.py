"""Unit tests for DSV2 ORC scanning and file reading."""

from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import orc

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _write_orc(path: Path, table: pa.Table, **kwargs) -> None:
    with pa.OSFile(str(path), "wb") as sink:
        orc.write_table(table, sink, **kwargs)


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def test_orc_reader_yields_batches(tmp_path):
    path = tmp_path / "data.orc"
    expected = pa.table({"id": [0, 1, 2, 3, 4], "name": ["a", "b", "c", "d", "e"]})
    _write_orc(path, expected)

    scanner = OrcScanner(schema=expected.schema, batch_size=2)
    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema() == expected.schema
    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_reads_multiple_stripes(tmp_path):
    path = tmp_path / "multi.orc"
    expected = pa.table({"id": list(range(100_000))})
    _write_orc(path, expected, stripe_size=64 * 1024)
    assert orc.ORCFile(str(path)).nstripes > 1

    batches = list(
        OrcScanner(schema=expected.schema, batch_size=4096)
        .create_reader()
        .read(_manifest(path))
    )

    assert len(batches) > 1
    assert all(batch.num_rows <= 4096 for batch in batches)
    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_aligns_explicit_schema_across_files(tmp_path):
    left_path = tmp_path / "left.orc"
    right_path = tmp_path / "right.orc"
    _write_orc(left_path, pa.table({"id": [1], "left": ["a"]}))
    _write_orc(right_path, pa.table({"id": [2], "right": [True]}))
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("left", pa.string()),
            ("right", pa.bool_()),
        ]
    )

    batches = list(
        OrcScanner(schema=schema).create_reader().read(_manifest(left_path, right_path))
    )
    result = pa.concat_tables(batches)

    assert result.schema == schema
    assert result.to_pylist() == [
        {"id": 1, "left": "a", "right": None},
        {"id": 2, "left": None, "right": True},
    ]


def test_orc_reader_projection_filter_and_limit(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [0, 1, 2, 3], "label": ["a", "b", "c", "d"]})
    _write_orc(path, table)

    scanner, residual = OrcScanner(schema=table.schema, batch_size=2).push_filters(
        col("id") >= 1
    )
    scanner = scanner.prune_columns(["label"]).push_limit(2)
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert residual is None
    assert scanner.read_schema().names == ["label"]
    assert result.to_pylist() == [{"label": "b"}, {"label": "c"}]


def test_orc_reader_empty_projection_preserves_rows(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [0, 1, 2]})
    _write_orc(path, table)

    scanner = OrcScanner(schema=table.schema, batch_size=2).prune_columns([])
    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema().names == []
    assert sum(batch.num_rows for batch in batches) == 3
    assert all(
        batch.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
        for batch in batches
    )


@pytest.mark.parametrize(
    "columns", [None, ["year", "id"]], ids=["no_projection", "projection"]
)
def test_orc_reader_synthesizes_partition_and_path(tmp_path, columns):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    _write_orc(path, pa.table({"id": [1, 2], "year": ["file", "file"]}))
    # The datasource passes the final schema, including synthesized columns.
    schema = pa.schema(
        [("id", pa.int64()), ("path", pa.string()), ("year", pa.string())]
    )

    scanner = OrcScanner(
        schema=schema,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        synthesized_columns=(PathColumn(),),
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert result.schema.names == scanner.read_schema().names
    assert result.column("year").to_pylist() == ["2024", "2024"]
    if columns is None:
        assert result.to_pylist() == [
            {"id": 1, "year": "2024", "path": str(path)},
            {"id": 2, "year": "2024", "path": str(path)},
        ]
    else:
        assert result.to_pylist() == [
            {"year": "2024", "id": 1},
            {"year": "2024", "id": 2},
        ]


def test_orc_reader_reports_corrupt_file_path(tmp_path):
    path = tmp_path / "broken.orc"
    path.write_bytes(b"not an ORC file")

    with pytest.raises((pa.ArrowInvalid, OSError)) as exc:
        list(
            OrcScanner(schema=pa.schema([("id", pa.int64())]))
            .create_reader()
            .read(_manifest(path))
        )

    assert str(path) in str(exc.value)


def test_orc_reader_rejects_pickle_object_columns(monkeypatch):
    storage = pa.array([b"payload"], type=pa.large_binary())
    extension = pa.ExtensionArray.from_storage(ArrowPythonObjectType(), storage)
    table = pa.table({"object": extension})
    reader = OrcFileReader()

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        reader._on_batch_read(table)

    monkeypatch.setenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", "1")
    reader._on_batch_read(table)


def test_orc_reader_empty_file_yields_no_batches(tmp_path):
    path = tmp_path / "empty.orc"
    table = pa.table({"id": pa.array([], type=pa.int64())})
    _write_orc(path, table)

    assert (
        list(OrcScanner(schema=table.schema).create_reader().read(_manifest(path)))
        == []
    )


def test_orc_reader_nested_struct(tmp_path):
    path = tmp_path / "nested.orc"
    payload_type = pa.struct([("values", pa.list_(pa.int32()))])
    expected = pa.table(
        {
            "id": [1, 2],
            "payload": pa.array(
                [{"values": [1, 2]}, {"values": [3]}], type=payload_type
            ),
        }
    )
    _write_orc(path, expected)

    batches = list(
        OrcScanner(schema=expected.schema).create_reader().read(_manifest(path))
    )

    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_projects_synthesized_path(tmp_path):
    path = tmp_path / "data.orc"
    _write_orc(path, pa.table({"id": [1, 2]}))
    schema = pa.schema([("id", pa.int64())])
    scanner = OrcScanner(
        schema=schema, synthesized_columns=(PathColumn(),)
    ).prune_columns(["path"])

    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema().names == ["path"]
    assert pa.concat_tables(batches).to_pylist() == [
        {"path": str(path)},
        {"path": str(path)},
    ]


def test_orc_reader_adds_synthesized_path_missing_from_schema(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [1, 2]})
    _write_orc(path, table)
    scanner = OrcScanner(schema=table.schema, synthesized_columns=(PathColumn(),))

    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert scanner.read_schema().names == ["id", "path"]
    assert result.schema.names == scanner.read_schema().names
    assert result.to_pylist() == [
        {"id": 1, "path": str(path)},
        {"id": 2, "path": str(path)},
    ]


def test_orc_scanner_read_schema_projects_synthesized_column_missing_from_schema():
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64())]),
        synthesized_columns=(PathColumn(),),
    ).prune_columns(["path"])

    assert scanner.read_schema() == pa.schema([("path", pa.string())])


def test_orc_scanner_read_schema_uses_synthesized_type_for_existing_field():
    schema = pa.schema([("id", pa.int64()), ("path", pa.int64())])
    scanner = OrcScanner(
        schema=schema, synthesized_columns=(PathColumn(),)
    ).prune_columns(["path"])

    assert scanner.read_schema() == pa.schema([("path", pa.string())])


def test_orc_reader_uses_synthesized_type_when_replacing_file_column(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [1, 2], "path": [10, 20], "value": ["a", "b"]})
    _write_orc(path, table)

    scanner = OrcScanner(schema=table.schema, synthesized_columns=(PathColumn(),))
    batches = list(scanner.create_reader().read(_manifest(path)))
    result = pa.concat_tables(batches)

    assert scanner.read_schema().field("path").type == pa.string()
    assert scanner.read_schema().names == ["id", "path", "value"]
    assert result.schema.names == scanner.read_schema().names
    assert result.schema.field("path").type == pa.string()
    assert result.column("path").to_pylist() == [str(path), str(path)]
    assert result.column("value").to_pylist() == ["a", "b"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
