"""Unit tests for the generated row ID column behind
``CheckpointConfig(generated_id_column=...)``.

:class:`GeneratedIdColumn` is a synthesized column: the V2 Parquet reader asks
it for the IDs of each batch, passing the row group the batch came from.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data.checkpoint.generated_id import (
    GENERATED_ID_COLUMN_TYPE,
    GeneratedIdColumn,
    _build_generated_ids,
)
from ray.data.expressions import col

ROW_GROUP_SIZE = 10
NUM_ROW_GROUPS = 3
NUM_ROWS = ROW_GROUP_SIZE * NUM_ROW_GROUPS
ID_COLUMN = "generated_id"


def _write_file(path, first_value=0, extra_columns=None):
    columns = {"value": list(range(first_value, first_value + NUM_ROWS))}
    columns.update(extra_columns or {})
    pq.write_table(pa.table(columns), path, row_group_size=ROW_GROUP_SIZE)
    return str(path)


def _all_row_groups_chunk():
    return FileChunk(
        unit_ids=tuple(range(NUM_ROW_GROUPS)),
        num_rows=NUM_ROWS,
        size_bytes=NUM_ROWS * 8,
    ).to_metadata()


def _manifest(rows):
    """``rows`` is a list of ``(path, chunk_metadata_or_None)``."""
    return FileManifest.construct_manifest(
        paths=[path for path, _ in rows],
        sizes=[0] * len(rows),
        chunk_metadatas=[chunk for _, chunk in rows],
    )


def _read_ids(paths, **reader_kwargs):
    reader = ParquetFileReader(
        synthesized_columns=(GeneratedIdColumn(ID_COLUMN),), **reader_kwargs
    )
    manifest = _manifest([(path, _all_row_groups_chunk()) for path in paths])
    tables = list(reader.read(manifest))
    return pa.concat_tables(tables)


def test_build_generated_ids_matches_type():
    ids = _build_generated_ids(
        "bucket/dir/a.parquet",
        row_group_index=2,
        num_row_groups=5,
        row_group_num_rows=100,
        rows_before=7,
        num_rows=3,
    )
    assert ids.type == GENERATED_ID_COLUMN_TYPE
    assert ids.to_pylist() == [
        {
            "path_prefix": "bucket/dir",
            "file_name": "a.parquet",
            "fragment": 2,
            "num_fragments": 5,
            "num_rows": 100,
            "row_id": row_id,
        }
        for row_id in (7, 8, 9)
    ]


def test_ids_name_file_row_group_and_position(tmp_path):
    path = _write_file(tmp_path / "a.parquet")

    table = _read_ids([path])

    ids = table.column(ID_COLUMN).to_pylist()
    assert len(ids) == NUM_ROWS
    for value, row_id in zip(table.column("value").to_pylist(), ids):
        assert row_id["path_prefix"] == str(tmp_path)
        assert row_id["file_name"] == "a.parquet"
        assert row_id["fragment"] == value // ROW_GROUP_SIZE
        assert row_id["num_fragments"] == NUM_ROW_GROUPS
        assert row_id["num_rows"] == ROW_GROUP_SIZE
        assert row_id["row_id"] == value % ROW_GROUP_SIZE


def test_ids_unique_across_files_and_row_groups(tmp_path):
    paths = [
        _write_file(tmp_path / "a.parquet"),
        _write_file(tmp_path / "b.parquet", first_value=NUM_ROWS),
    ]

    ids = _read_ids(paths).column(ID_COLUMN).to_pylist()

    assert len(ids) == 2 * NUM_ROWS
    assert len({tuple(sorted(i.items())) for i in ids}) == 2 * NUM_ROWS


def test_row_id_continues_across_batches(tmp_path):
    """A row group read in several batches keeps counting ``row_id``."""
    path = _write_file(tmp_path / "a.parquet")

    table = _read_ids([path], batch_size=3)

    for value, row_id in zip(
        table.column("value").to_pylist(), table.column(ID_COLUMN).to_pylist()
    ):
        assert row_id["row_id"] == value % ROW_GROUP_SIZE


def test_ids_under_pushed_down_filter_are_deterministic(tmp_path):
    """With a pushed-down filter, ``row_id`` counts the rows that survive it,
    the same way on every read of the same pipeline."""
    path = _write_file(tmp_path / "a.parquet")
    predicate = (col("value") % 2) == 0

    first = _read_ids([path], predicate=predicate)
    second = _read_ids([path], predicate=predicate)

    assert first.equals(second)
    ids = first.column(ID_COLUMN).to_pylist()
    for row_group in range(NUM_ROW_GROUPS):
        row_ids = [i["row_id"] for i in ids if i["fragment"] == row_group]
        assert row_ids == list(range(ROW_GROUP_SIZE // 2))


def test_whole_file_read_raises(tmp_path):
    """A whole-file read can't name row groups, so the column refuses to
    build IDs instead of writing wrong ones."""
    path = _write_file(tmp_path / "a.parquet")
    reader = ParquetFileReader(synthesized_columns=(GeneratedIdColumn(ID_COLUMN),))

    with pytest.raises(ValueError, match="read as a single unit"):
        list(reader.read(_manifest([(path, None)])))


def test_projection_without_the_column_skips_it(tmp_path):
    path = _write_file(tmp_path / "a.parquet")

    table = _read_ids([path], columns=["value"])

    assert table.column_names == ["value"]


def test_infer_schema_advertises_the_column(tmp_path):
    path = _write_file(tmp_path / "a.parquet")
    datasource = ParquetDatasourceV2(
        [path], extra_synthesized_columns=(GeneratedIdColumn(ID_COLUMN),)
    )

    schema = datasource.infer_schema(_manifest([(path, None)]))

    assert schema.field(ID_COLUMN).type == GENERATED_ID_COLUMN_TYPE
    scanner = datasource.create_scanner(schema)
    assert scanner.read_schema().field(ID_COLUMN).type == GENERATED_ID_COLUMN_TYPE


def test_infer_schema_raises_on_name_clash(tmp_path):
    path = _write_file(
        tmp_path / "a.parquet", extra_columns={ID_COLUMN: list(range(NUM_ROWS))}
    )
    datasource = ParquetDatasourceV2(
        [path], extra_synthesized_columns=(GeneratedIdColumn(ID_COLUMN),)
    )

    with pytest.raises(ValueError, match="already exists"):
        datasource.infer_schema(_manifest([(path, None)]))


def test_no_column_without_request(tmp_path):
    path = _write_file(tmp_path / "a.parquet")
    datasource = ParquetDatasourceV2([path])

    schema = datasource.infer_schema(_manifest([(path, None)]))

    assert schema.names == ["value"]
    assert isinstance(datasource.create_scanner(schema), ParquetScanner)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
