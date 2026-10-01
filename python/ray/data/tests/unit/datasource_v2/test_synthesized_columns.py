"""Unit tests for synthesized columns.

The reader reports which :class:`ReadUnit` each batch came from, and a
:class:`SynthesizedColumn` turns that into a column: ``path`` and ``row_hash``
today, with ``row_hash`` identical whether a file is read whole or one row
group at a time.
"""

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.synthesized_columns import (
    PathColumn,
    RowHashColumn,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_footer_types import (
    ParquetRowGroupChunkMetadata,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileManifest,
    create_chunk_metadata,
)

ROW_GROUP_SIZE = 25
NUM_ROW_GROUPS = 4
NUM_ROWS = ROW_GROUP_SIZE * NUM_ROW_GROUPS
SCHEMA = pa.schema([("id", pa.int64())])


def _write_file(path, num_rows=NUM_ROWS, extra_columns=None):
    columns = {"id": list(range(num_rows))}
    columns.update(extra_columns or {})
    pq.write_table(pa.table(columns), path, row_group_size=ROW_GROUP_SIZE)
    return str(path)


def _row_group_chunk(row_group_ids, *, aligned=False):
    row_group_ids = tuple(row_group_ids)
    per_group = (ROW_GROUP_SIZE,) * len(row_group_ids) if aligned else ()
    return create_chunk_metadata(
        ParquetRowGroupChunkMetadata,
        row_group_ids=row_group_ids,
        num_rows=ROW_GROUP_SIZE * len(row_group_ids),
        uncompressed_size=ROW_GROUP_SIZE * 8 * len(row_group_ids),
        fully_matched=True,
        rg_sizes=tuple(n * 8 for n in per_group),
        rg_rows=per_group,
    )


def _manifest(rows):
    """``rows`` is a list of ``(path, chunk_metadata_or_None)``."""
    return FileManifest.construct_manifest(
        paths=[path for path, _ in rows],
        sizes=[0] * len(rows),
        chunk_metadatas=[chunk for _, chunk in rows],
    )


def _read(reader, manifest):
    tables = list(reader.read(manifest))
    return pa.concat_tables(tables) if tables else SCHEMA.empty_table()


def _scanner(**kwargs):
    return ParquetScanner(schema=SCHEMA, **kwargs)


@pytest.mark.parametrize(
    "synthesized_columns, expected_unit_ids",
    [
        ((), ["{path}"]),
        ((PathColumn(),), ["{path}"]),
        ((RowHashColumn(),), ["{path}#rg0", "{path}#rg1", "{path}#rg2", "{path}#rg3"]),
    ],
    ids=["none", "path_only", "needs_boundaries"],
)
def test_read_unit_boundaries_control_row_group_fan_out(
    tmp_path, synthesized_columns, expected_unit_ids
):
    """A file's row groups scan as one unit unless a column needs boundaries."""
    path = _write_file(tmp_path / "data.parquet")
    manifest = _manifest([(path, _row_group_chunk(range(NUM_ROW_GROUPS)))])
    reader = ParquetFileReader(synthesized_columns=synthesized_columns)
    dataset = pds.dataset([path], format="parquet")

    fragments = reader._get_fragments_to_read(dataset, manifest)

    assert [f.unit.id for f in fragments] == [
        unit_id.format(path=path) for unit_id in expected_unit_ids
    ]
    assert [f.unit_start_row for f in fragments] == [
        ROW_GROUP_SIZE * i for i in range(len(expected_unit_ids))
    ]
    assert all(f.unit.source == path for f in fragments)


def test_whole_file_manifest_row_is_one_unit_even_with_boundaries(tmp_path):
    """No footer fan-out for a whole-file row (today's behavior, kept); the
    one unit still knows its row count so a column can position rows in it."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(RowHashColumn(),))
    dataset = pds.dataset([path], format="parquet")

    fragments = reader._get_fragments_to_read(dataset, _manifest([(path, None)]))

    assert [(f.unit.id, f.unit_start_row) for f in fragments] == [(path, 0)]
    assert fragments[0].unit.num_rows == NUM_ROWS


def test_row_hash_is_the_same_whether_read_whole_or_by_row_group(tmp_path):
    """``unit_start_row`` seeds row hashes so a row-group read of a file
    hashes each row exactly as a whole-file read does."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(RowHashColumn(),))

    whole = _read(reader, _manifest([(path, None)]))
    by_id_whole = dict(
        zip(whole.column("id").to_pylist(), whole.column("row_hash").to_pylist())
    )

    subset = _read(reader, _manifest([(path, _row_group_chunk([1, 3]))]))
    by_id_subset = dict(
        zip(subset.column("id").to_pylist(), subset.column("row_hash").to_pylist())
    )
    assert len(by_id_subset) == 2 * ROW_GROUP_SIZE
    assert by_id_subset == {i: by_id_whole[i] for i in by_id_subset}


def test_synthesized_column_replaces_same_named_file_column(tmp_path):
    path = _write_file(
        tmp_path / "data.parquet", extra_columns={"path": [0] * NUM_ROWS}
    )
    reader = ParquetFileReader(synthesized_columns=(PathColumn(),))

    table = _read(reader, _manifest([(path, None)]))

    assert table.schema.field("path").type == pa.string()
    assert set(table.column("path").to_pylist()) == {path}


def test_read_schema_appends_synthesized_columns_respecting_projection():
    columns = (PathColumn(), RowHashColumn())

    assert _scanner(synthesized_columns=columns).read_schema().names == [
        "id",
        "path",
        "row_hash",
    ]
    assert (
        _scanner(synthesized_columns=columns).read_schema().field("row_hash").type
        == pa.uint64()
    )
    assert _scanner(
        synthesized_columns=columns, columns=("id",)
    ).read_schema().names == ["id"]

    # In production ``infer_schema`` already advertises the columns, so the
    # scanner's schema carries them: not duplicated, and a projection that
    # keeps one selects it from the schema.
    inferred = pa.schema(
        [("id", pa.int64()), ("path", pa.string()), ("row_hash", pa.uint64())]
    )
    scanner = ParquetScanner(schema=inferred, synthesized_columns=columns)
    assert scanner.read_schema().names == ["id", "path", "row_hash"]
    assert scanner.prune_columns(["row_hash", "id"]).read_schema().names == [
        "row_hash",
        "id",
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
