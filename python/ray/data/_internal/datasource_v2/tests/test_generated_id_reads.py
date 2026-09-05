"""Unit tests for generated-ID attachment in the V2 Parquet reader.

Exercises :mod:`ray.data.checkpoint.generated_id` wiring through
:class:`ParquetFileReader` directly against a local tmpdir — these tests do
not spin up Ray. The generated ID column is configured ambiently through
``DataContext.checkpoint_config``, mirroring how the planner-side code reads
it.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.parquet_datasource_v2 import ParquetDatasourceV2
from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.generated_id import (
    FILE_NAME_FIELD,
    FRAGMENT_FIELD,
    GENERATED_ID_COLUMN_TYPE,
    NUM_FRAGMENTS_FIELD,
    NUM_ROWS_FIELD,
    ROW_ID_FIELD,
)
from ray.data.context import DataContext
from ray.data.expressions import col

GEN_ID_COL = "__test_gen_id"


@pytest.fixture
def generated_id_checkpoint_config(tmp_path):
    """Install a generated-ID ``CheckpointConfig`` on the current context."""
    ctx = DataContext.get_current()
    original = ctx.checkpoint_config
    ctx.checkpoint_config = CheckpointConfig(
        generated_id_column=GEN_ID_COL,
        checkpoint_path=str(tmp_path / "checkpoint"),
    )
    yield ctx.checkpoint_config
    ctx.checkpoint_config = original


def _write_parquet(path: str, num_rows: int, row_group_size: int, offset: int = 0):
    table = pa.table({"x": list(range(offset, offset + num_rows))})
    pq.write_table(table, path, row_group_size=row_group_size)


def _whole_file_manifest(paths):
    return FileManifest.construct_manifest(
        paths=paths,
        sizes=[os.path.getsize(p) for p in paths],
        chunk_metadatas=[None] * len(paths),
    )


def _id_tuples(table: pa.Table):
    """Extract ``(file_name, fragment, row_id)`` per row from the ID column."""
    ids = table.column(GEN_ID_COL).to_pylist()
    return [(v[FILE_NAME_FIELD], v[FRAGMENT_FIELD], v[ROW_ID_FIELD]) for v in ids]


def test_ids_unique_across_files_and_row_groups(
    tmp_path, generated_id_checkpoint_config
):
    paths = []
    for f in range(2):
        path = str(tmp_path / f"f{f}.parquet")
        # 6 rows in 2 row groups of 3.
        _write_parquet(path, num_rows=6, row_group_size=3, offset=f * 100)
        paths.append(path)

    tables = list(ParquetFileReader().read(_whole_file_manifest(paths)))
    combined = pa.concat_tables(tables)
    assert combined.num_rows == 12

    assert combined.schema.field(GEN_ID_COL).type == GENERATED_ID_COLUMN_TYPE
    triples = _id_tuples(combined)
    assert len(set(triples)) == 12

    for id_value in combined.column(GEN_ID_COL).to_pylist():
        assert id_value[NUM_FRAGMENTS_FIELD] == 2
        assert id_value[NUM_ROWS_FIELD] == 3
        assert id_value[FRAGMENT_FIELD] in (0, 1)
        assert 0 <= id_value[ROW_ID_FIELD] < 3


def test_row_id_continues_across_batches_within_row_group(
    tmp_path, generated_id_checkpoint_config
):
    path = str(tmp_path / "data.parquet")
    # One row group of 6 rows, read in batches of 2.
    _write_parquet(path, num_rows=6, row_group_size=6)

    reader = ParquetFileReader(batch_size=2)
    tables = list(reader.read(_whole_file_manifest([path])))
    combined = pa.concat_tables(tables)
    row_ids = [v[ROW_ID_FIELD] for v in combined.column(GEN_ID_COL).to_pylist()]
    assert row_ids == list(range(6))


def test_collision_with_existing_column_raises(
    tmp_path, generated_id_checkpoint_config
):
    path = str(tmp_path / "data.parquet")
    pq.write_table(pa.table({"x": [1, 2], GEN_ID_COL: [1, 2]}), path)

    with pytest.raises(ValueError, match="conflicts with an existing"):
        list(ParquetFileReader().read(_whole_file_manifest([path])))


def test_filter_pushdown_positions_are_deterministic(
    tmp_path, generated_id_checkpoint_config
):
    """With a pushed-down predicate, ``row_id`` indexes the *filtered* stream.

    IDs are attached after the pyarrow scanner applies the predicate
    (matching RayTurbo's reader), so a resumed run with the same pipeline
    deterministically reassigns the same IDs. ``num_rows`` still reports the
    on-disk row-group size, so a filtered row group can never be marked
    fully checkpointed and is re-read (then row-filtered) on resume.
    """
    path = str(tmp_path / "data.parquet")
    # One row group: x = 0..5. Keep x >= 3 -> filtered-stream positions 0..2.
    _write_parquet(path, num_rows=6, row_group_size=6)

    def _read():
        reader = ParquetFileReader(predicate=col("x") >= 3)
        return pa.concat_tables(reader.read(_whole_file_manifest([path])))

    combined = _read()
    assert combined.column("x").to_pylist() == [3, 4, 5]
    ids = combined.column(GEN_ID_COL).to_pylist()
    assert [v[ROW_ID_FIELD] for v in ids] == [0, 1, 2]
    # On-disk row-group size, not the filtered count.
    assert all(v[NUM_ROWS_FIELD] == 6 for v in ids)
    # Deterministic across identical runs (the resume correctness contract).
    assert _read().column(GEN_ID_COL).to_pylist() == ids


def test_column_projection_keeps_generated_id(tmp_path, generated_id_checkpoint_config):
    path = str(tmp_path / "data.parquet")
    _write_parquet(path, num_rows=4, row_group_size=4)

    reader = ParquetFileReader(columns=["x"])
    combined = pa.concat_tables(reader.read(_whole_file_manifest([path])))
    assert set(combined.column_names) == {"x", GEN_ID_COL}


def test_infer_schema_advertises_generated_id(tmp_path, generated_id_checkpoint_config):
    path = str(tmp_path / "data.parquet")
    _write_parquet(path, num_rows=4, row_group_size=4)

    datasource = ParquetDatasourceV2([path])
    schema = datasource.infer_schema(_whole_file_manifest([path]))
    assert schema.field(GEN_ID_COL).type == GENERATED_ID_COLUMN_TYPE


def _file_fragments_value(fragments):
    """One file's struct value from (fragment_id, num_rows, committed) tuples."""
    entries = []
    num_fully = 0
    for fragment_id, num_rows, committed in fragments:
        fully = len(committed) == num_rows
        num_fully += fully
        entries.append(
            {
                "fragment_id": fragment_id,
                "num_rows": num_rows,
                "num_checkpointed_rows": len(committed),
                "checkpointed_row_ids": (
                    [] if fully else [i in set(committed) for i in range(num_rows)]
                ),
            }
        )
    return {
        "num_fragments": len(fragments),
        "fully_checkpointed": num_fully == len(fragments),
        "fragments": entries,
    }


def _checkpointed_manifest(paths, fragments_by_path):
    """Whole-file manifest whose rows carry compact checkpoint structs."""
    from ray.data.checkpoint.generated_id import CHECKPOINTED_FILE_FRAGMENTS_TYPE

    values = [
        (
            _file_fragments_value(fragments_by_path[p])
            if p in fragments_by_path
            else None
        )
        for p in paths
    ]
    return FileManifest.construct_manifest(
        paths=paths,
        sizes=[os.path.getsize(p) for p in paths],
        chunk_metadatas=[None] * len(paths),
        checkpoint_file_fragments=[
            pa.array([v], type=CHECKPOINTED_FILE_FRAGMENTS_TYPE)[0] for v in values
        ],
    )


def test_reader_skips_fully_checkpointed_row_groups(
    tmp_path, generated_id_checkpoint_config
):
    path = str(tmp_path / "data.parquet")
    # 2 row groups of 3 rows; both fully committed -> nothing to read.
    _write_parquet(path, num_rows=6, row_group_size=3)
    manifest = _checkpointed_manifest(
        [path], {path: [(0, 3, [0, 1, 2]), (1, 3, [0, 1, 2])]}
    )
    tables = list(ParquetFileReader().read(manifest))
    assert tables == []


def test_reader_filters_partially_checkpointed_row_group(
    tmp_path, generated_id_checkpoint_config
):
    path = str(tmp_path / "data.parquet")
    # Row group 0 (x=0..2): row 1 committed. Row group 1 (x=3..5): fully
    # committed. x equals the file row position.
    _write_parquet(path, num_rows=6, row_group_size=3)
    manifest = _checkpointed_manifest([path], {path: [(0, 3, [1]), (1, 3, [0, 1, 2])]})
    combined = pa.concat_tables(ParquetFileReader().read(manifest))
    assert combined.column("x").to_pylist() == [0, 2]
    # Surviving rows keep their original in-group positions.
    ids = combined.column(GEN_ID_COL).to_pylist()
    assert [(v[FRAGMENT_FIELD], v[ROW_ID_FIELD]) for v in ids] == [(0, 0), (0, 2)]


def test_reader_partial_filter_across_batches(tmp_path, generated_id_checkpoint_config):
    path = str(tmp_path / "data.parquet")
    # One row group of 6 rows, rows 1 and 4 committed, read in batches of 2.
    _write_parquet(path, num_rows=6, row_group_size=6)
    manifest = _checkpointed_manifest([path], {path: [(0, 6, [1, 4])]})
    reader = ParquetFileReader(batch_size=2)
    combined = pa.concat_tables(reader.read(manifest))
    assert combined.column("x").to_pylist() == [0, 2, 3, 5]
    row_ids = [v[ROW_ID_FIELD] for v in combined.column(GEN_ID_COL).to_pylist()]
    assert row_ids == [0, 2, 3, 5]


def test_reader_no_skip_for_unmatched_checkpoint(
    tmp_path, generated_id_checkpoint_config
):
    path = str(tmp_path / "data.parquet")
    other = str(tmp_path / "other.parquet")
    _write_parquet(path, num_rows=4, row_group_size=2)
    _write_parquet(other, num_rows=4, row_group_size=2)
    # The checkpoint only covers a different file; this one reads in full.
    manifest = _checkpointed_manifest([path], {other: [(0, 2, [0, 1])]})
    combined = pa.concat_tables(ParquetFileReader().read(manifest))
    assert combined.num_rows == 4


def test_reader_ignores_manifest_without_checkpoint_column(
    tmp_path, generated_id_checkpoint_config
):
    """First run: generated-ID on, but no checkpoint column on the manifest."""
    path = str(tmp_path / "data.parquet")
    _write_parquet(path, num_rows=4, row_group_size=2)
    combined = pa.concat_tables(ParquetFileReader().read(_whole_file_manifest([path])))
    assert combined.num_rows == 4
    assert GEN_ID_COL in combined.column_names


def test_reads_unaffected_without_config(tmp_path):
    path = str(tmp_path / "data.parquet")
    _write_parquet(path, num_rows=4, row_group_size=2)

    assert DataContext.get_current().checkpoint_config is None
    combined = pa.concat_tables(ParquetFileReader().read(_whole_file_manifest([path])))
    assert combined.column_names == ["x"]
    assert combined.num_rows == 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
