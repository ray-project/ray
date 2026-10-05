"""Unit tests for dropping already-done rows of partly finished read units.

A resumed job hands each read task, through
``TaskContext.kwargs[EXCLUDED_ROWS_KWARG_NAME]``, a boolean mask per read unit
it finished only partly. The reader drops the masked rows after synthesized
columns are added, so those columns still see every row's position.
"""

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.synthesized_columns import (
    RowHashColumn,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)

ROW_GROUP_SIZE = 25
NUM_ROW_GROUPS = 4
NUM_ROWS = ROW_GROUP_SIZE * NUM_ROW_GROUPS


def _write_file(path):
    pq.write_table(
        pa.table({"id": list(range(NUM_ROWS))}), path, row_group_size=ROW_GROUP_SIZE
    )
    return str(path)


def _manifest(path):
    chunk = FileChunk(
        unit_ids=tuple(range(NUM_ROW_GROUPS)),
        num_rows=NUM_ROWS,
        size_bytes=NUM_ROWS * 8,
    ).to_metadata()
    return FileManifest.construct_manifest(
        paths=[path], sizes=[0], chunk_metadatas=[chunk]
    )


def _mask(done_rows, size=ROW_GROUP_SIZE):
    mask = np.zeros(size, dtype=bool)
    mask[list(done_rows)] = True
    return mask


def _read(reader, path, excluded_rows=None):
    tables = list(reader.read(_manifest(path), excluded_rows=excluded_rows))
    return pa.concat_tables(tables) if tables else None


@pytest.mark.parametrize("batch_size", [7, 1000], ids=["small", "large"])
def test_masked_rows_are_dropped(tmp_path, batch_size):
    """Masks apply by row position within the unit, across batch edges."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        batch_size=batch_size, synthesized_columns=(RowHashColumn(),)
    )
    done_in_rg1 = range(3, 20)

    table = _read(reader, path, {f"{path}#rg1": _mask(done_in_rg1)})

    dropped = {ROW_GROUP_SIZE + r for r in done_in_rg1}
    # Row groups read in parallel interleave their batches; compare as sets.
    assert sorted(table.column("id").to_pylist()) == [
        i for i in range(NUM_ROWS) if i not in dropped
    ]


def test_synthesized_columns_keep_original_positions(tmp_path):
    """Kept rows get the same ``row_hash`` as when nothing is excluded."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(batch_size=7, synthesized_columns=(RowHashColumn(),))

    full = _read(reader, path)
    partial = _read(reader, path, {f"{path}#rg2": _mask(range(0, 25, 2))})

    expected = dict(zip(full["id"].to_pylist(), full["row_hash"].to_pylist()))
    got = dict(zip(partial["id"].to_pylist(), partial["row_hash"].to_pylist()))
    assert got == {i: expected[i] for i in got}


def test_limit_counts_rows_after_exclusion(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(limit=10, synthesized_columns=(RowHashColumn(),))

    table = _read(reader, path, {f"{path}#rg0": _mask(range(5))})

    assert table.column("id").to_pylist() == list(range(5, 15))


def test_fully_masked_batches_are_not_yielded(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(batch_size=5, synthesized_columns=(RowHashColumn(),))

    tables = list(
        reader.read(_manifest(path), excluded_rows={f"{path}#rg0": _mask(range(25))})
    )

    assert all(t.num_rows > 0 for t in tables)
    assert sum(t.num_rows for t in tables) == NUM_ROWS - ROW_GROUP_SIZE


def test_short_mask_keeps_rows_past_its_end(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(RowHashColumn(),))

    table = _read(reader, path, {f"{path}#rg3": np.array([True, True])})

    assert 75 not in table.column("id").to_pylist()
    assert 76 not in table.column("id").to_pylist()
    assert len(table) == NUM_ROWS - 2


@pytest.mark.parametrize(
    "excluded_rows",
    [None, {}, {"other.parquet#rg0": np.ones(ROW_GROUP_SIZE, dtype=bool)}],
    ids=["none", "empty", "unknown_unit"],
)
def test_nothing_excluded_reads_everything(tmp_path, excluded_rows):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(RowHashColumn(),))

    table = _read(reader, path, excluded_rows)

    assert sorted(table.column("id").to_pylist()) == list(range(NUM_ROWS))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
