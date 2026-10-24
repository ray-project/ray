"""Unit tests for how the Parquet reader honors read unit boundaries.

A column with ``requires_read_unit_boundaries`` needs every batch to come
from one row group. Without a pushed-down filter, the reader scans a manifest
row's row groups together and splits the batches at their footer row counts;
with a filter it scans each row group on its own. Either way, every row must
get the same unit and position.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pds
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
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data.expressions import col

ROW_GROUP_SIZE = 25
NUM_ROW_GROUPS = 4
NUM_ROWS = ROW_GROUP_SIZE * NUM_ROW_GROUPS
ALL_ROW_GROUPS = tuple(range(NUM_ROW_GROUPS))


@dataclass(frozen=True)
class _PositionColumn(SynthesizedColumn):
    """Records each row's read unit and its position within the unit."""

    name = "position"
    type = pa.struct([("unit", pa.string()), ("row", pa.int64())])
    requires_read_unit_boundaries = True

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        rows = np.arange(position.rows_before, position.rows_before + num_rows)
        return pa.StructArray.from_arrays(
            [pa.array([position.unit.id] * num_rows), pa.array(rows)],
            fields=list(self.type),
        )


def _write_file(path) -> str:
    pq.write_table(
        pa.table({"id": list(range(NUM_ROWS))}), path, row_group_size=ROW_GROUP_SIZE
    )
    return str(path)


def _reader(**kwargs) -> ParquetFileReader:
    kwargs.setdefault("synthesized_columns", (_PositionColumn(),))
    return ParquetFileReader(**kwargs)


def _chunk(row_group_ids: Iterable[int]) -> Dict[str, Any]:
    row_group_ids = tuple(row_group_ids)
    return FileChunk(
        unit_ids=row_group_ids,
        num_rows=ROW_GROUP_SIZE * len(row_group_ids),
        size_bytes=ROW_GROUP_SIZE * 8 * len(row_group_ids),
    ).to_metadata()


def _manifest(rows: List[Tuple[str, Optional[Dict[str, Any]]]]) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[path for path, _ in rows],
        sizes=[0] * len(rows),
        chunk_metadatas=[chunk for _, chunk in rows],
    )


def _unit_id(path: str, row_group: int) -> str:
    return f"{path}#rg{row_group}"


def _read(reader: ParquetFileReader, manifest: FileManifest) -> pa.Table:
    return pa.concat_tables(list(reader.read(manifest)))


def _assert_positions(
    table: pa.Table, path: str, row_within_group: Callable[[int], int]
) -> None:
    """Row ``id`` lives in row group ``id // ROW_GROUP_SIZE``;
    ``row_within_group(id)`` says where the position counter should be."""
    assert table.num_rows > 0
    for row_id, position in zip(table["id"].to_pylist(), table["position"].to_pylist()):
        assert position == {
            "unit": _unit_id(path, row_id // ROW_GROUP_SIZE),
            "row": row_within_group(row_id),
        }


@pytest.mark.parametrize(
    "batch_size", [7, 25, 1000], ids=["sub_group", "exact_group", "multi_group"]
)
@pytest.mark.parametrize(
    "row_group_ids", [ALL_ROW_GROUPS, (1, 3)], ids=["contiguous", "pruned"]
)
def test_one_unit_per_batch_with_correct_positions(tmp_path, batch_size, row_group_ids):
    """The contract itself: every yielded batch holds rows of one row group,
    each tagged with its row group and offset — for batches that undershoot,
    hit, and overshoot a row group, over contiguous and pruned row groups."""
    path = _write_file(tmp_path / "data.parquet")
    reader = _reader(batch_size=batch_size)

    tables = list(reader.read(_manifest([(path, _chunk(row_group_ids))])))

    for table in tables:
        assert len({p["unit"] for p in table["position"].to_pylist()}) == 1
    combined = pa.concat_tables(tables)
    assert sorted(combined["id"].to_pylist()) == [
        i
        for g in row_group_ids
        for i in range(g * ROW_GROUP_SIZE, (g + 1) * ROW_GROUP_SIZE)
    ]
    _assert_positions(combined, path, lambda row_id: row_id % ROW_GROUP_SIZE)


def test_multi_unit_scan_table_is_split_at_footer_counts(tmp_path, monkeypatch):
    """The splitting loop in isolation: force the scan to return one table
    spanning three pruned row groups; the reader must cut it back apart."""
    path = _write_file(tmp_path / "data.parquet")
    reader = _reader()
    scan_one_fragment = reader._iter_fragment_tables

    def scan_as_one_table(fragment, scanner_kwargs):
        yield pa.concat_tables(list(scan_one_fragment(fragment, scanner_kwargs)))

    monkeypatch.setattr(reader, "_iter_fragment_tables", scan_as_one_table)

    tables = list(reader.read(_manifest([(path, _chunk([0, 1, 3]))])))

    assert [t.num_rows for t in tables] == [ROW_GROUP_SIZE] * 3
    _assert_positions(
        pa.concat_tables(tables), path, lambda row_id: row_id % ROW_GROUP_SIZE
    )


@pytest.mark.parametrize(
    "reader_kwargs, expected_group_unit_ids",
    [
        pytest.param(
            {"synthesized_columns": ()},
            lambda path: [[path]],
            id="no_boundary_column__one_whole_file_scan",
        ),
        pytest.param(
            {},
            lambda path: [[_unit_id(path, g) for g in ALL_ROW_GROUPS]],
            id="no_filter__one_combined_scan",
        ),
        pytest.param(
            {"predicate": (col("id") % 2) == 0},
            lambda path: [[_unit_id(path, g)] for g in ALL_ROW_GROUPS],
            id="filter__one_scan_per_row_group",
        ),
    ],
)
def test_scan_group_strategy(tmp_path, reader_kwargs, expected_group_unit_ids):
    """Which scans get built: combined without a filter, one per row group with
    one, and a single whole-file scan when no column needs boundaries."""
    path = _write_file(tmp_path / "data.parquet")
    reader = _reader(**reader_kwargs)
    dataset = pds.dataset([path], format="parquet")

    groups = reader._get_scan_groups(
        dataset, _manifest([(path, _chunk(ALL_ROW_GROUPS))])
    )

    assert [[u.unit.id for u in g.units] for g in groups] == expected_group_unit_ids(
        path
    )


def test_filtered_positions_count_surviving_rows(tmp_path):
    """With a filter, ``rows_before`` is a post-filter count: positions only
    count the rows that survive the predicate within each row group."""
    path = _write_file(tmp_path / "data.parquet")
    reader = _reader(predicate=(col("id") % 2) == 0)

    table = _read(reader, _manifest([(path, _chunk(ALL_ROW_GROUPS))]))

    assert table.num_rows == NUM_ROWS // 2
    _assert_positions(table, path, lambda row_id: (row_id % ROW_GROUP_SIZE) // 2)


@pytest.mark.parametrize("batch_size", [7, 1000], ids=["small", "large"])
def test_row_hash_unchanged_by_combined_scans(tmp_path, batch_size):
    """``row_hash`` was defined against whole-file reads; a combined scan
    with splitting must reproduce it exactly, even over pruned groups."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        batch_size=batch_size, synthesized_columns=(RowHashColumn(),)
    )

    whole = _read(reader, _manifest([(path, None)]))
    chunked = _read(reader, _manifest([(path, _chunk([0, 2, 3]))]))

    expected = dict(zip(whole["id"].to_pylist(), whole["row_hash"].to_pylist()))
    got = dict(zip(chunked["id"].to_pylist(), chunked["row_hash"].to_pylist()))
    assert len(got) == 3 * ROW_GROUP_SIZE
    assert got == {i: expected[i] for i in got}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
