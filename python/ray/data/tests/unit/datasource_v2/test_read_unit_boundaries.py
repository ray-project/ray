"""Unit tests for how the Parquet reader honors read unit boundaries.

A column with ``requires_read_unit_boundaries`` needs every batch to come
from one row group. Without a pushed-down filter, the reader scans a manifest
row's row groups together and splits the batches at their footer row counts;
with a filter it scans each row group on its own. Either way, every row must
get the same unit and position.
"""

from dataclasses import dataclass

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


def _write_file(path):
    pq.write_table(
        pa.table({"id": list(range(NUM_ROWS))}), path, row_group_size=ROW_GROUP_SIZE
    )
    return str(path)


def _chunk(row_group_ids):
    row_group_ids = tuple(row_group_ids)
    return FileChunk(
        unit_ids=row_group_ids,
        num_rows=ROW_GROUP_SIZE * len(row_group_ids),
        size_bytes=ROW_GROUP_SIZE * 8 * len(row_group_ids),
    ).to_metadata()


def _manifest(rows):
    return FileManifest.construct_manifest(
        paths=[path for path, _ in rows],
        sizes=[0] * len(rows),
        chunk_metadatas=[chunk for _, chunk in rows],
    )


def _read(reader, manifest):
    return pa.concat_tables(list(reader.read(manifest)))


def _assert_positions_match_rows(table, path):
    """Row ``id`` sits in row group ``id // ROW_GROUP_SIZE`` at position
    ``id % ROW_GROUP_SIZE`` (no filter, so nothing is dropped)."""
    for row_id, position in zip(
        table.column("id").to_pylist(), table.column("position").to_pylist()
    ):
        assert position == {
            "unit": f"{path}#rg{row_id // ROW_GROUP_SIZE}",
            "row": row_id % ROW_GROUP_SIZE,
        }


@pytest.mark.parametrize("batch_size", [7, 25, 1000], ids=["small", "exact", "large"])
@pytest.mark.parametrize(
    "row_group_ids", [range(NUM_ROW_GROUPS), [1, 3]], ids=["all", "non_contiguous"]
)
def test_combined_scan_splits_batches_at_row_group_edges(
    tmp_path, batch_size, row_group_ids
):
    """Batches smaller than, equal to and larger than a row group, over
    contiguous and pruned (non-contiguous) row groups, all report each row's
    own row group and position."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        batch_size=batch_size, synthesized_columns=(_PositionColumn(),)
    )

    table = _read(reader, _manifest([(path, _chunk(row_group_ids))]))

    assert sorted(table.column("id").to_pylist()) == [
        i
        for g in row_group_ids
        for i in range(g * ROW_GROUP_SIZE, (g + 1) * ROW_GROUP_SIZE)
    ]
    _assert_positions_match_rows(table, path)


def test_every_batch_comes_from_one_row_group(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        batch_size=1000, synthesized_columns=(_PositionColumn(),)
    )

    tables = list(reader.read(_manifest([(path, _chunk(range(NUM_ROW_GROUPS)))])))

    assert len(tables) >= NUM_ROW_GROUPS
    for table in tables:
        units = {p["unit"] for p in table.column("position").to_pylist()}
        assert len(units) == 1


def test_scanned_table_spanning_row_groups_is_split(tmp_path, monkeypatch):
    """Even if the scan hands back one table holding several row groups, the
    reader splits it so each yielded table belongs to one row group."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(_PositionColumn(),))
    scan_one_fragment = reader._iter_fragment_tables

    def scan_as_one_table(fragment, scanner_kwargs):
        yield pa.concat_tables(list(scan_one_fragment(fragment, scanner_kwargs)))

    monkeypatch.setattr(reader, "_iter_fragment_tables", scan_as_one_table)

    tables = list(reader.read(_manifest([(path, _chunk([0, 1, 3]))])))

    assert [t.num_rows for t in tables] == [ROW_GROUP_SIZE] * 3
    for table in tables:
        assert len({p["unit"] for p in table.column("position").to_pylist()}) == 1
    _assert_positions_match_rows(pa.concat_tables(tables), path)


def test_scan_groups_combine_row_groups_without_a_filter(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(synthesized_columns=(_PositionColumn(),))
    dataset = pds.dataset([path], format="parquet")

    groups = reader._get_scan_groups(
        dataset, _manifest([(path, _chunk(range(NUM_ROW_GROUPS)))])
    )

    assert len(groups) == 1
    assert [u.unit.id for u in groups[0].units] == [
        f"{path}#rg{i}" for i in range(NUM_ROW_GROUPS)
    ]


def test_scan_groups_split_per_row_group_with_a_filter(tmp_path):
    """A pushed-down filter drops rows inside the scan, so each row group is
    scanned on its own and positions count the rows that survive."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        predicate=(col("id") % 2) == 0, synthesized_columns=(_PositionColumn(),)
    )
    manifest = _manifest([(path, _chunk(range(NUM_ROW_GROUPS)))])
    dataset = pds.dataset([path], format="parquet")

    groups = reader._get_scan_groups(dataset, manifest)
    table = _read(reader, manifest)

    assert [len(g.units) for g in groups] == [1] * NUM_ROW_GROUPS
    for row_id, position in zip(
        table.column("id").to_pylist(), table.column("position").to_pylist()
    ):
        assert position == {
            "unit": f"{path}#rg{row_id // ROW_GROUP_SIZE}",
            "row": (row_id % ROW_GROUP_SIZE) // 2,
        }


def test_scan_groups_unchanged_without_boundary_columns(tmp_path):
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader()
    dataset = pds.dataset([path], format="parquet")

    groups = reader._get_scan_groups(
        dataset, _manifest([(path, _chunk(range(NUM_ROW_GROUPS)))])
    )

    assert len(groups) == 1
    assert [u.unit.id for u in groups[0].units] == [path]


@pytest.mark.parametrize("batch_size", [7, 1000], ids=["small", "large"])
def test_row_hash_matches_per_row_group_scans(tmp_path, batch_size):
    """``row_hash`` is the same with combined scans (no filter) as with the
    whole-file read it was defined against."""
    path = _write_file(tmp_path / "data.parquet")
    reader = ParquetFileReader(
        batch_size=batch_size, synthesized_columns=(RowHashColumn(),)
    )

    whole = _read(reader, _manifest([(path, None)]))
    combined = _read(reader, _manifest([(path, _chunk([0, 2, 3]))]))

    expected = dict(zip(whole["id"].to_pylist(), whole["row_hash"].to_pylist()))
    got = dict(zip(combined["id"].to_pylist(), combined["row_hash"].to_pylist()))
    assert got == {i: expected[i] for i in got}
    assert len(got) == 3 * ROW_GROUP_SIZE


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
