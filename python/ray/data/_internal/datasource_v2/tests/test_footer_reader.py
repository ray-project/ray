"""Tests for ``FooterReader`` -- footer IO and row-group chunking.

``FooterReader`` is a plain class (the actor is built from it separately), so
these drive it directly against local Parquet fixtures. No Ray cluster needed.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader
from ray.data.expressions import col


def _write(path, table, row_group_size):
    pq.write_table(table, path, row_group_size=row_group_size)
    return str(path), path.stat().st_size


@pytest.fixture
def four_row_groups(tmp_path):
    """One file, 4 row groups of 25 rows; ``id`` ascends 0..99."""
    table = pa.table({"id": list(range(100)), "pad": ["x" * 8] * 100})
    return _write(tmp_path / "data.parquet", table, row_group_size=25)


def _reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


class TestReadAndChunk:
    def test_yields_every_row_group_without_a_predicate(self, four_row_groups):
        path, size = four_row_groups

        chunks = _reader()._read_and_chunk(path, size)

        assert chunks.path == path
        assert chunks.size == size
        assert [rg.rg_idx for rg in chunks.row_groups] == [0, 1, 2, 3]
        assert [rg.num_rows for rg in chunks.row_groups] == [25, 25, 25, 25]
        # No predicate means every group is an exact survivor, which is what
        # lets limit push-down count rows without re-filtering.
        assert all(rg.fully_matched for rg in chunks.row_groups)
        assert all(rg.uncompressed_size > 0 for rg in chunks.row_groups)

    def test_predicate_prunes_row_groups_by_statistics(self, four_row_groups):
        path, size = four_row_groups

        # id >= 50 excludes the first two row groups (0-24, 25-49) outright.
        chunks = _reader(filter_expr=col("id") >= 50)._read_and_chunk(path, size)

        assert [rg.rg_idx for rg in chunks.row_groups] == [2, 3]

    def test_fully_matched_marks_only_wholly_surviving_groups(self, four_row_groups):
        path, size = four_row_groups

        # id >= 30 splits row group 1 (25-49) and fully covers 2 and 3.
        chunks = _reader(filter_expr=col("id") >= 30)._read_and_chunk(path, size)

        by_idx = {rg.rg_idx: rg.fully_matched for rg in chunks.row_groups}
        assert by_idx[1] is False, "partially matching group must not count as exact"
        assert by_idx[2] is True
        assert by_idx[3] is True

    def test_predicate_matching_nothing_yields_no_row_groups(self, four_row_groups):
        path, size = four_row_groups

        chunks = _reader(filter_expr=col("id") > 10_000)._read_and_chunk(path, size)

        assert chunks.row_groups == ()

    def test_projection_shrinks_accounted_size(self, four_row_groups):
        path, size = four_row_groups

        full = _reader()._read_and_chunk(path, size)
        projected = _reader(projected_cols=["id"])._read_and_chunk(path, size)

        # The reader only fetches projected columns, so bin sizing must account
        # for those bytes alone -- otherwise bins are sized for data never read.
        assert sum(rg.uncompressed_size for rg in projected.row_groups) < sum(
            rg.uncompressed_size for rg in full.row_groups
        )
        # Row counts are a property of the file, not the projection.
        assert [rg.num_rows for rg in projected.row_groups] == [
            rg.num_rows for rg in full.row_groups
        ]

    def test_coalescing_merges_contiguous_groups(self, four_row_groups):
        path, size = four_row_groups

        uncoalesced = _reader()._read_and_chunk(path, size)
        per_rg = uncoalesced.row_groups[0].uncompressed_size
        coalesced = _reader(coalesce_bytes=per_rg * 2)._read_and_chunk(path, size)

        assert len(coalesced.row_groups) < len(uncoalesced.row_groups)
        # Coalescing regroups descriptors; it must not lose rows.
        assert sum(rg.num_rows for rg in coalesced.row_groups) == 100


class TestProjectedLeafIndices:
    def test_none_when_no_projection(self, four_row_groups):
        path, _ = four_row_groups
        row_group = pq.ParquetFile(path).metadata.row_group(0)

        # ``None`` signals "all columns" so callers can take the cheap path.
        assert _reader()._projected_leaf_indices(row_group) is None

    def test_selects_only_projected_leaves(self, four_row_groups):
        path, _ = four_row_groups
        row_group = pq.ParquetFile(path).metadata.row_group(0)

        indices = _reader(projected_cols=["id"])._projected_leaf_indices(row_group)

        assert indices is not None
        paths = [row_group.column(j).path_in_schema for j in indices]
        assert paths == ["id"]

    def test_nested_column_expands_to_all_its_leaves(self, tmp_path):
        table = pa.table(
            {
                "outer": [{"a": 1, "b": 2}] * 4,
                "other": [9] * 4,
            }
        )
        path, _ = _write(tmp_path / "nested.parquet", table, row_group_size=2)
        row_group = pq.ParquetFile(path).metadata.row_group(0)

        indices = _reader(projected_cols=["outer"])._projected_leaf_indices(row_group)

        paths = [row_group.column(j).path_in_schema for j in indices]
        assert paths == ["outer.a", "outer.b"]


class TestReadFootersBatching:
    @pytest.fixture
    def three_files(self, tmp_path):
        files = []
        for i in range(3):
            table = pa.table({"id": list(range(10))})
            files.append(_write(tmp_path / f"f{i}.parquet", table, row_group_size=5))
        return files

    @pytest.mark.parametrize(
        "result_batch_size,expected_batch_sizes",
        [(1, [1, 1, 1]), (2, [2, 1]), (3, [3]), (10, [3])],
    )
    def test_batches_results(
        self, three_files, result_batch_size, expected_batch_sizes
    ):
        # ``@ray.method`` only tags the function, so it stays directly callable
        # as a generator -- no actor needed.
        batches = list(
            _reader().read_footers(three_files, result_batch_size=result_batch_size)
        )

        assert [len(b) for b in batches] == expected_batch_sizes
        # Every file appears exactly once regardless of batching.
        paths = [fc.path for batch in batches for fc in batch]
        assert sorted(paths) == sorted(p for p, _ in three_files)

    def test_empty_input_yields_nothing(self):
        assert list(_reader().read_footers([], result_batch_size=1)) == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
