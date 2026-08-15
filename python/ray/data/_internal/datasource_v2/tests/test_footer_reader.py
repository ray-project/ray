"""Tests for ``FooterReader`` -- footer IO and row-group chunking.

``FooterReader`` is a plain class (the actor is built from it separately), so
these drive it directly against local Parquet fixtures. No Ray cluster needed.
"""

import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader
from ray.data.expressions import col


def _write(path, table, row_group_size, **write_kwargs):
    pq.write_table(table, path, row_group_size=row_group_size, **write_kwargs)
    return str(path), path.stat().st_size


@pytest.fixture
def four_row_groups(tmp_path):
    """One file, 4 row groups of 25 rows; ``id`` ascends 0..99."""
    table = pa.table({"id": list(range(100)), "pad": ["x" * 8] * 100})
    return _write(tmp_path / "data.parquet", table, row_group_size=25)


def _reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


def _filter_leaves(reader, metadata, rg_idx=0):
    return reader._locate_filter_columns(metadata.row_group(rg_idx))


def _has_filter_nulls(reader, metadata, rg_idx=0):
    """Run the null-count check the way ``_rg_can_fully_match`` does."""
    row_group = metadata.row_group(rg_idx)
    return reader._has_filter_nulls(row_group, _filter_leaves(reader, metadata).indices)


def _rg_can_fully_match(reader, metadata, rg_idx=0):
    """Run the fully-match guard the way ``_read_and_chunk`` does."""
    row_group = metadata.row_group(rg_idx)
    return reader._rg_can_fully_match(row_group, _filter_leaves(reader, metadata))


class _FakeStatistics:
    def __init__(self, has_null_count, null_count):
        self.has_null_count = has_null_count
        self.null_count = null_count


class _FakeColumn:
    def __init__(self, path_in_schema, statistics):
        self.path_in_schema = path_in_schema
        self.statistics = statistics


class _FakeMetadata:
    """Enough of ``FileMetaData`` for ``_has_filter_nulls``: one row group.

    Statistics states that no PyArrow writer emits -- notably a ``Statistics``
    with ``has_null_count`` false -- are reachable in files written by other
    Parquet implementations, so they are faked rather than written.
    """

    def __init__(self, *columns):
        self._columns = columns

    def row_group(self, rg_idx):
        return self

    @property
    def num_columns(self):
        return len(self._columns)

    def column(self, j):
        return self._columns[j]


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

    def test_projection_accounts_only_the_projected_columns(self, four_row_groups):
        path, size = four_row_groups
        full = _reader()._read_and_chunk(path, size)
        ids = _reader(projected_cols=["id"])._read_and_chunk(path, size)
        pads = _reader(projected_cols=["pad"])._read_and_chunk(path, size)

        def total(c):
            return sum(rg.uncompressed_size for rg in c.row_groups)

        assert total(ids) + total(pads) == total(full)
        assert 0 < total(ids) < total(full)
        assert [rg.num_rows for rg in ids.row_groups] == [
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


class TestNullsAreNeverExactSurvivors:
    """A null in a predicate column means ``num_rows`` is not an exact count.

    Parquet min/max statistics are computed over non-null values only, so
    bounds can never rule out nulls. Under three-valued logic a null row
    satisfies neither ``filter`` nor ``~filter``, so the negation test alone
    would mark such a group fully matched and limit push-down would stop early
    and silently under-deliver.
    """

    @pytest.mark.parametrize(
        "values,survivors,expected_exact_rows",
        [
            # ``survivors`` is how many rows really pass ``id >= 30``.
            # ``expected_exact_rows`` is how many of those the footer alone can
            # vouch for. A row group is all or nothing here -- either its whole
            # ``num_rows`` is an exact survivor count or the group contributes
            # nothing -- and a null makes ``num_rows`` an overstatement, so the
            # first two files end up vouching for no rows at all.
            pytest.param([35, 40, 45, None], 3, 0, id="one-null-rest-satisfy"),
            pytest.param([35, None, None, None], 1, 0, id="mostly-null"),
            pytest.param([35, 40, 45, 50], 4, 4, id="no-nulls-is-exact"),
        ],
    )
    def test_exact_counts_come_only_from_null_free_groups(
        self, tmp_path, values, survivors, expected_exact_rows
    ):
        path, size = _write(
            tmp_path / "n.parquet",
            pa.table({"id": pa.array(values, pa.int64())}),
            row_group_size=len(values),
        )

        chunks = _reader(filter_expr=col("id") >= 30)._read_and_chunk(path, size)

        # Measured rather than restated: this guard exists because a null row
        # passes neither the filter nor its negation, so pin what the filter
        # actually returns instead of trusting the number above.
        assert pq.read_table(path, filters=[("id", ">=", 30)]).num_rows == survivors

        exact_rows = sum(rg.num_rows for rg in chunks.row_groups if rg.fully_matched)
        assert exact_rows == expected_exact_rows

    @pytest.mark.parametrize(
        "column", ["id", "sepal.length"], ids=["plain", "dotted-flat-name"]
    )
    def test_guard_applies_to_dotted_flat_column_names(self, tmp_path, column):
        """A flat column's *name* may contain dots -- ``sepal.length`` is real.

        Matching a leaf by its first dot-separated segment gets nested columns
        right and this case silently wrong: the guard finds no matching leaf,
        concludes "no nulls", and the group is marked fully matched.
        """
        path, _ = _write(
            tmp_path / "dotted.parquet",
            pa.table({column: pa.array([35, 40, 45, None], pa.int64())}),
            row_group_size=4,
        )
        reader = _reader(filter_expr=col(column) >= 30)

        assert _has_filter_nulls(reader, pq.ParquetFile(path).metadata)

    def test_file_without_statistics_fails_closed(self, tmp_path):
        """No statistics at all means nothing was verified, so assume nulls.

        The data here is null-free; the point is that the footer does not say
        so, and inferring "no nulls" from silence is what over-counts.
        """
        path, size = _write(
            tmp_path / "nostats.parquet",
            pa.table({"id": pa.array([35, 40, 45, 50], pa.int64())}),
            row_group_size=4,
            write_statistics=False,
        )
        reader = _reader(filter_expr=col("id") >= 30)

        assert _has_filter_nulls(reader, pq.ParquetFile(path).metadata)
        assert not any(
            rg.fully_matched for rg in reader._read_and_chunk(path, size).row_groups
        )

    @pytest.mark.parametrize(
        "statistics,expected",
        [
            pytest.param(None, True, id="no-statistics"),
            pytest.param(
                _FakeStatistics(has_null_count=False, null_count=0),
                True,
                id="null-count-absent",
            ),
            pytest.param(
                _FakeStatistics(has_null_count=True, null_count=1),
                True,
                id="null-count-positive",
            ),
            pytest.param(
                _FakeStatistics(has_null_count=True, null_count=0),
                False,
                id="null-count-zero",
            ),
        ],
    )
    def test_only_a_written_zero_null_count_counts_as_null_free(
        self, statistics, expected
    ):
        """A null count is only trustworthy when the writer actually wrote one.

        ``null_count`` reads as ``0`` on statistics that never recorded it, so
        the value is meaningless unless ``has_null_count`` is set.
        """
        reader = _reader(filter_expr=col("id") >= 30)

        metadata = _FakeMetadata(_FakeColumn("id", statistics))

        assert _has_filter_nulls(reader, metadata) is expected

    def test_unlocatable_predicate_column_fails_closed(self, tmp_path):
        """Not finding a predicate column means unverified, not fully matched.

        The found columns can still be null-free; ``_rg_can_fully_match`` is
        what fails closed on the missing one.
        """
        path, _ = _write(
            tmp_path / "missing.parquet",
            pa.table({"id": pa.array([35, 40, 45, 50], pa.int64())}),
            row_group_size=4,
        )
        reader = _reader(filter_expr=col("id") >= 30)
        reader.filter_columns = {"id", "not_in_this_file"}
        metadata = pq.ParquetFile(path).metadata

        assert not _has_filter_nulls(reader, metadata)
        assert not _rg_can_fully_match(reader, metadata)

    def test_survives_a_sharper_statistics_pruner(self, tmp_path):
        """Correctness must not rest on PyArrow declining to prune.

        PyArrow currently does not use ``null_count`` to sharpen the inverted
        predicate, so ``~filter`` fails to prune and the negation test looks
        safe. Pruning would be *semantically correct* here, though -- no row
        satisfies ``id < 30``. This stands in for an engine that does prune, and
        pins that the null check alone carries the invariant.
        """
        path, size = _write(
            tmp_path / "adv.parquet",
            pa.table({"id": pa.array([35, 40, 45, None], pa.int64())}),
            row_group_size=4,
        )
        reader = _reader(filter_expr=col("id") >= 30)

        class _PrunesInvertedPredicate:
            def __init__(self, fragment, positive):
                self._fragment, self._positive = fragment, positive

            def __getattr__(self, name):
                return getattr(self._fragment, name)

            def split_by_row_group(self, expr):
                if str(expr) != str(self._positive):
                    return []
                return self._fragment.split_by_row_group(expr)

        class _Format:
            def __init__(self, real, positive):
                self._real, self._positive = real, positive

            def make_fragment(self, *args, **kwargs):
                return _PrunesInvertedPredicate(
                    self._real.make_fragment(*args, **kwargs), self._positive
                )

        reader.file_format = _Format(reader.file_format, reader.filter)
        chunks = reader._read_and_chunk(path, size)

        assert not any(rg.fully_matched for rg in chunks.row_groups)


class TestMissingFilterColumnsSkipPruning:
    """A file that predates a filter column must not abort footer reads.

    PyArrow's ``split_by_row_group`` raises ``ArrowInvalid`` when the field is
    absent. Schema evolution null-fills that column at read time, so every
    row group is kept and none is an exact survivor.
    """

    def test_keeps_every_group_and_marks_none_fully_matched(self, tmp_path):
        path, size = _write(
            tmp_path / "no_b.parquet",
            pa.table({"a": pa.array([1, 2, 3, 4], pa.int64())}),
            row_group_size=2,
        )

        chunks = _reader(filter_expr=col("b") > 0)._read_and_chunk(path, size)

        assert [rg.rg_idx for rg in chunks.row_groups] == [0, 1]
        assert not any(rg.fully_matched for rg in chunks.row_groups)

    def test_does_not_abort_sibling_files_in_the_batch(self, tmp_path):
        with_b = _write(
            tmp_path / "with_b.parquet",
            pa.table({"a": [1, 2], "b": [10, 20]}),
            row_group_size=2,
        )
        without_b = _write(
            tmp_path / "without_b.parquet",
            pa.table({"a": [3, 4]}),
            row_group_size=2,
        )

        reader = _reader(filter_expr=col("b") > 0)
        batches = list(
            reader.read_footers(  # pyrefly: ignore[not-callable]
                [with_b, without_b], result_batch_size=1
            )
        )
        by_path = {fc.path: fc for batch in batches for fc in batch}

        assert set(by_path) == {with_b[0], without_b[0]}
        assert all(rg.fully_matched for rg in by_path[with_b[0]].row_groups)
        assert not any(rg.fully_matched for rg in by_path[without_b[0]].row_groups)

    def test_split_exception_keeps_every_group(self, four_row_groups):
        """A raise from the *positive* split must fail closed, not abort."""
        path, size = four_row_groups
        reader = _reader(filter_expr=col("id") >= 50)

        class _Raises:
            def __init__(self, fragment):
                self._fragment = fragment

            def __getattr__(self, name):
                return getattr(self._fragment, name)

            def split_by_row_group(self, expr):
                raise RuntimeError("injected split failure")

        class _Format:
            def __init__(self, real):
                self._real = real

            def make_fragment(self, *args, **kwargs):
                return _Raises(self._real.make_fragment(*args, **kwargs))

        reader.file_format = _Format(reader.file_format)
        chunks = reader._read_and_chunk(path, size)

        assert [rg.rg_idx for rg in chunks.row_groups] == [0, 1, 2, 3]
        assert not any(rg.fully_matched for rg in chunks.row_groups)


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

    def test_none_when_projection_matches_no_leaf(self, four_row_groups):
        # Reachable under schema evolution: the projection names a column this
        # file predates. Empty indices would size the row group at zero bytes and
        # let the packer fill a bin without bound, so it falls back to "all
        # columns" -- an over-count, which only makes read tasks smaller.
        path, _ = four_row_groups
        row_group = pq.ParquetFile(path).metadata.row_group(0)

        assert (
            _reader(projected_cols=["absent"])._projected_leaf_indices(row_group)
            is None
        )

    def test_projection_matching_no_leaf_sizes_all_columns(self, four_row_groups):
        path, size = four_row_groups

        absent = _reader(projected_cols=["absent"])._read_and_chunk(path, size)
        full = _reader()._read_and_chunk(path, size)

        assert [rg.uncompressed_size for rg in absent.row_groups] == [
            rg.uncompressed_size for rg in full.row_groups
        ]
        assert all(rg.uncompressed_size > 0 for rg in absent.row_groups)

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

        assert indices is not None
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
        # as a generator -- no actor needed. Pyrefly still types it as a remote
        # method shell, which is not callable.
        batches = list(
            _reader().read_footers(  # pyrefly: ignore[not-callable]
                three_files, result_batch_size=result_batch_size
            )
        )

        assert [len(b) for b in batches] == expected_batch_sizes
        # Every file appears exactly once regardless of batching.
        paths = [fc.path for batch in batches for fc in batch]
        assert sorted(paths) == sorted(p for p, _ in three_files)

    def test_empty_input_yields_nothing(self):
        # pyrefly: ignore[not-callable]
        assert list(_reader().read_footers([], result_batch_size=1)) == []

    def test_preserve_order_yields_in_listing_order(self, three_files, monkeypatch):
        # Enough concurrency for every read to be in flight at once, and delays
        # that shrink with position, so completion order is the reverse of the
        # listing order and the default path could not yield listing order by luck.
        reader = FooterReader(
            filesystem=LocalFileSystem(), io_concurrency=len(three_files)
        )
        read_and_chunk = reader._read_and_chunk
        delay_by_path = {
            path: 0.05 * (len(three_files) - i)
            for i, (path, _) in enumerate(three_files)
        }

        def delayed_read_and_chunk(path, size):
            time.sleep(delay_by_path[path])
            return read_and_chunk(path, size)

        monkeypatch.setattr(reader, "_read_and_chunk", delayed_read_and_chunk)

        batches = list(
            reader.read_footers(  # pyrefly: ignore[not-callable]
                three_files, result_batch_size=1, preserve_order=True
            )
        )

        paths = [fc.path for batch in batches for fc in batch]
        assert paths == [path for path, _ in three_files]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
