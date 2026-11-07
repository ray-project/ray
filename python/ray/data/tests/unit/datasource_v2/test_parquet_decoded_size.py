"""Tests for the Parquet decoded-size estimate, against what a real decode allocates.

Every leaf it sizes must come out exact; a leaf without exact counts falls back.
"""

import datetime
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _UNSUPPORTED,
    _leaf_layout,
    build_leaf_profiles,
    estimate_row_group,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
    read_footer_stats,
)

pa = pytest.importorskip("pyarrow")
pds = pytest.importorskip("pyarrow.dataset")
pq = pytest.importorskip("pyarrow.parquet")

# PyArrow 17 writes no SizeStatistics and lacks ``binary_type`` and ``list_type``;
# 23 has all of them (18 to 22 unchecked).
requires_pyarrow_23 = pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("23.0.0"),
    reason="PyArrow 17 writes no SizeStatistics",
)

N = 5000


def _write(table, path, **kwargs):
    # One row group by default, so each estimate meets exactly one decode.
    kwargs.setdefault("row_group_size", len(table))
    pq.write_table(table, path, **kwargs)
    return path


def _table(values, arrow_type=None):
    return pa.table({"a": pa.array(values, arrow_type)})


def _allocated(path, columns=None):
    """Bytes PyArrow allocates scanning ``path`` as a Ray read task does.

    ``get_total_buffer_size``, since ``nbytes`` leaves out a nested array's bitmap.
    """
    from pyarrow.fs import LocalFileSystem

    fragment = pds.ParquetFileFormat().make_fragment(
        str(path), filesystem=LocalFileSystem()
    )
    scanner = fragment.scanner(schema=fragment.physical_schema, columns=columns)
    return scanner.to_table().get_total_buffer_size()


def _read_size_statistics(metadata):
    """Each leaf's ``SizeStatistics``, as the footer reader reads them."""
    return read_footer_stats(metadata)


def _no_size_statistics(metadata):
    """``_read_size_statistics`` for a writer that records none, as PyArrow 17."""
    return None


def _estimate(path, size_statistics=_read_size_statistics):
    """The estimate over ``path``'s modelled leaves, read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema, file_metadata=metadata
    )
    assert profiles is not None
    stats = size_statistics(metadata)
    total = 0
    for rg_idx in range(metadata.num_row_groups):
        leaves = leaf_chunks(
            row_group=metadata.row_group(rg_idx),
            leaf_profiles=profiles,
            leaf_indices=range(metadata.num_columns),
            size_stats=None if stats is None else stats[rg_idx],
        )
        total += estimate_row_group(leaves=leaves)
    return total


def _case(label, table, stats=True, write=None):
    """A ``_CASES`` row, whose estimate must equal what PyArrow allocates."""
    case = SimpleNamespace(table=table, stats=stats, write=write or {})
    return pytest.param(case, id=label, marks=[requires_pyarrow_23] if stats else [])


# Fixed-width types, each two values repeated.
_MORE_FIXED_WIDTH_TYPES = [
    (pa.int8(), [1, 2]),
    (pa.int16(), [1, 2]),
    (pa.uint8(), [1, 2]),
    (pa.uint16(), [1, 2]),
    (pa.int32(), [1, 2]),
    (pa.uint32(), [1, 2]),
    (pa.uint64(), [1, 2]),
    (pa.float32(), [1.0, 2.0]),
    (pa.date32(), [datetime.date(2020, 1, 1), datetime.date(2021, 2, 3)]),
    # Stored as INT32 days and decoded as date32, so 4 bytes, not 8.
    (pa.date64(), [datetime.date(2020, 1, 1), datetime.date(2021, 2, 3)]),
    (pa.time64("us"), [1, 2]),
    (pa.timestamp("us"), [1, 2]),
    (pa.duration("ms"), [1, 2]),
    (pa.decimal128(10, 2), [Decimal("1.00"), Decimal("2.50")]),
    (pa.decimal256(50, 2), [Decimal("1.00"), Decimal("2.50")]),
    (pa.binary(7), [b"1234567", b"abcdefg"]),
    (pa.float16(), [np.float16(1.0), np.float16(2.0)]),
]


def _tensor_table():
    from ray.data.extensions.tensor_extension import ArrowTensorArray

    return pa.table({"a": ArrowTensorArray.from_numpy(np.zeros((N, 4), np.float32))})


_CASES = [
    # With SizeStatistics, every array is exact. Two leaves under one list count
    # its offsets once.
    _case(
        "list-of-struct",
        _table(
            [[{"x": j, "y": "q"} for j in range(i % 3)] for i in range(N)],
            pa.list_(pa.struct([("x", pa.int64()), ("y", pa.string())])),
        ),
    ),
    _case(
        "map",
        _table(
            [[(f"k{j}", j) for j in range(i % 3)] for i in range(N)],
            pa.map_(pa.string(), pa.int64()),
        ),
    ),
    # Written as a repeated group, but decoded without an offsets buffer.
    _case("fixed-size-list", _table([[0.0] * 4] * N, pa.list_(pa.float32(), 4))),
    # Ray's default tensor type: an extension over large_list<float>.
    _case("tensor", _tensor_table()),
    _case("bool", _table([i % 2 == 0 for i in range(N)])),
    # 64-bit offsets, recovered from the stored Arrow schema.
    _case("large-string", _table([f"v{i}" for i in range(N)], pa.large_string())),
    # Converted, not zero-copied, so it drops the all-valid bitmap.
    _case(
        "int96-timestamp",
        _table([1, 2] * (N // 2), pa.timestamp("ns")),
        write={"use_deprecated_int96_timestamps": True},
    ),
    *(
        _case(f"type-{arrow_type}", _table(values * (N // 2), arrow_type))
        for arrow_type, values in _MORE_FIXED_WIDTH_TYPES
    ),
    # Without SizeStatistics a flat fixed-width leaf stays exact: the footer's
    # null count sizes its bitmap.
    _case(
        "ints-with-nulls-without-size-statistics",
        _table([None if i % 3 == 0 else i for i in range(N)], pa.int64()),
        stats=False,
    ),
]


@pytest.mark.parametrize("case", _CASES)
def test_estimate_against_pyarrow(tmp_path, case):
    path = _write(case.table, tmp_path / "f.parquet", **case.write)
    estimate = _estimate(
        path,
        size_statistics=_read_size_statistics if case.stats else _no_size_statistics,
    )

    assert estimate == _allocated(path)


def test_inexact_leaves_without_size_statistics_fall_back(tmp_path):
    """Without SizeStatistics, string and list leaves fall back; the int64 stays exact."""
    table = pa.table(
        {
            "i": pa.array(np.arange(N, dtype="int64")),
            "s": pa.array([f"v{i}" for i in range(N)]),
            "l": pa.array([[i] * (i % 3) for i in range(N)]),
        }
    )
    path = _write(table, tmp_path / "f.parquet")

    assert _estimate(path, size_statistics=_no_size_statistics) == _allocated(
        path, columns=["i"]
    )


def _with(stats_for_leaf_0):
    """``_read_size_statistics`` replaced by ``stats_for_leaf_0(stats)``."""

    def size_statistics(metadata):
        all_stats = _read_size_statistics(metadata)
        assert all_stats is not None
        (stats,) = all_stats[0]
        return [[stats_for_leaf_0(stats)]]

    return size_statistics


@requires_pyarrow_23
def test_partial_size_statistics(tmp_path):
    """Histograms alone size a leaf; ones that disagree with the footer are dropped."""
    ints = _write(_table(np.arange(N, dtype="int64")), tmp_path / "ints.parquet")
    lists = _table(
        [[[1, 2], [], None][: i % 4] for i in range(N)], pa.list_(pa.list_(pa.int64()))
    )
    lists = _write(lists, tmp_path / "lists.parquet")

    def off_by_one(stats):
        definition = stats.definition_level_histogram
        return stats._replace(
            definition_level_histogram=definition[:-1] + (definition[-1] + 1,)
        )

    # Values plus the all-valid bitmap PyArrow keeps on a zero-copy int64.
    histogram_only = _with(
        lambda stats: LeafSizeStats(
            unencoded_byte_array_data_bytes=None,
            repetition_level_histogram=(),
            definition_level_histogram=(0, N),
        )
    )
    assert _estimate(ints, size_statistics=histogram_only) == N * 8 + N // 8
    assert _estimate(lists, size_statistics=_with(off_by_one)) == 0


def test_delta_byte_array_without_sizes_is_unmodelled(tmp_path):
    """DELTA_BYTE_ARRAY strings without trusted bytes, which parquet-cpp never
    writes, are left to the caller; the int64 beside them stays exact."""
    table = pa.table(
        {
            "i": pa.array(np.arange(N, dtype="int64")),
            "s": pa.array([f"s{i % 101}" for i in range(N)]),
        }
    )
    path = _write(
        table,
        tmp_path / "f.parquet",
        use_dictionary=False,
        column_encoding={"s": "DELTA_BYTE_ARRAY"},
    )
    expected = _allocated(path, columns=["i"])

    assert _estimate(path) == expected
    assert _estimate(path, size_statistics=_no_size_statistics) == expected


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.list_view(pa.int32()),
        pa.string_view(),
        pa.dictionary(pa.int32(), pa.string()),
    ],
)
def test_unmodeled_arrow_type_is_unsupported(arrow_type):
    """A layout this module can't size, which leaves the leaf to the caller."""
    assert _leaf_layout(arrow_type)[0] == _UNSUPPORTED


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
