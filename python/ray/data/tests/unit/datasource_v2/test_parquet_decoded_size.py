"""Tests for the Parquet decoded-size estimate, against what a real decode allocates.

Every leaf it sizes must come out exact; a leaf without exact counts falls back.
"""

import datetime
import math
import os
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _BINARY,
    _FIXED_SIZE_LIST,
    _FIXED_WIDTH,
    _LIST,
    _STRUCT,
    _UNSUPPORTED,
    Read,
    _leaf_layout,
    _walk,
    added_bytes,
    build_leaf_profiles,
    estimate_row_group,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
    read_footer_stats,
)
from ray.data._internal.utils.arrow_utils import get_pyarrow_version

# PyArrow 17 writes no SizeStatistics; 23 does (18 to 22 unchecked).
pyarrow_version = get_pyarrow_version()
requires_pyarrow_23 = pytest.mark.skipif(
    pyarrow_version is None or pyarrow_version < parse_version("23.0.0"),
    reason="PyArrow 17 writes no SizeStatistics",
)

N = 5000


def _write(table, path, **kwargs):
    # One row group by default, so each estimate meets exactly one decode.
    kwargs.setdefault("row_group_size", len(table))
    pq.write_table(table, path, **kwargs)
    return path


def _table(values, arrow_type=None, nullable=True):
    array = pa.array(values, arrow_type)
    return pa.table([array], schema=pa.schema([pa.field("a", array.type, nullable)]))


def _allocated(path, columns=None, schema=None, **format_kwargs):
    """Bytes PyArrow allocates scanning ``path`` under ``schema`` if given, else
    under the file's own schema.

    ``get_total_buffer_size``, since ``nbytes`` leaves out a nested array's bitmap.
    """
    from pyarrow.fs import LocalFileSystem

    fragment = pds.ParquetFileFormat(**format_kwargs).make_fragment(
        str(path), filesystem=LocalFileSystem()
    )
    scanner = fragment.scanner(
        schema=schema or fragment.physical_schema, columns=columns
    )
    return scanner.to_table().get_total_buffer_size()


def _read_size_statistics(metadata):
    """Each leaf's ``SizeStatistics``, as the footer reader reads them."""
    return read_footer_stats(metadata)


def _no_size_statistics(metadata):
    """``_read_size_statistics`` for a writer that records none, as PyArrow 17."""
    return None


def _estimate(path, size_statistics=_read_size_statistics, added=(0, 0)):
    """The estimate over ``path``'s modelled leaves, read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema, file_metadata=metadata
    )
    assert profiles is not None
    stats = size_statistics(metadata)
    read = Read(added_row_bytes=added[0], added_row_group_bytes=added[1])
    total = 0
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        leaves = leaf_chunks(
            row_group=row_group,
            leaf_profiles=profiles,
            leaf_indices=range(metadata.num_columns),
            size_stats=None if stats is None else stats[rg_idx],
        )
        total += estimate_row_group(
            row_group=row_group, leaves=leaves.values(), read=read
        )
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


_STRINGS = _table([f"v{i % 5}" for i in range(N)])

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
    # Nulls at each level, and required fields, which add no definition level.
    _case(
        "struct-with-nulls",
        _table(
            [
                None if i % 5 == 0 else {"x": i, "y": None if i % 7 == 0 else f"v{i}"}
                for i in range(N)
            ],
            pa.struct([("x", pa.int64()), ("y", pa.string())]),
        ),
    ),
    _case(
        "required-field-under-nullable-struct",
        _table(
            [None if i % 5 == 0 else {"x": i, "y": f"v{i}"} for i in range(N)],
            pa.struct([pa.field("x", pa.int64(), nullable=False), ("y", pa.string())]),
        ),
    ),
    _case(
        "list-with-nulls",
        _table(
            [
                None if i % 4 == 0 else [None if j == 1 else j for j in range(i % 3)]
                for i in range(N)
            ],
            pa.list_(pa.int64()),
        ),
    ),
    _case(
        "map-with-nulls",
        _table(
            [
                None
                if i % 4 == 0
                else [(f"k{j}", None if j == 1 else j) for j in range(i % 3)]
                for i in range(N)
            ],
            pa.map_(pa.string(), pa.int64()),
        ),
    ),
    _case(
        "fixed-size-list-with-null-elements",
        _table(
            [[None if (i + k) % 3 == 0 else 1.0 for k in range(4)] for i in range(N)],
            pa.list_(pa.float32(), 4),
        ),
    ),
    _case(
        "required-list-of-required-ints",
        _table(
            [list(range(i % 3)) for i in range(N)],
            pa.list_(pa.field("item", pa.int64(), nullable=False)),
            nullable=False,
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


@pytest.mark.parametrize(
    "encoding, slack", [("PLAIN", 1.02), ("DELTA_LENGTH_BYTE_ARRAY", 1.1)]
)
def test_strings_without_a_dictionary_are_bounded_by_the_footer(
    tmp_path, encoding, slack
):
    """Without SizeStatistics, a string chunk with no dictionary page is sized from
    its uncompressed bytes: never below what PyArrow allocates."""
    values = [None if i % 7 == 0 else f"value-{i}" * (i % 4) for i in range(N)]
    path = _write(
        _table(values),
        tmp_path / "f.parquet",
        use_dictionary=False,
        column_encoding={"a": encoding},
    )

    estimate = _estimate(path, size_statistics=_no_size_statistics)
    assert _allocated(path) <= estimate <= slack * _allocated(path)


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
    "created_by, trusted",
    [
        ("parquet-rs version 53.0.0", True),
        ("parquet-mr version 1.14.0 (build abc)", True),
        ("parquet-cpp-arrow version 24.0.0", False),
        ("MyETL 2.0", False),
        (None, False),
    ],
)
def test_delta_byte_array_bytes_trusted_only_from_known_writers(
    tmp_path, created_by, trusted
):
    """Only writers known to count every DELTA_BYTE_ARRAY value have their bytes
    used; any other writer's string leaves fall back."""
    path = _write(_table(["a", "b"]), tmp_path / "f.parquet")
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        file_metadata=SimpleNamespace(created_by=created_by),
    )
    assert profiles is not None
    (profile,) = profiles
    assert profile is not None
    assert profile.counts_delta_byte_array is trusted


@pytest.mark.parametrize(
    "arrow_type, expected",
    [
        (pa.int64(), [([(1, _FIXED_WIDTH)], 0)]),
        (
            pa.struct([("a", pa.int64()), ("b", pa.string())]),
            [([(1, _STRUCT), (2, _FIXED_WIDTH)], 0), ([(1, _STRUCT), (3, _BINARY)], 0)],
        ),
        (
            pa.list_(pa.list_(pa.int64())),
            [([(1, _LIST), (2, _LIST), (3, _FIXED_WIDTH)], 2)],
        ),
        (
            pa.list_(pa.struct([("a", pa.int64()), ("tags", pa.list_(pa.string()))])),
            [
                ([(1, _LIST), (2, _STRUCT), (3, _FIXED_WIDTH)], 1),
                ([(1, _LIST), (2, _STRUCT), (4, _LIST), (5, _BINARY)], 2),
            ],
        ),
        (
            pa.map_(pa.string(), pa.int64()),
            [
                ([(1, _LIST), (2, _STRUCT), (3, _BINARY)], 1),
                ([(1, _LIST), (2, _STRUCT), (4, _FIXED_WIDTH)], 1),
            ],
        ),
        (
            pa.list_(pa.float32(), 4),
            [([(1, _FIXED_SIZE_LIST), (2, _FIXED_WIDTH)], 1)],
        ),
    ],
    ids=["int", "struct", "list-of-list", "list-of-struct", "map", "fixed-size-list"],
)
def test_walk_records_each_leaf_with_its_ancestors(arrow_type, expected):
    """Each leaf's ``(node_id, kind)`` from the top down, and its list depth; leaves
    under one struct or list share its node."""
    walk = _walk(pa.schema([("c", arrow_type)]))
    leaves = [
        ([(node.node_id, node.kind) for node in chain], depth)
        for chain, depth in walk.leaves
    ]
    assert leaves == expected


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


@pytest.mark.parametrize(
    "table, format_kwargs, expected",
    [
        (_STRINGS, {"dictionary_columns": ["a"]}, None),
        (_STRINGS, {"binary_type": pa.large_binary()}, None),
        (pa.table({"a": _STRINGS["a"].dictionary_encode()}), {}, [None]),
    ],
    ids=["dictionary-columns", "binary-type", "stored-dictionary"],
)
def test_dictionaries_and_type_options_fall_back(
    tmp_path, table, format_kwargs, expected
):
    """Read options the reader drops on a cast leave the file unmodelled; a stored
    dictionary leaves its leaf."""
    path = _write(table, tmp_path / "f.parquet", store_schema=not format_kwargs)
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        pinned_schema=None,
        format_kwargs=format_kwargs,
        file_metadata=metadata,
    )
    assert profiles == expected


def test_pinned_struct_with_reordered_fields_is_unmodelled(tmp_path):
    """The cast matches struct fields by name: reordered, the pinned types don't
    line up with the stored leaves, so those leaves fall back."""
    a, b = pa.field("a", pa.list_(pa.int64())), pa.field("b", pa.list_(pa.int32()))
    path = _write(
        pa.table({"s": pa.array([{"a": [1, 2, 3], "b": [4]}] * N, pa.struct([a, b]))}),
        tmp_path / "f.parquet",
    )
    metadata = pq.read_metadata(str(path))

    def profiles(*fields):
        return build_leaf_profiles(
            parquet_schema=metadata.schema,
            pinned_schema=pa.schema([pa.field("s", pa.struct(list(fields)))]),
            file_metadata=metadata,
        )

    widened = pa.field("b", pa.list_(pa.int64()))
    mapped = profiles(a, widened)
    assert mapped is not None and None not in mapped
    assert profiles(widened, a) == [None, None]


def test_added_columns_are_bounded(tmp_path):
    """The path, a Hive value cut from it, and a column the pin fills with nulls."""
    path = str(tmp_path / "year=2024" / "f.parquet")
    os.makedirs(os.path.dirname(path))
    _write(_STRINGS, path)
    fields = [pa.field(name, pa.string()) for name in ("path", "year", "notes")]
    added = added_bytes(added=fields, path=path)
    held = pa.table(
        {
            "path": pa.array([path] * N),
            "year": pa.array(["2024"] * N),
            "notes": pa.nulls(N, pa.string()),
        }
    ).get_total_buffer_size()

    estimate = _estimate(path, added=added) - _estimate(path)
    assert estimate == math.ceil(N * added[0]) + added[1]
    assert 1 <= estimate / held <= 1.1
    # A type with no bit width still counts.
    union = pa.field("u", pa.dense_union([pa.field("x", pa.int64())]))
    assert added_bytes(added=[union], path=path)[0] > 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
