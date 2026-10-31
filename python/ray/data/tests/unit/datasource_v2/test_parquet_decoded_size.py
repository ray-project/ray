"""Tests for the Parquet decoded-size estimate, against what a real decode allocates.

With ``SizeStatistics`` the estimate is exact; otherwise each bound or guess is
held to its documented range.
"""

import datetime
import math
import os
from decimal import Decimal

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _leaf_layout,
    _UnsupportedLayout,
    added_bytes,
    build_leaf_profiles,
    dictionary_page_read_size,
    estimate_row_group_decoded_size,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    DICTIONARY_PAGE_HEADER_READ_BYTES,
    LeafSizeStats,
    read_dictionary_page,
    read_size_statistics,
)

pa = pytest.importorskip("pyarrow")
pds = pytest.importorskip("pyarrow.dataset")
pq = pytest.importorskip("pyarrow.parquet")

# PyArrow 17 writes no SizeStatistics or view types and lacks ``binary_type`` and
# ``list_type``; 23 has all of them (18 to 22 unchecked).
requires_pyarrow_23 = pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("23.0.0"),
    reason="PyArrow 17 writes no SizeStatistics or view types",
)

N = 5000

# Fixed-width types beyond those in ``_cases``, each two values repeated.
_MORE_FIXED_WIDTH_TYPES = [
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
    (pa.duration("ms"), [1, 2]),
    (pa.decimal256(50, 2), [Decimal("1.00"), Decimal("2.50")]),
    (pa.binary(7), [b"1234567", b"abcdefg"]),
    (pa.float16(), np.array([1.0, 2.0], dtype=np.float16)),
]


def _tensor_table():
    from ray.data.extensions.tensor_extension import ArrowTensorArray

    return pa.table({"t": ArrowTensorArray.from_numpy(np.zeros((N, 4), np.float32))})


def _cases():
    """Every leaf layout and nesting shape, one row group each."""
    cases = {
        "int64-plain": pa.table({"a": pa.array(np.arange(N, dtype="int64"))}),
        # Dictionary-encoded on disk, so uncompressed size badly understates it.
        "int64-low-cardinality": pa.table({"a": pa.array(np.zeros(N, dtype="int64"))}),
        "float64": pa.table({"a": pa.array(np.random.rand(N))}),
        "bool": pa.table({"a": pa.array([i % 2 == 0 for i in range(N)])}),
        "string-low-cardinality": pa.table(
            {"a": pa.array([f"v{i % 5}" for i in range(N)])}
        ),
        "string-high-cardinality": pa.table(
            {"a": pa.array([f"val_{i}_{i * 7}" for i in range(N)])}
        ),
        "string-nullable-dict": pa.table(
            {"a": pa.array([None if i % 3 == 0 else f"v{i % 4}" for i in range(N)])}
        ),
        "string-large-values": pa.table(
            {"a": pa.array(["x" * 50_000 for _ in range(20)])}
        ),
        # 64-bit offsets, recovered from the embedded Arrow schema.
        "large-string": pa.table(
            {"a": pa.array([f"v{i}" for i in range(N)], pa.large_string())}
        ),
        "mixed-int-string": pa.table(
            {
                "i": pa.array(np.arange(N, dtype="int64")),
                "s": pa.array([f"s{i % 101}" for i in range(N)]),
            }
        ),
        "list-int64": pa.table(
            {"a": pa.array([[1, 2, 3]] * N, type=pa.list_(pa.int64()))}
        ),
        # Null and empty lists write a level entry but own no child slot.
        "list-empty-and-null": pa.table(
            {
                "a": pa.array(
                    [None if i % 5 == 0 else [1, 2][: i % 3] for i in range(N)],
                    type=pa.list_(pa.int64()),
                )
            }
        ),
        "list-of-list": pa.table(
            {
                "a": pa.array(
                    [[[1, 2], [], None][: i % 4] for i in range(N)],
                    type=pa.list_(pa.list_(pa.int64())),
                )
            }
        ),
        # Written as a repeated group, but decoded without an offsets buffer.
        "fixed-size-list": pa.table(
            {
                "a": pa.FixedSizeListArray.from_arrays(
                    pa.array(np.zeros(N * 4, dtype=np.float32)), 4
                )
            }
        ),
        # Ray's default tensor type: extension over large_list<float>.
        "tensor": _tensor_table(),
        "decimal128": pa.table({"a": pa.array(range(N), type=pa.decimal128(10, 2))}),
        "timestamp": pa.table(
            {"a": pa.array(np.arange(N, dtype="int64"), type=pa.timestamp("us"))}
        ),
        "int8": pa.table({"a": pa.array([i % 100 for i in range(N)], type=pa.int8())}),
        "int16": pa.table(
            {"a": pa.array([i % 3000 for i in range(N)], type=pa.int16())}
        ),
        "struct-string-list": pa.table(
            {
                "a": pa.array(
                    [{"s": f"v{i % 7}", "l": [1, 2]} for i in range(N)],
                    type=pa.struct([("s", pa.string()), ("l", pa.list_(pa.int64()))]),
                )
            }
        ),
        # A bitmap on the struct and another on the child, which also marks the
        # slots under a null struct.
        "struct-nulls-at-two-levels": pa.table(
            {
                "a": pa.array(
                    [
                        None if i % 3 == 0 else {"x": None if i % 5 == 0 else i}
                        for i in range(N)
                    ],
                    type=pa.struct([("x", pa.int64())]),
                )
            }
        ),
        # Two leaves under one list: its offsets must be counted once.
        "list-of-struct": pa.table(
            {
                "a": pa.array(
                    [[{"x": j, "y": "q"} for j in range(i % 3)] for i in range(N)],
                    type=pa.list_(pa.struct([("x", pa.int64()), ("y", pa.string())])),
                )
            }
        ),
        "map": pa.table(
            {
                "a": pa.array(
                    [[(f"k{j}", j) for j in range(i % 3)] for i in range(N)],
                    type=pa.map_(pa.string(), pa.int64()),
                )
            }
        ),
        # Required leaves carry no SizeStatistics at all.
        "required-columns": pa.table(
            {
                "i": pa.array(np.arange(N, dtype="int64")),
                "st": pa.array(
                    [{"x": i} for i in range(N)],
                    type=pa.struct([pa.field("x", pa.int32(), nullable=False)]),
                ),
                "s": pa.array([f"s{i}" for i in range(N)]),
            },
            schema=pa.schema(
                [
                    pa.field("i", pa.int64(), nullable=False),
                    pa.field(
                        "st",
                        pa.struct([pa.field("x", pa.int32(), nullable=False)]),
                        nullable=False,
                    ),
                    pa.field("s", pa.string()),
                ]
            ),
        ),
    }
    for arrow_type, values in _MORE_FIXED_WIDTH_TYPES:
        array = pa.array(values, type=arrow_type)
        cases[f"type-{arrow_type}"] = pa.table(
            {"a": pa.concat_arrays([array] * (N // 2))}
        )
    return cases


def _write(table, path, **kwargs):
    # One row group by default, so each estimate meets exactly one decode.
    kwargs.setdefault("row_group_size", len(table))
    pq.write_table(table, path, **kwargs)
    return path


def _allocated(path, columns=None, schema=None, **format_kwargs):
    """Bytes PyArrow allocates scanning ``path`` as a Ray read task does.

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


def _no_size_statistics(metadata):
    """``read_size_statistics`` for a writer that records none, as PyArrow 17."""
    return None


def _estimate(
    path,
    leaf_indices=None,
    size_statistics=read_size_statistics,
    read_pages=True,
    schema=None,
    format_kwargs=None,
    added=(0, 0),
):
    """The estimate over ``path``'s row groups, read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        metadata.schema,
        pinned_schema=schema,
        format_kwargs=format_kwargs,
        file_metadata=metadata,
    )
    assert profiles is not None
    stats = size_statistics(metadata)
    total = 0
    with open(path, "rb") as f:
        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            rg_stats = None if stats is None else stats[rg_idx]
            pages = {}
            for leaf_idx in range(row_group.num_columns) if read_pages else ():
                chunk = row_group.column(leaf_idx)
                leaf_stats = None if rg_stats is None else rg_stats[leaf_idx]
                nbytes = dictionary_page_read_size(
                    profiles[leaf_idx], chunk, leaf_stats
                )
                if nbytes:
                    f.seek(chunk.dictionary_page_offset)
                    page = read_dictionary_page(f.read(nbytes))
                    assert page is not None
                    pages[leaf_idx] = page
            estimate = estimate_row_group_decoded_size(
                row_group,
                profiles,
                leaf_indices=leaf_indices,
                size_stats=rg_stats,
                dictionary_pages=pages,
                format_kwargs=format_kwargs,
                added=added,
            )
            assert estimate is not None
            total += estimate
    return total


# ---------------------------------------------------------------------------
# The file as stored: exact with SizeStatistics
# ---------------------------------------------------------------------------


@requires_pyarrow_23
@pytest.mark.parametrize("label", list(_cases()))
def test_exact_with_size_statistics(tmp_path, label):
    path = _write(_cases()[label], tmp_path / "f.parquet")

    assert _estimate(path) == _allocated(path)


@requires_pyarrow_23
def test_projection_sizes_only_its_leaves(tmp_path):
    """A projection sizes only its leaves, counting a shared list's offsets once."""
    mixed = _write(_cases()["mixed-int-string"], tmp_path / "mixed.parquet")
    nested = _write(_cases()["list-of-struct"], tmp_path / "nested.parquet")

    assert _estimate(mixed, [0]) + _estimate(mixed, [1]) == _estimate(mixed)
    assert _estimate(mixed, [0]) == _allocated(mixed, columns=["i"])
    # The list's offsets, once per single-leaf estimate.
    shared = 4 * (N + 1)
    assert _estimate(nested, [0]) + _estimate(nested, [1]) - shared == _estimate(nested)


@requires_pyarrow_23
def test_row_groups_sum_to_the_file(tmp_path):
    table = _cases()["mixed-int-string"]
    path = _write(table, tmp_path / "f.parquet", row_group_size=len(table) // 4)

    assert pq.read_metadata(str(path)).num_row_groups == 4
    assert _estimate(path) == _allocated(path)


@pytest.mark.parametrize(
    "arrow_type, values, write_kwargs, keeps_bitmap",
    [
        (pa.int64(), [1, 2], {}, True),
        (pa.float32(), [1.0, 2.0], {}, True),
        (pa.timestamp("ns"), [1, 2], {}, True),
        (pa.int8(), [1, 2], {}, False),
        (pa.date32(), [datetime.date(2020, 1, 1)] * 2, {}, False),
        pytest.param(pa.string(), ["a", "b"], {}, False, marks=requires_pyarrow_23),
        # INT96 timestamps are converted, not zero-copied, so they drop it.
        pytest.param(
            pa.timestamp("ns"),
            [1, 2],
            {"use_deprecated_int96_timestamps": True},
            False,
            marks=requires_pyarrow_23,
        ),
    ],
)
def test_null_free_bitmap_follows_pyarrow(
    tmp_path, arrow_type, values, write_kwargs, keeps_bitmap
):
    """PyArrow keeps an all-valid bitmap only on types it decodes zero-copy."""
    table = pa.table({"a": pa.array(values * (N // 2), type=arrow_type)})
    path = _write(table, tmp_path / "f.parquet", **write_kwargs)

    decoded = pq.ParquetFile(str(path)).read_row_group(0).column(0).chunk(0)
    assert (decoded.buffers()[0] is not None) == keeps_bitmap
    assert _estimate(path) == _allocated(path)


def _with(stats_for_leaf_0):
    """``read_size_statistics`` replaced by ``stats_for_leaf_0(stats)``."""

    def size_statistics(metadata):
        all_stats = read_size_statistics(metadata)
        assert all_stats is not None
        (stats,) = all_stats[0]
        return [[stats_for_leaf_0(stats)]]

    return size_statistics


@requires_pyarrow_23
@pytest.mark.parametrize(
    "label, exact",
    [("list-int64", True), ("list-empty-and-null", True), ("list-of-list", False)],
)
def test_list_leaf_without_repetition_histogram(tmp_path, label, exact):
    """A list needs only the definition histogram; a list in a list, both."""
    path = _write(_cases()[label], tmp_path / "f.parquet")
    stripped = _with(lambda stats: stats._replace(repetition_level_histogram=()))

    if exact:
        assert _estimate(path, size_statistics=stripped) == _allocated(path)
    else:
        assert _estimate(path, size_statistics=stripped) > _allocated(path)


@requires_pyarrow_23
def test_partial_size_statistics(tmp_path):
    """Histograms that disagree with the footer's counts are dropped."""
    ints = _write(_cases()["int64-plain"], tmp_path / "ints.parquet")
    lists = _write(_cases()["list-of-list"], tmp_path / "lists.parquet")

    def off_by_one(stats):
        definition = stats.definition_level_histogram
        return stats._replace(
            definition_level_histogram=definition[:-1] + (definition[-1] + 1,)
        )

    # Values plus the all-valid bitmap PyArrow keeps on a zero-copy int64.
    histogram_only = _with(lambda stats: LeafSizeStats(None, (), (0, N)))
    assert _estimate(ints, size_statistics=histogram_only) == N * 8 + N // 8
    assert _estimate(lists, size_statistics=_with(off_by_one)) == _estimate(
        lists, size_statistics=_no_size_statistics
    )


def test_unmodeled_arrow_type_is_rejected():
    """A layout this module can't size raises, which sends the file to fallback."""
    with pytest.raises(_UnsupportedLayout):
        _leaf_layout(pa.list_view(pa.int32()))


# ---------------------------------------------------------------------------
# The file as stored: bounds and guesses
# ---------------------------------------------------------------------------
#
# An upper bound some column meets exactly is shaved to 0.95 of itself, so a
# bound's floor is 0.95 times the bytes held.


@requires_pyarrow_23
@pytest.mark.parametrize("arrow_type", [pa.string_view(), pa.binary_view()])
@pytest.mark.parametrize(
    "length, high", [(0, 1), (1, 1.0625), (12, 1.75), (13, 1), (60, 1)]
)
def test_view_types_are_bounded(tmp_path, arrow_type, length, high):
    """Views count every value out of line: exact past 12 bytes, 1.75x at 12."""
    table = pa.table({"a": pa.array(["x" * length] * N, arrow_type)})
    path = _write(table, tmp_path / "f.parquet")

    assert 0.95 <= _estimate(path) / _allocated(path) <= high


def _arrow_dictionary(distinct, length, n=N, value_type=None, null_every=0):
    values = pa.array([f"{i:0{length}d}" for i in range(distinct)], value_type)
    indices = pa.array(
        [
            None if null_every and i % null_every == 0 else i % distinct
            for i in range(n)
        ],
        pa.int32(),
    )
    return pa.DictionaryArray.from_arrays(indices, values)


def _duplicated_dictionary(rows):
    """100 distinct entries and a copy of the first, one row per index in ``rows``."""
    values = pa.array([f"{i:012d}" for i in range(100)] + [f"{0:012d}"])
    indices = pa.array([i % 101 for i in rows], pa.int32())
    return pa.DictionaryArray.from_arrays(indices, values)


def _reversed(array):
    return pa.DictionaryArray.from_arrays(array.indices, array.dictionary[::-1])


@pytest.mark.parametrize(
    "table, write_kwargs, high",
    [
        (pa.table({"a": _arrow_dictionary(50, 200)}), {}, 1.25),
        (
            pa.table({"a": _arrow_dictionary(50, 200, value_type=pa.large_string())}),
            {},
            1.25,
        ),
        (pa.table({"a": _arrow_dictionary(50, 12, null_every=3)}), {}, 1.25),
        # PyArrow writes the Arrow dictionary whole: one row decodes all 1000.
        (pa.table({"a": _arrow_dictionary(1000, 12, n=1)}), {}, 1.25),
        (pa.table({"a": _arrow_dictionary(50, 60)}), {"compression": "zstd"}, 1.25),
        (pa.table({"a": _arrow_dictionary(50, 12)}), {"version": "1.0"}, 1.25),
        (
            pa.table({"a": _arrow_dictionary(500, 12)}),
            {"row_group_size": N // 4},
            1.25,
        ),
        (
            pa.table(
                {"s": pa.StructArray.from_arrays([_arrow_dictionary(50, 12)], ["d"])}
            ),
            {},
            1.25,
        ),
        # A duplicate entry makes PyArrow write every page PLAIN; the unused
        # dictionary page still lists every value.
        (pa.table({"a": _duplicated_dictionary(range(N))}), {}, 5),
        # Chunks with dictionaries of their own make the writer keep the first
        # as the dictionary page and write the rest PLAIN, every repeat of a
        # value included.
        (
            pa.table(
                {
                    "a": pa.chunked_array(
                        [
                            _arrow_dictionary(50, 100),
                            _reversed(_arrow_dictionary(50, 100)),
                        ]
                    )
                }
            ),
            {},
            15,
        ),
    ],
    ids=[
        "string",
        "large-string",
        "nulls",
        "one-row-whole-dictionary",
        "zstd",
        "format-1.0",
        "row-groups",
        "in-struct",
        "unused-dictionary-page",
        "writer-gave-up",
    ],
)
def test_arrow_dictionary_is_bounded_not_hydrated(tmp_path, table, write_kwargs, high):
    """An Arrow dictionary is bounded by the chunk's pages, not sized hydrated."""
    path = _write(table, tmp_path / "f.parquet", **write_kwargs)

    assert 0.95 <= _estimate(path) / _allocated(path) <= high


@requires_pyarrow_23
@pytest.mark.parametrize(
    "column, expected",
    [
        (pa.array([f"v{i % 5}" for i in range(N)]), DICTIONARY_PAGE_HEADER_READ_BYTES),
        (
            pa.array([f"v{i % 5}" for i in range(N)], pa.string_view()),
            DICTIONARY_PAGE_HEADER_READ_BYTES,
        ),
        # Sized from the chunk's bytes alone.
        (_arrow_dictionary(5, 2), 0),
        (pa.array(np.zeros(N, dtype="int64")), 0),
    ],
    ids=["string", "view", "arrow-dictionary", "int64"],
)
def test_dictionary_page_read_size(tmp_path, column, expected):
    """Only dictionary-indexed strings without footer bytes read the page header."""
    path = _write(pa.table({"a": column}), tmp_path / "f.parquet")
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(metadata.schema)
    size_stats = read_size_statistics(metadata)
    assert profiles is not None and size_stats is not None
    (profile,) = profiles
    (stats,) = size_stats[0]
    chunk = metadata.row_group(0).column(0)

    assert chunk.has_dictionary_page
    assert dictionary_page_read_size(profile, chunk, stats) == 0
    assert dictionary_page_read_size(profile, chunk, None) == expected


@pytest.mark.parametrize("label", list(_cases()))
def test_without_size_statistics(tmp_path, label):
    """Without SizeStatistics, flat columns stay exact and the rest within range."""
    table = _cases()[label]
    path = _write(table, tmp_path / "f.parquet")
    estimate = _estimate(path, size_statistics=_no_size_statistics)
    actual = _allocated(path)

    flat_fixed_width = all(
        pa.types.is_primitive(field.type)
        or pa.types.is_decimal(field.type)
        or pa.types.is_fixed_size_binary(field.type)
        for field in table.schema
    )
    if flat_fixed_width:
        assert estimate == actual
    else:
        assert 0.95 <= estimate / actual <= 1.5


def _skewed(common, rare):
    """``common`` in every row but the ``rare`` ones, each distinct."""
    return pa.table({"a": pa.array([common] * (N - len(rare)) + rare)})


_HIGH_CARDINALITY = pa.table({"a": pa.array([f"{i:012d}" for i in range(N)])})


@pytest.mark.parametrize(
    "table, write_kwargs, read_pages, low, high",
    [
        # Values of one length, so 1.2 times the mean is 1.2 times over...
        (_HIGH_CARDINALITY, {"dictionary_pagesize_limit": 4096}, True, 1.1, 1.2),
        # ...and without the page header, each value as long as the chunk.
        (_HIGH_CARDINALITY, {}, False, 100, math.inf),
        # The guess's known gap: nothing says which values most rows hold.
        (_skewed("a", [f"{i:0100d}" for i in range(99)]), {}, True, 10, math.inf),
        (_skewed("x" * 100, [f"{i:02d}" for i in range(99)]), {}, True, 0, 0.1),
        # PLAIN pages: every value's bytes, less a 4-byte length each.
        (_HIGH_CARDINALITY, {"use_dictionary": False}, True, 0.95, 1),
        (
            _HIGH_CARDINALITY,
            {"use_dictionary": False, "column_encoding": "DELTA_LENGTH_BYTE_ARRAY"},
            True,
            0.95,
            1.05,
        ),
    ],
    ids=[
        "dictionary-fallback",
        "page-header-unread",
        "common-value-short",
        "common-value-long",
        "plain",
        "delta-length",
    ],
)
def test_string_bytes_without_size_statistics(
    tmp_path, table, write_kwargs, read_pages, low, high
):
    path = _write(table, tmp_path / "f.parquet", **write_kwargs)
    estimate = _estimate(
        path, size_statistics=_no_size_statistics, read_pages=read_pages
    )

    assert low <= estimate / _allocated(path) <= high


def test_delta_byte_array_is_a_guess(tmp_path):
    """DELTA_BYTE_ARRAY values are a guess; the int64 beside them stays exact."""
    path = _write(
        _cases()["mixed-int-string"],
        tmp_path / "f.parquet",
        use_dictionary=False,
        column_encoding={"s": "DELTA_BYTE_ARRAY"},
    )
    strings = pq.read_metadata(str(path)).row_group(0).column(1)
    expected = (
        _allocated(path, columns=["i"])
        + 4 * (N + 1)
        + 128 * strings.total_uncompressed_size
    )

    assert _estimate(path) == expected
    assert _estimate(path, size_statistics=_no_size_statistics) == expected


# ---------------------------------------------------------------------------
# Ray's read: the schema pin, read options and added columns
# ---------------------------------------------------------------------------


def _nulls_hide_a_dictionary():
    """A struct with nulls over a non-nullable dictionary field."""
    field = pa.field("d", pa.dictionary(pa.int32(), pa.string()), nullable=False)
    mask = pa.array([i % 3 == 0 for i in range(N)])
    return pa.StructArray.from_arrays(
        [_arrow_dictionary(50, 12)], fields=[field], mask=mask
    )


_STRINGS = pa.array([f"v{i % 5}" for i in range(N)])
_LISTS = pa.array([[1, 2]] * N, pa.list_(pa.int64()))


@requires_pyarrow_23
@pytest.mark.parametrize(
    "column, schema_type, format_kwargs, high",
    [
        # The pin, cast the leaves follow one for one.
        (pa.array(range(N), pa.int32()), pa.int64(), {}, 1.02),
        (_STRINGS, pa.large_string(), {}, 1),
        (_LISTS, pa.large_list(pa.int64()), {}, 1),
        (_arrow_dictionary(50, 12), pa.string(), {}, 1),
        # Numbers cast to text: sized as stored plus a crude bound.
        (pa.array(range(N), pa.int64()), pa.string(), {}, 1.25),
        # Each slot the null struct hides copies a dictionary value.
        (
            _nulls_hide_a_dictionary(),
            pa.struct([pa.field("d", pa.string(), nullable=False)]),
            {},
            math.inf,
        ),
        # Read options, on a file without a stored Arrow schema.
        (_STRINGS, None, {"dictionary_columns": ["a"]}, 1.1),
        (_STRINGS, None, {"binary_type": pa.large_binary()}, 1),
        (_STRINGS, None, {"binary_type": pa.binary_view()}, 1.1),
        (_LISTS, None, {"list_type": pa.LargeListType}, 1),
    ],
    ids=[
        "int32-to-int64",
        "string-to-large-string",
        "list-to-large-list",
        "dictionary-to-string",
        "int64-to-string",
        "hidden-dictionary-slots",
        "dictionary-columns",
        "binary-type-large",
        "binary-type-view",
        "list-type-large",
    ],
)
def test_ray_read(tmp_path, column, schema_type, format_kwargs, high):
    # The read options retype only a file without a stored Arrow schema.
    path = _write(
        pa.table({"a": column}), tmp_path / "f.parquet", store_schema=not format_kwargs
    )
    schema = None if schema_type is None else pa.schema([("a", schema_type)])
    estimate = _estimate(path, schema=schema, format_kwargs=format_kwargs)

    assert 1 <= estimate / _allocated(path, schema=schema, **format_kwargs) <= high


def test_added_columns_are_bounded(tmp_path):
    """The path, a Hive value cut from it, and a column the pin fills with nulls."""
    path = str(tmp_path / "year=2024" / "f.parquet")
    os.makedirs(os.path.dirname(path))
    _write(pa.table({"a": _STRINGS}), path)
    fields = [pa.field(name, pa.string()) for name in ("path", "year", "notes")]
    added = added_bytes(fields, path)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
