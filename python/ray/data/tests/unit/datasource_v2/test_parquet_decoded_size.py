"""Tests for Arrow decoded-size estimation from Parquet footer metadata.

The point of the estimator is accuracy against what a read task actually
materializes, so most of these compare against the buffers a real decode
allocates rather than against a hand-computed formula -- a formula test would
pass just as happily with the wrong formula.
"""

import datetime
import os
from decimal import Decimal

import numpy as np
import pytest

from ray.data._internal.datasource_v2.chunkers.parquet_decoded_size import (
    _leaf_layout,
    _UnsupportedLayout,
    build_leaf_profiles,
    decoded_size_or_fallback,
    dictionary_decode_size,
    dictionary_page_read_size,
    estimate_row_group_decoded_size,
    sum_exact,
)
from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
    DICTIONARY_PAGE_HEADER_READ_BYTES,
    LeafSizeStats,
    read_size_statistics,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

N = 5000

# Tolerance for the end-to-end bin tests only, which compare totals across
# several row groups and bins; the per-case matrix below is exact.
_TOLERANCE = 0.02


def _tensor_table():
    from ray.data.extensions.tensor_extension import ArrowTensorArray

    return pa.table({"t": ArrowTensorArray.from_numpy(np.zeros((N, 4), np.float32))})


def _cases():
    """The measured case matrix: every leaf layout and nesting shape."""
    return {
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


def _write(table, path, **kwargs):
    # One row group, so the estimate is compared against exactly one decode.
    pq.write_table(table, path, row_group_size=len(table), **kwargs)
    return path


def _allocated(path, columns=None):
    """Bytes PyArrow allocates decoding ``path``, one row group at a time.

    ``get_total_buffer_size`` rather than ``nbytes``: ``nbytes`` leaves out a
    nested array's own validity bitmap, so it understates what a decode costs.
    """
    parquet_file = pq.ParquetFile(str(path))
    return sum(
        parquet_file.read_row_group(rg, columns=columns).get_total_buffer_size()
        for rg in range(parquet_file.num_row_groups)
    )


def _estimate_whole_file(path, leaf_indices=None):
    """Decoded-size estimate summed over every row group of a single file."""
    metadata = pq.read_metadata(str(path))
    size_stats = read_size_statistics(metadata)
    assert size_stats is not None, "PyArrow-written files should carry SizeStatistics"
    leaf_profiles = build_leaf_profiles(metadata.schema)
    total = 0
    for rg_idx in range(metadata.num_row_groups):
        estimate = estimate_row_group_decoded_size(
            metadata.row_group(rg_idx),
            leaf_profiles,
            leaf_indices,
            size_stats[rg_idx],
        )
        assert estimate is not None
        total += estimate
    return total


def _estimate_row_group_0(path, size_stats):
    metadata = pq.read_metadata(str(path))
    return estimate_row_group_decoded_size(
        metadata.row_group(0), build_leaf_profiles(metadata.schema), None, size_stats
    )


@pytest.mark.parametrize("label", list(_cases()))
def test_estimate_matches_allocated_bytes(tmp_path, label):
    path = _write(_cases()[label], tmp_path / f"{label}.parquet")

    assert _estimate_whole_file(path) == _allocated(path)


@pytest.mark.parametrize("label", list(_cases()))
def test_estimate_beats_the_fixed_encoding_ratio(tmp_path, label):
    """The whole point: decoded sizing must be closer than uncompressed x 5.

    The fixed ratio spans well over two orders of magnitude across this matrix --
    far under-sizing dictionary-encoded data and far over-sizing plain numerics --
    which is what makes it unusable for a decision as final as bin assignment.
    """
    path = _write(_cases()[label], tmp_path / f"{label}.parquet")

    metadata = pq.read_metadata(str(path))
    actual = _allocated(path)
    uncompressed = sum(
        metadata.row_group(rg).column(leaf).total_uncompressed_size
        for rg in range(metadata.num_row_groups)
        for leaf in range(metadata.num_columns)
    )

    new_error = abs(_estimate_whole_file(path) / actual - 1.0)
    old_error = abs(
        uncompressed * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT / actual - 1.0
    )
    assert new_error <= old_error


def test_estimate_is_scoped_to_projected_leaves(tmp_path):
    """A projection must size only the leaves the read task decodes."""
    path = _write(_cases()["mixed-int-string"], tmp_path / "mixed.parquet")

    int_only = _estimate_whole_file(path, leaf_indices=[0])
    string_only = _estimate_whole_file(path, leaf_indices=[1])
    both = _estimate_whole_file(path, leaf_indices=None)

    assert int_only + string_only == both
    assert int_only == _allocated(path, columns=["i"])


def test_shared_ancestor_is_counted_once(tmp_path):
    """Leaves under one list share its offsets; per-leaf sizing double-counts."""
    path = _write(_cases()["list-of-struct"], tmp_path / "list-of-struct.parquet")

    x_only = _estimate_whole_file(path, leaf_indices=[0])
    y_only = _estimate_whole_file(path, leaf_indices=[1])
    both = _estimate_whole_file(path, leaf_indices=[0, 1])

    # The list's offsets (and the struct, which has no nulls and so no buffers)
    # appear in each single-leaf estimate but only once in the joint one.
    rows_with_list = N  # every row holds a (possibly empty) list
    assert x_only + y_only - both == 4 * (rows_with_list + 1)
    assert both == _allocated(path)


def test_multiple_row_groups_sum_to_whole_file(tmp_path):
    path = tmp_path / "multi.parquet"
    table = _cases()["mixed-int-string"]
    pq.write_table(table, path, row_group_size=len(table) // 4)

    assert pq.read_metadata(str(path)).num_row_groups == 4
    assert _estimate_whole_file(path) == _allocated(path)


def test_arrow_dictionary_is_bounded_not_hydrated(tmp_path):
    """A dictionary column decodes to indices plus one copy of each value.

    The footer has no distinct-value count, so without the dictionary page
    header the dictionary is bounded by the chunk's uncompressed size, which
    holds the dictionary page. That overshoots by the index pages; sizing it
    fully hydrated overshoots by orders of magnitude.
    """
    values = pa.array([f"{i % 50:0200d}" for i in range(N)]).dictionary_encode()
    path = _write(pa.table({"a": values}), tmp_path / "dictionary.parquet")

    estimate = _estimate_whole_file(path)
    actual = _allocated(path)
    hydrated = 200 * N

    assert actual <= estimate <= 1.25 * actual
    assert estimate < hydrated / 10


@pytest.mark.parametrize(
    "arrow_type, values, write_kwargs, keeps_bitmap",
    [
        (pa.int64(), [1, 2], {}, True),
        (pa.float32(), [1.0, 2.0], {}, True),
        (pa.timestamp("ns"), [1, 2], {}, True),
        (pa.int8(), [1, 2], {}, False),
        (pa.date32(), [datetime.date(2020, 1, 1)] * 2, {}, False),
        (pa.string(), ["a", "b"], {}, False),
        # INT96 timestamps are converted, not zero-copied, so they drop it.
        (pa.timestamp("ns"), [1, 2], {"use_deprecated_int96_timestamps": True}, False),
    ],
)
def test_null_free_optional_leaf_bitmap_follows_pyarrow(
    tmp_path, arrow_type, values, write_kwargs, keeps_bitmap
):
    """PyArrow keeps an all-valid bitmap only on types it decodes zero-copy."""
    table = pa.table({"a": pa.array(values * (N // 2), type=arrow_type)})
    path = _write(table, tmp_path / "bitmap.parquet", **write_kwargs)

    decoded = pq.ParquetFile(str(path)).read_row_group(0).column(0).chunk(0)
    assert (decoded.buffers()[0] is not None) == keeps_bitmap
    assert _estimate_whole_file(path) == _allocated(path)


# ---------------------------------------------------------------------------
# Partial footers: each leaf takes whatever its footer records
# ---------------------------------------------------------------------------


def _without_size_statistics(stats):
    """``stats`` as a writer without SizeStatistics leaves them: PyArrow 17
    records only which encodings the data pages use."""
    if stats is None:
        return None
    return LeafSizeStats(None, (), (), stats.data_page_encodings)


def test_unread_dictionary_page_header_costs_only_its_own_leaf(tmp_path):
    """Without SizeStatistics a dictionary-encoded string's bytes come from its
    dictionary page header, which the footer reader fetches (see "Without
    SizeStatistics" below). Unread, that leaf takes the fixed ratio and the
    int64 beside it stays exact."""
    path = _write(_cases()["mixed-int-string"], tmp_path / "mixed.parquet")
    strings = pq.read_metadata(str(path)).row_group(0).column(1)
    expected = (
        _allocated(path, columns=["i"])
        + strings.total_uncompressed_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )

    assert strings.has_dictionary_page
    assert _estimate_row_group_0(path, None) == expected
    # One entry for two leaves: the string, past its end, has none.
    assert _estimate_row_group_0(path, [LeafSizeStats(None, (), (0, N))]) == expected


def test_fixed_width_leaf_needs_no_unencoded_bytes(tmp_path):
    """The gate must be BYTE_ARRAY-scoped, not applied to every leaf.

    PyArrow legitimately omits sub-field 1 on numeric leaves, so a gate requiring
    it everywhere would silently disable exact sizing for every flat numeric
    schema while still paying the decode cost.
    """
    path = _write(_cases()["int64-plain"], tmp_path / "ints.parquet")

    estimate = _estimate_row_group_0(path, [LeafSizeStats(None, (), (0, N))])
    # Values plus the all-valid bitmap PyArrow keeps on a zero-copy int64.
    assert estimate == N * 8 + N // 8


def test_required_leaf_needs_no_size_statistics(tmp_path):
    """PyArrow writes no SizeStatistics for a required fixed-width leaf.

    Its size is the row count times its width, so their absence there must not
    loosen the strings beside it.
    """
    path = _write(_cases()["required-columns"], tmp_path / "required.parquet")
    size_stats = read_size_statistics(pq.read_metadata(str(path)))[0]

    # Only the data pages' encodings, from ``ColumnMetaData.encoding_stats``.
    assert size_stats[0][:3] == (None, (), ())
    assert _estimate_row_group_0(path, size_stats) == _allocated(path)


@pytest.mark.parametrize(
    "label, exact",
    [("list-int64", True), ("list-empty-and-null", True), ("list-of-list", False)],
)
def test_list_leaf_without_repetition_histogram(tmp_path, label, exact):
    """Each level entry under a list is a child slot unless its definition level
    says the list was empty or null, so the definition histogram alone sizes
    one level of list. A list in a list also needs the repetition histogram, to
    say which entries start an inner list; without it every entry is taken to,
    which bounds the inner lists from above."""
    path = _write(_cases()[label], tmp_path / "list.parquet")
    (stats,) = read_size_statistics(pq.read_metadata(str(path)))[0]
    stripped = stats._replace(repetition_level_histogram=())
    estimate = _estimate_row_group_0(path, [stripped])
    actual = _allocated(path)

    if exact:
        assert estimate == actual
    else:
        assert actual < estimate


def test_histogram_disagreeing_with_footer_is_ignored(tmp_path):
    """Histogram totals must match the footer's own value and row counts. Ones
    that do not mean the walk drifted, so the leaf is sized as if its writer
    had recorded no SizeStatistics."""
    path = _write(_cases()["list-of-list"], tmp_path / "list.parquet")
    (stats,) = read_size_statistics(pq.read_metadata(str(path)))[0]
    definition = stats.definition_level_histogram
    off_by_one = stats._replace(
        definition_level_histogram=definition[:-1] + (definition[-1] + 1,)
    )

    assert _estimate_row_group_0(path, [stats]) == _allocated(path)
    assert _estimate_row_group_0(path, [off_by_one]) == _estimate_row_group_0(
        path, None
    )


def test_unmodeled_arrow_type_is_rejected():
    """A layout this module does not model raises, which ``build_leaf_profiles``
    turns into ``None`` and so a fallback for the whole file. Every type PyArrow
    can write is modeled, so the check is on the layout itself."""
    with pytest.raises(_UnsupportedLayout):
        _leaf_layout(pa.list_view(pa.int32()))


@pytest.mark.parametrize("arrow_type", [pa.string_view(), pa.binary_view()])
@pytest.mark.parametrize(
    "length, max_ratio", [(0, 1), (1, 1.0625), (12, 1.75), (13, 1), (60, 1)]
)
def test_view_types_never_undershoot(tmp_path, arrow_type, length, max_ratio):
    """A view holds a value of up to 12 bytes inline and points at a longer one.

    The footer alone has the values' total bytes but not how they split around
    12, so the estimate puts them all out of line: exact for empty values and
    past 12 bytes, at most 1.75x when every value is exactly 12 (16 + 12 bytes
    estimated for 16 allocated). The dictionary page closes most of that gap;
    see the footer-reader tests below.
    """
    table = pa.table({"a": pa.array(["x" * length] * N, arrow_type)})
    path = _write(table, tmp_path / "view.parquet")

    estimate = _estimate_whole_file(path)
    actual = _allocated(path)

    assert actual <= estimate <= max_ratio * actual


def test_decoded_size_or_fallback_reproduces_the_old_math():
    assert decoded_size_or_fallback(1234, 999) == 1234
    assert (
        decoded_size_or_fallback(None, 999)
        == 999 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )


# ---------------------------------------------------------------------------
# Through the footer reader, which reads Arrow dictionaries' page headers and
# view columns' whole dictionary pages
# ---------------------------------------------------------------------------


def _estimate_through_footer_reader(path):
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader

    chunks = FooterReader(LocalFileSystem())._read_and_chunk(
        str(path), os.path.getsize(path)
    )
    return sum_exact(row_group.decoded_size for row_group in chunks.row_groups)


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
    """An Arrow dictionary of 100 entries plus a duplicate of the first, with
    one row per index in ``rows``."""
    values = pa.array([f"{i:012d}" for i in range(100)] + [f"{0:012d}"])
    indices = pa.array([i % 101 for i in rows], pa.int32())
    return pa.DictionaryArray.from_arrays(indices, values)


@pytest.mark.parametrize(
    "table, write_kwargs",
    [
        (pa.table({"a": _arrow_dictionary(50, 200)}), {}),
        (pa.table({"a": _arrow_dictionary(50, 200, value_type=pa.large_string())}), {}),
        (pa.table({"a": _arrow_dictionary(50, 12, null_every=3)}), {}),
        # PyArrow writes the Arrow dictionary whole, so one row decodes all 1000.
        (pa.table({"a": _arrow_dictionary(1000, 12, n=1)}), {}),
        (pa.table({"a": _arrow_dictionary(50, 60)}), {"compression": "zstd"}),
        (pa.table({"a": _arrow_dictionary(50, 12)}), {"version": "1.0"}),
        (pa.table({"a": _arrow_dictionary(500, 12)}), {"row_group_size": N // 4}),
        (
            pa.table(
                {"s": pa.StructArray.from_arrays([_arrow_dictionary(50, 12)], ["d"])}
            ),
            {},
        ),
        # A duplicate entry makes PyArrow write every page PLAIN, leaving the
        # dictionary page unused; it still lists every value, once each.
        (pa.table({"a": _duplicated_dictionary(range(N))}), {}),
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
    ],
)
def test_arrow_dictionary_is_exact_with_its_page_header(tmp_path, table, write_kwargs):
    path = tmp_path / "dictionary.parquet"
    pq.write_table(table, path, **write_kwargs)

    assert _estimate_through_footer_reader(path) == _allocated(path)


def test_arrow_dictionary_page_with_unused_entries_is_bounded(tmp_path):
    """An unused dictionary page lists entries the column never uses, which the
    reader's rebuilt dictionary leaves out. The footer cannot say which, so this
    stays an upper bound."""
    table = pa.table({"a": _duplicated_dictionary(range(0, 3 * N, 3))})
    path = _write(table, tmp_path / "dictionary.parquet")

    assert _allocated(path) <= _estimate_through_footer_reader(path)


def _reversed(array):
    """The same dictionary values, listed in reverse."""
    return pa.DictionaryArray.from_arrays(array.indices, array.dictionary[::-1])


def _in_list(array):
    offsets = pa.array(range(0, len(array) + 1, 2), pa.int32())
    return pa.ListArray.from_arrays(offsets, array)


def _in_struct(array):
    return pa.StructArray.from_arrays([array], ["d"])


@pytest.mark.parametrize(
    "chunks, write_kwargs",
    [
        # The PLAIN pages hold only values the page already lists...
        ([_arrow_dictionary(50, 100), _reversed(_arrow_dictionary(50, 100))], {}),
        # ...or 30 it does not.
        ([_arrow_dictionary(50, 12), _arrow_dictionary(80, 12)], {}),
        (
            [
                _arrow_dictionary(50, 12, value_type=pa.large_string()),
                _arrow_dictionary(80, 12, value_type=pa.large_string()),
            ],
            {},
        ),
        (
            [
                _arrow_dictionary(50, 12, null_every=3),
                _arrow_dictionary(80, 12, null_every=3),
            ],
            {},
        ),
        (
            [_in_list(_arrow_dictionary(50, 12)), _in_list(_arrow_dictionary(80, 12))],
            {},
        ),
        (
            [
                _in_struct(_arrow_dictionary(50, 12)),
                _in_struct(_arrow_dictionary(80, 12)),
            ],
            {},
        ),
        # Each row group starts on a new dictionary page and falls back in turn.
        (
            [_arrow_dictionary(50, 12, n=N // 4), _arrow_dictionary(80, 12, n=N // 4)]
            * 2,
            {"row_group_size": N // 2},
        ),
        (
            [_arrow_dictionary(50, 60), _arrow_dictionary(80, 60)],
            {"compression": "zstd"},
        ),
    ],
    ids=[
        "same-values",
        "new-values",
        "large-string",
        "nulls",
        "in-list",
        "in-struct",
        "row-groups",
        "zstd",
    ],
)
def test_arrow_dictionary_the_writer_gave_up_on_is_exact_once_decoded(
    tmp_path, chunks, write_kwargs
):
    """Chunks with dictionaries of their own make PyArrow's writer keep the first
    as the dictionary page and write the rest of the row group PLAIN. The read
    adds those values' new entries to the dictionary, which only the values can
    say, so the footer reader decodes each such chunk."""
    path = tmp_path / "dictionary.parquet"
    pq.write_table(pa.table({"a": pa.chunked_array(chunks)}), path, **write_kwargs)
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(metadata.schema)
    stats = read_size_statistics(metadata)

    assert all(
        dictionary_decode_size(
            profiles[0], metadata.row_group(i).column(0), stats[i][0]
        )
        for i in range(metadata.num_row_groups)
    )
    assert _estimate_through_footer_reader(path) == _allocated(path)


def test_arrow_dictionary_left_undecoded_keeps_its_bound(tmp_path, monkeypatch):
    """A chunk past the decode limit, or one that fails to decode, keeps the
    bound its page header gives -- over, never under -- and a failed decode must
    not fail the footer read."""
    import ray.data._internal.datasource_v2.chunkers.parquet_decoded_size as decoded
    import ray.data._internal.datasource_v2.listing.footer_reader as footer_reader

    def undecodable(*args, **kwargs):
        raise OSError("undecodable")

    chunks = [_arrow_dictionary(50, 100), _reversed(_arrow_dictionary(50, 100))]
    path = _write(pa.table({"a": pa.chunked_array(chunks)}), tmp_path / "d.parquet")
    exact = _estimate_through_footer_reader(path)
    with monkeypatch.context() as patch:
        patch.setattr(decoded, "_DICTIONARY_DECODE_LIMIT", 1)
        over_limit = _estimate_through_footer_reader(path)
    monkeypatch.setattr(footer_reader, "ParquetFile", undecodable)

    assert exact == _allocated(path) < over_limit
    assert _estimate_through_footer_reader(path) == over_limit


def test_dictionary_page_read_sizes(tmp_path):
    """Columns the writer dictionary-encoded on its own decode to plain arrays,
    so they cost no reads beyond the footer; an Arrow dictionary reads its page
    header, and a view column its whole dictionary page. None is decoded whole:
    no writer gave up on its dictionary."""
    values = [f"v{i % 5}" for i in range(N)]
    table = pa.table(
        {
            "plain": pa.array(values),
            "arrow": _arrow_dictionary(5, 2),
            "view": pa.array(values, pa.string_view()),
        }
    )
    path = _write(table, tmp_path / "dictionary.parquet")
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(metadata.schema)
    stats = read_size_statistics(metadata)[0]
    row_group = metadata.row_group(0)
    view = row_group.column(2)

    assert row_group.column(0).has_dictionary_page
    assert [
        dictionary_page_read_size(profiles[i], row_group.column(i), stats[i])
        for i in range(3)
    ] == [
        0,
        DICTIONARY_PAGE_HEADER_READ_BYTES,
        view.data_page_offset - view.dictionary_page_offset,
    ]
    assert not any(
        dictionary_decode_size(profiles[i], row_group.column(i), stats[i])
        for i in range(3)
    )


def test_failed_dictionary_page_read_keeps_the_bound(tmp_path, monkeypatch):
    """A page that cannot be read loosens its column to the bound; it must not
    fail the footer read or send the file to fallback."""
    import ray.data._internal.datasource_v2.listing.footer_reader as footer_reader

    def unreadable(buf, decompress=None):
        raise OSError("unreadable")

    path = _write(pa.table({"a": _arrow_dictionary(50, 200)}), tmp_path / "d.parquet")
    monkeypatch.setattr(footer_reader, "read_dictionary_page", unreadable)

    assert _estimate_through_footer_reader(path) == _estimate_whole_file(path)
    assert _allocated(path) < _estimate_whole_file(path)


def _short_views(length, arrow_type=None, null_every=0):
    """``N`` values of ``length`` bytes, 100 of them distinct."""
    return pa.array(
        [
            None
            if null_every and i % null_every == 0
            else f"{i % 100:012d}"[12 - length :]
            for i in range(N)
        ],
        arrow_type or pa.string_view(),
    )


@pytest.mark.parametrize("arrow_type", [pa.string_view(), pa.binary_view()])
@pytest.mark.parametrize("length", [0, 1, 12])
def test_short_view_values_are_exact_with_their_dictionary_page(
    tmp_path, arrow_type, length
):
    """Values of up to 12 bytes stay inside their views, which the dictionary
    page's value lengths show and the footer's byte total cannot."""
    path = _write(
        pa.table({"a": _short_views(length, arrow_type)}), tmp_path / "v.parquet"
    )

    assert _estimate_through_footer_reader(path) == _allocated(path)


_TWELVE_BYTE_VIEWS = pa.table({"a": _short_views(12)})
_UNIQUE_TWELVE_BYTE_VIEWS = pa.table(
    {"a": pa.array([f"{i:012d}" for i in range(N)], pa.string_view())}
)
# Fills the dictionary within the first write batch, so the rest goes PLAIN.
_SMALL_DICTIONARY = {"dictionary_pagesize_limit": 64, "write_batch_size": 100}


@pytest.mark.parametrize(
    "table, write_kwargs",
    [
        (pa.table({"a": _short_views(12, null_every=3)}), {}),
        (
            pa.table(
                {
                    "a": pa.array(
                        [[f"{j:012d}" for j in range(i % 4)] for i in range(N)],
                        pa.list_(pa.string_view()),
                    )
                }
            ),
            {},
        ),
        (_TWELVE_BYTE_VIEWS, {"compression": "none"}),
        (_TWELVE_BYTE_VIEWS, {"compression": "gzip"}),
        (_TWELVE_BYTE_VIEWS, {"compression": "brotli"}),
        (_TWELVE_BYTE_VIEWS, {"compression": "zstd"}),
        (_TWELVE_BYTE_VIEWS, {"compression": "lz4"}),
        (_TWELVE_BYTE_VIEWS, {"data_page_version": "2.0"}),
        (_TWELVE_BYTE_VIEWS, {"version": "1.0"}),
        # The dictionary fills and later values are written PLAIN; the page
        # stands in as a sample of them.
        (_UNIQUE_TWELVE_BYTE_VIEWS, _SMALL_DICTIONARY),
    ],
    ids=[
        "nulls",
        "in-list",
        "uncompressed",
        "gzip",
        "brotli",
        "zstd",
        "lz4",
        "data-page-v2",
        "format-1.0",
        "dictionary-fallback",
    ],
)
def test_short_view_values_are_exact_however_written(tmp_path, table, write_kwargs):
    path = tmp_path / "v.parquet"
    pq.write_table(table, path, **write_kwargs)

    assert _estimate_through_footer_reader(path) == _allocated(path)


def _views(values):
    return pa.table({"a": pa.array(values, pa.string_view())})


@pytest.mark.parametrize(
    "table, write_kwargs",
    [
        (_views(["x" * (5 if i % 2 else 20) for i in range(N)]), {}),
        (_views([f"{i:0{8 + i % 9}d}" for i in range(N)]), {}),
        (
            pa.table(
                {
                    "a": pa.array(
                        [
                            ["x" * (5 if j % 2 else 20) for j in range(i % 4)]
                            for i in range(N)
                        ],
                        pa.list_(pa.string_view()),
                    )
                }
            ),
            {},
        ),
        # After the fallback the PLAIN values look like the page's.
        (_views([f"{i:0{5 if i % 2 else 20}d}" for i in range(N)]), _SMALL_DICTIONARY),
    ],
    ids=["two-lengths", "all-distinct", "in-list", "dictionary-fallback"],
)
def test_view_values_either_side_of_12_bytes_are_exact_when_the_page_is_typical(
    tmp_path, table, write_kwargs
):
    """Values on both sides of the 12-byte inline limit: the page shows which
    distinct values are long, and when repeats use them as evenly as the page
    lists them, how the rest of the bytes split as well."""
    path = tmp_path / "v.parquet"
    pq.write_table(table, path, **write_kwargs)

    assert _estimate_through_footer_reader(path) == _allocated(path)


def test_view_values_a_misleading_page_undershoots_within_its_floor(tmp_path):
    """The page has each distinct value's length but not how often it is used.
    Here it misleads both ways of reading it: 1,000 short values and a
    10,000-byte one appear once each, and a 13-byte value fills all other rows.
    The estimate undershoots, but by at most 12 bytes per repeat, so it stays
    above 16/28 of the decoded size."""
    values = [f"{i:012d}" for i in range(1000)] + ["y" * 10_000]
    path = _write(
        _views(values + ["z" * 13] * (N - len(values))), tmp_path / "v.parquet"
    )
    estimate = _estimate_through_footer_reader(path)
    actual = _allocated(path)

    assert 16 / 28 * actual <= estimate < actual


@pytest.mark.parametrize(
    "table, write_kwargs",
    [
        # No dictionary page to consult.
        (_TWELVE_BYTE_VIEWS, {"use_dictionary": False}),
        # After the fallback, values longer than any in the page: their bytes
        # cannot all fit in views of the page's longest value.
        (
            _views([f"{i:0{12 if i < N // 2 else 24}d}" for i in range(N)]),
            _SMALL_DICTIONARY,
        ),
    ],
    ids=["no-dictionary", "dictionary-fallback-to-longer"],
)
def test_view_values_the_dictionary_page_cannot_place_stay_bounded(
    tmp_path, table, write_kwargs
):
    path = tmp_path / "v.parquet"
    pq.write_table(table, path, **write_kwargs)

    assert _allocated(path) <= _estimate_through_footer_reader(path)
    assert _estimate_through_footer_reader(path) <= _estimate_whole_file(path)


# ---------------------------------------------------------------------------
# Without SizeStatistics: the footer rules, as for a file PyArrow 17 wrote
# ---------------------------------------------------------------------------


def _estimate_without_size_statistics(path, monkeypatch):
    """The footer reader's estimate with every leaf's SizeStatistics removed."""
    import ray.data._internal.datasource_v2.listing.footer_reader as footer_reader

    def stripped(metadata):
        return [
            [_without_size_statistics(stats) for stats in leaves]
            for leaves in read_size_statistics(metadata)
        ]

    with monkeypatch.context() as patch:
        patch.setattr(footer_reader, "read_size_statistics", stripped)
        return _estimate_through_footer_reader(path)


def test_strings_without_their_bytes_read_their_dictionary_page_header(tmp_path):
    """The footer reader's one extra read for a file without SizeStatistics: a
    dictionary-encoded string chunk's page header."""
    path = _write(_cases()["string-low-cardinality"], tmp_path / "strings.parquet")
    metadata = pq.read_metadata(str(path))
    (profile,) = build_leaf_profiles(metadata.schema)
    chunk = metadata.row_group(0).column(0)
    (stats,) = read_size_statistics(metadata)[0]

    assert dictionary_page_read_size(profile, chunk, stats) == 0
    for stripped in (_without_size_statistics(stats), None):
        assert (
            dictionary_page_read_size(profile, chunk, stripped)
            == DICTIONARY_PAGE_HEADER_READ_BYTES
        )


# The cases with empty or null lists.
_EMPTY_LISTS = ["list-empty-and-null", "list-of-list", "list-of-struct", "map"]


@pytest.mark.parametrize(
    "label",
    [
        label
        for label in _cases()
        if label not in _EMPTY_LISTS and label != "mixed-int-string"
    ],
)
def test_estimate_without_size_statistics_matches_allocated_bytes(
    tmp_path, monkeypatch, label
):
    """Fixed-width values need only level counts, which the footer's value and
    null counts give outside a list, and inside one free of empty and null
    lists. A dictionary-encoded string's values are each taken at the
    dictionary's mean length, which its page header gives: exact when they
    share one length or each appears once, as in every string case here."""
    path = _write(_cases()[label], tmp_path / f"{label}.parquet")

    assert _estimate_without_size_statistics(path, monkeypatch) == _allocated(path)


def test_dictionary_mean_strays_as_far_as_the_rows_mean(tmp_path, monkeypatch):
    """``s0`` to ``s100`` average 294/101 bytes in the dictionary and
    14,549/5,000 in the rows, which use the 2-byte values slightly more often
    than the 4-byte one: 6 bytes over."""
    path = _write(_cases()["mixed-int-string"], tmp_path / "mixed.parquet")

    assert _estimate_without_size_statistics(path, monkeypatch) == _allocated(path) + 6


@pytest.mark.parametrize("label", _EMPTY_LISTS)
def test_list_children_without_size_statistics_are_bounded(
    tmp_path, monkeypatch, label
):
    """An empty or null list writes a level entry but owns no child slot. The
    definition histogram counts those entries; the footer's null count lumps
    them in with null values, so without the histogram a list's children take
    a slot per entry: over, never under, and by more the more lists are empty
    or null."""
    path = _write(_cases()[label], tmp_path / f"{label}.parquet")
    estimate = _estimate_without_size_statistics(path, monkeypatch)
    actual = _allocated(path)

    assert actual < estimate <= 1.6 * actual


def test_plain_strings_without_size_statistics_are_bounded(tmp_path, monkeypatch):
    """PLAIN pages hold each value behind a 4-byte length, which the value count
    takes off; the pages' headers and level runs stay in, a small overshoot."""
    path = _write(
        _cases()["string-high-cardinality"],
        tmp_path / "plain.parquet",
        use_dictionary=False,
    )
    estimate = _estimate_without_size_statistics(path, monkeypatch)
    actual = _allocated(path)

    assert actual < estimate <= 1.01 * actual


@pytest.mark.parametrize(
    "write_kwargs",
    [{"dictionary_pagesize_limit": 4096}, _SMALL_DICTIONARY],
    ids=["4KiB-dictionary", "64B-dictionary"],
)
def test_strings_after_a_dictionary_fallback_are_exact_when_lengths_match(
    tmp_path, monkeypatch, write_kwargs
):
    """The dictionary fills and the rest of the chunk is written PLAIN. The
    footer has the pages' bytes but not how many values went each way, so every
    value is taken at the dictionary's mean length: exact when they all share
    it."""
    table = pa.table({"a": pa.array([f"{i:012d}" for i in range(N)])})
    path = _write(table, tmp_path / "fallback.parquet", **write_kwargs)

    assert _estimate_without_size_statistics(path, monkeypatch) == _allocated(path)


def _skewed(common, rare):
    """``common`` in every row but 99, which hold the distinct ``rare``."""
    return pa.table({"a": pa.array([common] * (N - len(rare)) + rare)})


@pytest.mark.parametrize(
    "table, write_kwargs, over",
    [
        # The value most rows hold is shorter than the dictionary's mean...
        (_skewed("a", [f"{i:0100d}" for i in range(99)]), {}, True),
        # ...or longer.
        (_skewed("x" * 100, [f"{i:02d}" for i in range(99)]), {}, False),
        # Values grow down the column: those written PLAIN after the dictionary
        # filled are longer than the dictionary's mean.
        (
            _cases()["string-high-cardinality"],
            {"dictionary_pagesize_limit": 4096},
            False,
        ),
    ],
    ids=["common-value-short", "common-value-long", "growing-after-fallback"],
)
def test_strings_without_size_statistics_can_misjudge_lengths(
    tmp_path, monkeypatch, table, write_kwargs, over
):
    """The footer rules' known gap. A dictionary-encoded string's values are
    taken at the dictionary's mean length, since neither the footer nor the
    page header says which values the rows hold most. The error can go either
    way, by any factor, and nothing in the footer flags it; SizeStatistics
    close it."""
    path = _write(table, tmp_path / "skewed.parquet", **write_kwargs)
    actual = _allocated(path)

    assert _estimate_through_footer_reader(path) == actual
    assert (_estimate_without_size_statistics(path, monkeypatch) > actual) == over


def test_delta_length_strings_without_size_statistics_are_bounded(
    tmp_path, monkeypatch
):
    """DELTA_LENGTH_BYTE_ARRAY packs the lengths apart from the bytes, in as
    little as nothing, so the rules take none off: the packed lengths are the
    overshoot."""
    path = _write(
        _cases()["mixed-int-string"],
        tmp_path / "delta-length.parquet",
        use_dictionary=False,
        column_encoding={"s": "DELTA_LENGTH_BYTE_ARRAY"},
    )
    estimate = _estimate_without_size_statistics(path, monkeypatch)
    actual = _allocated(path)

    assert actual < estimate <= 1.05 * actual


def test_delta_byte_array_strings_take_the_fixed_ratio(tmp_path, monkeypatch):
    """DELTA_BYTE_ARRAY stores each value as the suffix that differs from the
    one before it, so neither its pages' bytes nor PyArrow 24's SizeStatistics
    (see ``_levels``) give the values' bytes. That leaf takes the fixed ratio
    with or without SizeStatistics, and the int64 beside it stays exact."""
    path = _write(
        _cases()["mixed-int-string"],
        tmp_path / "delta.parquet",
        use_dictionary=False,
        column_encoding={"s": "DELTA_BYTE_ARRAY"},
    )
    strings = pq.read_metadata(str(path)).row_group(0).column(1)
    expected = (
        _allocated(path, columns=["i"])
        + strings.total_uncompressed_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )

    assert _estimate_through_footer_reader(path) == expected
    assert _estimate_without_size_statistics(path, monkeypatch) == expected


# ---------------------------------------------------------------------------
# Leaf widths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "arrow_type, values",
    [
        (pa.int8(), [1, 2]),
        (pa.uint8(), [1, 2]),
        (pa.int16(), [1, 2]),
        (pa.uint16(), [1, 2]),
        (pa.int32(), [1, 2]),
        (pa.uint32(), [1, 2]),
        (pa.int64(), [1, 2]),
        (pa.uint64(), [1, 2]),
        (pa.float32(), [1.0, 2.0]),
        (pa.float64(), [1.0, 2.0]),
        (pa.timestamp("us"), [1, 2]),
        (pa.date32(), [datetime.date(2020, 1, 1), datetime.date(2021, 2, 3)]),
        # Stored as INT32 days and decoded as date32, so 4 bytes, not 8.
        (pa.date64(), [datetime.date(2020, 1, 1), datetime.date(2021, 2, 3)]),
        (pa.time64("us"), [1, 2]),
        (pa.duration("ms"), [1, 2]),
        (pa.decimal128(10, 2), [Decimal("1.00"), Decimal("2.50")]),
        (pa.decimal256(50, 2), [Decimal("1.00"), Decimal("2.50")]),
        (pa.binary(7), [b"1234567", b"abcdefg"]),
        (pa.float16(), np.array([1.0, 2.0], dtype=np.float16)),
    ],
)
def test_fixed_width_types_are_exact(tmp_path, arrow_type, values):
    array = pa.array(values, type=arrow_type)
    table = pa.table({"a": pa.concat_arrays([array] * (N // 2))})
    path = _write(table, tmp_path / "widths.parquet")

    assert _estimate_whole_file(path) == _allocated(path)


def test_null_free_optional_bool_is_not_double_counted(tmp_path):
    """Keying the validity bitmap off nullability instead of actual nulls would
    estimate a null-free ``bool`` column at 2.00x its real size."""
    table = pa.table({"a": pa.array([i % 2 == 0 for i in range(N)])})
    path = _write(table, tmp_path / "bools.parquet")

    # Data buffer only: one bit per value, no bitmap.
    assert _estimate_whole_file(path) == N // 8


# ---------------------------------------------------------------------------
# End to end: footer read -> decoded sizing -> bin packing
# ---------------------------------------------------------------------------


def _pack_through_footer_reader(paths, max_bin_bytes):
    """Run the real listing path: footer read, then bin packing, no Ray runtime.

    ``FooterReader`` is a plain class that ``FooterReaderActor`` wraps, so it can
    be driven directly. That covers footer decode, the estimator, coalescing and
    the packer in one go, which is where a wiring mistake between them would hide.
    """
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
        _file_chunks_to_manifest,
    )
    from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader
    from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
        OnlineBinPacker,
    )

    reader = FooterReader(LocalFileSystem())
    packer = OnlineBinPacker(max_bin_bytes=max_bin_bytes)
    manifests = []

    def drain():
        while packer.has_partition():
            manifests.append(packer.next_partition())

    for path in paths:
        chunks = reader._read_and_chunk(str(path), os.path.getsize(path))
        packer.add_input(_file_chunks_to_manifest(chunks))
        drain()
    packer.finalize()
    drain()
    return manifests


def _bin_decoded_totals(manifests):
    return [
        sum(meta["decoded_size"] for meta in manifest.file_chunk_metadatas)
        for manifest in manifests
    ]


@pytest.mark.parametrize(
    "label",
    # Opposite ends of the encoding-ratio range: dictionary-encoded strings,
    # where uncompressed size badly *understates* the Arrow block, and plain
    # numerics, where it overstates it once scaled by the fixed 5x.
    ["string-low-cardinality", "int64-plain"],
)
def test_bins_land_near_target_block_size(tmp_path, label):
    path = tmp_path / f"{label}.parquet"
    table = _cases()[label]
    # Several row groups, so the packer has boundaries to cut on.
    pq.write_table(table, path, row_group_size=len(table) // 10)

    actual_bytes = _allocated(path)
    # A budget that must yield roughly four bins if sizing is right.
    target = actual_bytes // 4

    totals = _bin_decoded_totals(_pack_through_footer_reader([path], target))

    # Sizing is accurate in aggregate: the bins account for the real Arrow bytes.
    assert sum(totals) == pytest.approx(actual_bytes, rel=_TOLERANCE)
    # And each bin lands at or under the budget. Bins may undershoot, since a bin
    # seals as soon as no further row group fits.
    for total in totals:
        assert total <= target
    assert 3 <= len(totals) <= 6, totals


def test_dictionary_encoded_strings_would_collapse_into_one_bin_when_sized_on_disk(
    tmp_path,
):
    """The regression this change exists to prevent.

    Low-cardinality strings compress so well that their uncompressed footer size
    is a small fraction of the Arrow block they decode to. Budgeting on that
    number packs far too much into one read task -- the OOM direction -- which is
    exactly what decoded sizing fixes.
    """
    path = tmp_path / "dict-strings.parquet"
    table = _cases()["string-low-cardinality"]
    pq.write_table(table, path, row_group_size=len(table) // 10)

    metadata = pq.read_metadata(str(path))
    uncompressed = sum(
        metadata.row_group(rg).column(0).total_uncompressed_size
        for rg in range(metadata.num_row_groups)
    )
    actual_bytes = _allocated(path)
    target = actual_bytes // 4

    # Sizing on uncompressed bytes, the whole file fits in one bin many times over.
    assert uncompressed < target
    # Sizing on decoded bytes, it does not.
    assert len(_bin_decoded_totals(_pack_through_footer_reader([path], target))) > 1


def test_multiple_files_share_bins_by_decoded_size(tmp_path):
    table = _cases()["mixed-int-string"]
    paths = []
    for i in range(4):
        path = tmp_path / f"part{i}.parquet"
        pq.write_table(table, path, row_group_size=len(table) // 4)
        paths.append(path)

    per_file_bytes = _allocated(paths[0])
    manifests = _pack_through_footer_reader(paths, per_file_bytes * 2)
    totals = _bin_decoded_totals(manifests)

    assert sum(totals) == pytest.approx(per_file_bytes * 4, rel=_TOLERANCE)
    for total in totals:
        assert total <= per_file_bytes * 2
    # Every row group is accounted for exactly once across the bins.
    covered = [
        (str(path), rg_id)
        for manifest in manifests
        for path, meta in zip(manifest.paths, manifest.file_chunk_metadatas)
        for rg_id in meta["row_group_ids"]
    ]
    assert len(covered) == len(set(covered)) == 4 * 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
