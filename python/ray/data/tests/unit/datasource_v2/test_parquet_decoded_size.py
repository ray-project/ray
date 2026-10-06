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
    build_leaf_profiles,
    decoded_size_or_fallback,
    estimate_row_group_decoded_size,
)
from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
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

    The footer has no distinct-value count, so the dictionary is bounded by the
    chunk's uncompressed size, which holds the dictionary page. That overshoots
    by the index pages; sizing it fully hydrated overshoots by orders of
    magnitude.
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
# Falling back
# ---------------------------------------------------------------------------


def test_returns_none_without_size_statistics(tmp_path):
    path = _write(_cases()["string-high-cardinality"], tmp_path / "flat.parquet")

    assert _estimate_row_group_0(path, None) is None


def test_returns_none_when_byte_array_leaf_lacks_unencoded_bytes(tmp_path):
    """A BYTE_ARRAY leaf's character bytes exist nowhere else in the footer."""
    path = _write(_cases()["string-high-cardinality"], tmp_path / "strings.parquet")
    stripped = [LeafSizeStats(None, (), (0, N))]

    assert _estimate_row_group_0(path, stripped) is None


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

    Its size is the row count times its width, so a missing entry there must
    not send the whole row group -- strings beside it included -- to fallback.
    """
    path = _write(_cases()["required-columns"], tmp_path / "required.parquet")
    size_stats = read_size_statistics(pq.read_metadata(str(path)))[0]

    assert size_stats[0] is None
    assert _estimate_row_group_0(path, size_stats) == _allocated(path)


def test_missing_entry_for_optional_leaf_returns_none(tmp_path):
    """Anywhere but a required flat leaf, a missing entry is a fallback.

    A writer always has histograms to record for an optional leaf, so ``None``
    there means the walk gave up -- and must not turn into a number.
    """
    path = _write(_cases()["int64-plain"], tmp_path / "ints.parquet")

    assert _estimate_row_group_0(path, [None]) is None


def test_missing_entry_returns_none(tmp_path):
    path = _write(_cases()["mixed-int-string"], tmp_path / "mixed.parquet")

    # Two leaves in the read set, only one entry available.
    assert _estimate_row_group_0(path, [LeafSizeStats(None, (), (0, N))]) is None


def test_list_leaf_without_repetition_histogram_returns_none(tmp_path):
    """Without it there is no list length to size offsets or children from."""
    path = _write(_cases()["list-int64"], tmp_path / "list.parquet")
    (stats,) = read_size_statistics(pq.read_metadata(str(path)))[0]
    stripped = stats._replace(repetition_level_histogram=())

    assert _estimate_row_group_0(path, [stats]) is not None
    assert _estimate_row_group_0(path, [stripped]) is None


def test_histogram_disagreeing_with_footer_returns_none(tmp_path):
    """Histogram totals must match the footer's own value and row counts."""
    path = _write(_cases()["list-int64"], tmp_path / "list.parquet")
    (stats,) = read_size_statistics(pq.read_metadata(str(path)))[0]
    definition = stats.definition_level_histogram
    off_by_one = stats._replace(
        definition_level_histogram=definition[:-1] + (definition[-1] + 1,)
    )

    assert _estimate_row_group_0(path, [off_by_one]) is None


def test_unmodeled_arrow_type_returns_none(tmp_path):
    """A type whose layout is not modeled sends the whole file to fallback."""
    path = _write(
        pa.table({"a": pa.array(["a", "b"] * 10, pa.string_view())}),
        tmp_path / "view.parquet",
    )
    metadata = pq.read_metadata(str(path))

    assert build_leaf_profiles(metadata.schema) is None
    assert _estimate_row_group_0(path, read_size_statistics(metadata)[0]) is None


def test_decoded_size_or_fallback_reproduces_the_old_math():
    assert decoded_size_or_fallback(1234, 999) == 1234
    assert (
        decoded_size_or_fallback(None, 999)
        == 999 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )


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
