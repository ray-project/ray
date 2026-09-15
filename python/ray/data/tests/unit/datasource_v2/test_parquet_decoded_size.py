"""Tests for Arrow decoded-size estimation from Parquet footer metadata.

The point of the estimator is accuracy against what a read task actually
materializes, so most of these compare against a real ``pa.Table.nbytes`` rather
than against a hand-computed formula -- a formula test would pass just as happily
with the wrong formula.
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
    parquet_leaf_fixed_width,
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

# Arrow allocates a validity bitmap for some null-free optional leaves anyway, so
# the estimate can sit a bitmap's worth (1/8 byte per value) below the real
# nbytes. That is ~1.5% on an 8-byte type and irrelevant against a bin budget of
# hundreds of megabytes, but it means "exact" here means "within a bitmap".
_TOLERANCE = 0.02


def _cases():
    """The measured case matrix: every branch of the width table and the gate."""
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
        "mixed-int-string": pa.table(
            {
                "i": pa.array(np.arange(N, dtype="int64")),
                "s": pa.array([f"s{i % 101}" for i in range(N)]),
            }
        ),
        "list-int64": pa.table(
            {"a": pa.array([[1, 2, 3]] * N, type=pa.list_(pa.int64()))}
        ),
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
    }


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


@pytest.mark.parametrize("label", list(_cases()))
def test_estimate_matches_actual_nbytes(tmp_path, label):
    table = _cases()[label]
    path = tmp_path / f"{label}.parquet"
    # One row group, so the estimate is compared against exactly one table.
    pq.write_table(table, path, row_group_size=len(table))

    estimate = _estimate_whole_file(path)
    actual = pq.read_table(str(path)).nbytes

    assert estimate == pytest.approx(actual, rel=_TOLERANCE), (
        f"{label}: estimated {estimate}, actual {actual} "
        f"(ratio {estimate / actual:.3f})"
    )


@pytest.mark.parametrize("label", list(_cases()))
def test_estimate_beats_the_fixed_encoding_ratio(tmp_path, label):
    """The whole point: decoded sizing must be closer than uncompressed x 5.

    The fixed ratio spans well over two orders of magnitude across this matrix --
    far under-sizing dictionary-encoded data and far over-sizing plain numerics --
    which is what makes it unusable for a decision as final as bin assignment.
    """
    table = _cases()[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(table, path, row_group_size=len(table))

    metadata = pq.read_metadata(str(path))
    actual = pq.read_table(str(path)).nbytes
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
    path = tmp_path / "mixed.parquet"
    table = _cases()["mixed-int-string"]
    pq.write_table(table, path, row_group_size=len(table))

    int_only = _estimate_whole_file(path, leaf_indices=[0])
    string_only = _estimate_whole_file(path, leaf_indices=[1])
    both = _estimate_whole_file(path, leaf_indices=None)

    assert int_only + string_only == both
    assert int_only == pytest.approx(
        pq.read_table(str(path), columns=["i"]).nbytes, rel=_TOLERANCE
    )


def test_multiple_row_groups_sum_to_whole_file(tmp_path):
    path = tmp_path / "multi.parquet"
    table = _cases()["mixed-int-string"]
    pq.write_table(table, path, row_group_size=len(table) // 4)

    assert pq.read_metadata(str(path)).num_row_groups == 4
    assert _estimate_whole_file(path) == pytest.approx(
        pq.read_table(str(path)).nbytes, rel=_TOLERANCE
    )


# ---------------------------------------------------------------------------
# Falling back
# ---------------------------------------------------------------------------


def test_returns_none_without_size_statistics(tmp_path):
    path = tmp_path / "flat.parquet"
    table = _cases()["string-high-cardinality"]
    pq.write_table(table, path, row_group_size=len(table))
    metadata = pq.read_metadata(str(path))

    assert (
        estimate_row_group_decoded_size(
            metadata.row_group(0), build_leaf_profiles(metadata.schema), None, None
        )
        is None
    )


def test_returns_none_when_byte_array_leaf_lacks_unencoded_bytes(tmp_path):
    """A BYTE_ARRAY leaf's character bytes exist nowhere else in the footer."""
    path = tmp_path / "strings.parquet"
    table = _cases()["string-high-cardinality"]
    pq.write_table(table, path, row_group_size=len(table))
    metadata = pq.read_metadata(str(path))
    stripped = [LeafSizeStats(None, (), (0, N))]

    assert (
        estimate_row_group_decoded_size(
            metadata.row_group(0), build_leaf_profiles(metadata.schema), None, stripped
        )
        is None
    )


def test_fixed_width_leaf_needs_no_unencoded_bytes(tmp_path):
    """The gate must be BYTE_ARRAY-scoped, not applied to every leaf.

    PyArrow legitimately omits sub-field 1 on numeric leaves, so a gate requiring
    it everywhere would silently disable exact sizing for every flat numeric
    schema while still paying the decode cost.
    """
    path = tmp_path / "ints.parquet"
    table = _cases()["int64-plain"]
    pq.write_table(table, path, row_group_size=len(table))
    metadata = pq.read_metadata(str(path))

    estimate = estimate_row_group_decoded_size(
        metadata.row_group(0),
        build_leaf_profiles(metadata.schema),
        None,
        [LeafSizeStats(None, (), (0, N))],
    )
    assert estimate == N * 8


def test_missing_leaf_entry_returns_none(tmp_path):
    path = tmp_path / "mixed.parquet"
    table = _cases()["mixed-int-string"]
    pq.write_table(table, path, row_group_size=len(table))
    metadata = pq.read_metadata(str(path))

    # Two leaves in the read set, only one entry available.
    assert (
        estimate_row_group_decoded_size(
            metadata.row_group(0),
            build_leaf_profiles(metadata.schema),
            None,
            [LeafSizeStats(None, (), (0, N))],
        )
        is None
    )


def test_decoded_size_or_fallback_reproduces_the_old_math():
    assert decoded_size_or_fallback(1234, 999) == 1234
    assert (
        decoded_size_or_fallback(None, 999)
        == 999 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )


# ---------------------------------------------------------------------------
# Width table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "arrow_type, values, expected_width",
    [
        # INT32 storage narrowed by the logical INT annotation's bit width.
        (pa.int8(), [1, 2], 1),
        (pa.uint8(), [1, 2], 1),
        (pa.int16(), [1, 2], 2),
        (pa.uint16(), [1, 2], 2),
        (pa.int32(), [1, 2], 4),
        (pa.int64(), [1, 2], 8),
        (pa.float32(), [1.0, 2.0], 4),
        (pa.float64(), [1.0, 2.0], 8),
        # INT64 storage, and Arrow keeps 8 bytes.
        (pa.timestamp("us"), [1, 2], 8),
        (pa.date32(), [datetime.date(2020, 1, 1), datetime.date(2021, 2, 3)], 4),
        # FIXED_LEN_BYTE_ARRAY variants, all identified by logical type.
        (pa.decimal128(10, 2), [Decimal("1.00"), Decimal("2.50")], 16),
        (pa.decimal256(50, 2), [Decimal("1.00"), Decimal("2.50")], 32),
        (pa.binary(7), [b"1234567", b"abcdefg"], 7),
        (pa.float16(), np.array([1.0, 2.0], dtype=np.float16), 2),
    ],
)
def test_fixed_width_table(tmp_path, arrow_type, values, expected_width):
    path = tmp_path / "widths.parquet"
    pq.write_table(pa.table({"a": pa.array(values, type=arrow_type)}), path)

    column = pq.read_metadata(str(path)).schema.column(0)
    assert parquet_leaf_fixed_width(column) == expected_width


def test_variable_width_leaves_have_no_fixed_width(tmp_path):
    path = tmp_path / "variable.parquet"
    pq.write_table(
        pa.table(
            {
                "s": pa.array(["a", "bb"]),
                # BOOLEAN is bit-packed, so it has no whole-byte per-value width.
                "b": pa.array([True, False]),
            }
        ),
        path,
    )
    schema = pq.read_metadata(str(path)).schema

    assert parquet_leaf_fixed_width(schema.column(0)) is None
    assert parquet_leaf_fixed_width(schema.column(1)) is None


def test_null_free_optional_bool_is_not_double_counted(tmp_path):
    """Keying the validity bitmap off nullability instead of actual nulls would
    estimate a null-free ``bool`` column at 2.00x its real size."""
    path = tmp_path / "bools.parquet"
    table = pa.table({"a": pa.array([i % 2 == 0 for i in range(N)])})
    pq.write_table(table, path, row_group_size=N)

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

    actual_nbytes = pq.read_table(str(path)).nbytes
    # A budget that must yield roughly four bins if sizing is right.
    target = actual_nbytes // 4

    totals = _bin_decoded_totals(_pack_through_footer_reader([path], target))

    # Sizing is accurate in aggregate: the bins account for the real Arrow bytes.
    assert sum(totals) == pytest.approx(actual_nbytes, rel=_TOLERANCE)
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
    actual_nbytes = pq.read_table(str(path)).nbytes
    target = actual_nbytes // 4

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

    per_file_nbytes = pq.read_table(str(paths[0])).nbytes
    manifests = _pack_through_footer_reader(paths, per_file_nbytes * 2)
    totals = _bin_decoded_totals(manifests)

    assert sum(totals) == pytest.approx(per_file_nbytes * 4, rel=_TOLERANCE)
    for total in totals:
        assert total <= per_file_nbytes * 2
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
