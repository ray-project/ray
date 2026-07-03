from unittest.mock import MagicMock

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.listing_utils import partition_files
from ray.data._internal.datasource_v2.partitioners.file_affinity_partitioner import (
    FileAffinityPartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
    InMemorySizeEstimator,
    ParquetFooterDerivedInMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner


def test_weighted_round_robin_partitioner_can_emit_before_overflow():
    partitioner = WeightedRoundRobinPartitioner(
        num_buckets=1,
        min_bucket_size=1,
        max_bucket_size=3,
        emit_before_overflow=True,
    )

    partitioner.add_item("a", 2)
    partitioner.add_item("b", 2)

    assert partitioner.has_partition()
    assert partitioner.next_partition() == ["a"]

    partitioner.finalize()
    assert partitioner.has_partition()
    assert partitioner.next_partition() == ["b"]


class _FileSizeStubEstimator(InMemorySizeEstimator):
    """In-memory size == the manifest's on-disk ``file_sizes`` (test control)."""

    def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
        return np.asarray(manifest.file_sizes)


def _affinity_table(paths, sizes, metas):
    return pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: paths,
            FILE_SIZE_COLUMN_NAME: sizes,
            FILE_CHUNK_METADATA_COLUMN_NAME: metas,
        }
    )


def _affinity_outputs(table, max_bucket_size):
    return list(
        partition_files(
            iter([table]),
            MagicMock(),
            partitioner=FileAffinityPartitioner(
                in_memory_size_estimator=_FileSizeStubEstimator(),
                max_bucket_size=max_bucket_size,
            ),
        )
    )


def _manifest(paths, sizes=None, metas=None):
    """Build a FileManifest for driving a partitioner directly (no cluster)."""
    n = len(paths)
    return FileManifest.construct_manifest(
        paths,
        sizes if sizes is not None else [1] * n,
        metas if metas is not None else [None] * n,
    )


def _drain_partitions(partitioner):
    out = []
    while partitioner.has_partition():
        out.append(
            partitioner.next_partition().as_block()[PATH_COLUMN_NAME].to_pylist()
        )
    return out


@pytest.mark.parametrize("pack_files", [True, False])
def test_file_affinity_groups_by_file_and_bounds_size(monkeypatch, pack_files):
    # File "a": 4 chunks of size 1; "b": 1 chunk. max_bucket_size=2 -> "a"
    # overflows its own bucket (unaffected by packing) into two 2-chunk
    # partitions; "b" (weight 1, never overflows) is a pack candidate but ends
    # up alone regardless -- nothing else arrives to pack it with before
    # finalize(), so packing is a no-op here either way.
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "1" if pack_files else "0")
    table = _affinity_table(["a", "a", "a", "a", "b"], [1, 1, 1, 1, 1], [None] * 5)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "a"], ["a", "a"], ["b"]]
    assert all(len(set(p)) == 1 for p in partitions)  # each partition single-file


def test_file_affinity_contiguous_input_flushes_per_file_pack_disabled(monkeypatch):
    # The real indexer emits each file's chunks contiguously (one atomic
    # record-list per file through make_async_gen), so flush-on-path-change
    # flushes a file as soon as the next file's chunks begin arriving. This
    # hand-crafted non-contiguous input (a,b,a,b) is NOT a shape the indexer
    # produces; it exercises the path-change flush in isolation, with packing
    # disabled so each contiguous run emits its own partition (see the
    # ..._pack_enabled variant below for the default-on packing behavior on
    # this same input).
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    table = _affinity_table(["a", "b", "a", "b"], [1, 1, 1, 1], [None] * 4)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a"], ["b"], ["a"], ["b"]]
    assert all(len(set(p)) == 1 for p in partitions)  # never mixes files


def test_file_affinity_contiguous_input_packs_across_runs_pack_enabled():
    # Same interleaved input as the pack-disabled variant above, but with
    # packing at its default (on): each completed run (weight 1, never
    # overflows against max_bucket_size=2) is a pack candidate. "a" completes,
    # joins an empty pack (weight 1); "b" completes, 1+1=2 is not > 2 so it
    # joins too (pack weight 2, items=[a,b]); the next "a" completes and
    # 2+1=3 > 2, so the full pack flushes as one partition ["a","b"] and the
    # new "a" starts a fresh pack; at finalize the trailing "b" joins that
    # pack and it flushes as ["a","b"] too.
    table = _affinity_table(["a", "b", "a", "b"], [1, 1, 1, 1], [None] * 4)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "b"], ["a", "b"]]


def test_file_affinity_small_file_is_single_partition():
    table = _affinity_table(["a", "a"], [1, 1], [None, None])
    partitions = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 100)
    ]
    assert partitions == [["a", "a"]]


def test_file_affinity_finalize_preserves_arrival_order(monkeypatch):
    # Regression test: finalize() used to re-sort emitted partitions by path
    # "for deterministic output," which silently discarded any upstream
    # shuffle (shuffle_files already guarantees determinism itself -- it
    # sorts by path *before* permuting). Partitions that flush at finalize()
    # (i.e. never hit the max_bucket_size overflow) must preserve the order
    # the input manifest arrived in, not re-derive an alphabetical order.
    # Packing disabled so each file emits its own partition -- see
    # test_file_affinity_packing_preserves_arrival_order for the packed case.
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    table = _affinity_table(["z", "a", "m"], [1, 1, 1], [None, None, None])
    partitions = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 100)
    ]
    assert partitions == [["z"], ["a"], ["m"]]


def test_file_affinity_packing_preserves_arrival_order():
    # Same non-alphabetical arrival order as the pack-disabled test above, but
    # with packing at its default (on): all three tiny files fit under
    # max_bucket_size=100, so they pack into ONE partition at finalize(). The
    # concatenated manifest's path order must still be arrival order (z, a, m),
    # not alphabetical -- guards against a future "helpful" sort of the pack.
    table = _affinity_table(["z", "a", "m"], [1, 1, 1], [None, None, None])
    partitions = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 100)
    ]
    assert partitions == [["z", "a", "m"]]


def test_file_affinity_empty_input_emits_nothing():
    table = _affinity_table([], [], [])
    assert _affinity_outputs(table, 2) == []


def test_file_affinity_sorts_partition_chunks_by_row_group_start():
    # Row groups arrive out of order; the emitted partition is ascending.
    metas = [
        {"row_group_start": 2, "row_group_end": 3},
        {"row_group_start": 0, "row_group_end": 1},
        {"row_group_start": 1, "row_group_end": 2},
    ]
    table = _affinity_table(["a", "a", "a"], [1, 1, 1], metas)
    outputs = _affinity_outputs(table, 100)
    assert len(outputs) == 1
    starts = [
        cm["row_group_start"]
        for cm in outputs[0][FILE_CHUNK_METADATA_COLUMN_NAME].to_pylist()
    ]
    assert starts == [0, 1, 2]


def test_file_affinity_sorts_line_delimited_chunks_by_byte_offset():
    # FileAffinityPartitioner is the default for ALL V2 datasources, so it must
    # also order line-delimited (CSV/JSON) chunks by their byte offset, not only
    # Parquet row groups (covered above).
    metas = [
        {"chunk_byte_start_idx": 200, "chunk_byte_end_idx": 300},
        {"chunk_byte_start_idx": 0, "chunk_byte_end_idx": 100},
        {"chunk_byte_start_idx": 100, "chunk_byte_end_idx": 200},
    ]
    table = _affinity_table(["a", "a", "a"], [1, 1, 1], metas)
    outputs = _affinity_outputs(table, 100)
    assert len(outputs) == 1
    starts = [
        cm["chunk_byte_start_idx"]
        for cm in outputs[0][FILE_CHUNK_METADATA_COLUMN_NAME].to_pylist()
    ]
    assert starts == [0, 100, 200]


def test_footer_derived_estimator_reads_hint_and_falls_back():
    # The estimator reads each chunk's footer-derived ``in_memory_size`` hint and
    # falls back to ``file_size × ratio`` for chunks without one (e.g. a
    # whole-file ``None`` chunk on a corrupt footer / non-Parquet input).
    manifest = FileManifest.construct_manifest(
        ["a", "b", "c"],
        [10, 20, 30],
        [
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 1234},
            None,
            {"row_group_start": 0, "row_group_end": 2, "in_memory_size": 5678},
        ],
    )
    sizes = ParquetFooterDerivedInMemorySizeEstimator().estimate_in_memory_sizes(
        manifest
    )
    assert list(sizes) == [
        1234.0,
        20 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        5678.0,
    ]


def test_footer_derived_estimator_handles_none_file_size():
    # File sizes can be unavailable (e.g. HTTPFileSystem) -> a null size column
    # surfaces as None/NaN. With no footer hint, the estimate must coerce to 0.0
    # rather than raise TypeError/ValueError on ``float(...)``.
    manifest = FileManifest.construct_manifest(["a", "b"], [None, 50], [None, None])
    sizes = ParquetFooterDerivedInMemorySizeEstimator().estimate_in_memory_sizes(
        manifest
    )
    assert list(sizes) == [0.0, 50 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT]


def test_footer_derived_estimator_treats_zero_hint_as_missing():
    # Regression: a footer hint of exactly 0 on a chunk with real on-disk bytes
    # is a suspicious footer-accounting corner case (e.g. an all-dictionary or
    # all-struct schema), not a genuine zero-byte chunk. Using it as-is would
    # stamp 0 weight onto real data, letting it skip FileAffinityPartitioner's
    # max_bucket_size flush entirely (the chunk's weight never advances). It
    # must fall back to on_disk_size x ratio, same as a missing (None) hint.
    manifest = FileManifest.construct_manifest(
        ["a", "b"],
        [10, 0],
        [
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 0},
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 0},
        ],
    )
    sizes = ParquetFooterDerivedInMemorySizeEstimator().estimate_in_memory_sizes(
        manifest
    )
    # Chunk "a" has real on-disk bytes (10) -> falls back to a nonzero ratio
    # estimate instead of the suspicious 0 hint.
    assert sizes[0] == 10 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    # Chunk "b" is genuinely empty (0 on-disk bytes too) -> the fallback also
    # computes 0, so behavior for a truly empty chunk is unchanged.
    assert sizes[1] == 0.0


def test_file_affinity_accumulates_fractional_weights_without_truncation():
    # Each chunk's estimated in-memory size is 1.6 (a float, as real estimators
    # return -- e.g. on_disk_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT). With
    # max_bucket_size=3, float accumulation flushes after the 2nd chunk
    # (1.6 + 1.6 = 3.2 >= 3), yielding partitions of sizes [2, 1]. Truncating each
    # chunk's weight to an int before accumulating (the pre-fix bug) would instead
    # sum 1 + 1 + 1 = 3, flushing only after the 3rd chunk into a single partition.
    class _FractionalEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return np.full(len(manifest), 1.6)

    table = _affinity_table(["a", "a", "a"], [1, 1, 1], [None, None, None])
    outputs = list(
        partition_files(
            iter([table]),
            MagicMock(),
            partitioner=FileAffinityPartitioner(
                in_memory_size_estimator=_FractionalEstimator(), max_bucket_size=3
            ),
        )
    )
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in outputs]
    assert partitions == [["a", "a"], ["a"]]


def test_file_affinity_handles_none_size_and_nan_estimate_without_raising():
    # None file sizes (HTTPFileSystem) and a NaN in-memory estimate must not
    # crash ``add_input``'s int(...) coercions; both are treated as 0 and the
    # file is still partitioned.
    class _NanEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return np.full(len(manifest), np.nan)

    table = _affinity_table(["a", "a"], [None, None], [None, None])
    outputs = list(
        partition_files(
            iter([table]),
            MagicMock(),
            partitioner=FileAffinityPartitioner(
                in_memory_size_estimator=_NanEstimator(), max_bucket_size=100
            ),
        )
    )
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in outputs]
    assert partitions == [["a", "a"]]


def test_file_affinity_flushes_previous_file_before_finalize(monkeypatch):
    # Pipelining proof: a file's partition becomes available as soon as the
    # NEXT file's chunks start arriving -- BEFORE finalize() -- so ReadFiles can
    # decode file "a" while later files' footers are still being read. Packing
    # disabled: with packing on, a completed small file joins the pending pack
    # instead of becoming immediately visible via has_partition(), which is
    # exactly the pipelining-latency trade-off packing makes (see the class
    # docstring and PR description) -- this test isolates the pipelining
    # mechanism itself, independent of that trade-off.
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    partitioner = FileAffinityPartitioner(
        in_memory_size_estimator=_FileSizeStubEstimator(), max_bucket_size=100
    )
    # File "a" arrives first; with no path change and no finalize it stays
    # buffered -- nothing is available yet.
    partitioner.add_input(_manifest(["a", "a"]))
    assert not partitioner.has_partition()
    # File "b" starts arriving -> "a" is provably complete and flushes now,
    # without waiting for finalize().
    partitioner.add_input(_manifest(["b"]))
    assert partitioner.has_partition()
    assert partitioner.next_partition().as_block()[PATH_COLUMN_NAME].to_pylist() == [
        "a",
        "a",
    ]
    # "b" is still open -- it only flushes at finalize.
    assert not partitioner.has_partition()
    partitioner.finalize()
    assert partitioner.next_partition().as_block()[PATH_COLUMN_NAME].to_pylist() == [
        "b"
    ]


def test_file_affinity_multi_block_file_stays_single_partition(monkeypatch):
    # A file whose chunks straddle two manifest blocks (two add_input calls)
    # must still land in ONE partition: _current_open_path is instance state, so
    # the same path continuing across a block boundary does NOT spuriously flush.
    # File "b" (fully in block 2) is its own partition. Packing disabled so
    # this test isolates the straddling invariant from cross-file packing
    # (with packing on, "a" and "b" -- both under max_bucket_size=100 -- would
    # additionally pack together into one partition; that's covered by the
    # dedicated packing tests, not this one).
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    partitioner = FileAffinityPartitioner(
        in_memory_size_estimator=_FileSizeStubEstimator(), max_bucket_size=100
    )
    partitioner.add_input(_manifest(["a", "a"]))  # block 1: a's first chunks
    partitioner.add_input(_manifest(["a", "b"]))  # block 2: a continues, then b
    partitioner.finalize()
    assert _drain_partitions(partitioner) == [["a", "a", "a"], ["b"]]


def test_file_affinity_overflow_then_path_change_never_mixes_files(monkeypatch):
    # File "a": 3 chunks, max_bucket_size=2 -> the size cap flushes ["a","a"]
    # mid-file and the 3rd chunk starts a fresh bucket; file "b" then arrives and
    # the path change flushes a's remaining ["a"] before "b". The overflow and
    # path-change flush paths cooperate and never mix files in a partition.
    # Packing disabled: packing intentionally allows small non-overflowing
    # remainders (like "a"'s trailing 1-chunk bucket here) to combine with
    # other small files' remainders -- that's the packing behavior itself, not
    # a violation of the invariant this test targets (overflow-vs-path-change
    # cooperation), so this test isolates the pre-packing behavior.
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    table = _affinity_table(["a", "a", "a", "b"], [1, 1, 1, 1], [None] * 4)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "a"], ["a"], ["b"]]
    assert all(len(set(p)) == 1 for p in partitions)


@pytest.mark.parametrize("pipeline_flush", [True, False])
def test_file_affinity_kill_switch_gates_incremental_flush(monkeypatch, pipeline_flush):
    # RAY_DATA_PARTITIONER_PIPELINE_FLUSH=0 reverts to finalize-only flushing.
    # Either way the final partition set is identical -- only WHEN "a" becomes
    # available differs (before finalize when on, at finalize when off).
    # Packing disabled so has_partition() timing isolates pipeline_flush alone
    # (with packing on, a completed "a" joins the pending pack instead of
    # output_queue, which would make has_partition() false regardless of
    # pipeline_flush -- see test_file_affinity_pack_files_kill_switch for the
    # packing knob's own dedicated test).
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "0")
    monkeypatch.setenv(
        "RAY_DATA_PARTITIONER_PIPELINE_FLUSH", "1" if pipeline_flush else "0"
    )
    partitioner = FileAffinityPartitioner(
        in_memory_size_estimator=_FileSizeStubEstimator(), max_bucket_size=100
    )
    partitioner.add_input(_manifest(["a", "a"]))
    partitioner.add_input(_manifest(["b"]))
    assert partitioner.has_partition() is pipeline_flush
    partitioner.finalize()
    assert _drain_partitions(partitioner) == [["a", "a"], ["b"]]


def test_file_affinity_pack_flushes_before_overflow():
    # Weights sum past max_bucket_size partway through: a(3), b(3), c(3) with
    # cap=5. "a" opens a pack (weight 3); "b" completes and 3+3=6 > 5, so the
    # pack (just ["a"]) flushes and "b" starts a fresh pack; "c" completes and
    # 3+3=6 > 5 again, so ["b"] flushes and "c" starts the next pack; finalize
    # flushes the trailing ["c"]. Packing doesn't force merges that don't fit.
    table = _affinity_table(["a", "b", "c"], [3, 3, 3], [None] * 3)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 5)]
    assert partitions == [["a"], ["b"], ["c"]]

    # Smaller weights that DO fit two-per-pack: a(2), b(2), c(2), d(2), cap=5.
    # "a"+"b" pack (weight 4); "c" completes and 4+2=6 > 5, so ["a","b"]
    # flushes and "c" starts a new pack; "d" completes and 2+2=4 is not > 5,
    # so it joins "c"'s pack; finalize flushes the trailing ["c","d"].
    table2 = _affinity_table(["a", "b", "c", "d"], [2, 2, 2, 2], [None] * 4)
    partitions2 = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table2, 5)
    ]
    assert partitions2 == [["a", "b"], ["c", "d"]]


def test_file_affinity_large_file_unaffected_by_packing():
    # A file that overflows its own bucket (4 chunks of weight 1, cap=2) still
    # splits into standalone overflow partitions exactly as pre-packing; small
    # files "b" and "c" that arrive afterward (weight 1 each, never overflow)
    # pack together instead of each getting their own partition.
    table = _affinity_table(
        ["a", "a", "a", "a", "b", "c"], [1, 1, 1, 1, 1, 1], [None] * 6
    )
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "a"], ["a", "a"], ["b", "c"]]


def test_file_affinity_mixed_large_and_small_files_in_one_manifest():
    # A single add_input call with a large-overflowing file interleaved (at
    # file granularity) with several small ones: overflow partitions stay
    # single-file; small ones pack, in correct arrival order relative to each
    # other and to the large file's split partitions.
    table = _affinity_table(
        ["x", "a", "a", "a", "a", "y"],
        [1, 1, 1, 1, 1, 1],
        [None] * 6,
    )
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    # "x" (weight 1) opens a pack; "a" starting means "x" completes and joins
    # the pack (pack=[x], weight 1); "a" overflows on its own at 2 chunks
    # (standalone, unaffected by packing) twice; "y" (weight 1) completes at
    # finalize and joins the still-open pack containing "x" (1+1=2 not > 2).
    assert partitions == [["a", "a"], ["a", "a"], ["x", "y"]]


def test_file_affinity_max_bucket_size_none_disables_packing():
    # max_bucket_size=None means no size cap at all -- _complete_file's
    # explicit None guard preserves the exact pre-packing behavior (one
    # partition per file) rather than needing None-handling in the pack-size
    # comparison itself. Not reachable via the real read_api.py call site
    # (which always resolves a concrete number before constructing the
    # partitioner), but the class's documented contract for direct use.
    table = _affinity_table(["a", "b", "c"], [1, 1, 1], [None] * 3)
    partitions = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, None)
    ]
    assert partitions == [["a"], ["b"], ["c"]]


@pytest.mark.parametrize("pack_files", [True, False])
def test_file_affinity_pack_files_kill_switch(monkeypatch, pack_files):
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "1" if pack_files else "0")
    table = _affinity_table(["a", "b", "c"], [1, 1, 1], [None] * 3)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 10)]
    if pack_files:
        assert partitions == [["a", "b", "c"]]
    else:
        assert partitions == [["a"], ["b"], ["c"]]


@pytest.mark.parametrize("pipeline_flush", [True, False])
@pytest.mark.parametrize("pack_files", [True, False])
def test_file_affinity_pipeline_flush_x_pack_files_interaction(
    monkeypatch, pipeline_flush, pack_files
):
    # The two kill switches are orthogonal axes (pipeline_flush controls WHEN
    # a completed file becomes flush-eligible; pack_files controls WHETHER it
    # emits standalone or packed). Assert no combination drops or
    # double-counts data -- the final partition SET (as a sorted list of
    # sorted path-tuples, since packing order/grouping legitimately differs
    # per combination) always covers exactly the 3 input files once each.
    monkeypatch.setenv(
        "RAY_DATA_PARTITIONER_PIPELINE_FLUSH", "1" if pipeline_flush else "0"
    )
    monkeypatch.setenv("RAY_DATA_PARTITIONER_PACK_FILES", "1" if pack_files else "0")
    table = _affinity_table(["a", "b", "c"], [1, 1, 1], [None] * 3)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 10)]
    all_paths = sorted(p for part in partitions for p in part)
    assert all_paths == ["a", "b", "c"]


def test_file_affinity_packing_is_deterministic_across_runs():
    # A pre-shuffled (fixed permutation) manifest run through two separate
    # partitioner instances must produce identical packed partitions every
    # time -- packing introduces no new randomness or dict-ordering
    # dependence on top of the existing deterministic arrival-order contract.
    table = _affinity_table(["m", "z", "a", "y"], [1, 1, 1, 1], [None] * 4)
    run1 = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 3)]
    run2 = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 3)]
    assert run1 == run2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
