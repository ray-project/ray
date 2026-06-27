import queue
import threading
import time
from typing import Iterator, List, Optional

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BatchTimings,
    BlockFetchTiming,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.iter_batches import (
    BatchIterator,
    prefetch_batches_locally,
    restore_original_order,
)
from ray.data._internal.block_batching.util import WaitBlockPrefetcher
from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry, RefBundle
from ray.data._internal.stats import DatasetStats, PipelineStage, TimeSpan
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


def ref_bundle_generator(num_rows: int, num_blocks: int) -> Iterator[RefBundle]:
    for i in range(num_blocks):
        block = pa.table({"foo": [i] * num_rows})
        metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            input_files=[],
            exec_stats=None,
        )
        schema = block.schema
        yield RefBundle(
            blocks=(BlockEntry(ray.put(block), metadata),),
            owns_blocks=True,
            schema=schema,
        )


@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
def test_prefetch_batches_locally(
    ray_start_regular_shared, num_batches_to_prefetch, batch_size
):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, block_refs: List[ObjectRef[Block]]):
            if batch_size is None:
                assert len(block_refs) == num_batches_to_prefetch
            else:
                assert (
                    sum(len(ray.get(block_ref)) for block_ref in block_refs)
                    >= batch_size * num_batches_to_prefetch
                )
            self.windows.append(block_refs)

    num_blocks = 10
    num_rows = 2
    prefetcher = DummyPrefetcher()
    ref_bundles = list(ref_bundle_generator(num_blocks=num_blocks, num_rows=num_rows))
    prefetch_block_iter = prefetch_batches_locally(
        iter(ref_bundles),
        prefetcher=prefetcher,
        num_batches_to_prefetch=num_batches_to_prefetch,
        batch_size=batch_size,
    )

    block_count = 0
    prefetched_blocks = []
    previous_num_windows = 1

    for block in prefetch_block_iter:
        prefetched_blocks.append(block)
        block_count += 1
        remaining_rows = (num_blocks - block_count) * num_rows
        if batch_size is None and block_count < num_blocks - num_batches_to_prefetch:
            # Test that we are actually prefetching in advance if this is not the last
            # block.
            assert len(prefetcher.windows) == previous_num_windows + 1
            previous_num_windows = len(prefetcher.windows)
        elif (
            batch_size is not None
            and remaining_rows > batch_size * num_batches_to_prefetch
        ):
            # Test that we are actually prefetching in advance if this is not the last
            # batch.
            assert len(prefetcher.windows) == previous_num_windows + 1
            previous_num_windows = len(prefetcher.windows)

    # Test that original blocks are unchanged.
    expected_blocks = []
    for ref_bundle in ref_bundles:
        expected_blocks.extend(ref_bundle.block_refs)
    assert prefetched_blocks == expected_blocks


def test_restore_from_original_order():
    base_iterator = [
        Batch(BatchMetadata(batch_idx=1), None),
        Batch(BatchMetadata(batch_idx=0), None),
        Batch(BatchMetadata(batch_idx=3), None),
        Batch(BatchMetadata(batch_idx=2), None),
    ]

    ordered = list(restore_original_order(iter(base_iterator)))
    idx = [batch.metadata.batch_idx for batch in ordered]
    assert idx == [0, 1, 2, 3]


def test_restore_original_order_stats():
    base_iterator = [
        Batch(BatchMetadata(batch_idx=2), None),
        Batch(BatchMetadata(batch_idx=0), None),
        Batch(BatchMetadata(batch_idx=1), None),
    ]

    ordered = list(restore_original_order(iter(base_iterator)))

    assert [batch.metadata.batch_idx for batch in ordered] == [0, 1, 2]


def test_attribute_blocked_time_overlap_attribution():
    stats = DatasetStats(metadata={}, parent=None)
    batch_iterator = BatchIterator(iter([]), stats=stats)
    timings = BatchTimings()
    timings.production_wait = TimeSpan(start_s=10.0, end_s=20.0)
    timings.batching = TimeSpan(start_s=20.0, end_s=30.0)
    timings.format = TimeSpan(start_s=30.0, end_s=40.0)
    timings.finalize = TimeSpan(start_s=50.0, end_s=60.0)
    batch = Batch(BatchMetadata(batch_idx=0, num_rows=8, timings=timings), None)

    batch_iterator._attribute_blocked_time(
        batch, blocked_start_s=15.0, blocked_end_s=35.0
    )

    assert stats.iter_blocked_production_wait_s.get() == pytest.approx(5.0)
    assert stats.iter_blocked_batching_s.get() == pytest.approx(10.0)
    assert stats.iter_blocked_format_s.get() == pytest.approx(5.0)
    assert stats.iter_blocked_collate_s.get() == 0
    assert stats.iter_blocked_finalize_s.get() == 0
    assert stats.iter_batches_total == 1
    assert stats.iter_rows_total == 8


def _make_span(start: float, end: float) -> Optional[TimeSpan]:
    """Create a TimeSpan, or None if the stage didn't execute (both zero)."""
    if start == 0.0 and end == 0.0:
        return None
    return TimeSpan(start_s=start, end_s=end)


def _make_batch_with_timings(
    production_wait_start=0.0,
    production_wait_end=0.0,
    data_transfer_start=0.0,
    data_transfer_end=0.0,
    batching_start=0.0,
    batching_end=0.0,
    format_start=0.0,
    format_end=0.0,
    collate_start=0.0,
    collate_end=0.0,
    finalize_start=0.0,
    finalize_end=0.0,
    num_rows=0,
):
    """Helper to construct a Batch with specific stage timing windows."""
    timings = BatchTimings()
    timings.production_wait = _make_span(production_wait_start, production_wait_end)
    timings.data_transfer = _make_span(data_transfer_start, data_transfer_end)
    timings.batching = _make_span(batching_start, batching_end)
    timings.format = _make_span(format_start, format_end)
    timings.collate = _make_span(collate_start, collate_end)
    timings.finalize = _make_span(finalize_start, finalize_end)
    return Batch(BatchMetadata(batch_idx=0, num_rows=num_rows, timings=timings), None)


def _make_report_iterator(stats):
    """Create a BatchIterator wired to the given stats without a real pipeline."""
    it = BatchIterator.__new__(BatchIterator)
    it._stats = stats
    return it


class TestReportBatchTimingsEdgeCases:
    """Edge case tests for overlap-based blocked attribution."""

    def test_zero_overlap_stage_finished_before_blocked(self):
        """Fetch [0, 1.5] finished before training blocked at t=2 → 0 attribution."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(
            production_wait_start=0.0, production_wait_end=1.5
        )
        it._attribute_blocked_time(batch, blocked_start_s=2.0, blocked_end_s=3.0)
        assert stats.iter_blocked_production_wait_s.get() == 0.0

    def test_zero_overlap_blocked_before_stage(self):
        """Training blocked [0, 1], stage ran [2, 3] → 0 attribution."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(format_start=2.0, format_end=3.0)
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=1.0)
        assert stats.iter_blocked_format_s.get() == 0.0

    def test_partial_overlap(self):
        """Fetch [0, 2], blocked [1, 3] → overlap = min(2,3)-max(0,1) = 1.0."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(
            production_wait_start=0.0, production_wait_end=2.0
        )
        it._attribute_blocked_time(batch, blocked_start_s=1.0, blocked_end_s=3.0)
        assert stats.iter_blocked_production_wait_s.get() == pytest.approx(1.0)

    def test_full_overlap_stage_inside_blocked(self):
        """Stage [1, 2] entirely inside blocked [0, 3] → full 1.0 credit."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(batching_start=1.0, batching_end=2.0)
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=3.0)
        assert stats.iter_blocked_batching_s.get() == pytest.approx(1.0)

    def test_no_collate_fn_zero_attribution(self):
        """collate stage has start_s=0 → skipped, 0 attribution."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(format_start=1.0, format_end=2.0)
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=3.0)
        assert stats.iter_blocked_format_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_collate_s.get() == 0.0

    def test_no_finalize_fn_zero_attribution(self):
        """finalize stage has start_s=0 → skipped, 0 attribution."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(collate_start=1.0, collate_end=2.0)
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=3.0)
        assert stats.iter_blocked_collate_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_finalize_s.get() == 0.0

    def test_prefetch_hides_fetch_from_training(self):
        """Effective prefetch: fetch done before training blocks → 0 fetch attribution."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(
            production_wait_start=0.0,
            production_wait_end=1.5,
            collate_start=2.3,
            collate_end=2.6,
        )
        # Training only starts blocking at t=2 (prefetch worked)
        it._attribute_blocked_time(batch, blocked_start_s=2.0, blocked_end_s=2.6)
        assert stats.iter_blocked_production_wait_s.get() == 0.0
        assert stats.iter_blocked_collate_s.get() == pytest.approx(0.3)

    def test_accumulation_across_batches(self):
        """Two batches each contribute to fetch — values accumulate."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        # Batch 1: fetch [0,1], blocked [0,2] → overlap 1.0
        b1 = _make_batch_with_timings(
            production_wait_start=0.0, production_wait_end=1.0, num_rows=10
        )
        it._attribute_blocked_time(b1, blocked_start_s=0.0, blocked_end_s=2.0)
        # Batch 2: fetch [5,6], blocked [5,7] → overlap 1.0
        b2 = _make_batch_with_timings(
            production_wait_start=5.0, production_wait_end=6.0, num_rows=20
        )
        it._attribute_blocked_time(b2, blocked_start_s=5.0, blocked_end_s=7.0)

        assert stats.iter_blocked_production_wait_s.get() == pytest.approx(2.0)
        assert stats.iter_batches_total == 2
        assert stats.iter_rows_total == 30

    def test_overlap_invariant_sum_leq_total(self):
        """sum(iter_blocked_*) <= iter_total_blocked_s always holds."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        stats.iter_total_blocked_s.add(5.0)
        batch = _make_batch_with_timings(
            production_wait_start=0.0,
            production_wait_end=1.0,
            batching_start=1.0,
            batching_end=2.0,
            format_start=2.0,
            format_end=3.0,
            num_rows=5,
        )
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=5.0)

        total = stats.iter_total_blocked_s.get()
        sum_stages = (
            stats.iter_blocked_production_wait_s.get()
            + stats.iter_blocked_batching_s.get()
            + stats.iter_blocked_format_s.get()
            + stats.iter_blocked_collate_s.get()
            + stats.iter_blocked_finalize_s.get()
        )
        assert sum_stages <= total + 1e-9

    def test_blocked_inside_stage(self):
        """Stage [0, 10] fully contains blocked [3, 5] → overlap = 2.0."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(
            production_wait_start=0.0, production_wait_end=10.0
        )
        it._attribute_blocked_time(batch, blocked_start_s=3.0, blocked_end_s=5.0)
        assert stats.iter_blocked_production_wait_s.get() == pytest.approx(2.0)

    def test_all_stages_simultaneous_overlap(self):
        """Multiple stages overlap with blocked window simultaneously."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        batch = _make_batch_with_timings(
            production_wait_start=0.0,
            production_wait_end=1.0,
            batching_start=1.0,
            batching_end=2.0,
            format_start=2.0,
            format_end=3.0,
            collate_start=3.0,
            collate_end=4.0,
            finalize_start=4.0,
            finalize_end=5.0,
            num_rows=100,
        )
        # Blocked window covers all stages
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=5.0)
        assert stats.iter_blocked_production_wait_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_batching_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_format_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_collate_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_finalize_s.get() == pytest.approx(1.0)
        assert stats.iter_batches_total == 1
        assert stats.iter_rows_total == 100


class TestTimeSpan:
    """Tests for TimeSpan dataclass."""

    def test_default_values(self):
        """Default TimeSpan has start_s=0 and end_s=0."""
        t = TimeSpan()
        assert t.start_s == 0.0
        assert t.end_s == 0.0

    def test_duration(self):
        """Duration is end_s - start_s."""
        t = TimeSpan(start_s=1.0, end_s=3.5)
        assert t.duration == pytest.approx(2.5)

    def test_zero_duration(self):
        """Default TimeSpan has zero duration."""
        t = TimeSpan()
        assert t.duration == 0.0


class TestMergeFetch:
    """Tests for BatchTimings.merge_fetch() with multiple blocks per batch."""

    def test_merge_single_block(self):
        """Merging a single block preserves its fetch window."""
        dst = BatchTimings()
        src = BatchTimings()
        src.production_wait = TimeSpan(start_s=1.0, end_s=2.0)
        dst.merge_fetch(src)
        assert dst.production_wait.start_s == 1.0
        assert dst.production_wait.end_s == 2.0

    def test_merge_multiple_blocks_expands_window(self):
        """Merging multiple blocks produces the union window."""
        dst = BatchTimings()

        # Block 1: fetched [1.0, 2.0]
        src1 = BatchTimings()
        src1.production_wait = TimeSpan(start_s=1.0, end_s=2.0)
        dst.merge_fetch(src1)

        # Block 2: fetched [3.0, 4.0]
        src2 = BatchTimings()
        src2.production_wait = TimeSpan(start_s=3.0, end_s=4.0)
        dst.merge_fetch(src2)

        # Block 3: fetched [5.0, 6.0]
        src3 = BatchTimings()
        src3.production_wait = TimeSpan(start_s=5.0, end_s=6.0)
        dst.merge_fetch(src3)

        # Union: [1.0, 6.0]
        assert dst.production_wait.start_s == 1.0
        assert dst.production_wait.end_s == 6.0

    def test_merge_unrecorded_block_ignored(self):
        """Merging a block with no fetch timing (start_s=0) is a no-op."""
        dst = BatchTimings()
        dst.production_wait = TimeSpan(start_s=2.0, end_s=3.0)

        src = BatchTimings()  # fetch defaults to (0.0, 0.0)
        dst.merge_fetch(src)

        assert dst.production_wait.start_s == 2.0
        assert dst.production_wait.end_s == 3.0

    def test_merge_overlapping_blocks(self):
        """Overlapping fetch windows are correctly merged."""
        dst = BatchTimings()

        src1 = BatchTimings()
        src1.production_wait = TimeSpan(start_s=1.0, end_s=5.0)
        dst.merge_fetch(src1)

        src2 = BatchTimings()
        src2.production_wait = TimeSpan(start_s=3.0, end_s=7.0)
        dst.merge_fetch(src2)

        # Union: [1.0, 7.0]
        assert dst.production_wait.start_s == 1.0
        assert dst.production_wait.end_s == 7.0

    def test_merge_into_empty_destination(self):
        """Merging into an empty BatchTimings takes the source window."""
        dst = BatchTimings()  # fetch = (0.0, 0.0)
        src = BatchTimings()
        src.production_wait = TimeSpan(start_s=10.0, end_s=20.0)
        dst.merge_fetch(src)
        assert dst.production_wait.start_s == 10.0
        assert dst.production_wait.end_s == 20.0

    def test_merge_data_transfer_multiple_blocks(self):
        """data_transfer windows are unioned across multiple blocks."""
        dst = BatchTimings()

        src1 = BlockFetchTiming(
            data_transfer=TimeSpan(start_s=1.0, end_s=2.0)
        )
        dst.merge_fetch(src1)

        src2 = BlockFetchTiming(
            data_transfer=TimeSpan(start_s=3.0, end_s=4.0)
        )
        dst.merge_fetch(src2)

        # Union: [1.0, 4.0]
        assert dst.data_transfer.start_s == 1.0
        assert dst.data_transfer.end_s == 4.0

    def test_merge_data_transfer_overlapping_blocks(self):
        """Overlapping data_transfer windows are correctly merged."""
        dst = BatchTimings()

        dst.merge_fetch(
            BlockFetchTiming(data_transfer=TimeSpan(start_s=1.0, end_s=5.0))
        )
        dst.merge_fetch(
            BlockFetchTiming(data_transfer=TimeSpan(start_s=3.0, end_s=7.0))
        )

        assert dst.data_transfer.start_s == 1.0
        assert dst.data_transfer.end_s == 7.0

    def test_merge_both_stages_independent(self):
        """production_wait and data_transfer merge independently."""
        dst = BatchTimings()

        # Block 1: prod [1,2], xfer [2,3]
        dst.merge_fetch(
            BlockFetchTiming(
                production_wait=TimeSpan(start_s=1.0, end_s=2.0),
                data_transfer=TimeSpan(start_s=2.0, end_s=3.0),
            )
        )
        # Block 2: prod [5,6], xfer [6,7]
        dst.merge_fetch(
            BlockFetchTiming(
                production_wait=TimeSpan(start_s=5.0, end_s=6.0),
                data_transfer=TimeSpan(start_s=6.0, end_s=7.0),
            )
        )

        # Each stage unions independently.
        assert dst.production_wait.start_s == 1.0
        assert dst.production_wait.end_s == 6.0
        assert dst.data_transfer.start_s == 2.0
        assert dst.data_transfer.end_s == 7.0

    def test_merge_data_transfer_none_preserves_destination(self):
        """Merging a block with no data_transfer timing leaves dst unchanged."""
        dst = BatchTimings()
        dst.data_transfer = TimeSpan(start_s=2.0, end_s=3.0)

        # src has only production_wait, data_transfer is None
        dst.merge_fetch(
            BlockFetchTiming(production_wait=TimeSpan(start_s=1.0, end_s=2.0))
        )

        assert dst.data_transfer.start_s == 2.0
        assert dst.data_transfer.end_s == 3.0


class TestEndToEndTimingPropagation:
    """Tests that stage timings propagate correctly through the full pipeline."""

    def test_batch_carries_timings_through_pipeline(self):
        """A Batch's metadata.timings carries all stage windows."""
        timings = BatchTimings()
        timings.production_wait = TimeSpan(start_s=1.0, end_s=2.0)
        timings.batching = TimeSpan(start_s=2.0, end_s=3.0)
        timings.format = TimeSpan(start_s=3.0, end_s=4.0)
        timings.collate = TimeSpan(start_s=4.0, end_s=5.0)
        timings.finalize = TimeSpan(start_s=5.0, end_s=6.0)

        batch = Batch(BatchMetadata(batch_idx=0, num_rows=50, timings=timings), None)

        # Verify all stages are accessible via stages() iterator
        stage_dict = dict(batch.metadata.timings.stages())
        assert len(stage_dict) == 6
        assert stage_dict[PipelineStage.PRODUCTION_WAIT].start_s == 1.0
        assert stage_dict[PipelineStage.BATCHING].end_s == 3.0
        assert stage_dict[PipelineStage.FORMAT].start_s == 3.0
        assert stage_dict[PipelineStage.COLLATE].end_s == 5.0
        assert stage_dict[PipelineStage.FINALIZE].start_s == 5.0
        assert batch.metadata.num_rows == 50

    def test_full_pipeline_attribution(self):
        """End-to-end: all 5 stages with realistic timing, full overlap."""
        stats = DatasetStats(metadata={}, parent=None)
        it = _make_report_iterator(stats)
        stats.iter_total_blocked_s.add(5.0)

        batch = _make_batch_with_timings(
            production_wait_start=0.0,
            production_wait_end=0.5,
            batching_start=0.5,
            batching_end=1.0,
            format_start=1.0,
            format_end=2.0,
            collate_start=2.0,
            collate_end=2.5,
            finalize_start=2.5,
            finalize_end=3.0,
            num_rows=256,
        )

        # Blocked window covers all stages
        it._attribute_blocked_time(batch, blocked_start_s=0.0, blocked_end_s=5.0)

        # Each stage gets its full duration
        assert stats.iter_blocked_production_wait_s.get() == pytest.approx(0.5)
        assert stats.iter_blocked_batching_s.get() == pytest.approx(0.5)
        assert stats.iter_blocked_format_s.get() == pytest.approx(1.0)
        assert stats.iter_blocked_collate_s.get() == pytest.approx(0.5)
        assert stats.iter_blocked_finalize_s.get() == pytest.approx(0.5)
        assert stats.iter_batches_total == 1
        assert stats.iter_rows_total == 256

        # Invariant: sum = 3.0 <= total_blocked = 5.0
        sum_stages = (
            stats.iter_blocked_production_wait_s.get()
            + stats.iter_blocked_batching_s.get()
            + stats.iter_blocked_format_s.get()
            + stats.iter_blocked_collate_s.get()
            + stats.iter_blocked_finalize_s.get()
        )
        assert sum_stages == pytest.approx(3.0)
        assert sum_stages <= stats.iter_total_blocked_s.get() + 1e-9


def test_finalize_fn_uses_single_thread(ray_start_regular_shared):
    """Tests that finalize_fn is not run with multiple threads."""
    ref_bundles_iter = ref_bundle_generator(num_blocks=20, num_rows=2)

    q = queue.Queue()
    semaphore = threading.Semaphore(value=1)

    def finalize_enforce_single_thread(batch):
        already_acquired = not semaphore.acquire(blocking=False)
        if already_acquired:
            e = AssertionError("finalize_fn is being run concurrently.")
            q.put(e, block=True)
        semaphore.release()
        return batch

    # Test that finalize_fn is called in a single thread,
    # even if prefetch_batches is set.
    output_batches = BatchIterator(
        ref_bundles_iter,
        collate_fn=lambda batch: batch,
        finalize_fn=finalize_enforce_single_thread,
        prefetch_batches=4,
    )

    # Force execution of the iterator.
    # This step should not raise an exception.
    list(output_batches)

    try:
        e = q.get(block=False, timeout=0.1)
        raise e
    except queue.Empty:
        pass


# Test for 3 cases
# 1. Batch size is less than block size
# 2. Batch size is more than block size
# 3. Block size is not divisble by batch size
@pytest.mark.parametrize("batch_size", [1, 4, 3])
@pytest.mark.parametrize("drop_last", [True, False])
@pytest.mark.parametrize("prefetch_batches", [0, 1])
def test_iter_batches_e2e(
    ray_start_regular_shared, batch_size, drop_last, prefetch_batches
):
    def collate_fn(batch: pd.DataFrame):
        return batch + 1

    ref_bundles_iter = ref_bundle_generator(num_blocks=4, num_rows=2)

    output_batches = BatchIterator(
        ref_bundles_iter,
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
        batch_format="pandas",
        collate_fn=collate_fn,
        drop_last=drop_last,
        preserve_order=True,
    )

    output_batches = list(output_batches)

    assert len(output_batches) > 0
    for df in output_batches:
        # Check batch formatting.
        assert isinstance(df, pd.DataFrame)
        # Check batch size.
        if batch_size == 3 and not drop_last:
            assert len(df) in {2, 3}
        else:
            assert len(df) == batch_size

    concat_df = pd.concat(output_batches)
    # Test that collate_fn is applied.
    assert concat_df["foo"].iloc[0] == 1
    # Make sure order is preserved.
    for i in range(len(concat_df) - 1):
        assert concat_df["foo"].iloc[i + 1] >= concat_df["foo"].iloc[i]


def test_iter_batches_counts_rows_at_pipeline_exit(ray_start_regular_shared):
    stats = DatasetStats(metadata={}, parent=None)
    ref_bundles_iter = ref_bundle_generator(num_blocks=4, num_rows=2)

    output_batches = list(
        BatchIterator(
            ref_bundles_iter,
            stats=stats,
            batch_size=3,
            prefetch_batches=0,
            batch_format="pandas",
            drop_last=True,
        )
    )

    assert len(output_batches) == 2
    assert [len(batch) for batch in output_batches] == [3, 3]
    assert stats.iter_batches_total == 2
    assert stats.iter_rows_total == 6


def test_iter_batches_e2e_async(ray_start_regular_shared):
    """We add time.sleep in 3 places:
    1. In the base generator to simulate streaming executor blocking on next results.
    2. In the collate_fn to simulate expensive slicing/formatting/collation
    3. In the user thread to simulate training.
    """

    def collate_fn(batch):
        time.sleep(2)
        return batch

    ref_bundles = ref_bundle_generator(num_blocks=20, num_rows=2)
    start_time = time.time()
    output_batches = BatchIterator(
        ref_bundles,
        batch_size=None,
        collate_fn=collate_fn,
        prefetch_batches=4,
    )
    batches = []
    for batch in output_batches:
        time.sleep(1.5)
        batches.append(batch)
    end_time = time.time()

    # 20 batches, 1.5 second sleep. Should be less than 45 seconds, even with some
    # overhead.
    # If there was no overlap, then we would expect this to take at least 20*2.5 = 50
    assert end_time - start_time < 45, end_time - start_time

    assert len(batches) == 20
    assert all(len(batch) == 2 for batch in batches)


@pytest.mark.parametrize("preserve_order", [True, False])
def test_iter_batches_preserve_order_flag(
    ray_start_regular_shared, preserve_order, restore_data_context
):
    """When `execution_options.preserve_order` is True, batches must come
    out in input order even with a multi-worker format threadpool. When
    False, ordering is not guaranteed (but the full set of batches must
    still be produced)."""
    # Variable per-batch collate cost makes worker-completion order
    # arbitrary so the reorder path actually does work when enabled.
    def collate_fn(batch):
        idx = int(batch["foo"][0])
        time.sleep(0.05 * (idx % 4))
        return batch

    num_blocks = 16
    ref_bundles = ref_bundle_generator(num_blocks=num_blocks, num_rows=1)
    output_batches = list(
        BatchIterator(
            ref_bundles,
            batch_size=1,
            collate_fn=collate_fn,
            batch_format="pandas",
            prefetch_batches=4,
            preserve_order=preserve_order,
        )
    )

    indices = [int(df["foo"].iloc[0]) for df in output_batches]
    assert sorted(indices) == list(range(num_blocks))
    if preserve_order:
        assert indices == list(range(num_blocks)), indices


def _ref_bundles_with_size(
    num_blocks: int, num_rows: int, size_bytes_per_block: int
) -> Iterator[RefBundle]:
    """Create ref bundles with explicit size_bytes for testing."""
    for i in range(num_blocks):
        block = pa.table({"foo": [i] * num_rows})
        metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes_per_block,
            input_files=[],
            exec_stats=None,
        )
        schema = block.schema
        yield RefBundle(
            blocks=(BlockEntry(ray.put(block), metadata),),
            owns_blocks=True,
            schema=schema,
        )


@pytest.mark.parametrize(
    "num_batches_to_prefetch,expected_bytes_sequence",
    [
        # No prefetching: all 5 blocks report 0 prefetched bytes
        (0, [0, 0, 0, 0, 0]),
        # prefetch 2 blocks: with 5 blocks of 100 bytes each
        # After yield block 0: window has 1,2 -> 200 (added block 2)
        # After yield block 1: window has 2,3 -> 200 (added block 3)
        # After yield block 2: window has 3,4 -> 200 (added block 4)
        # After yield block 3: window has 4 -> 100 (no more to add)
        # After yield block 4: window empty -> 0
        (2, [200, 200, 200, 100, 0]),
    ],
)
def test_prefetch_bytes_tracking(
    ray_start_regular_shared, num_batches_to_prefetch, expected_bytes_sequence
):
    """Test iter_prefetched_bytes is set correctly during prefetching.

    Tests prefetch_batches_locally directly to verify exact values,
    bypassing async BatchIterator which has non-deterministic timing.
    """
    stats = DatasetStats(metadata={}, parent=None)

    # Create 5 ref bundles, each with size_bytes=100
    num_blocks = 5
    ref_bundles = list(
        _ref_bundles_with_size(num_blocks, num_rows=2, size_bytes_per_block=100)
    )

    prefetcher = WaitBlockPrefetcher()
    block_iter = prefetch_batches_locally(
        iter(ref_bundles),
        prefetcher=prefetcher,
        num_batches_to_prefetch=num_batches_to_prefetch,
        batch_size=None,
        stats=stats,
    )

    # Track iter_prefetched_bytes after each block is yielded
    recorded_bytes = []
    for _ in block_iter:
        recorded_bytes.append(stats.iter_prefetched_bytes)

    assert recorded_bytes == expected_bytes_sequence, f"Got {recorded_bytes}"


@pytest.mark.parametrize("prefetch_batches", [0, 2])
def test_prefetch_bytes_callback(ray_start_regular_shared, prefetch_batches):
    """Test prefetch_bytes_callback is invoked correctly by BatchIterator."""
    reported_bytes = []

    def prefetch_callback(num_bytes: int):
        reported_bytes.append(num_bytes)

    stats = DatasetStats(metadata={}, parent=None)

    # Create 5 ref bundles
    num_blocks = 5
    ref_bundles = list(
        _ref_bundles_with_size(num_blocks, num_rows=2, size_bytes_per_block=100)
    )

    output_batches = BatchIterator(
        iter(ref_bundles),
        stats=stats,
        batch_size=None,
        prefetch_batches=prefetch_batches,
        prefetch_bytes_callback=prefetch_callback,
    )

    # Consume all batches
    batches = list(output_batches)

    assert len(batches) == 5

    # Callback is called 5 times (per batch) + 1 time at epoch end
    assert len(reported_bytes) == 6, f"Expected 6, got {len(reported_bytes)}"

    # All values should be non-negative
    assert all(b >= 0 for b in reported_bytes), f"Negative: {reported_bytes}"

    # Last value should be 0 (after_epoch_end)
    assert reported_bytes[-1] == 0, f"Last should be 0: {reported_bytes}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
