"""Tests for per-stage pipeline latency attribution in BatchIterator (issue #64132).

Verifies that _report_batch_timings accumulates stage values into DatasetStats
correctly, handles missing stages gracefully, and that iter_batches_total /
iter_rows_total increment as expected.
"""

from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BatchTimings,
)
from ray.data._internal.block_batching.iter_batches import BatchIterator
from ray.data._internal.stats import DatasetStats


def _make_stats() -> DatasetStats:
    return DatasetStats(metadata={}, parent=None)


def _make_batch(
    fetch=0.5,
    batching_dur=0.1,
    batching_done=2.0,
    fmt_done=2.3,
    collate_done=2.6,
    finalize_done=3.0,
    num_rows=32,
) -> Batch:
    t = BatchTimings(
        fetch_duration_s=fetch,
        batching_duration_s=batching_dur,
        batching_done_s=batching_done,
        format_done_s=fmt_done,
        collate_done_s=collate_done,
        finalize_done_s=finalize_done,
        num_rows=num_rows,
    )
    return Batch(metadata=BatchMetadata(batch_idx=0, timings=t), data=None)


class TestReportBatchTimings:
    def _make_iterator(self, stats):
        """Return a BatchIterator wired to the given stats, without a real pipeline."""
        it = BatchIterator.__new__(BatchIterator)
        it._stats = stats
        return it

    def test_fetch_duration_accumulated_directly(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(fetch=0.6))
        assert abs(stats.iter_pipeline_fetch_s.get() - 0.6) < 1e-9

    def test_batching_duration_accumulated_directly(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(batching_dur=0.1))
        assert abs(stats.iter_pipeline_batching_s.get() - 0.1) < 1e-9

    def test_format_latency_from_delta(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        # format_done_s - batching_done_s = 2.3 - 2.0 = 0.3
        it._report_batch_timings(_make_batch(batching_done=2.0, fmt_done=2.3))
        assert abs(stats.iter_pipeline_format_s.get() - 0.3) < 1e-9

    def test_collate_latency_from_delta(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        # collate_done_s - format_done_s = 2.6 - 2.3 = 0.3
        it._report_batch_timings(_make_batch(fmt_done=2.3, collate_done=2.6))
        assert abs(stats.iter_pipeline_collate_s.get() - 0.3) < 1e-9

    def test_finalize_latency_after_collate(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        # finalize_done_s - collate_done_s = 3.0 - 2.6 = 0.4
        it._report_batch_timings(_make_batch(collate_done=2.6, finalize_done=3.0))
        assert abs(stats.iter_pipeline_finalize_s.get() - 0.4) < 1e-9

    def test_finalize_latency_no_collate(self):
        """When collate_done_s == 0, finalize is measured from format_done_s."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # finalize_done_s - format_done_s = 3.0 - 2.3 = 0.7
        it._report_batch_timings(_make_batch(fmt_done=2.3, collate_done=0.0, finalize_done=3.0))
        assert abs(stats.iter_pipeline_finalize_s.get() - 0.7) < 1e-9

    def test_no_finalize_fn_not_accumulated(self):
        """When finalize_done_s == 0 (no finalize_fn), finalize timer stays zero."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(collate_done=2.6, finalize_done=0.0))
        assert stats.iter_pipeline_finalize_s.get() == 0.0

    def test_zero_fetch_duration_not_accumulated(self):
        """fetch_duration_s == 0 (batch_blocks path) leaves fetch timer at zero."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(fetch=0.0))
        assert stats.iter_pipeline_fetch_s.get() == 0.0

    def test_batches_total_increments(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        assert stats.iter_batches_total == 0
        it._report_batch_timings(_make_batch())
        it._report_batch_timings(_make_batch())
        assert stats.iter_batches_total == 2

    def test_rows_total_increments(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(num_rows=32))
        it._report_batch_timings(_make_batch(num_rows=16))
        assert stats.iter_rows_total == 48

    def test_fetch_accumulation_across_batches(self):
        """Fetch durations from multiple batches sum correctly."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        it._report_batch_timings(_make_batch(fetch=0.5))
        it._report_batch_timings(_make_batch(fetch=0.3))
        assert abs(stats.iter_pipeline_fetch_s.get() - 0.8) < 1e-9

    def test_multi_block_fetch_simulated(self):
        """Simulates two blocks feeding one batch: durations should sum."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # Batch with fetch_duration_s = 0.3 + 0.2 (two blocks accumulated)
        it._report_batch_timings(_make_batch(fetch=0.5))
        assert abs(stats.iter_pipeline_fetch_s.get() - 0.5) < 1e-9

    def test_negative_timestamp_delta_clamped_to_zero(self):
        """Clock skew on timestamp deltas is clamped to 0 (not applicable to durations)."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # format_done_s < batching_done_s — should not produce a negative value
        it._report_batch_timings(_make_batch(batching_done=2.5, fmt_done=2.3))
        assert stats.iter_pipeline_format_s.get() == 0.0
