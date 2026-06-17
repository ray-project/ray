"""Tests for per-stage blocked-time attribution in BatchIterator (issue #64132).

Verifies that _report_batch_timings computes overlap correctly for each pipeline
stage given (blocked_start, blocked_end) from the training thread, accumulates
into DatasetStats, and handles edge cases (zero overlap, partial overlap, etc.).
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
    fetch_start=0.0,
    fetch_end=1.5,
    batching_start=1.5,
    batching_done=2.0,
    fmt_done=2.3,
    collate_done=2.6,
    finalize_done=3.0,
    num_rows=32,
) -> Batch:
    t = BatchTimings(
        fetch_start_s=fetch_start,
        fetch_end_s=fetch_end,
        batching_start_s=batching_start,
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

    def _report(self, it, batch, blocked_start, blocked_end):
        it._report_batch_timings(batch, blocked_start, blocked_end)

    # ── Full-overlap cases (stage entirely inside blocked window) ──────────────

    def test_fetch_full_overlap(self):
        """Stage [0, 1.5] fully inside blocked [0, 2]. Attribution = 1.5."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=1.5), 0.0, 2.0)
        assert abs(stats.iter_blocked_fetch_s.get() - 1.5) < 1e-9

    def test_batching_full_overlap(self):
        """Stage [1.5, 2.0] fully inside blocked [0, 3]. Attribution = 0.5."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(batching_start=1.5, batching_done=2.0), 0.0, 3.0)
        assert abs(stats.iter_blocked_batching_s.get() - 0.5) < 1e-9

    def test_format_full_overlap(self):
        """Format [2.0, 2.3] inside blocked [0, 3]. Attribution = 0.3."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(batching_done=2.0, fmt_done=2.3), 0.0, 3.0)
        assert abs(stats.iter_blocked_format_s.get() - 0.3) < 1e-9

    def test_collate_full_overlap(self):
        """Collate [2.3, 2.6] inside blocked [0, 3]. Attribution = 0.3."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(fmt_done=2.3, collate_done=2.6), 0.0, 3.0)
        assert abs(stats.iter_blocked_collate_s.get() - 0.3) < 1e-9

    def test_finalize_full_overlap_after_collate(self):
        """Finalize [2.6, 3.0] inside blocked [0, 4]. Attribution = 0.4."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(collate_done=2.6, finalize_done=3.0), 0.0, 4.0)
        assert abs(stats.iter_blocked_finalize_s.get() - 0.4) < 1e-9

    def test_finalize_no_collate(self):
        """When collate_done_s == 0, finalize is measured from format_done_s."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # Finalize [2.3, 3.0] inside blocked [0, 4]. Attribution = 0.7.
        self._report(
            it,
            _make_batch(fmt_done=2.3, collate_done=0.0, finalize_done=3.0),
            0.0,
            4.0,
        )
        assert abs(stats.iter_blocked_finalize_s.get() - 0.7) < 1e-9

    # ── Zero-overlap cases (stage finished before training blocked) ───────────

    def test_fetch_zero_overlap_finished_before_blocked(self):
        """Fetch [0, 1.5] done before training blocked at t=2. Overlap = 0."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # blocked_start=2.0 > fetch_end=1.5 → no overlap
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=1.5), 2.0, 3.0)
        assert stats.iter_blocked_fetch_s.get() == 0.0

    def test_batching_zero_overlap(self):
        """Batching done before training blocked. Attribution = 0."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(batching_start=0.5, batching_done=1.0), 2.0, 3.0)
        assert stats.iter_blocked_batching_s.get() == 0.0

    def test_no_fetch_timestamps_zero_attribution(self):
        """fetch_start_s == 0 (batch_blocks path) → no fetch attribution."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=0.0), 0.0, 2.0)
        assert stats.iter_blocked_fetch_s.get() == 0.0

    def test_no_finalize_fn_zero_attribution(self):
        """finalize_done_s == 0 (no finalize_fn) → zero finalize attribution."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(collate_done=2.6, finalize_done=0.0), 0.0, 3.0)
        assert stats.iter_blocked_finalize_s.get() == 0.0

    # ── Partial overlap (prefetch hides part of the stage) ────────────────────

    def test_partial_overlap_fetch(self):
        """Fetch [0, 2], training blocked [1, 3]. Overlap = min(2,3)-max(0,1) = 1."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=2.0), 1.0, 3.0)
        assert abs(stats.iter_blocked_fetch_s.get() - 1.0) < 1e-9

    # ── Counters ──────────────────────────────────────────────────────────────

    def test_batches_total_increments(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        assert stats.iter_batches_total == 0
        self._report(it, _make_batch(), 0.0, 5.0)
        self._report(it, _make_batch(), 0.0, 5.0)
        assert stats.iter_batches_total == 2

    def test_rows_total_increments(self):
        stats = _make_stats()
        it = self._make_iterator(stats)
        self._report(it, _make_batch(num_rows=32), 0.0, 5.0)
        self._report(it, _make_batch(num_rows=16), 0.0, 5.0)
        assert stats.iter_rows_total == 48

    # ── Accumulation across batches ───────────────────────────────────────────

    def test_fetch_attribution_accumulates_across_batches(self):
        """Two batches each blocked fully during fetch — both contribute."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # Batch 1: fetch [0,1], blocked [0,2] → overlap 1.0
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=1.0), 0.0, 2.0)
        # Batch 2: fetch [5,6], blocked [5,7] → overlap 1.0
        self._report(it, _make_batch(fetch_start=5.0, fetch_end=6.0), 5.0, 7.0)
        assert abs(stats.iter_blocked_fetch_s.get() - 2.0) < 1e-9

    def test_prefetch_hides_fetch_from_training(self):
        """Effective prefetching: fetch done before training blocks → 0 attribution."""
        stats = _make_stats()
        it = self._make_iterator(stats)
        # Fetch [0, 1.5], training only starts blocking at t=2 (prefetch worked)
        self._report(it, _make_batch(fetch_start=0.0, fetch_end=1.5), 2.0, 2.6)
        # Collate [2.3, 2.6] is what actually blocked training
        assert stats.iter_blocked_fetch_s.get() == 0.0
        assert abs(stats.iter_blocked_collate_s.get() - 0.3) < 1e-9
