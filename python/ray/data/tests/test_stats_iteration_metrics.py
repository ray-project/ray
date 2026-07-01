"""Tests for per-stage blocked attribution metrics in DatasetStats (issue #64132).

Verifies that:
- DatasetStats exposes the new fine-grained blocked attribution timer fields.
- iter_batches_total and iter_rows_total accumulate correctly.
- to_summary() includes the new fields in IterStatsSummary.
- IterStatsSummary.to_string() renders per-stage breakdown when values are present.
"""

from ray.data._internal.stats import DatasetStats, IterStatsSummary


def _make_stats() -> DatasetStats:
    return DatasetStats(metadata={}, parent=None)


class TestDatasetStatsNewFields:
    def test_per_stage_timers_present(self):
        stats = _make_stats()
        assert hasattr(stats, "iter_blocked_fetch_s")
        assert hasattr(stats, "iter_blocked_batching_s")
        assert hasattr(stats, "iter_blocked_format_s")
        assert hasattr(stats, "iter_blocked_collate_s")
        assert hasattr(stats, "iter_blocked_finalize_s")

    def test_per_stage_timers_start_at_zero(self):
        stats = _make_stats()
        assert stats.iter_blocked_fetch_s.get() == 0.0
        assert stats.iter_blocked_batching_s.get() == 0.0
        assert stats.iter_blocked_format_s.get() == 0.0
        assert stats.iter_blocked_collate_s.get() == 0.0
        assert stats.iter_blocked_finalize_s.get() == 0.0

    def test_batches_total_present_and_zero(self):
        stats = _make_stats()
        assert hasattr(stats, "iter_batches_total")
        assert stats.iter_batches_total == 0

    def test_rows_total_present_and_zero(self):
        stats = _make_stats()
        assert hasattr(stats, "iter_rows_total")
        assert stats.iter_rows_total == 0

    def test_timer_accumulation(self):
        stats = _make_stats()
        stats.iter_blocked_fetch_s.add(0.5)
        stats.iter_blocked_fetch_s.add(0.3)
        assert abs(stats.iter_blocked_fetch_s.get() - 0.8) < 1e-9

    def test_counter_accumulation(self):
        stats = _make_stats()
        stats.iter_batches_total += 1
        stats.iter_batches_total += 1
        stats.iter_rows_total += 64
        assert stats.iter_batches_total == 2
        assert stats.iter_rows_total == 64

    def test_old_pipeline_names_do_not_exist(self):
        """Ensure the old iter_pipeline_* names were fully removed."""
        stats = _make_stats()
        assert not hasattr(stats, "iter_pipeline_fetch_s")
        assert not hasattr(stats, "iter_pipeline_batching_s")
        assert not hasattr(stats, "iter_pipeline_format_s")
        assert not hasattr(stats, "iter_pipeline_collate_s")
        assert not hasattr(stats, "iter_pipeline_finalize_s")


class TestIterStatsSummaryNewFields:
    def _make_summary(self, stats: DatasetStats) -> IterStatsSummary:
        return stats.to_summary().iter_stats

    def test_new_fields_in_summary(self):
        stats = _make_stats()
        summary = self._make_summary(stats)
        assert hasattr(summary, "blocked_fetch_time")
        assert hasattr(summary, "blocked_batching_time")
        assert hasattr(summary, "blocked_format_time")
        assert hasattr(summary, "blocked_collate_time")
        assert hasattr(summary, "blocked_finalize_time")
        assert hasattr(summary, "batches_total")
        assert hasattr(summary, "rows_total")

    def test_summary_reflects_accumulated_values(self):
        stats = _make_stats()
        stats.iter_blocked_fetch_s.add(0.5)
        stats.iter_blocked_batching_s.add(0.2)
        stats.iter_batches_total = 10
        stats.iter_rows_total = 320

        summary = self._make_summary(stats)
        assert abs(summary.blocked_fetch_time.get() - 0.5) < 1e-9
        assert abs(summary.blocked_batching_time.get() - 0.2) < 1e-9
        assert summary.batches_total == 10
        assert summary.rows_total == 320

    def test_to_string_shows_stage_breakdown(self):
        stats = _make_stats()
        stats.iter_blocked_fetch_s.add(1.5)
        stats.iter_blocked_format_s.add(0.8)
        stats.iter_batches_total = 5
        stats.iter_rows_total = 160
        stats.iter_total_blocked_s.add(2.3)

        text = str(stats.to_summary().iter_stats)
        assert "block fetch" in text
        assert "format" in text
        assert "Total batches consumed: 5" in text
        assert "Total rows consumed: 160" in text
        # The section header must use "blocked", not "pipeline".
        assert "Per-stage training-thread blocked time breakdown" in text

    def test_to_string_omits_zero_stages(self):
        stats = _make_stats()
        stats.iter_blocked_fetch_s.add(0.5)
        stats.iter_total_blocked_s.add(0.5)

        text = str(stats.to_summary().iter_stats)
        assert "block fetch" in text
        assert "batching" not in text
        assert "collate" not in text
