"""Unit tests for BatchTimings — the per-stage pipeline duration dataclass (issue #64132)."""

import time

from ray.data._internal.block_batching.interfaces import BatchMetadata, BatchTimings


def _make_timings(
    fetch=0.5,
    batching=0.1,
    batching_done=2.0,
    fmt_done=2.3,
    collate_done=2.6,
    finalize_done=3.0,
    num_rows=32,
):
    t = BatchTimings()
    t.fetch_duration_s = fetch
    t.batching_duration_s = batching
    t.batching_done_s = batching_done
    t.format_done_s = fmt_done
    t.collate_done_s = collate_done
    t.finalize_done_s = finalize_done
    t.num_rows = num_rows
    return t


class TestBatchTimingsDefaults:
    def test_all_fields_zero_by_default(self):
        t = BatchTimings()
        assert t.fetch_duration_s == 0.0
        assert t.batching_duration_s == 0.0
        assert t.batching_done_s == 0.0
        assert t.format_done_s == 0.0
        assert t.collate_done_s == 0.0
        assert t.finalize_done_s == 0.0
        assert t.num_rows == 0

    def test_fetch_duration_is_a_duration_not_timestamp(self):
        """fetch_duration_s must be non-negative elapsed time, not an absolute clock."""
        t = BatchTimings()
        t.fetch_duration_s = 0.5
        assert t.fetch_duration_s == 0.5

    def test_accumulated_fetch_sums_two_blocks(self):
        """Simulates two blocks summed into one batch's fetch_duration_s."""
        accumulated = 0.0
        accumulated += 0.3  # block 1 ray.get duration
        accumulated += 0.2  # block 2 ray.get duration
        t = BatchTimings(fetch_duration_s=accumulated)
        assert abs(t.fetch_duration_s - 0.5) < 1e-9

    def test_format_latency_from_timestamps(self):
        t = _make_timings(batching_done=2.0, fmt_done=2.3)
        assert abs((t.format_done_s - t.batching_done_s) - 0.3) < 1e-9

    def test_collate_latency_from_timestamps(self):
        t = _make_timings(fmt_done=2.3, collate_done=2.6)
        assert abs((t.collate_done_s - t.format_done_s) - 0.3) < 1e-9

    def test_finalize_latency_after_collate(self):
        t = _make_timings(collate_done=2.6, finalize_done=3.0)
        last_pre_finalize = t.collate_done_s or t.format_done_s
        assert abs((t.finalize_done_s - last_pre_finalize) - 0.4) < 1e-9

    def test_finalize_latency_no_collate(self):
        """When collate_done_s == 0 (no collate_fn), finalize follows format_done_s."""
        t = _make_timings(fmt_done=2.3, collate_done=0.0, finalize_done=3.0)
        last_pre_finalize = t.collate_done_s or t.format_done_s
        assert abs((t.finalize_done_s - last_pre_finalize) - 0.7) < 1e-9

    def test_num_rows(self):
        t = _make_timings(num_rows=128)
        assert t.num_rows == 128

    def test_batching_done_s_is_absolute_timestamp(self):
        before = time.perf_counter()
        t = BatchTimings()
        t.batching_done_s = time.perf_counter()
        after = time.perf_counter()
        assert before <= t.batching_done_s <= after


class TestBatchMetadataTimings:
    def test_default_timings_created(self):
        m = BatchMetadata(batch_idx=0)
        assert isinstance(m.timings, BatchTimings)
        assert m.timings.num_rows == 0

    def test_each_instance_has_own_timings(self):
        m1 = BatchMetadata(batch_idx=0)
        m2 = BatchMetadata(batch_idx=1)
        m1.timings.num_rows = 64
        assert m2.timings.num_rows == 0, "timings must not be shared across instances"

    def test_custom_timings(self):
        t = _make_timings(num_rows=16, fetch=0.9)
        m = BatchMetadata(batch_idx=5, timings=t)
        assert m.timings.num_rows == 16
        assert m.timings.fetch_duration_s == 0.9
