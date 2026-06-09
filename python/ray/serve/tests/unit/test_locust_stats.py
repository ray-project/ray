"""Unit tests for the locust benchmark stats helpers (bucketing, per-stage
percentiles, snapshot diffing)."""

import types

import pytest

from ray.serve._private.benchmarks.locust_utils import (
    ResponseTimeSnapshot,
    _fine_bucket_response_time,
    on_stage_finished,
)


def test_fine_bucket_resolution():
    # 0.1ms resolution below 100ms (vs locust's default 1ms).
    assert _fine_bucket_response_time(8.34) == 8.3
    assert _fine_bucket_response_time(8.36) == 8.4
    # Coarser above, to keep the histogram bounded.
    assert _fine_bucket_response_time(147) == 147
    assert _fine_bucket_response_time(3432) == 3430


def _fake_runner(response_times, num_requests):
    entry = types.SimpleNamespace(
        response_times=response_times, num_requests=num_requests
    )
    return types.SimpleNamespace(
        stats=types.SimpleNamespace(entries={("", "GET"): entry})
    )


def test_on_stage_finished_diffs_cumulative_snapshots():
    """Per-stage stats come from cumulative-end minus stage-start, so a prior
    stage's traffic doesn't bleed into this stage's percentiles or RPS."""
    # Stage started at 40 cumulative requests; ends at 100 -> 60 in-stage.
    runner = _fake_runner({8.0: 90, 13.0: 10}, num_requests=100)
    prev_snapshot = ResponseTimeSnapshot(response_times={8.0: 40}, num_requests=40)
    stages = []

    snapshot = on_stage_finished(
        runner, stages, stage_duration_s=10, prev_snapshot=prev_snapshot
    )

    stage = stages[0]
    assert stage.rps == 6.0  # 60 requests / 10s, not a trailing-window snapshot
    assert stage.p50_latency == 8.0  # over the in-stage {8.0: 50, 13.0: 10}
    assert stage.p99_latency == 13.0
    # Returned snapshot seeds the next stage's diff.
    assert snapshot.num_requests == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
