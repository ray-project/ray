import asyncio
import json
import logging
import random
import sys
from typing import List

import pytest

from ray.serve._private import backpressure
from ray.serve._private.backpressure import (
    COMPUTED_RETRY_AFTER_MIN_S,
    FALLBACK_RUNG_COMPUTED,
    FALLBACK_RUNG_NONE,
    FALLBACK_RUNG_STATIC,
    FALLBACK_RUNG_STATIC_FALLBACK,
    DrainRateEstimator,
    DrainRateTracker,
    RetryAfterDecision,
    apply_retry_after_jitter,
    compute_retry_after_decision,
    log_backpressure_rejection,
)
from ray.serve._private.constants import (
    RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.logging_utils import configure_component_logger
from ray.serve._private.metrics_utils import MetricsPusher
from ray.serve.config import BackpressureConfig
from ray.serve.schema import LoggingConfig


class FakeClock:
    def __init__(self):
        self.now_s = 0.0

    def __call__(self) -> float:
        return self.now_s

    def advance(self, seconds: float):
        self.now_s += seconds


class CountingRand:
    """Deterministic stand-in for `random.random` that counts its calls."""

    def __init__(self, value: float):
        self.value = value
        self.num_calls = 0

    def __call__(self) -> float:
        self.num_calls += 1
        return self.value


def make_estimator(**kwargs) -> DrainRateEstimator:
    kwargs.setdefault("warmup_samples", 1)
    kwargs.setdefault("min_window_drains", 8)
    kwargs.setdefault("min_window_busy_s", 10.0)
    kwargs.setdefault("clock", FakeClock())
    estimator = DrainRateEstimator(**kwargs)
    # Establish the sampling baseline.
    estimator.sample(has_pending_work=True)
    return estimator


def drain(estimator: DrainRateEstimator, n: int):
    for _ in range(n):
        estimator.record_drain()


def observe_interval(
    estimator: DrainRateEstimator,
    num_drains: int,
    *,
    elapsed_s: float = 1.0,
    has_pending_work: bool = True,
):
    drain(estimator, num_drains)
    estimator._clock.advance(elapsed_s)
    estimator.sample(has_pending_work=has_pending_work)


def warm_estimator(drains_per_s: int) -> DrainRateEstimator:
    """A warm estimator that has observed one busy 1s interval with
    `drains_per_s` drains, so its seconds per drain is `1 / max(n, 1)`."""
    estimator = make_estimator(warmup_samples=1)
    observe_interval(estimator, drains_per_s)
    return estimator


def run_periodic_drains(
    estimator: DrainRateEstimator,
    *,
    seconds: int,
    every_s: int,
    batch: int = 1,
    depth: int = 8,
) -> List[float]:
    """One drain batch every `every_s` busy seconds; returns the estimate for
    `depth` after each second."""
    estimates = []
    for second in range(1, seconds + 1):
        observe_interval(estimator, batch if second % every_s == 0 else 0)
        estimates.append(estimator.estimate_s(depth))
    return estimates


class TestDrainRateEstimator:
    def test_first_sample_only_sets_baseline(self):
        estimator = make_estimator()
        assert estimator.num_samples == 0
        assert estimator.seconds_per_drain is None
        assert estimator.drain_rate is None
        assert estimator.estimate_s(10) is None

    def test_busy_seconds_per_drain(self):
        """8 drains over 32 busy seconds: 4 s per drain, so a backlog of 8
        takes 32 s."""
        estimator = make_estimator()
        observe_interval(estimator, 8, elapsed_s=32)
        assert estimator.busy_seconds == 32
        assert estimator.drains_in_window == 8
        assert estimator.seconds_per_drain == 4
        assert estimator.drain_rate == 0.25
        assert estimator.estimate_s(8) == 32

    def test_stall_grows_estimate_smoothly(self):
        """Pending work with no drains keeps adding busy time, so the estimate
        grows by depth / drains for every stalled second."""
        estimator = make_estimator()
        observe_interval(estimator, 8, elapsed_s=32)
        estimates = []
        for _ in range(8):
            observe_interval(estimator, 0, has_pending_work=True)
            estimates.append(estimator.estimate_s(8))
        assert estimates == [33, 34, 35, 36, 37, 38, 39, 40]
        assert estimator.busy_seconds == 40
        assert estimator.drains_in_window == 8
        assert estimator.seconds_per_drain == 5

    def test_stall_without_any_drain_is_defined(self):
        """With no drain in the window, it counts as one drain."""
        estimator = make_estimator(warmup_samples=3)
        for _ in range(3):
            observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.warm
        assert estimator.drains_in_window == 0
        assert estimator.drain_rate == 0
        assert estimator.seconds_per_drain == 3
        assert estimator.estimate_s(2) == 6

    def test_idle_interval_is_skipped(self):
        """No pending work and no drains: the busy clock stops, and neither
        the estimate nor the warmup count moves."""
        estimator = make_estimator(warmup_samples=3)
        observe_interval(estimator, 8, elapsed_s=32)
        before = (estimator.estimate_s(8), estimator.num_samples)
        for _ in range(100):
            observe_interval(estimator, 0, has_pending_work=False)
        assert (estimator.estimate_s(8), estimator.num_samples) == before
        assert estimator.busy_seconds == 32

        # The idle gap doesn't stretch the next busy interval.
        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.busy_seconds == 33

    def test_final_drain_interval_counts_as_busy(self):
        """An interval in which the queue emptied still drained requests, so
        it counts even though nothing is pending at the sample."""
        estimator = make_estimator()
        observe_interval(estimator, 3, has_pending_work=False)
        assert estimator.num_samples == 1
        assert estimator.busy_seconds == 1
        assert estimator.drains_in_window == 3

    def test_fast_drains(self):
        """Many drains per second: the 10 busy seconds bound the window, and
        the estimate is stable."""
        estimator = make_estimator()
        estimates = run_periodic_drains(
            estimator, seconds=30, every_s=1, batch=20, depth=100
        )
        assert estimator.busy_seconds == 10
        assert estimator.drains_in_window == 200
        assert estimator.seconds_per_drain == pytest.approx(0.05)
        assert {round(e, 9) for e in estimates[10:]} == {5.0}

    def test_sparse_drains(self):
        """One drain every 4 busy seconds: the 8 drains bound the window and
        the estimate for 8 waiting stays within 29-32 s (time since the last
        drain), instead of swinging with every empty second."""
        estimator = make_estimator()
        estimates = run_periodic_drains(estimator, seconds=80, every_s=4)
        assert min(estimates[40:]) == pytest.approx(29)
        assert max(estimates[40:]) == pytest.approx(32)
        assert estimator.drains_in_window == 8

    def test_very_sparse_drains_are_kept(self):
        """One drain every 15 minutes: history is kept by drains, not time,
        so the window still spans the last 8 drains and learns the gap."""
        estimator = make_estimator()
        run_periodic_drains(estimator, seconds=900 * 10, every_s=900)
        # Right after a drain: 7 full gaps plus that drain's second.
        assert estimator.drains_in_window == 8
        assert estimator.seconds_per_drain == pytest.approx((7 * 900 + 1) / 8)
        # Just before the next drain, the window spans exactly 8 gaps.
        run_periodic_drains(estimator, seconds=899, every_s=900)
        assert estimator.seconds_per_drain == pytest.approx(900)
        assert len(estimator._checkpoints) <= 1 + 8 + 12

    def test_batched_drains(self):
        """8 drains together every 4 s: whole batches enter the window, so the
        estimate stays within a fixed band rather than drifting."""
        estimator = make_estimator()
        estimates = run_periodic_drains(estimator, seconds=80, every_s=4, batch=8)
        assert {round(e, 9) for e in estimates[40:]} == {
            round(8 * 10 / 16, 9),
            round(8 * 10 / 24, 9),
        }

    def test_window_uses_all_history_until_minimums(self):
        """Before 8 drains and 10 busy seconds exist, the window is all of
        the busy history."""
        estimator = make_estimator(warmup_samples=3)
        observe_interval(estimator, 1)
        observe_interval(estimator, 0)
        observe_interval(estimator, 2)
        assert estimator.warm
        assert estimator.busy_seconds == 3
        assert estimator.drains_in_window == 3
        assert estimator.seconds_per_drain == 1

    def test_window_selection_and_pruning(self):
        """The window starts at the most recent checkpoint at least 8 drains
        and 10 busy seconds behind now, which may be inside a gap between
        drains; checkpoints that can't become a start are pruned."""
        estimator = make_estimator()
        # 20 busy seconds without drains (1 s samples), then 8 drains.
        for _ in range(20):
            observe_interval(estimator, 0)
        observe_interval(estimator, 8)
        # Checkpoints B=0..20 have N=0 and B=21 has N=8. The latest 10 busy
        # seconds back is B=11, inside the gap.
        assert estimator.busy_seconds == 10
        assert estimator.drains_in_window == 8
        assert list(estimator._checkpoints) == [
            (float(b), 0) for b in range(11, 21)
        ] + [(21.0, 8)]

        # 12 more busy seconds (one sample) with 8 drains: B=21 is now both 8
        # drains and 12 busy seconds back, and everything before it goes.
        observe_interval(estimator, 8, elapsed_s=12)
        assert estimator.busy_seconds == 12
        assert estimator.drains_in_window == 8
        assert list(estimator._checkpoints) == [(21.0, 8), (33.0, 16)]

    def test_history_stays_small(self):
        """At most the start, the last checkpoint before each of up to 8
        drain samples, and the checkpoints within the last 10 busy seconds."""
        estimator = make_estimator()
        for second in range(5000):
            observe_interval(
                estimator,
                (second * 7) % 5 if second % 3 else 0,
                has_pending_work=second % 11 != 0,
            )
            assert len(estimator._checkpoints) <= 1 + 8 + 12

    @pytest.mark.parametrize(
        "num_samples,expected_warm", [(0, False), (2, False), (3, True)]
    )
    def test_warmup(self, num_samples: int, expected_warm: bool):
        estimator = make_estimator(warmup_samples=3)
        for _ in range(num_samples):
            observe_interval(estimator, 5)

        assert estimator.warm is expected_warm
        assert (estimator.estimate_s(10) is not None) is expected_warm

    def test_non_positive_elapsed_time_is_ignored(self):
        estimator = make_estimator()
        observe_interval(estimator, 3, elapsed_s=0)
        assert estimator.num_samples == 0

        # The drains are picked up by the next valid interval.
        observe_interval(estimator, 0, elapsed_s=1.0)
        assert estimator.drains_in_window == 3

    def test_reset_keeps_counter(self):
        estimator = make_estimator()
        observe_interval(estimator, 5)
        assert estimator.warm

        estimator.reset()
        assert estimator.seconds_per_drain is None
        assert not estimator.warm
        assert estimator.drain_counter == 5
        assert estimator.busy_clock_s == 0
        assert list(estimator._checkpoints) == [(0.0, 5)]

        # Drains recorded before the reset don't leak into the next interval.
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 2)
        assert estimator.drains_in_window == 2

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"warmup_samples": 0},
            {"min_window_drains": 0},
            {"min_window_busy_s": 0},
        ],
    )
    def test_invalid_args(self, kwargs):
        with pytest.raises(ValueError):
            DrainRateEstimator(**kwargs)


class TestBusyClock:
    """The busy clock is a cumulative total that only busy intervals advance,
    and the window is a span of it ending now."""

    def test_busy_clock_is_cumulative(self):
        estimator = make_estimator()
        observe_interval(estimator, 1, elapsed_s=2)
        observe_interval(estimator, 7, elapsed_s=8)
        observe_interval(estimator, 6, elapsed_s=5)
        # Positions 2, 10 and 15 on the clock: a running total, not a sum of
        # per-row values. (B=0 is pruned: the window already starts at B=2.)
        assert estimator.busy_clock_s == 15
        assert list(estimator._checkpoints) == [(2.0, 1), (10.0, 8), (15.0, 14)]

    def test_idle_time_does_not_advance_it(self):
        estimator = make_estimator()
        observe_interval(estimator, 1, elapsed_s=2)
        for _ in range(10):
            observe_interval(estimator, 0, elapsed_s=3600, has_pending_work=False)
        assert estimator.busy_clock_s == 2
        assert estimator.num_samples == 1

    def test_stall_advances_it(self):
        estimator = make_estimator()
        observe_interval(estimator, 8, elapsed_s=32)
        for _ in range(8):
            observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.busy_clock_s == 40
        assert estimator.drain_counter == 8

    def test_window_is_a_span_ending_now(self):
        """Busy time and drains in the window are both differences between
        the newest checkpoint and the window's start checkpoint."""
        estimator = make_estimator()
        observe_interval(estimator, 1, elapsed_s=2)
        observe_interval(estimator, 7, elapsed_s=8)
        observe_interval(estimator, 6, elapsed_s=5)
        # The latest checkpoint at least 8 drains and 10 busy seconds back is
        # B=2 (13 drains, 13 s back); B=10 is only 6 drains back.
        start_busy_s, start_drains = estimator._checkpoints[0]
        assert (start_busy_s, start_drains) == (2.0, 1)
        assert estimator.busy_seconds == estimator.busy_clock_s - start_busy_s == 13
        assert estimator.drains_in_window == estimator.drain_counter - start_drains
        assert estimator.drains_in_window == 13


class TestVariableSampleIntervals:
    """The window starts at a real sample checkpoint; it never cuts a sample
    interval to make the busy time exactly 10 s."""

    def test_long_interval_is_not_split(self):
        estimator = make_estimator()
        observe_interval(estimator, 8)
        # The sampler was late: one 30 s busy interval with no drain.
        observe_interval(estimator, 0, elapsed_s=30)
        observe_interval(estimator, 8)
        # The checkpoint at B=31 is only 1 s back, so the window starts at
        # B=1, taking the whole 30 s interval: 31 s for 8 drains.
        assert estimator.busy_seconds == 31
        assert estimator.drains_in_window == 8
        assert estimator.seconds_per_drain == pytest.approx(31 / 8)

    def test_start_is_always_a_recorded_checkpoint(self):
        estimator = make_estimator()
        positions = {0.0}
        for second in range(400):
            elapsed_s = (1.7, 0.5, 1.3, 5.0)[second % 4]
            observe_interval(
                estimator, 1 if second % 3 == 0 else 0, elapsed_s=elapsed_s
            )
            positions.add(estimator.busy_clock_s)
            start_busy_s = estimator.busy_clock_s - estimator.busy_seconds
            assert any(
                start_busy_s == pytest.approx(position) for position in positions
            )
            if estimator.drains_in_window >= 8:
                assert estimator.busy_seconds >= 10

    def test_slow_sample_interval(self):
        """With 5 s samples and a drain every sample, the window is the last
        two samples (10 s, 2 drains) until 8 drains are covered."""
        estimator = make_estimator()
        for _ in range(20):
            observe_interval(estimator, 1, elapsed_s=5)
        assert estimator.busy_seconds == 40
        assert estimator.drains_in_window == 8
        assert estimator.seconds_per_drain == 5


def reference_seconds_per_drain(checkpoints, min_drains=8, min_busy_s=10.0):
    """The busy-clock window rule over the full, unpruned checkpoint history."""
    busy_now, drains_now = checkpoints[-1]
    start_busy_s, start_drains = checkpoints[0]
    for busy_s, drains in reversed(checkpoints):
        if drains_now - drains >= min_drains and busy_now - busy_s >= min_busy_s:
            start_busy_s, start_drains = busy_s, drains
            break
    return (busy_now - start_busy_s) / max(drains_now - start_drains, 1)


class TestMatchesReference:
    """Matches the busy-clock reference: its test vectors, and the window rule
    applied to the full history under random workloads."""

    def test_reference_vectors(self):
        # Warmup: nothing until 3 busy samples, then all history so far.
        estimator = make_estimator(warmup_samples=3)
        observe_interval(estimator, 1)
        observe_interval(estimator, 1)
        assert estimator.estimate_s(5) is None
        observe_interval(estimator, 1)
        assert estimator.estimate_s(5) == 5.0

        # Dense: 4 drains per second; the 10 busy seconds set the window.
        estimator = make_estimator(warmup_samples=3)
        run_periodic_drains(estimator, seconds=60, every_s=1, batch=4)
        assert estimator.estimate_s(8) == 2.0

        # One drain every 4 s, D=8: 29 s right after a drain, then 30, 31, 32.
        estimator = make_estimator(warmup_samples=3)
        run_periodic_drains(estimator, seconds=100, every_s=4)
        assert estimator.estimate_s(8) == 29.0
        assert run_periodic_drains(estimator, seconds=3, every_s=4) == [
            30.0,
            31.0,
            32.0,
        ]

        # Idle: 30 idle seconds leave the estimate unchanged.
        estimator = make_estimator(warmup_samples=3)
        run_periodic_drains(estimator, seconds=20, every_s=1)
        before = estimator.estimate_s(10)
        for _ in range(30):
            observe_interval(estimator, 0, has_pending_work=False)
        assert estimator.estimate_s(10) == before == 10.0

        # Stall: one drain every 8 s, then pending work with no drains; the
        # estimate grows by D / 8 per stalled second.
        estimator = make_estimator(warmup_samples=3)
        run_periodic_drains(estimator, seconds=200, every_s=8)
        start = estimator.estimate_s(30)
        assert start == 30 * 57 / 8
        for _ in range(60):
            observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.estimate_s(30) == start + 60 * 30 / 8

    @pytest.mark.parametrize("variable_intervals", [False, True])
    def test_random_workloads(self, variable_intervals: bool):
        rng = random.Random(0)
        for _ in range(10):
            estimator = make_estimator()
            full_history = [(0.0, 0)]
            busy_clock_s = 0.0
            for _ in range(2000):
                drains = 0 if rng.random() < 0.6 else rng.choice([1, 1, 2, 8, 20])
                pending = rng.random() < 0.7
                elapsed_s = (
                    rng.choice([0.5, 1.0, 1.3, 1.7, 5.0, 30.0, 3600.0])
                    if variable_intervals
                    else 1.0
                )
                observe_interval(
                    estimator, drains, elapsed_s=elapsed_s, has_pending_work=pending
                )
                if drains or pending:
                    busy_clock_s += elapsed_s
                    full_history.append((busy_clock_s, estimator.drain_counter))
                if len(full_history) > 1:
                    assert estimator.seconds_per_drain == pytest.approx(
                        reference_seconds_per_drain(full_history)
                    )


class TestGroundTruthInstrumentation:
    def test_counter_reaches_c0_plus_d_exactly_when_backlog_drains(self):
        """The acceptance harness defines drain time T for a rejection as the
        time until the drain counter reaches C0 + D, where C0 and D are the
        counter and queue depth logged with the rejection."""
        estimator = make_estimator()
        drain(estimator, 120)

        decision = compute_retry_after_decision(
            backpressure_config=BackpressureConfig(retry_after_s=5),
            observed_queue_depth=10,
            estimator=estimator,
            request_id="req-1",
            rand=CountingRand(0.5),
        )
        c0, d = decision.drain_counter, decision.observed_queue_depth
        assert (c0, d) == (120, 10)

        drain(estimator, d - 1)
        assert estimator.drain_counter < c0 + d

        drain(estimator, 1)
        assert estimator.drain_counter == c0 + d


class TestJitter:
    @pytest.mark.parametrize(
        "rand_value,expected",
        [(0.0, 9.0), (0.5, 10.0), (0.75, 10.5)],
    )
    def test_apply_jitter(self, rand_value: float, expected: float):
        rand = CountingRand(rand_value)
        assert apply_retry_after_jitter(
            10, jitter_fraction=0.1, rand=rand
        ) == pytest.approx(expected)
        assert rand.num_calls == 1

    def test_default_fraction(self):
        assert RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION == 0.1
        assert apply_retry_after_jitter(10, rand=CountingRand(0.0)) == pytest.approx(
            9.0
        )

    def test_zero_fraction_is_identity(self):
        assert (
            apply_retry_after_jitter(7, jitter_fraction=0, rand=CountingRand(0.9)) == 7
        )


class TestComputeRetryAfterDecision:
    def decide(
        self,
        *,
        policy: str = "static",
        retry_after_s=None,
        depth: int = 10,
        estimator=None,
        rand_value: float = 0.5,
        jitter_fraction: float = 0.1,
    ):
        rand = CountingRand(rand_value)
        decision = compute_retry_after_decision(
            backpressure_config=BackpressureConfig(
                retry_after_policy=policy, retry_after_s=retry_after_s
            ),
            observed_queue_depth=depth,
            estimator=estimator or make_estimator(),
            request_id="test-request-id",
            jitter_fraction=jitter_fraction,
            rand=rand,
        )
        return decision, rand

    def test_static(self):
        decision, rand = self.decide(retry_after_s=5)
        assert decision.policy == "static"
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC
        assert decision.pre_jitter_s == 5
        assert decision.post_jitter_s == 5
        assert rand.num_calls == 1

    def test_static_without_retry_after_s(self):
        decision, rand = self.decide(retry_after_s=None)
        assert decision.fallback_rung == FALLBACK_RUNG_NONE
        assert decision.pre_jitter_s is None
        assert decision.post_jitter_s is None
        assert rand.num_calls == 0

    @pytest.mark.parametrize(
        "retry_after_s,rand_value,expected",
        [
            (5, 0.0, 5),  # Low end of the jitter window: ceil(4.5).
            (5, 0.999999, 6),  # High end, rounded up.
            (7.5, 0.5, 8),  # No jitter, rounded up.
            (0, 0.9, 0),
        ],
    )
    def test_static_jitter_and_rounding(
        self, retry_after_s: float, rand_value: float, expected: int
    ):
        decision, _ = self.decide(retry_after_s=retry_after_s, rand_value=rand_value)
        assert decision.pre_jitter_s == retry_after_s
        assert decision.post_jitter_s == expected
        assert isinstance(decision.post_jitter_s, int)

    def test_static_is_not_clamped(self):
        """The computed policy's 1s floor doesn't apply to static values, and
        neither has an upper bound."""
        decision, _ = self.decide(retry_after_s=120)
        assert decision.post_jitter_s == 120

    def test_huge_static_value_does_not_overflow(self):
        decision, _ = self.decide(retry_after_s=1.7e308, rand_value=0.999999)
        assert decision.post_jitter_s == int(1.7e308)

    def test_computed_warm(self):
        decision, rand = self.decide(
            policy="queue_drain_rate",
            retry_after_s=30,
            depth=10,
            estimator=warm_estimator(drains_per_s=5),
        )
        assert decision.policy == "queue_drain_rate"
        assert decision.fallback_rung == FALLBACK_RUNG_COMPUTED
        assert decision.busy_seconds == 1
        assert decision.drains_in_window == 5
        assert decision.seconds_per_drain == pytest.approx(0.2)
        assert decision.drain_rate == 5.0
        assert decision.estimator_warm
        assert decision.pre_jitter_s == pytest.approx(2.0)
        assert decision.post_jitter_s == 2
        assert rand.num_calls == 1

    @pytest.mark.parametrize(
        "depth,drains_per_s,rand_value,expected_pre,expected_post",
        [
            # Raised to the 1s floor; jitter below 1s still emits 1.
            (1, 100, 0.0, 1, 1),
            # No upper cap: long backlogs report their expected wait.
            (1000, 1, 0.999999, 1000, 1100),
            (1000, 1, 0.0, 1000, 900),
        ],
    )
    def test_computed_floor_and_no_cap(
        self,
        depth: int,
        drains_per_s: int,
        rand_value: float,
        expected_pre: float,
        expected_post: int,
    ):
        decision, _ = self.decide(
            policy="queue_drain_rate",
            depth=depth,
            estimator=warm_estimator(drains_per_s=drains_per_s),
            rand_value=rand_value,
        )
        assert decision.pre_jitter_s == expected_pre
        assert decision.post_jitter_s == expected_post
        assert decision.post_jitter_s >= COMPUTED_RETRY_AFTER_MIN_S

    def test_computed_cold_falls_back_to_static(self):
        decision, rand = self.decide(policy="queue_drain_rate", retry_after_s=5)
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK
        assert not decision.estimator_warm
        assert decision.drain_rate is None
        assert decision.busy_seconds is None
        assert decision.drains_in_window is None
        assert decision.seconds_per_drain is None
        assert decision.pre_jitter_s == 5
        assert decision.post_jitter_s == 5
        assert rand.num_calls == 1

    def test_computed_cold_without_fallback_has_no_header(self):
        decision, rand = self.decide(policy="queue_drain_rate", retry_after_s=None)
        assert decision.fallback_rung == FALLBACK_RUNG_NONE
        assert decision.post_jitter_s is None
        assert rand.num_calls == 0

    def test_computed_warm_without_drains(self):
        """Busy time with no drain is a real stall: the estimate is computed
        (no drain counts as one), not the static fallback."""
        decision, _ = self.decide(
            policy="queue_drain_rate",
            retry_after_s=5,
            estimator=warm_estimator(drains_per_s=0),
        )
        assert decision.estimator_warm
        assert decision.fallback_rung == FALLBACK_RUNG_COMPUTED
        assert decision.drain_rate == 0.0
        assert decision.drains_in_window == 0
        assert decision.seconds_per_drain == 1
        assert decision.pre_jitter_s == 10
        assert decision.post_jitter_s == 10

    def test_estimator_failure_falls_back(self):
        estimator = warm_estimator(drains_per_s=5)

        def fail(queue_depth):
            raise RuntimeError("boom")

        estimator.estimate_s = fail
        decision, _ = self.decide(
            policy="queue_drain_rate", retry_after_s=5, estimator=estimator
        )
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK
        assert decision.post_jitter_s == 5

    def test_snapshot_fields(self):
        estimator = warm_estimator(drains_per_s=4)
        drain(estimator, 38)
        decision, _ = self.decide(
            policy="queue_drain_rate", depth=11, estimator=estimator, rand_value=0.5
        )
        assert decision.estimator_num_samples == estimator.num_samples == 1
        assert decision.to_log_fields() == {
            "backpressure_request_id": "test-request-id",
            "backpressure_retry_after_policy": "queue_drain_rate",
            "backpressure_retry_after_fallback_rung": "computed",
            "backpressure_observed_queue_depth": 11,
            "backpressure_drain_rate": 4.0,
            "backpressure_busy_seconds": 1.0,
            "backpressure_drains_in_window": 4,
            "backpressure_seconds_per_drain": 0.25,
            "backpressure_estimator_warm": True,
            "backpressure_estimator_num_samples": 1,
            "backpressure_retry_after_pre_jitter_s": 2.75,
            "backpressure_retry_after_post_jitter_s": 3,
            "backpressure_drain_counter": 42,
        }

    def test_snapshot_is_not_affected_by_later_samples(self):
        estimator = warm_estimator(drains_per_s=4)
        decision, _ = self.decide(policy="queue_drain_rate", estimator=estimator)
        observe_interval(estimator, 8)
        drain(estimator, 3)
        assert estimator.num_samples == 2
        assert estimator.drains_in_window == 12
        assert decision.estimator_num_samples == 1
        assert decision.drain_rate == 4.0
        assert decision.busy_seconds == 1
        assert decision.drains_in_window == 4
        assert decision.drain_counter == 4

    def test_uses_module_random_by_default(self, monkeypatch):
        rand = CountingRand(0.0)
        monkeypatch.setattr(backpressure.random, "random", rand)
        decision = compute_retry_after_decision(
            backpressure_config=BackpressureConfig(retry_after_s=10),
            observed_queue_depth=1,
            estimator=make_estimator(),
            request_id="req-1",
            jitter_fraction=0.2,
        )
        assert decision.post_jitter_s == 8
        assert rand.num_calls == 1


class FakeStopEvent:
    def __init__(self):
        self._set = False

    def set(self):
        self._set = True

    def is_set(self) -> bool:
        return self._set


class FakeMetricsPusher:
    def __init__(self):
        self.calls: List[str] = []
        self.tasks = {}
        self.stop_event = FakeStopEvent()

    def register_or_update_task(self, name, task_func, interval_s):
        self.calls.append(f"register:{name}:{interval_s}")
        self.tasks[name] = task_func

    async def graceful_shutdown(self):
        self.calls.append("shutdown")
        self.tasks.clear()


class FakeMetricsPusherFactory:
    """Creates `FakeMetricsPusher`s and remembers them."""

    def __init__(self):
        self.pushers: List[FakeMetricsPusher] = []

    def __call__(self) -> FakeMetricsPusher:
        self.pushers.append(FakeMetricsPusher())
        return self.pushers[-1]


class ManualSleep:
    """`async_sleep` for `MetricsPusher` that only returns when released, so a
    test decides exactly when each sampling interval ends."""

    def __init__(self):
        self._waiters: List[asyncio.Future] = []

    async def __call__(self, _interval_s: float):
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        await waiter

    def end_interval(self):
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)


async def run_ready_tasks():
    """Let every task that's ready run until it blocks again (no wall-clock
    sleeping: each iteration only yields to the event loop)."""
    for _ in range(20):
        await asyncio.sleep(0)


def count_samples(estimator: DrainRateEstimator) -> List[None]:
    """Record each call to `estimator.sample()`."""
    calls: List[None] = []
    sample = estimator.sample

    def counting_sample(**kwargs):
        calls.append(None)
        sample(**kwargs)

    estimator.sample = counting_sample
    return calls


class TestDrainRateTracker:
    def test_sampler_follows_policy(self):
        factory = FakeMetricsPusherFactory()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True,
            estimator=make_estimator(),
            metrics_pusher_factory=factory,
            sample_interval_s=0.5,
        )

        # Static policy never starts the sampler.
        tracker.update_policy("static")
        assert not tracker.sampling
        assert factory.pushers == []

        tracker.update_policy("queue_drain_rate")
        assert tracker.sampling
        [pusher] = factory.pushers
        assert pusher.calls == [f"register:{DrainRateTracker.SAMPLE_TASK_NAME}:0.5"]

        # Re-applying the same policy (e.g. an unrelated config update) is a
        # no-op, so it doesn't reset the learned rate or add a timer.
        tracker.update_policy("queue_drain_rate")
        assert len(factory.pushers) == 1
        assert len(pusher.calls) == 1

        tracker.update_policy("static")
        assert not tracker.sampling
        assert pusher.stop_event.is_set()

    def test_restart_resets_history_but_not_counter(self):
        factory = FakeMetricsPusherFactory()
        estimator = make_estimator()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True,
            estimator=estimator,
            metrics_pusher_factory=factory,
        )
        tracker.update_policy("queue_drain_rate")
        sample = factory.pushers[-1].tasks[DrainRateTracker.SAMPLE_TASK_NAME]

        sample()
        tracker.record_drain()
        estimator._clock.advance(1)
        sample()
        assert estimator.drains_in_window == 1
        assert estimator.busy_seconds == 1

        tracker.update_policy("static")
        tracker.update_policy("queue_drain_rate")
        assert estimator.seconds_per_drain is None
        assert estimator.num_samples == 0
        assert tracker.drain_counter == 1

        # The restart uses a new pusher; the old sampler function is inert.
        assert len(factory.pushers) == 2
        estimator._clock.advance(1)
        sample()
        sample()
        assert estimator.num_samples == 0

    def test_counter_maintained_under_static_policy(self):
        factory = FakeMetricsPusherFactory()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True, metrics_pusher_factory=factory
        )
        for _ in range(3):
            tracker.record_drain()
        assert tracker.drain_counter == 3
        assert factory.pushers == []

    @pytest.mark.asyncio
    async def test_policy_update_after_shutdown_is_ignored(self):
        factory = FakeMetricsPusherFactory()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True, metrics_pusher_factory=factory
        )
        await tracker.shutdown()
        tracker.update_policy("queue_drain_rate")
        assert not tracker.sampling
        assert factory.pushers == []

    @pytest.mark.asyncio
    async def test_shutdown_stops_current_sampler(self):
        factory = FakeMetricsPusherFactory()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True, metrics_pusher_factory=factory
        )
        tracker.update_policy("queue_drain_rate")
        await tracker.shutdown()
        assert not tracker.sampling
        assert factory.pushers[-1].calls[-1] == "shutdown"

    @pytest.mark.asyncio
    async def test_real_pusher_samples_once_per_interval(self):
        """Drives a real `MetricsPusher` with a manually ended sleep."""
        sleep = ManualSleep()
        estimator = make_estimator(warmup_samples=3)
        samples = count_samples(estimator)
        tracker = DrainRateTracker(
            has_pending_work=lambda: True,
            estimator=estimator,
            metrics_pusher_factory=lambda: MetricsPusher(async_sleep=sleep),
        )
        tracker.update_policy("queue_drain_rate")
        tracker.update_policy("queue_drain_rate")

        # The sampler runs once when it starts (the baseline), then once per
        # interval. Busy intervals with no drains count toward warmup.
        await run_ready_tasks()
        assert len(samples) == 1
        for expected_samples in (1, 2, 3):
            estimator._clock.advance(1)
            sleep.end_interval()
            await run_ready_tasks()
            assert estimator.num_samples == expected_samples
        assert len(samples) == 4
        assert estimator.warm

        await tracker.shutdown()
        assert not tracker.sampling
        estimator._clock.advance(1)
        sleep.end_interval()
        await run_ready_tasks()
        assert len(samples) == 4

    @pytest.mark.asyncio
    async def test_restart_leaves_a_single_sampler(self):
        """Stopping and restarting sampling in the same event-loop turn must
        not leave the first sampler running alongside the second."""
        sleep = ManualSleep()
        estimator = make_estimator(warmup_samples=3)
        samples = count_samples(estimator)
        tracker = DrainRateTracker(
            has_pending_work=lambda: True,
            estimator=estimator,
            metrics_pusher_factory=lambda: MetricsPusher(async_sleep=sleep),
        )
        tracker.update_policy("queue_drain_rate")
        tracker.update_policy("static")
        tracker.update_policy("queue_drain_rate")

        await run_ready_tasks()
        assert len(samples) == 1

        estimator._clock.advance(1)
        sleep.end_interval()
        await run_ready_tasks()
        assert len(samples) == 2
        assert estimator.num_samples == 1

        await tracker.shutdown()


@pytest.fixture
def json_serve_logs(tmp_path):
    """Configure the Serve logger with the production JSON file handler and
    return a function that reads back the emitted records."""
    serve_logger = logging.getLogger(SERVE_LOGGER_NAME)
    saved = (list(serve_logger.handlers), serve_logger.propagate, serve_logger.level)

    configure_component_logger(
        component_name="test_component",
        component_id="test_id",
        logging_config=LoggingConfig(encoding="JSON", logs_dir=str(tmp_path)),
        max_bytes=10_000_000,
        backup_count=1,
    )

    def read_records():
        for handler in serve_logger.handlers:
            handler.flush()
        records = []
        for log_file in tmp_path.glob("*.log"):
            for line in log_file.read_text().splitlines():
                records.append(json.loads(line))
        return records

    yield read_records

    for handler in serve_logger.handlers:
        handler.close()
    serve_logger.handlers = saved[0]
    serve_logger.propagate = saved[1]
    serve_logger.setLevel(saved[2])


def test_rejection_record_survives_json_logging(json_serve_logs):
    decision = RetryAfterDecision(
        request_id="a550c60b-bc73-42ba-beb9-6ae989f37811",
        policy="queue_drain_rate",
        fallback_rung="computed",
        observed_queue_depth=9,
        drain_counter=1234,
        drain_rate=4.5,
        busy_seconds=12.0,
        drains_in_window=54,
        seconds_per_drain=12.0 / 54,
        estimator_warm=True,
        estimator_num_samples=7,
        pre_jitter_s=2.0,
        post_jitter_s=3,
    )
    log_backpressure_rejection("Request dropped due to backpressure.", decision)

    records = [
        r
        for r in json_serve_logs()
        if r.get("message", "").startswith("Request dropped")
    ]
    assert len(records) == 1
    record = records[0]
    assert record["levelname"] == "WARNING"
    for key, value in decision.to_log_fields().items():
        assert record[key] == value
    assert record["backpressure_request_id"] == "a550c60b-bc73-42ba-beb9-6ae989f37811"
    assert record["backpressure_estimator_num_samples"] == 7
    assert record["backpressure_busy_seconds"] == 12.0
    assert record["backpressure_drains_in_window"] == 54
    assert record["backpressure_seconds_per_drain"] == pytest.approx(12.0 / 54)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
