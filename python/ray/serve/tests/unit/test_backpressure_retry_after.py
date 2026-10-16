import asyncio
import json
import logging
import math
import sys
from typing import List

import pytest

from ray.serve._private import backpressure
from ray.serve._private.backpressure import (
    COMPUTED_RETRY_AFTER_MAX_S,
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
from ray.serve._private.constants import SERVE_LOGGER_NAME
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
    kwargs.setdefault("alpha", 0.3)
    kwargs.setdefault("warmup_samples", 1)
    kwargs.setdefault("clock", FakeClock())
    return DrainRateEstimator(**kwargs)


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


def warm_estimator(rate: int) -> DrainRateEstimator:
    """A warm estimator that has observed one 1s interval with `rate` drains."""
    estimator = make_estimator(alpha=1.0, warmup_samples=1)
    estimator.sample(has_pending_work=True)
    observe_interval(estimator, rate, elapsed_s=1.0)
    return estimator


class TestDrainRateEstimator:
    def test_first_sample_only_sets_baseline(self):
        estimator = make_estimator()
        drain(estimator, 5)
        estimator.sample(has_pending_work=True)
        assert estimator.rate is None
        assert not estimator.warm

    def test_counter_delta_over_elapsed_time(self):
        estimator = make_estimator()
        drain(estimator, 100)
        estimator.sample(has_pending_work=True)

        observe_interval(estimator, 7, elapsed_s=1.0)
        assert estimator.rate == pytest.approx(7.0)
        assert estimator.drain_counter == 107

        observe_interval(estimator, 0, elapsed_s=0.5, has_pending_work=True)
        # 0.3 * 0 + 0.7 * 7
        assert estimator.rate == pytest.approx(4.9)

    def test_ewma(self):
        estimator = make_estimator(alpha=0.3)
        estimator.sample(has_pending_work=True)

        observe_interval(estimator, 10)
        assert estimator.rate == pytest.approx(10.0)

        observe_interval(estimator, 20)
        # 0.3 * 20 + 0.7 * 10
        assert estimator.rate == pytest.approx(13.0)

    @pytest.mark.parametrize(
        "num_samples,expected_warm", [(0, False), (2, False), (3, True)]
    )
    def test_warmup(self, num_samples: int, expected_warm: bool):
        estimator = make_estimator(warmup_samples=3)
        estimator.sample(has_pending_work=True)
        for _ in range(num_samples):
            observe_interval(estimator, 5)

        assert estimator.warm is expected_warm
        assert (estimator.estimate_s(10) is not None) is expected_warm

    def test_idle_interval_is_skipped(self):
        estimator = make_estimator(warmup_samples=2)
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 10)
        assert estimator.rate == pytest.approx(10.0)

        assert estimator.num_samples == 1

        # Nothing drained and nothing pending: no rate decay, no warmup credit.
        for _ in range(5):
            observe_interval(estimator, 0, has_pending_work=False)
        assert estimator.rate == pytest.approx(10.0)
        assert estimator.num_samples == 1
        assert not estimator.warm

        # An idle gap doesn't stretch the next interval: the baseline moved.
        observe_interval(estimator, 10)
        assert estimator.rate == pytest.approx(10.0)
        assert estimator.warm

    def test_drain_without_pending_work_still_counts(self):
        """The queue emptied during the interval: a real observation."""
        estimator = make_estimator()
        estimator.sample(has_pending_work=False)
        observe_interval(estimator, 4, has_pending_work=False)
        assert estimator.rate == pytest.approx(4.0)
        assert estimator.warm

    def test_busy_zero_interval_decays_rate_and_counts_toward_warmup(self):
        estimator = make_estimator(alpha=0.3, warmup_samples=2)
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 10)
        assert estimator.num_samples == 1
        assert not estimator.warm

        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.rate == pytest.approx(7.0)
        assert estimator.num_samples == 2
        assert estimator.warm

    def test_non_positive_elapsed_time_is_ignored(self):
        estimator = make_estimator()
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 3, elapsed_s=0)
        assert estimator.rate is None

        # The drains are picked up by the next valid interval.
        observe_interval(estimator, 0, elapsed_s=1.0)
        assert estimator.rate == pytest.approx(3.0)

    def test_estimate(self):
        estimator = make_estimator()
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 5)
        assert estimator.estimate_s(10) == pytest.approx(2.0)

    def test_warm_zero_rate_estimate_is_infinite(self):
        """Pending work that never drained is a real slow-drain observation."""
        estimator = make_estimator()
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.warm
        assert estimator.rate == 0
        assert estimator.estimate_s(10) == math.inf

    def test_estimate_unavailable_for_invalid_rate(self):
        estimator = make_estimator()
        assert estimator.estimate_s(10) is None

        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.warm
        for invalid_rate in (float("nan"), float("inf"), float("-inf"), -1.0):
            estimator._rate = invalid_rate
            assert estimator.estimate_s(10) is None

    def test_reset_rate_keeps_counter(self):
        estimator = make_estimator()
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 5)
        assert estimator.warm

        estimator.reset_rate()
        assert estimator.rate is None
        assert not estimator.warm
        assert estimator.drain_counter == 5

        # Drains recorded before the reset don't leak into the next interval.
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 2)
        assert estimator.rate == pytest.approx(2.0)

    @pytest.mark.parametrize("alpha", [0, -0.1, 1.1])
    def test_invalid_alpha(self, alpha: float):
        with pytest.raises(ValueError):
            DrainRateEstimator(alpha=alpha)

    def test_invalid_warmup_samples(self):
        with pytest.raises(ValueError):
            DrainRateEstimator(warmup_samples=0)


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
        [(0.0, 8.0), (0.5, 10.0), (0.75, 11.0)],
    )
    def test_apply_jitter(self, rand_value: float, expected: float):
        rand = CountingRand(rand_value)
        assert apply_retry_after_jitter(
            10, jitter_fraction=0.2, rand=rand
        ) == pytest.approx(expected)
        assert rand.num_calls == 1

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
        jitter_fraction: float = 0.2,
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
            (5, 0.0, 4),  # Low end of the jitter window.
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
        """The computed policy's [1, 60] bounds don't apply to static values."""
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
            estimator=warm_estimator(rate=5),
        )
        assert decision.policy == "queue_drain_rate"
        assert decision.fallback_rung == FALLBACK_RUNG_COMPUTED
        assert decision.drain_rate == 5.0
        assert decision.estimator_warm
        assert decision.pre_jitter_s == pytest.approx(2.0)
        assert decision.post_jitter_s == 2
        assert rand.num_calls == 1

    @pytest.mark.parametrize(
        "depth,rate,rand_value,expected_pre,expected_post",
        [
            # Clamped up to 1s; jitter below 1s still emits 1.
            (1, 100, 0.0, 1, 1),
            # Clamped down to 60s; jitter above 60s still emits 60.
            (1000, 1, 0.999999, 60, 60),
            (1000, 1, 0.0, 60, 48),
        ],
    )
    def test_computed_clamp(
        self,
        depth: int,
        rate: int,
        rand_value: float,
        expected_pre: float,
        expected_post: int,
    ):
        decision, _ = self.decide(
            policy="queue_drain_rate",
            depth=depth,
            estimator=warm_estimator(rate=rate),
            rand_value=rand_value,
        )
        assert decision.pre_jitter_s == expected_pre
        assert decision.post_jitter_s == expected_post
        assert 1 <= decision.post_jitter_s <= COMPUTED_RETRY_AFTER_MAX_S

    def test_computed_cold_falls_back_to_static(self):
        decision, rand = self.decide(policy="queue_drain_rate", retry_after_s=5)
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK
        assert not decision.estimator_warm
        assert decision.drain_rate is None
        assert decision.pre_jitter_s == 5
        assert decision.post_jitter_s == 5
        assert rand.num_calls == 1

    def test_computed_cold_without_fallback_has_no_header(self):
        decision, rand = self.decide(policy="queue_drain_rate", retry_after_s=None)
        assert decision.fallback_rung == FALLBACK_RUNG_NONE
        assert decision.post_jitter_s is None
        assert rand.num_calls == 0

    @pytest.mark.parametrize("rand_value,expected_post", [(0.0, 48), (0.999999, 60)])
    def test_computed_warm_zero_rate_clamps_to_max(
        self, rand_value: float, expected_post: int
    ):
        """A warm zero rate is computed (clamped to 60s), not the fallback."""
        decision, _ = self.decide(
            policy="queue_drain_rate",
            retry_after_s=5,
            estimator=warm_estimator(rate=0),
            rand_value=rand_value,
        )
        assert decision.policy == "queue_drain_rate"
        assert decision.estimator_warm
        assert decision.drain_rate == 0.0
        assert decision.fallback_rung == FALLBACK_RUNG_COMPUTED
        assert decision.pre_jitter_s == COMPUTED_RETRY_AFTER_MAX_S
        assert decision.post_jitter_s == expected_post
        assert 1 <= decision.post_jitter_s <= COMPUTED_RETRY_AFTER_MAX_S

    @pytest.mark.parametrize("invalid_rate", [float("nan"), -1.0])
    def test_computed_invalid_rate_falls_back(self, invalid_rate: float):
        estimator = warm_estimator(rate=5)
        estimator._rate = invalid_rate
        decision, _ = self.decide(
            policy="queue_drain_rate", retry_after_s=5, estimator=estimator
        )
        assert decision.estimator_warm
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK
        assert decision.post_jitter_s == 5

    def test_estimator_failure_falls_back(self):
        estimator = warm_estimator(rate=5)

        def fail(queue_depth):
            raise RuntimeError("boom")

        estimator.estimate_s = fail
        decision, _ = self.decide(
            policy="queue_drain_rate", retry_after_s=5, estimator=estimator
        )
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK
        assert decision.post_jitter_s == 5

    def test_snapshot_fields(self):
        estimator = warm_estimator(rate=4)
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
            "backpressure_estimator_warm": True,
            "backpressure_estimator_num_samples": 1,
            "backpressure_retry_after_pre_jitter_s": 2.75,
            "backpressure_retry_after_post_jitter_s": 3,
            "backpressure_drain_counter": 42,
        }

    def test_snapshot_is_not_affected_by_later_samples(self):
        estimator = warm_estimator(rate=4)
        decision, _ = self.decide(policy="queue_drain_rate", estimator=estimator)
        observe_interval(estimator, 8)
        drain(estimator, 3)
        assert estimator.num_samples == 2
        assert decision.estimator_num_samples == 1
        assert decision.drain_rate == 4.0
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

    def test_restart_resets_rate_but_not_counter(self):
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
        assert estimator.rate == pytest.approx(1.0)

        tracker.update_policy("static")
        tracker.update_policy("queue_drain_rate")
        assert estimator.rate is None
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
