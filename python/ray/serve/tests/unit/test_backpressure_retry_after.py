import json
import logging
import sys
from typing import List

import pytest

from ray._common.test_utils import async_wait_for_condition
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

        # Nothing drained and nothing pending: no rate decay, no warmup credit.
        for _ in range(5):
            observe_interval(estimator, 0, has_pending_work=False)
        assert estimator.rate == pytest.approx(10.0)
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
        assert not estimator.warm

        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.rate == pytest.approx(7.0)
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

    def test_estimate_unavailable_for_invalid_rate(self):
        estimator = make_estimator()
        assert estimator.estimate_s(10) is None

        # Warm, but every interval drained nothing.
        estimator.sample(has_pending_work=True)
        observe_interval(estimator, 0, has_pending_work=True)
        assert estimator.warm
        assert estimator.rate == 0
        assert estimator.estimate_s(10) is None

        for invalid_rate in (float("nan"), float("inf"), -1.0):
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

    def test_computed_zero_rate_falls_back(self):
        estimator = warm_estimator(rate=0)
        decision, _ = self.decide(
            policy="queue_drain_rate", retry_after_s=5, estimator=estimator
        )
        assert decision.estimator_warm
        assert decision.drain_rate == 0.0
        assert decision.fallback_rung == FALLBACK_RUNG_STATIC_FALLBACK

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
        assert decision.to_log_fields() == {
            "backpressure_retry_after_policy": "queue_drain_rate",
            "backpressure_retry_after_fallback_rung": "computed",
            "backpressure_observed_queue_depth": 11,
            "backpressure_drain_rate": 4.0,
            "backpressure_estimator_warm": True,
            "backpressure_retry_after_pre_jitter_s": 2.75,
            "backpressure_retry_after_post_jitter_s": 3,
            "backpressure_drain_counter": 42,
        }

    def test_uses_module_random_by_default(self, monkeypatch):
        rand = CountingRand(0.0)
        monkeypatch.setattr(backpressure.random, "random", rand)
        decision = compute_retry_after_decision(
            backpressure_config=BackpressureConfig(retry_after_s=10),
            observed_queue_depth=1,
            estimator=make_estimator(),
            jitter_fraction=0.2,
        )
        assert decision.post_jitter_s == 8
        assert rand.num_calls == 1


class FakeMetricsPusher:
    def __init__(self):
        self.calls: List[str] = []
        self.tasks = {}

    def start(self):
        self.calls.append("start")

    def register_or_update_task(self, name, task_func, interval_s):
        self.calls.append(f"register:{name}:{interval_s}")
        self.tasks[name] = task_func

    def stop_tasks(self):
        self.calls.append("stop")
        self.tasks.clear()

    async def graceful_shutdown(self):
        self.calls.append("shutdown")
        self.tasks.clear()


class TestDrainRateTracker:
    def test_sampler_follows_policy(self):
        pusher = FakeMetricsPusher()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True,
            estimator=make_estimator(),
            metrics_pusher=pusher,
            sample_interval_s=0.5,
        )

        # Static policy never starts the sampler.
        tracker.update_policy("static")
        assert not tracker.sampling
        assert pusher.calls == []

        tracker.update_policy("queue_drain_rate")
        assert tracker.sampling
        assert pusher.calls == [
            "start",
            f"register:{DrainRateTracker.SAMPLE_TASK_NAME}:0.5",
        ]

        # Re-applying the same policy (e.g. an unrelated config update) is a
        # no-op, so it doesn't reset the learned rate or add a timer.
        tracker.update_policy("queue_drain_rate")
        assert len(pusher.calls) == 2

        tracker.update_policy("static")
        assert not tracker.sampling
        assert pusher.calls[-1] == "stop"
        assert pusher.tasks == {}

    def test_restart_resets_rate_but_not_counter(self):
        pusher = FakeMetricsPusher()
        estimator = make_estimator()
        tracker = DrainRateTracker(
            has_pending_work=lambda: True, estimator=estimator, metrics_pusher=pusher
        )
        tracker.update_policy("queue_drain_rate")
        sample = pusher.tasks[DrainRateTracker.SAMPLE_TASK_NAME]

        sample()
        tracker.record_drain()
        estimator._clock.advance(1)
        sample()
        assert estimator.rate == pytest.approx(1.0)

        tracker.update_policy("static")
        tracker.update_policy("queue_drain_rate")
        assert estimator.rate is None
        assert tracker.drain_counter == 1

    def test_counter_maintained_under_static_policy(self):
        tracker = DrainRateTracker(
            has_pending_work=lambda: True, metrics_pusher=FakeMetricsPusher()
        )
        for _ in range(3):
            tracker.record_drain()
        assert tracker.drain_counter == 3

    @pytest.mark.asyncio
    async def test_policy_update_after_shutdown_is_ignored(self):
        pusher = FakeMetricsPusher()
        tracker = DrainRateTracker(has_pending_work=lambda: True, metrics_pusher=pusher)
        await tracker.shutdown()
        tracker.update_policy("queue_drain_rate")
        assert not tracker.sampling
        assert pusher.calls == ["shutdown"]

    @pytest.mark.asyncio
    async def test_real_timer_lifecycle(self):
        pending = {"value": True}
        estimator = make_estimator(clock=FakeClock())
        tracker = DrainRateTracker(
            has_pending_work=lambda: pending["value"],
            estimator=estimator,
            sample_interval_s=0.01,
        )
        tracker.update_policy("queue_drain_rate")
        tracker.update_policy("queue_drain_rate")
        assert len(tracker._metrics_pusher._async_tasks) == 1

        # Busy-zero intervals are counted, so the estimator warms up.
        async def advance_and_check():
            estimator._clock.advance(1)
            return estimator.warm

        await async_wait_for_condition(advance_and_check, retry_interval_ms=10)

        await tracker.shutdown()
        assert not tracker.sampling
        assert tracker._metrics_pusher._async_tasks == {}


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
        policy="queue_drain_rate",
        fallback_rung="computed",
        observed_queue_depth=9,
        drain_counter=1234,
        drain_rate=4.5,
        estimator_warm=True,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
