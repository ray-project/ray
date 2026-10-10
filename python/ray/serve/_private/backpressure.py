"""Retry-After computation for backpressure rejections.

A backpressure rejection is raised either by a router (proxy and handle
paths) or by a replica (direct ingress). Both use the same pieces:

- `DrainRateEstimator` turns a monotonic count of drain events into a
  smoothed drain rate. The router counts successful assignments to replicas;
  the replica counts requests releasing their execution slot.
- `DrainRateTracker` owns an estimator and the background timer that samples
  it while the `queue_drain_rate` policy is active.
- `compute_retry_after_decision` turns the config, the queue depth observed at
  rejection, and the estimator state into a `RetryAfterDecision`: one
  immutable snapshot that is both logged and used for the `Retry-After`
  header, so the logged value is exactly what the client received.
"""

import logging
import math
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from ray.serve._private.constants import (
    RAY_SERVE_BACKPRESSURE_DRAIN_RATE_EWMA_ALPHA,
    RAY_SERVE_BACKPRESSURE_DRAIN_RATE_SAMPLE_INTERVAL_S,
    RAY_SERVE_BACKPRESSURE_DRAIN_RATE_WARMUP_SAMPLES,
    RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION,
    SERVE_LOG_EXTRA_FIELDS,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher

if TYPE_CHECKING:
    from ray.serve.config import BackpressureConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)

RETRY_AFTER_POLICY_STATIC = "static"
RETRY_AFTER_POLICY_QUEUE_DRAIN_RATE = "queue_drain_rate"

# Which rung of the fallback ladder produced the Retry-After value.
FALLBACK_RUNG_COMPUTED = "computed"
FALLBACK_RUNG_STATIC_FALLBACK = "static_fallback"
FALLBACK_RUNG_STATIC = "static"
FALLBACK_RUNG_NONE = "none"

# Computed values are clamped to this range. Common client SDKs ignore
# Retry-After values above 60s.
COMPUTED_RETRY_AFTER_MIN_S = 1
COMPUTED_RETRY_AFTER_MAX_S = 60


class DrainRateEstimator:
    """EWMA of how fast a queue drains, from a monotonic drain-event counter.

    `record_drain` is called on the hot path for every drain event and only
    increments an integer. `sample` runs periodically and folds the counter
    delta since the previous sample into the EWMA.

    Not thread-safe: all methods must be called from the owning component's
    event loop. Reading `drain_counter` from another thread is safe (a single
    attribute read).
    """

    def __init__(
        self,
        *,
        alpha: float = RAY_SERVE_BACKPRESSURE_DRAIN_RATE_EWMA_ALPHA,
        warmup_samples: int = RAY_SERVE_BACKPRESSURE_DRAIN_RATE_WARMUP_SAMPLES,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not 0 < alpha <= 1:
            raise ValueError(f"alpha must be in (0, 1], got {alpha}.")
        if warmup_samples < 1:
            raise ValueError(f"warmup_samples must be >= 1, got {warmup_samples}.")

        self._alpha = alpha
        self._warmup_samples = warmup_samples
        self._clock = clock

        # Monotonic total; never reset, so the acceptance harness can measure
        # ground-truth drain time as "time until the counter advances by D".
        self._drain_counter = 0

        self._rate: Optional[float] = None
        self._num_samples = 0
        self._last_sample_time: Optional[float] = None
        self._last_sample_counter = 0

    @property
    def drain_counter(self) -> int:
        return self._drain_counter

    @property
    def rate(self) -> Optional[float]:
        """Smoothed drain rate in events per second, or None before any sample."""
        return self._rate

    @property
    def warm(self) -> bool:
        return self._num_samples >= self._warmup_samples

    def record_drain(self) -> None:
        self._drain_counter += 1

    def reset_rate(self) -> None:
        """Forget the learned rate and restart sampling from the current counter.

        The drain counter itself is left untouched.
        """
        self._rate = None
        self._num_samples = 0
        self._last_sample_time = None
        self._last_sample_counter = self._drain_counter

    def sample(self, has_pending_work: bool) -> None:
        """Fold the drain events since the previous call into the EWMA.

        `has_pending_work` tells whether the component currently has work that
        could drain. An interval with no drain events while work is pending
        is a real (slow-drain) observation of rate 0. An interval with no
        drain events and no pending work is idle and is skipped, so the rate
        doesn't decay just because traffic stopped.
        """
        now = self._clock()
        counter = self._drain_counter
        if self._last_sample_time is None:
            # First call only establishes the baseline.
            self._last_sample_time = now
            self._last_sample_counter = counter
            return

        elapsed_s = now - self._last_sample_time
        delta = counter - self._last_sample_counter
        if elapsed_s <= 0:
            return

        self._last_sample_time = now
        self._last_sample_counter = counter
        if delta == 0 and not has_pending_work:
            return

        interval_rate = delta / elapsed_s
        if self._rate is None:
            self._rate = interval_rate
        else:
            self._rate = self._alpha * interval_rate + (1 - self._alpha) * self._rate
        self._num_samples += 1

    def estimate_s(self, queue_depth: int) -> Optional[float]:
        """Seconds to drain `queue_depth` requests at the current rate.

        Returns None when no usable estimate exists (not warm yet, or the rate
        is zero or invalid); callers then fall back to the static value.
        """
        rate = self._rate
        if not self.warm or rate is None or not math.isfinite(rate) or rate <= 0:
            return None
        return queue_depth / rate


class DrainRateTracker:
    """Owns a `DrainRateEstimator` and the timer that samples it.

    The timer only runs while the `queue_drain_rate` policy is active, and it
    has its own `MetricsPusher` so it runs regardless of whether autoscaling
    metrics are being pushed. The drain counter is always maintained (it is
    one integer increment) so acceptance tooling can measure ground truth for
    the static policy too.

    Must be used from the owning component's event loop.
    """

    SAMPLE_TASK_NAME = "sample_backpressure_drain_rate"

    def __init__(
        self,
        has_pending_work: Callable[[], bool],
        *,
        estimator: Optional[DrainRateEstimator] = None,
        metrics_pusher: Optional[MetricsPusher] = None,
        sample_interval_s: float = RAY_SERVE_BACKPRESSURE_DRAIN_RATE_SAMPLE_INTERVAL_S,
    ):
        self._has_pending_work = has_pending_work
        self._estimator = estimator or DrainRateEstimator()
        self._metrics_pusher = metrics_pusher or MetricsPusher()
        self._sample_interval_s = sample_interval_s
        self._sampling = False
        self._shut_down = False

    @property
    def estimator(self) -> DrainRateEstimator:
        return self._estimator

    @property
    def drain_counter(self) -> int:
        return self._estimator.drain_counter

    @property
    def sampling(self) -> bool:
        return self._sampling

    def record_drain(self) -> None:
        self._estimator.record_drain()

    def update_policy(self, retry_after_policy: str) -> None:
        """Start or stop sampling to match the configured policy. Idempotent."""
        should_sample = retry_after_policy == RETRY_AFTER_POLICY_QUEUE_DRAIN_RATE
        # A config update can arrive while `shutdown` is awaiting; restarting
        # the timer then would leak it.
        if self._shut_down or should_sample == self._sampling:
            return

        if should_sample:
            # Don't carry over a rate learned before sampling was last stopped.
            self._estimator.reset_rate()
            self._metrics_pusher.start()
            self._metrics_pusher.register_or_update_task(
                self.SAMPLE_TASK_NAME, self._sample, self._sample_interval_s
            )
        else:
            self._metrics_pusher.stop_tasks()
        self._sampling = should_sample

    def _sample(self) -> None:
        self._estimator.sample(has_pending_work=self._has_pending_work())

    def decide(
        self, backpressure_config: "BackpressureConfig", observed_queue_depth: int
    ) -> "RetryAfterDecision":
        return compute_retry_after_decision(
            backpressure_config=backpressure_config,
            observed_queue_depth=observed_queue_depth,
            estimator=self._estimator,
        )

    async def shutdown(self) -> None:
        self._shut_down = True
        await self._metrics_pusher.graceful_shutdown()
        self._sampling = False


@dataclass(frozen=True)
class RetryAfterDecision:
    """Everything about one rejection's Retry-After value, captured once.

    `post_jitter_s` is the integer header value sent to the client (None means
    no header). `pre_jitter_s` is the post-clamp, pre-jitter value that
    acceptance scoring uses.
    """

    policy: str
    fallback_rung: str
    observed_queue_depth: int
    drain_counter: int
    drain_rate: Optional[float]
    estimator_warm: bool
    pre_jitter_s: Optional[float]
    post_jitter_s: Optional[int]

    def to_log_fields(self) -> Dict[str, Any]:
        return {
            "backpressure_retry_after_policy": self.policy,
            "backpressure_retry_after_fallback_rung": self.fallback_rung,
            "backpressure_observed_queue_depth": self.observed_queue_depth,
            "backpressure_drain_rate": self.drain_rate,
            "backpressure_estimator_warm": self.estimator_warm,
            "backpressure_retry_after_pre_jitter_s": self.pre_jitter_s,
            "backpressure_retry_after_post_jitter_s": self.post_jitter_s,
            "backpressure_drain_counter": self.drain_counter,
        }


def apply_retry_after_jitter(
    retry_after_s: float,
    *,
    jitter_fraction: float = RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION,
    rand: Optional[Callable[[], float]] = None,
) -> float:
    """Scale by a factor drawn uniformly from [1 - fraction, 1 + fraction).

    Draws exactly one random number (from `rand`, default `random.random`).
    """
    if rand is None:
        rand = random.random
    return retry_after_s * (1 + jitter_fraction * (2 * rand() - 1))


def compute_retry_after_decision(
    *,
    backpressure_config: "BackpressureConfig",
    observed_queue_depth: int,
    estimator: DrainRateEstimator,
    jitter_fraction: float = RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION,
    rand: Optional[Callable[[], float]] = None,
) -> RetryAfterDecision:
    """Pick the Retry-After value for a rejection.

    Fallback ladder for `queue_drain_rate`: the computed value if the
    estimator is warm, else the static `retry_after_s` if set, else no header.
    Never raises because of the estimator; a failure downgrades to the next
    rung.

    Jitter is sampled once here, and the result is rounded up to the integer
    delay-seconds that goes on the wire (RFC 9110).
    """
    policy = backpressure_config.retry_after_policy
    static_s = backpressure_config.retry_after_s

    drain_counter = estimator.drain_counter
    drain_rate = estimator.rate
    warm = estimator.warm

    pre_jitter_s: Optional[float] = None
    if policy == RETRY_AFTER_POLICY_QUEUE_DRAIN_RATE:
        try:
            computed_s = estimator.estimate_s(observed_queue_depth)
        except Exception:
            logger.debug("Failed to compute Retry-After estimate.", exc_info=True)
            computed_s = None

        if computed_s is not None:
            fallback_rung = FALLBACK_RUNG_COMPUTED
            pre_jitter_s = min(
                max(computed_s, COMPUTED_RETRY_AFTER_MIN_S), COMPUTED_RETRY_AFTER_MAX_S
            )
        elif static_s is not None:
            fallback_rung = FALLBACK_RUNG_STATIC_FALLBACK
            pre_jitter_s = static_s
        else:
            fallback_rung = FALLBACK_RUNG_NONE
    elif static_s is not None:
        fallback_rung = FALLBACK_RUNG_STATIC
        pre_jitter_s = static_s
    else:
        fallback_rung = FALLBACK_RUNG_NONE

    post_jitter_s: Optional[int] = None
    if pre_jitter_s is not None:
        jittered_s = apply_retry_after_jitter(
            pre_jitter_s, jitter_fraction=jitter_fraction, rand=rand
        )
        if not math.isfinite(jittered_s):
            # A huge (but finite) static value can overflow when scaled up.
            jittered_s = pre_jitter_s
        post_jitter_s = max(0, math.ceil(jittered_s))
        if fallback_rung == FALLBACK_RUNG_COMPUTED:
            post_jitter_s = min(
                max(post_jitter_s, COMPUTED_RETRY_AFTER_MIN_S),
                COMPUTED_RETRY_AFTER_MAX_S,
            )

    return RetryAfterDecision(
        policy=policy,
        fallback_rung=fallback_rung,
        observed_queue_depth=observed_queue_depth,
        drain_counter=drain_counter,
        drain_rate=drain_rate,
        estimator_warm=warm,
        pre_jitter_s=pre_jitter_s,
        post_jitter_s=post_jitter_s,
    )


def log_backpressure_rejection(message: str, decision: RetryAfterDecision) -> None:
    """Emit the single structured warning for a backpressure rejection."""
    logger.warning(message, extra={SERVE_LOG_EXTRA_FIELDS: decision.to_log_fields()})
