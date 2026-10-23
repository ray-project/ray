"""Retry-After computation for backpressure rejections.

A backpressure rejection is raised either by a router (proxy and handle
paths) or by a replica (direct ingress). Both use the same pieces:

- `DrainRateEstimator` keeps a monotonic count of drain events and a busy
  clock, and estimates the busy seconds per drain over a recent window. The
  router counts successful assignments to replicas; the replica counts
  requests releasing their execution slot.
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
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Deque, Dict, Optional, Tuple

from ray.serve._private.constants import (
    RAY_SERVE_BACKPRESSURE_BUSY_CLOCK_MIN_WINDOW_BUSY_S,
    RAY_SERVE_BACKPRESSURE_BUSY_CLOCK_MIN_WINDOW_DRAINS,
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

# Computed values are never below this, before or after jitter. There is no
# upper bound: the busy clock stays proportional to the real wait even for long
# backlogs, so the header tells clients the expected wait.
COMPUTED_RETRY_AFTER_MIN_S = 1


class DrainRateEstimator:
    """Busy clock: how long the component takes per drain while it's working.

    Two running totals feed it: drains (the monotonic drain counter) and busy
    seconds. `record_drain` is called on the hot path for every drain event and
    only increments an integer. `sample` runs periodically; an interval counts
    as busy if work was pending at the sample or at least one drain happened
    during it, and only busy intervals add to the busy clock. Idle time is
    ignored, so it can't look like slow draining.

    After every busy sample the pair (busy clock B, drain counter N) is a
    checkpoint. The window runs from the most recent checkpoint that is at
    least `min_window_drains` drains and at least `min_window_busy_s` busy
    seconds behind the newest one (or from the oldest checkpoint, until one
    qualifies) to now. Then `seconds_per_drain = window busy seconds /
    max(window drains, 1)` and the time to drain a backlog of D requests is
    `D * seconds_per_drain`. The drain minimum keeps sparse completions from
    resting on one or two gaps; the busy-time minimum keeps fast bursts from
    swinging it. History is kept by drains, not by time, so even a drain every
    15 minutes is measured.

    Not thread-safe: all methods must be called from the owning component's
    event loop. Reading `drain_counter` from another thread is safe (a single
    attribute read).
    """

    def __init__(
        self,
        *,
        warmup_samples: int = RAY_SERVE_BACKPRESSURE_DRAIN_RATE_WARMUP_SAMPLES,
        min_window_drains: int = RAY_SERVE_BACKPRESSURE_BUSY_CLOCK_MIN_WINDOW_DRAINS,
        min_window_busy_s: float = RAY_SERVE_BACKPRESSURE_BUSY_CLOCK_MIN_WINDOW_BUSY_S,
        clock: Callable[[], float] = time.monotonic,
    ):
        if warmup_samples < 1:
            raise ValueError(f"warmup_samples must be >= 1, got {warmup_samples}.")
        if min_window_drains < 1:
            raise ValueError(
                f"min_window_drains must be >= 1, got {min_window_drains}."
            )
        if min_window_busy_s <= 0:
            raise ValueError(
                f"min_window_busy_s must be positive, got {min_window_busy_s}."
            )

        self._warmup_samples = warmup_samples
        self._min_window_drains = min_window_drains
        self._min_window_busy_s = min_window_busy_s
        self._clock = clock

        # Monotonic total; never reset, so the acceptance harness can measure
        # ground-truth drain time as "time until the counter advances by D".
        self._drain_counter = 0

        # Cumulative busy seconds since the last reset; only busy intervals
        # advance it.
        self._busy_clock_s = 0.0
        # Checkpoints `(busy clock, drain counter)` at sample boundaries, oldest
        # first; the first one is the window start. Only checkpoints that can
        # still become a window start are kept (see `_update_window`).
        self._checkpoints: Deque[Tuple[float, int]] = deque([(0.0, 0)])
        # The current window's totals, recomputed at each busy sample so
        # rejections only read them.
        self._window_busy_s = 0.0
        self._window_drains = 0

        self._num_samples = 0
        self._last_sample_time: Optional[float] = None
        self._last_sample_counter = 0

    @property
    def drain_counter(self) -> int:
        return self._drain_counter

    @property
    def num_samples(self) -> int:
        """Number of counted (busy) sample intervals since the last reset."""
        return self._num_samples

    @property
    def warm(self) -> bool:
        return self._num_samples >= self._warmup_samples

    @property
    def busy_clock_s(self) -> float:
        """Cumulative busy seconds since the last reset."""
        return self._busy_clock_s

    @property
    def busy_seconds(self) -> float:
        """Busy seconds in the current window."""
        return self._window_busy_s

    @property
    def drains_in_window(self) -> int:
        return self._window_drains

    @property
    def seconds_per_drain(self) -> Optional[float]:
        """Busy seconds per drain over the window, or None before any busy
        interval. A window without drains counts as one drain, so a stall
        keeps growing the value instead of making it undefined."""
        if self._num_samples == 0:
            return None
        return self._window_busy_s / max(self._window_drains, 1)

    @property
    def drain_rate(self) -> Optional[float]:
        """Drains per busy second over the window, or None before any busy
        interval."""
        if self._num_samples == 0:
            return None
        return self._window_drains / self._window_busy_s

    def record_drain(self) -> None:
        self._drain_counter += 1

    def reset(self) -> None:
        """Forget the busy history and restart sampling from the current counter.

        The drain counter itself is left untouched.
        """
        self._busy_clock_s = 0.0
        self._checkpoints = deque([(0.0, self._drain_counter)])
        self._window_busy_s = 0.0
        self._window_drains = 0
        self._num_samples = 0
        self._last_sample_time = None
        self._last_sample_counter = self._drain_counter

    def sample(self, has_pending_work: bool) -> None:
        """Add the interval since the previous call to the busy clock if busy.

        `has_pending_work` tells whether the component currently has work that
        could drain. The interval is busy if work is pending (a stall still
        adds busy time, so the estimate grows with it) or if anything drained
        during it (so the last drains of a burst aren't lost when the queue
        has just emptied). Otherwise it is idle and skipped.
        """
        now = self._clock()
        counter = self._drain_counter
        if self._last_sample_time is None:
            # First call only establishes the baseline.
            self._last_sample_time = now
            self._last_sample_counter = counter
            self._checkpoints = deque([(self._busy_clock_s, counter)])
            return

        elapsed_s = now - self._last_sample_time
        drains = counter - self._last_sample_counter
        if elapsed_s <= 0:
            return

        self._last_sample_time = now
        self._last_sample_counter = counter
        if drains == 0 and not has_pending_work:
            return

        self._busy_clock_s += elapsed_s
        self._num_samples += 1
        self._checkpoints.append((self._busy_clock_s, counter))
        self._update_window()

    def _update_window(self) -> None:
        """Pick the window start and prune checkpoints that can't be one.

        The checkpoints far enough behind the newest one form a prefix for
        each minimum (B and N only grow), so the most recent checkpoint
        meeting both is the earlier of `drain_cut` (the latest one at least
        `min_window_drains` behind) and `busy_cut` (the latest one at least
        `min_window_busy_s` behind). As B and N grow, both only move forward.
        So a checkpoint can be a future window start only if it is at or after
        today's `busy_cut`, or it is the last one before the drain counter
        rises (the only kind `drain_cut` lands on). Anything else, and anything
        before today's start, is dropped. Since the values are cumulative,
        dropping a checkpoint never changes a window's busy time or drains.
        """
        checkpoints = self._checkpoints
        busy_now, drains_now = checkpoints[-1]
        busy_cut = drain_cut = None
        for i in range(len(checkpoints) - 1, -1, -1):
            busy_s, drains = checkpoints[i]
            if busy_cut is None and busy_now - busy_s >= self._min_window_busy_s:
                busy_cut = i
            if drain_cut is None and drains_now - drains >= self._min_window_drains:
                drain_cut = i
            if busy_cut is not None and drain_cut is not None:
                break
        # Until a checkpoint meets both minimums, the window is all history.
        start = (
            min(busy_cut, drain_cut)
            if busy_cut is not None and drain_cut is not None
            else 0
        )
        keep_from = busy_cut if busy_cut is not None else 0

        kept: Deque[Tuple[float, int]] = deque()
        for i in range(start, len(checkpoints)):
            is_last_before_drains = (
                i + 1 < len(checkpoints) and checkpoints[i + 1][1] > checkpoints[i][1]
            )
            if i == start or i >= keep_from or is_last_before_drains:
                kept.append(checkpoints[i])
        self._checkpoints = kept

        start_busy_s, start_drains = kept[0]
        self._window_busy_s = busy_now - start_busy_s
        self._window_drains = drains_now - start_drains

    def estimate_s(self, queue_depth: int) -> Optional[float]:
        """Seconds to drain `queue_depth` requests at the window's pace.

        Returns None until the estimator is warm; callers then fall back to the
        static value.
        """
        seconds_per_drain = self.seconds_per_drain
        if not self.warm or seconds_per_drain is None:
            return None
        return queue_depth * seconds_per_drain


class DrainRateTracker:
    """Owns a `DrainRateEstimator` and the timer that samples it.

    The timer only runs while the `queue_drain_rate` policy is active, and it
    has its own `MetricsPusher` so it runs regardless of whether autoscaling
    metrics are being pushed. Each sampling lifetime gets a new pusher, so a
    sampler from before a stop can never resume after a restart. The drain
    counter is always maintained (it is one integer increment) so acceptance
    tooling can measure ground truth for the static policy too.

    Must be used from the owning component's event loop.
    """

    SAMPLE_TASK_NAME = "sample_backpressure_drain_rate"

    def __init__(
        self,
        has_pending_work: Callable[[], bool],
        *,
        estimator: Optional[DrainRateEstimator] = None,
        metrics_pusher_factory: Callable[[], MetricsPusher] = MetricsPusher,
        sample_interval_s: float = RAY_SERVE_BACKPRESSURE_DRAIN_RATE_SAMPLE_INTERVAL_S,
    ):
        self._has_pending_work = has_pending_work
        self._estimator = estimator or DrainRateEstimator()
        self._metrics_pusher_factory = metrics_pusher_factory
        self._sample_interval_s = sample_interval_s
        # The pusher for the current sampling lifetime; None while not sampling.
        self._metrics_pusher: Optional[MetricsPusher] = None
        self._shut_down = False

    @property
    def estimator(self) -> DrainRateEstimator:
        return self._estimator

    @property
    def drain_counter(self) -> int:
        return self._estimator.drain_counter

    @property
    def sampling(self) -> bool:
        return self._metrics_pusher is not None

    def record_drain(self) -> None:
        self._estimator.record_drain()

    def update_policy(self, retry_after_policy: str) -> None:
        """Start or stop sampling to match the configured policy. Idempotent."""
        should_sample = retry_after_policy == RETRY_AFTER_POLICY_QUEUE_DRAIN_RATE
        # A config update can arrive while `shutdown` is awaiting; restarting
        # the timer then would leak it.
        if self._shut_down or should_sample == self.sampling:
            return

        if should_sample:
            # Don't carry over busy history from before sampling last stopped.
            self._estimator.reset()
            # A stopped pusher can't be restarted safely: `start()` clears the
            # stop event, which a sampler task that hasn't run yet would then
            # never see. A new pusher per lifetime keeps old samplers stopped.
            pusher = self._metrics_pusher_factory()

            def sample():
                # A sampler task runs its function once before it checks the
                # stop event, so an old one could still get here.
                if self._metrics_pusher is pusher:
                    self._sample()

            pusher.register_or_update_task(
                self.SAMPLE_TASK_NAME, sample, self._sample_interval_s
            )
            self._metrics_pusher = pusher
        else:
            stopped_pusher = self._metrics_pusher
            # Set: `sampling` was True, or we'd have returned above.
            assert stopped_pusher is not None
            # Only signal the stop: clearing the pusher's tasks (`stop_tasks`)
            # would make a sampler that hasn't run yet fail its task lookup.
            stopped_pusher.stop_event.set()
            self._metrics_pusher = None

    def _sample(self) -> None:
        self._estimator.sample(has_pending_work=self._has_pending_work())

    def decide(
        self,
        backpressure_config: "BackpressureConfig",
        observed_queue_depth: int,
        *,
        request_id: str,
    ) -> "RetryAfterDecision":
        return compute_retry_after_decision(
            backpressure_config=backpressure_config,
            observed_queue_depth=observed_queue_depth,
            estimator=self._estimator,
            request_id=request_id,
        )

    async def shutdown(self) -> None:
        self._shut_down = True
        if self._metrics_pusher is not None:
            pusher, self._metrics_pusher = self._metrics_pusher, None
            await pusher.graceful_shutdown()


@dataclass(frozen=True)
class RetryAfterDecision:
    """Everything about one rejection's Retry-After value, captured once.

    `post_jitter_s` is the integer header value sent to the client (None means
    no header). `pre_jitter_s` is the post-clamp, pre-jitter value that
    acceptance scoring uses. `busy_seconds`, `drains_in_window`,
    `seconds_per_drain` and `drain_rate` describe the estimator's window
    (`drain_rate` is drains per busy second); they're None before the first
    busy interval.
    """

    request_id: str
    policy: str
    fallback_rung: str
    observed_queue_depth: int
    drain_counter: int
    drain_rate: Optional[float]
    busy_seconds: Optional[float]
    drains_in_window: Optional[int]
    seconds_per_drain: Optional[float]
    estimator_warm: bool
    estimator_num_samples: int
    pre_jitter_s: Optional[float]
    post_jitter_s: Optional[int]

    def to_log_fields(self) -> Dict[str, Any]:
        return {
            "backpressure_request_id": self.request_id,
            "backpressure_retry_after_policy": self.policy,
            "backpressure_retry_after_fallback_rung": self.fallback_rung,
            "backpressure_observed_queue_depth": self.observed_queue_depth,
            "backpressure_drain_rate": self.drain_rate,
            "backpressure_busy_seconds": self.busy_seconds,
            "backpressure_drains_in_window": self.drains_in_window,
            "backpressure_seconds_per_drain": self.seconds_per_drain,
            "backpressure_estimator_warm": self.estimator_warm,
            "backpressure_estimator_num_samples": self.estimator_num_samples,
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
    request_id: str,
    jitter_fraction: float = RAY_SERVE_BACKPRESSURE_RETRY_AFTER_JITTER_FRACTION,
    rand: Optional[Callable[[], float]] = None,
) -> RetryAfterDecision:
    """Pick the Retry-After value for a rejection.

    Fallback ladder for `queue_drain_rate`: the computed value if the
    estimator is warm, else the static `retry_after_s` if set, else no header.
    The computed value estimates when the backlog seen at rejection will have
    drained; it's a hint, not a reservation.
    Never raises because of the estimator; a failure downgrades to the next
    rung.

    Jitter is sampled once here, and the result is rounded up to the integer
    delay-seconds that goes on the wire (RFC 9110).
    """
    policy = backpressure_config.retry_after_policy
    static_s = backpressure_config.retry_after_s

    drain_counter = estimator.drain_counter
    warm = estimator.warm
    num_samples = estimator.num_samples
    seconds_per_drain = estimator.seconds_per_drain
    has_window = seconds_per_drain is not None
    drain_rate = estimator.drain_rate if has_window else None
    busy_seconds = estimator.busy_seconds if has_window else None
    drains_in_window = estimator.drains_in_window if has_window else None

    pre_jitter_s: Optional[float] = None
    if policy == RETRY_AFTER_POLICY_QUEUE_DRAIN_RATE:
        try:
            computed_s = estimator.estimate_s(observed_queue_depth)
        except Exception:
            logger.debug("Failed to compute Retry-After estimate.", exc_info=True)
            computed_s = None

        if computed_s is not None:
            fallback_rung = FALLBACK_RUNG_COMPUTED
            pre_jitter_s = max(computed_s, COMPUTED_RETRY_AFTER_MIN_S)
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
            post_jitter_s = max(post_jitter_s, COMPUTED_RETRY_AFTER_MIN_S)

    return RetryAfterDecision(
        request_id=request_id,
        policy=policy,
        fallback_rung=fallback_rung,
        observed_queue_depth=observed_queue_depth,
        drain_counter=drain_counter,
        drain_rate=drain_rate,
        busy_seconds=busy_seconds,
        drains_in_window=drains_in_window,
        seconds_per_drain=seconds_per_drain,
        estimator_warm=warm,
        estimator_num_samples=num_samples,
        pre_jitter_s=pre_jitter_s,
        post_jitter_s=post_jitter_s,
    )


def log_backpressure_rejection(message: str, decision: RetryAfterDecision) -> None:
    """Emit the single structured warning for a backpressure rejection."""
    logger.warning(message, extra={SERVE_LOG_EXTRA_FIELDS: decision.to_log_fields()})
