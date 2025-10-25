import logging
import math
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Deque, Dict

from .backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.operators.map_operator import MapOperator

logger = logging.getLogger(__name__)


class ConcurrencyCapBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that caps the concurrency of each operator.

    This policy dynamically limits the number of concurrent tasks per operator
    based on queue pressure. It combines:

    - Adaptive threshold built from EWMA of the queue level and its
      absolute deviation: ``threshold = max(level + K * dev, current_queue_size_bytes)``.
      The ``current_queue_size_bytes`` term enables immediate upward revision to avoid
      throttling ramp-up throughput.

      Why this threshold works:
      - level + K*dev: Sets threshold above typical queue size by K standard deviations
      - max(..., current_queue_size_bytes): Prevents false throttling during legitimate ramp-up periods
      - When queue grows faster than EWMA can track, current_queue_size_bytes > level + K*dev
      - This allows the system to "catch up" to the new higher baseline before throttling

    - Quantized step controller that nudges running concurrency by
      ``{-1, 0, +1, +2}`` using normalized pressure and trend signals.

    Key Concepts:
    - Level (EWMA): Typical queue size; slowly tracks the central tendency.
    - Deviation (EWMA): Typical absolute deviation around the level; acts as
      a normalization scale for pressure and trend signals.
    - Threshold: Dynamic limit derived from observed signal: if the current
      queue exceeds the threshold, we consider backoff. The ``max(..., current_queue_size_bytes)``
      makes this instantaneously responsive upward.
    - Instantaneous pressure: How far the current queue is from threshold,
      normalized by deviation.
    - Trend: Whether the queue is rising/falling over a short horizon (recent
      vs older HISTORY_LEN/2 samples), normalized by deviation.

    Example:
        Consider an operator with configured cap=10 and queue pressure over time:

        Queue samples: [100, 120, 140, 160, 180, 200, 220, 240, 260, 280]
        Threshold: 150 (level=180, dev=20, K=4.0)

        Ramp-up scenario (queue growing, pressure < 0):
        - pressure_signal = (100-150)/20 = -2.5, trend_signal = -1.0
        - Decision: step = +2 (strong growth, low pressure)
        - Result: concurrency increases from 8 -> 10 (capped at configured max)

        Dial-down scenario (queue growing, pressure > 0):
        - pressure_signal = (200-150)/20 = +2.5, trend_signal = +1.0
        - Decision: step = -1 (high pressure, growing trend)
        - Result: concurrency decreases from 10 -> 9

        Stable scenario (queue stable, pressure ~ 0):
        - pressure_signal = (150-150)/20 = 0.0, trend_signal = 0.0
        - Decision: step = 0 (no change needed)
        - Result: concurrency stays at 10

    NOTE: Only support setting concurrency cap for `TaskPoolMapOperator` for now.
    TODO(chengsu): Consolidate with actor scaling logic of `ActorPoolMapOperator`.
    """

    # Queue history window for recent-trend estimation. Small window to capture recent trend.
    HISTORY_LEN = 10
    # Smoothing factor for both level and dev.
    EWMA_ALPHA = 0.2
    # Deviation multiplier to define "over-threshold".
    K_DEV = 4.0

    def __init__(self, *args, **kwargs):
        """Initialize the ConcurrencyCapBackpressurePolicy."""
        super().__init__(*args, **kwargs)

        # Explicit concurrency caps for each operator. Infinite if not specified.
        self._concurrency_caps: Dict["PhysicalOperator", float] = {}

        # Queue history for recent-trend estimation. Small window to capture recent trend.
        self._queue_history: Dict["PhysicalOperator", Deque[int]] = defaultdict(
            lambda: deque(maxlen=self.HISTORY_LEN)
        )

        # Per-operator cached threshold (bootstrapped from first sample).
        self._queue_thresholds: Dict["PhysicalOperator", int] = defaultdict(int)

        # EWMA state for level and absolute deviation.
        self._q_level_nbytes: Dict["PhysicalOperator", float] = defaultdict(float)

        # EWMA state for absolute deviation.
        self._q_level_dev: Dict["PhysicalOperator", float] = defaultdict(float)

        # Track last effective cap per operator for change detection.
        self._last_effective_caps: Dict["PhysicalOperator", int] = {}

        # Initialize caps from operators (infinite if unset)
        for op, _ in self._topology.items():
            if isinstance(op, TaskPoolMapOperator) and op.get_concurrency() is not None:
                self._concurrency_caps[op] = op.get_concurrency()
            else:
                self._concurrency_caps[op] = float("inf")

        # Whether to cap the concurrency of an operator based on its and downstream's queue size.
        self.enable_dynamic_output_queue_size_backpressure = (
            self._data_context.enable_dynamic_output_queue_size_backpressure
        )

        logger.debug(
            "ConcurrencyCapBackpressurePolicy caps: %s, cap based on queue size: %s",
            self._concurrency_caps,
            self.enable_dynamic_output_queue_size_backpressure,
        )

    def _update_ewma_asymmetric(self, prev_value: float, sample: float) -> float:
        """
        Update EWMA with asymmetric behavior: fast rise, slow fall.

        Args:
            prev_value: Previous EWMA value
            sample: New sample value

        Returns:
            Updated EWMA value
        """
        if prev_value <= 0:
            return sample

        alpha_up = 1.0 - (1.0 - self.EWMA_ALPHA) ** 2  # fast rise
        alpha = alpha_up if sample > prev_value else self.EWMA_ALPHA  # slow fall
        return (1 - alpha) * prev_value + alpha * sample

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Return whether `op` may accept another input now.

        Admission control logic:
          * Under threshold: Allow full concurrency up to configured cap
          * Over threshold: Adjust concurrency using step controller (-1,0,+1,+2)
            based on pressure and trend signals

        Args:
            op: The operator under consideration.

        Returns:
            True if admitting one more input is allowed.
        """
        running = op.metrics.num_tasks_running
        if (
            not isinstance(op, MapOperator)
            or not self.enable_dynamic_output_queue_size_backpressure
        ):
            return running < self._concurrency_caps[op]

        # Observe fresh queue size for this operator and its downstream.
        current_queue_size_bytes = (
            self._resource_manager.get_op_internal_object_store_usage(op)
            + self._resource_manager.get_op_outputs_object_store_usage_with_downstream(
                op
            )
        )

        # Update short history and refresh the adaptive threshold.
        self._queue_history[op].append(current_queue_size_bytes)
        threshold = self._update_queue_threshold(op, current_queue_size_bytes)

        # If configured to cap based on queue size, use the effective cap.
        if current_queue_size_bytes > threshold:
            # Over-threshold: potentially back off via effective cap.
            effective_cap = self._effective_cap(op)
            is_capped = running < effective_cap

            last_effective_cap = self._last_effective_caps.get(op, None)
            if last_effective_cap != effective_cap:
                logger.debug(
                    "Effective concurrency cap changed for operator %s: %d -> %d "
                    "running=%d tasks, queue=%d bytes, threshold=%d bytes",
                    op.name,
                    last_effective_cap,
                    effective_cap,
                    running,
                    current_queue_size_bytes,
                    threshold,
                )
                self._last_effective_caps[op] = effective_cap

            return is_capped
        else:
            # Under-threshold: only enforce the configured cap.
            return running < self._concurrency_caps[op]

    def _update_queue_threshold(
        self, op: "PhysicalOperator", current_queue_size_bytes: int
    ) -> int:
        """Update and return the current adaptive threshold for `op`.

        Motivation: Adaptive thresholds prevent both over-throttling (too aggressive) and
        under-throttling (too permissive). The logic balances responsiveness with stability:
        - Fast upward response to pressure spikes (immediate threshold increase)
        - Thresholds only increase, never decrease, since we don't know if low pressure is steady state

        Args:
            op: Operator whose threshold is being updated.
            current_queue_size_bytes: Current total queued bytes for this operator + downstream.

        Returns:
            The updated threshold in bytes.

        Examples:
            # Bootstrap: first sample sets threshold
            # Input: current_queue_size_bytes = 1000, level_prev = 0, dev_prev = 0
            # EWMA: level = 1000, dev = 0
            # Base: 1000 + 4*0 = 1000, Threshold: max(1000, 1000) = 1000
            # Result: 1000 (bootstrap)

            # Pressure increase: threshold updated immediately
            # Input: current_queue_size_bytes = 1500, level_prev = 1000, dev_prev = 100
            # EWMA: level = 1100, dev = 180, Base: 1100 + 4*180 = 1820
            # Threshold: max(1820, 1500) = 1820, prev_threshold = 1000
            # Result: 1820 (threshold increased)

            # Pressure decrease: threshold maintained (no decrease)
            # Input: current_queue_size_bytes = 100, level_prev = 200, dev_prev = 50
            # EWMA: level = 180, dev = 60, Base: 180 + 4*60 = 420
            # Threshold: max(420, 100) = 420, prev_threshold = 500
            # Result: 500 (threshold maintained, no decrease)

        """
        hist = self._queue_history[op]
        if not hist:
            return 0

        q = float(current_queue_size_bytes)

        # Step 1: update EWMAs
        level_prev = self._q_level_nbytes[op]
        dev_prev = self._q_level_dev[op]

        # Update EWMA level (typical queue size) with asymmetric behavior
        # Why asymmetric? Quick to detect problems, slow to recover (prevents oscillation)
        # Example: queue grows 100->200->150, EWMA follows 100->180->170
        #          (jumps up fast, drops down slow)
        level = self._update_ewma_asymmetric(level_prev, q)

        # Update EWMA deviation (typical absolute deviation) with asymmetric behavior
        # Same logic: quick to detect high variability, slow to recover (prevents noise)
        # Example: deviation jumps 10->30->20, EWMA follows 10->25->23 (fast up, slow down)
        dev_sample = abs(q - level)
        dev = self._update_ewma_asymmetric(dev_prev, dev_sample)

        self._q_level_nbytes[op] = level
        self._q_level_dev[op] = dev

        # Step 2: base threshold from level & dev
        # Example: level=1000, dev=200, K_DEV=4.0 -> base = 1000 + 4*200 = 1800
        base = level + self.K_DEV * dev

        # Step 3: fast ramp-up
        threshold = max(1, int(max(base, q)))

        # Step 4: cache & return
        prev_threshold = self._queue_thresholds[op]

        # Bootstrap
        if prev_threshold == 0:
            self._queue_thresholds[op] = max(1, threshold)
            return self._queue_thresholds[op]

        # Only increase threshold when there's clear pressure
        if threshold > prev_threshold:
            self._queue_thresholds[op] = max(1, threshold)
            return self._queue_thresholds[op]

        # Keep existing threshold when pressure decreases
        return prev_threshold

    def _effective_cap(self, op: "PhysicalOperator") -> int:
        """Compute a reduced concurrency cap via a tiny {-1,0,+1,+2} controller.

        Pressure and trend signals:
          - pressure_signal: How far current queue is above threshold (normalized by absolute deviation estimate)
            Formula: (current_queue_size_bytes - threshold) / max(dev, 1)

            Examples:
              * queue=200, threshold=150, dev=20 -> pressure = (200-150)/20 = +2.5
                Meaning: Queue is 2.5x absolute deviation estimate level above threshold (high pressure, throttle!)
              * queue=100, threshold=150, dev=20 -> pressure = (100-150)/20 = -2.5
                Meaning: Queue is 2.5x absolute deviation estimate level below threshold (low pressure, safe)
              * queue=150, threshold=150, dev=20 -> pressure = (150-150)/20 = 0.0
                Meaning: Queue exactly at threshold (neutral pressure)

          - trend_signal: Whether queue is growing or shrinking (normalized by absolute deviation estimate)
            Formula: (avg(recent_window) - avg(older_window)) / max(dev, 1)

            Examples:
              * recent_avg=180, older_avg=160, dev=20 -> trend = (180-160)/20 = +1.0
                Meaning: Queue growing at 1x absolute deviation estimate level (upward trend, getting worse)
              * recent_avg=140, older_avg=160, dev=20 -> trend = (140-160)/20 = -1.0
                Meaning: Queue shrinking at 1x absolute deviation estimate level (downward trend, getting better)
              * recent_avg=160, older_avg=160, dev=20 -> trend = (160-160)/20 = 0.0
                Meaning: Queue stable (no trend)

        Controller decision logic:
          - Decides concurrency adjustment {-1,0,+1,+2} based on pressure and trend signals

          Decision rules table:
            +----------+----------+----------+--------------------------------+------------------+
            | Pressure | Trend    | Step     | Action                        | Example          |
            +----------+----------+----------+--------------------------------+------------------+
            | >= +2.0  | >= +1.0  | -1       | Emergency backoff              | +2.5, +1.0 -> -1 |
            |          |          |          | (immediate reduction to        |                  |
            |          |          |          |  prevent overload)             |                  |
            +----------+----------+----------+--------------------------------+------------------+
            | >= +1.0  | >= 0.0   | 0        | Wait and see                   | +1.5, +0.5 -> 0  |
            |          |          |          | (let current level stabilize)  |                  |
            +----------+----------+----------+--------------------------------+------------------+
            | <= -1.0  | <= -1.0  | +1       | Conservative growth            | -1.5, -1.0 -> +1 |
            |          |          |          | (safe to increase when         |                  |
            |          |          |          |  improving)                    |                  |
            +----------+----------+----------+--------------------------------+------------------+
            | <= -2.0  | <= -2.0  | +2       | Aggressive growth              | -2.5, -2.0 -> +2 |
            |          |          |          | (underutilized and improving   |                  |
            |          |          |          |  rapidly)                      |                  |
            +----------+----------+----------+--------------------------------+------------------+
            | Other    | Other    | 0        | Hold                           | +0.5, -0.5 -> 0  |
            |          |          |          | (moderate signals, no clear    |                  |
            |          |          |          |  direction)                    |                  |
            +----------+----------+----------+--------------------------------+------------------+

            Logic summary:
            - High pressure + growing trend = emergency backoff
            - High pressure + stable trend = wait and see
            - Low pressure + shrinking trend = safe to grow
            - Very low pressure + strong improvement = aggressive growth
            - Moderate signals = maintain current concurrency

        Args:
            op: Operator whose effective cap we compute.

        Returns:
            An integer cap in [1, configured_cap].
        """
        hist = self._queue_history[op]
        running = op.metrics.num_tasks_running

        # Need enough samples to evaluate short trend (recent + older windows).
        recent_window = self.HISTORY_LEN // 2
        older_window = self.HISTORY_LEN // 2
        min_samples = recent_window + older_window

        if len(hist) < min_samples:
            return max(1, running)

        # Trend windows and normalized signals
        h = list(hist)
        recent_avg = sum(h[-recent_window:]) / float(recent_window)
        older_avg = sum(h[-(recent_window + older_window) : -recent_window]) / float(
            older_window
        )
        dev = max(1.0, self._q_level_dev[op])
        threshold = float(max(1, self._queue_thresholds[op]))
        current_queue_size_bytes = float(hist[-1])

        # Calculate normalized pressure and trend signals
        scale = max(1.0, float(dev))
        pressure_signal = (current_queue_size_bytes - threshold) / scale
        trend_signal = (recent_avg - older_avg) / scale

        # Quantized controller decision
        step = self._quantized_controller_step(pressure_signal, trend_signal)

        # Apply step to current running concurrency, clamp by configured cap.
        target = max(1, running + step)
        cap_cfg = self._concurrency_caps[op]
        if not math.isinf(cap_cfg):
            target = min(target, int(cap_cfg))
        return target

    def _quantized_controller_step(
        self, pressure_signal: float, trend_signal: float
    ) -> int:
        """Compute the quantized controller step based on pressure and trend signals.

        This method implements the decision logic for the quantized controller:
        - High pressure + growing trend = emergency backoff (-1)
        - High pressure + stable/declining trend = wait and see (0)
        - Low pressure + declining trend = safe to grow (+1)
        - Very low pressure + strong improvement = aggressive growth (+2)
        - Moderate signals = maintain current concurrency (0)

        Args:
            pressure_signal: Normalized pressure signal (queue vs threshold)
            trend_signal: Normalized trend signal (recent vs older average)

        Returns:
            Step adjustment: -1, 0, +1, or +2
        """
        if pressure_signal >= 2.0 and trend_signal >= 1.0:
            return -1
        elif pressure_signal >= 1.0 and trend_signal >= 0.0:
            return 0
        elif pressure_signal <= -2.0 and trend_signal <= -2.0:
            return +2
        elif pressure_signal <= -1.0 and trend_signal <= -1.0:
            return +1
        else:
            return 0
