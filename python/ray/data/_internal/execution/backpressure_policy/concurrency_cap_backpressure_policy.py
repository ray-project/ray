import logging
import math
from collections import defaultdict
from typing import TYPE_CHECKING, Dict

from python.ray._private.ray_constants import env_float

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
    based on queue pressure.

      - Maintain asymmetric EWMA of total queued bytes (this op + downstream) as the
        typical level: `level`.
      - Maintain asymmetric EWMA of absolute residual vs the *previous* level as a
        scale proxy: `dev = EWMA(|q - level_prev|)`.
      - Define deadband: [lower, upper] = [level - K_DEV*dev, level + K_DEV*dev].
      - If q > upper -> target cap = running - BACKOFF_FACTOR  (back off)
        If q < lower -> target cap = running + RAMPUP_FACTOR  (ramp up)
        Else         -> target cap = running      (hold)
      - Clamp to [1, configured_cap], admit iff running < target cap.

    NOTE: Only support setting concurrency cap for `TaskPoolMapOperator` for now.
    TODO(chengsu): Consolidate with actor scaling logic of `ActorPoolMapOperator`.
    """

    # Smoothing factor for the asymmetric EWMA (slow fall, faster rise).
    EWMA_ALPHA = env_float("RAY_DATA_CONCURRENCY_CAP_EWMA_ALPHA", 0.2)
    # Deadband width in units of the EWMA absolute deviation estimate.
    K_DEV = env_float("RAY_DATA_CONCURRENCY_CAP_K_DEV", 2.0)
    # Factor to back off when the queue is too large.
    BACKOFF_FACTOR = env_float("RAY_DATA_CONCURRENCY_CAP_BACKOFF_FACTOR", 1)
    # Factor to ramp up when the queue is too small.
    RAMPUP_FACTOR = env_float("RAY_DATA_CONCURRENCY_CAP_RAMPUP_FACTOR", 1)
    # Threshold for object store memory usage ratio to enable dynamic output queue size backpressure.
    OBJECT_STORE_USAGE_RATIO = env_float("RAY_DATA_CONCURRENCY_CAP_OBJECT_STORE_USAGE_RATIO", 0.1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Configured per-operator caps (∞ if unset).
        self._concurrency_caps: Dict["PhysicalOperator", float] = {}

        # EWMA state for level
        self._q_level_nbytes: Dict["PhysicalOperator", float] = defaultdict(float)

        # EWMA state for dev
        self._q_level_dev: Dict["PhysicalOperator", float] = defaultdict(float)

        # Per-operator cached threshold (bootstrapped from first sample).
        self._queue_thresholds: Dict["PhysicalOperator", int] = defaultdict(int)

        # Last effective cap for change logs.
        self._last_effective_caps: Dict["PhysicalOperator", int] = {}

        # Initialize caps from operators (infinite if unset)
        for op, _ in self._topology.items():
            if (
                isinstance(op, TaskPoolMapOperator)
                and op.get_max_concurrency_limit() is not None
            ):
                self._concurrency_caps[op] = op.get_max_concurrency_limit()
            else:
                self._concurrency_caps[op] = float("inf")

        # Whether to cap the concurrency of an operator based on its and downstream's queue size.
        self.enable_dynamic_output_queue_size_backpressure = (
            self._data_context.enable_dynamic_output_queue_size_backpressure
        )

        logger.debug(
            f"ConcurrencyCapBackpressurePolicy caps: {self._concurrency_caps}, "
            f"enabled: {self.enable_dynamic_output_queue_size_backpressure}",
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

    def _update_level_and_dev(self, op: "PhysicalOperator", q_bytes: int) -> None:
        """Update EWMA level and dev (residual w.r.t. previous level)."""
        q = float(q_bytes)

        level_prev = self._q_level_nbytes[op]
        dev_prev = self._q_level_dev[op]

        # Deviation vs the previous level
        dev_sample = abs(q - level_prev) if level_prev > 0 else 0.0
        dev = self._update_ewma_asymmetric(dev_prev, dev_sample)

        # Now update the level itself
        level = self._update_ewma_asymmetric(level_prev, q)

        self._q_level_nbytes[op] = level
        self._q_level_dev[op] = dev

        # For visibility, store the integer center of the band
        self._queue_thresholds[op] = max(1, int(level))

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Return whether `op` may accept another input now."""
        running = op.metrics.num_tasks_running

        # If not a MapOperator or feature disabled, just enforce configured cap.
        if (
            not isinstance(op, MapOperator)
            or not self.enable_dynamic_output_queue_size_backpressure
        ):
            return running < self._concurrency_caps[op]

        # If object store memory usage ratio is above threshold, skip dynamic output queue size backpressure.
        op_usage = self._resource_manager.get_op_usage(op)
        op_budget = self._resource_manager.get_budget(op)
        if (
            op_usage is not None
            and op_budget is not None
            and op_budget.object_store_memory > 0
            and op_usage.object_store_memory > 0
        ):
            if (
                op_budget.object_store_memory / op_usage.object_store_memory
                > self.OBJECT_STORE_USAGE_RATIO
            ):
                return running < self._concurrency_caps[op]

        # Current total queued bytes (this op + downstream)
        current_queue_size_bytes = (
            self._resource_manager.get_op_internal_object_store_usage(op)
            + self._resource_manager.get_op_outputs_object_store_usage_with_downstream(
                op
            )
        )

        # Update EWMA state (level & dev) and compute effective cap
        self._update_level_and_dev(op, current_queue_size_bytes)
        effective_cap = self._effective_cap(op, running, current_queue_size_bytes)

        last = self._last_effective_caps.get(op, None)
        if last != effective_cap:
            logger.debug(
                "Cap change %s: %s -> %s (running=%d, queue=%d, thr=%d)",
                op.name,
                last if last is not None else "None",
                effective_cap,
                running,
                current_queue_size_bytes,
                self._queue_thresholds[op],
            )
            self._last_effective_caps[op] = effective_cap

        return running < effective_cap

    def _effective_cap(
        self,
        op: "PhysicalOperator",
        running: int,
        current_queue_size_bytes: int,
    ) -> int:
        """A simple controller around EWMA level.
        Args:
            op: The operator to compute the effective cap for.
            running: The number of tasks currently running.
            current_queue_size_bytes: Current total queued bytes for this operator + downstream.
        Returns:
            The effective cap.
        """
        cap_cfg = self._concurrency_caps[op]

        level = float(self._q_level_nbytes[op])
        dev = max(1.0, float(self._q_level_dev[op]))
        upper = level + self.K_DEV * dev
        lower = level - self.K_DEV * dev

        if current_queue_size_bytes > upper:
            # back off
            target = running - self.BACKOFF_FACTOR
        elif current_queue_size_bytes < lower:
            # ramp up
            target = running + self.RAMPUP_FACTOR
        else:
            # hold
            target = running

        # Clamp to [1, configured_cap]
        target = max(1, target)
        if not math.isinf(cap_cfg):
            target = min(target, int(cap_cfg))
        return target
