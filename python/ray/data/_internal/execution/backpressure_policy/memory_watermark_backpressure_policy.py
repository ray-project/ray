import logging
import threading
import time
from typing import TYPE_CHECKING, Optional

from .backpressure_policy import BackpressurePolicy
from ray._common.utils import env_float
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


class MemoryWatermarkBackpressurePolicy(BackpressurePolicy):
    """Emergency circuit breaker based on absolute Object Store memory utilization.

    This policy monitors the cluster-wide Object Store memory usage via
    ``ray.cluster_resources()`` and ``ray.available_resources()``, independent
    of Ray Data's internal budget model (``ResourceManager``).  When the
    utilisation exceeds *HIGH_WATERMARK* the policy blocks new task submissions
    (``can_add_input`` returns ``False``).  It releases the block only after the
    utilisation drops below *LOW_WATERMARK* (hysteresis to prevent rapid cycling).

    **Key design constraints**:

    * ``max_task_output_bytes_to_read`` **always** returns ``None`` — never
      restricts output reading.  Returning ``0`` would override the liveness
      protection in ``ReservationOpResourceAllocator`` (``min(1, 0) = 0``),
      causing a deadlock.
    * GCS queries are TTL-cached (default 1 s) to bound RPC overhead.
    * State is protected by a ``threading.Lock`` because the streaming
      executor runs as a daemon thread.
    """

    # TTL for GCS resource queries, overridable via env var.
    RESOURCE_QUERY_INTERVAL_S: float = env_float(
        "RAY_DATA_MEMORY_WATERMARK_QUERY_INTERVAL_S", 1.0
    )

    @property
    def name(self) -> str:
        return "MemoryWatermark"

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(data_context, topology, resource_manager)

        self._high_watermark: float = data_context.memory_watermark_high
        self._low_watermark: float = data_context.memory_watermark_low

        # Validate: 0 < LOW < HIGH < 1
        if not (0.0 < self._low_watermark < self._high_watermark < 1.0):
            raise ValueError(
                f"Invalid watermark thresholds: low={self._low_watermark}, "
                f"high={self._high_watermark}. "
                f"Must satisfy 0.0 < low < high < 1.0."
            )

        self._throttled: bool = False
        self._lock = threading.Lock()
        self._cached_utilization: float = 0.0
        self._last_query_time: float = 0.0

        logger.debug(
            "MemoryWatermarkBackpressurePolicy initialized: "
            f"high={self._high_watermark}, low={self._low_watermark}, "
            f"query_interval={self.RESOURCE_QUERY_INTERVAL_S}s"
        )

    # ------------------------------------------------------------------
    # GCS query with TTL caching
    # ------------------------------------------------------------------

    def _query_utilization(self) -> float:
        """Query Object Store utilization with TTL caching.

        Returns the fraction of Object Store memory currently in use,
        clamped to [0.0, 1.0].  On error the last cached value is returned
        (conservative — NOT 0.0 which would mean "no pressure").
        """
        now = time.monotonic()
        if now - self._last_query_time < self.RESOURCE_QUERY_INTERVAL_S:
            return self._cached_utilization  # within TTL

        # Update _last_query_time *before* the RPC so that failures also
        # respect the TTL.  Without this, a persistent GCS outage would
        # cause every scheduling-cycle call (~100 ms) to retry the failing
        # RPC, adding load to an already-struggling GCS.
        self._last_query_time = now

        try:
            import ray

            cluster = ray.cluster_resources()
            available = ray.available_resources()
            total = cluster.get("object_store_memory", 0.0)
            avail = available.get("object_store_memory", 0.0)
            if total <= 0.0:
                # Ray not fully ready — be permissive.
                return self._cached_utilization

            util = max(0.0, min(1.0, 1.0 - (avail / total)))
            self._cached_utilization = util
            return util
        except Exception as e:
            logger.warning(
                "Failed to query object store memory utilization, "
                "using cached value. Error: %s",
                e,
            )
            # Return cached value — NOT 0.0.
            # 0.0 = "no pressure" = false safety; cached = conservative.
            return self._cached_utilization

    # ------------------------------------------------------------------
    # BackpressurePolicy interface
    # ------------------------------------------------------------------

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Block new task submissions when Object Store utilization is high.

        Uses hysteresis (dual HIGH/LOW watermarks) to prevent rapid on/off
        cycling.
        """
        util = self._query_utilization()
        with self._lock:
            if self._throttled:
                if util < self._low_watermark:
                    self._throttled = False
                    logger.info(
                        "MemoryWatermark backpressure released: "
                        f"utilization={util:.2%} < low_watermark="
                        f"{self._low_watermark:.2%}"
                    )
            else:
                if util >= self._high_watermark:
                    self._throttled = True
                    logger.warning(
                        "MemoryWatermark backpressure engaged: "
                        f"utilization={util:.2%} >= high_watermark="
                        f"{self._high_watermark:.2%}"
                    )
            return not self._throttled

    def max_task_output_bytes_to_read(
        self, op: "PhysicalOperator"
    ) -> Optional[int]:
        """ALWAYS returns None — never restricts output reading.

        CRITICAL: Returning 0 would override the liveness protection in
        ``ReservationOpResourceAllocator`` via ``min(1, 0) = 0``, causing a
        deadlock where output consumption is blocked, Object Store memory
        cannot be freed, and utilization stays above HIGH_WATERMARK forever.
        """
        return None
