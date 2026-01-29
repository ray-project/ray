import asyncio
import math
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional

from ray._common.pydantic_compat import BaseModel
from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S

# Number of recent loop iterations to track for rolling averages
_HEALTH_METRICS_HISTORY_SIZE = 100


class DurationStats(BaseModel):
    """Statistics for a collection of duration/latency measurements."""

    mean: float = 0.0
    std: float = 0.0
    min: float = 0.0
    max: float = 0.0

    @classmethod
    def from_values(cls, values: List[float]) -> "DurationStats":
        """Compute statistics from a list of values."""
        if not values:
            return cls()

        n = len(values)
        mean = sum(values) / n
        min_val = min(values)
        max_val = max(values)

        # Compute standard deviation
        if n > 1:
            variance = sum((x - mean) ** 2 for x in values) / n
            std = math.sqrt(variance)
        else:
            std = 0.0

        return cls(mean=mean, std=std, min=min_val, max=max_val)


class ControllerHealthMetrics(BaseModel):
    """Health metrics for the Ray Serve controller.

    These metrics help diagnose controller performance issues, especially
    as cluster size increases.
    """

    # Timestamps
    timestamp: float = 0.0  # When these metrics were collected
    controller_start_time: float = 0.0  # When the controller started
    uptime_s: float = 0.0  # Controller uptime in seconds

    # Control loop metrics
    num_control_loops: int = 0  # Total number of control loops executed
    loop_duration_s: Optional[
        DurationStats
    ] = None  # Loop duration stats (rolling window)
    loops_per_second: float = 0.0  # Control loop iterations per second

    # Sleep/scheduling metrics
    last_sleep_duration_s: float = 0.0  # Actual sleep duration of last iteration
    expected_sleep_duration_s: float = 0.0  # Expected sleep (CONTROL_LOOP_INTERVAL_S)
    event_loop_delay_s: float = 0.0  # Delay = actual - expected (positive = overloaded)

    # Event loop health
    num_asyncio_tasks: int = 0  # Number of pending asyncio tasks

    # Component update durations (rolling window stats)
    deployment_state_update_duration_s: Optional[DurationStats] = None
    application_state_update_duration_s: Optional[DurationStats] = None
    proxy_state_update_duration_s: Optional[DurationStats] = None
    node_update_duration_s: Optional[DurationStats] = None

    # Autoscaling metrics latency tracking (rolling window stats)
    # These track the delay between when metrics are generated and when they reach controller
    handle_metrics_delay_ms: Optional[DurationStats] = None
    replica_metrics_delay_ms: Optional[DurationStats] = None

    # Memory usage (in MB)
    process_memory_mb: float = 0.0


@dataclass
class ControllerHealthMetricsTracker:
    """Tracker for collecting controller health metrics over time."""

    controller_start_time: float = field(default_factory=time.time)

    # Rolling history of loop durations
    loop_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )

    # Rolling history of metrics delays
    handle_metrics_delays: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )
    replica_metrics_delays: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )

    # Rolling history of component update durations
    dsm_update_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )
    asm_update_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )
    proxy_update_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )
    node_update_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_HEALTH_METRICS_HISTORY_SIZE)
    )

    # Latest values (used in collect_metrics)
    last_sleep_duration_s: float = 0.0
    num_control_loops: int = 0

    def record_loop_duration(self, duration: float):
        self.loop_durations.append(duration)

    def record_handle_metrics_delay(self, delay_ms: float):
        self.handle_metrics_delays.append(delay_ms)

    def record_replica_metrics_delay(self, delay_ms: float):
        self.replica_metrics_delays.append(delay_ms)

    def record_dsm_update_duration(self, duration: float):
        self.dsm_update_durations.append(duration)

    def record_asm_update_duration(self, duration: float):
        self.asm_update_durations.append(duration)

    def record_proxy_update_duration(self, duration: float):
        self.proxy_update_durations.append(duration)

    def record_node_update_duration(self, duration: float):
        self.node_update_durations.append(duration)

    def collect_metrics(self) -> ControllerHealthMetrics:
        """Collect and return current health metrics."""
        now = time.time()

        # Calculate loop statistics from rolling history
        loop_duration_stats = DurationStats.from_values(list(self.loop_durations))

        # Calculate loops per second based on uptime and total loops
        uptime = now - self.controller_start_time
        loops_per_second = self.num_control_loops / uptime if uptime > 0 else 0.0

        # Calculate event loop delay (actual sleep - expected sleep)
        # Positive values indicate the event loop is overloaded
        event_loop_delay = max(
            0.0, self.last_sleep_duration_s - CONTROL_LOOP_INTERVAL_S
        )

        # Get asyncio task count
        try:
            loop = asyncio.get_event_loop()
            num_asyncio_tasks = len(asyncio.all_tasks(loop))
        except RuntimeError:
            num_asyncio_tasks = 0

        # Calculate metrics delay statistics
        handle_delay_stats = DurationStats.from_values(list(self.handle_metrics_delays))
        replica_delay_stats = DurationStats.from_values(
            list(self.replica_metrics_delays)
        )

        # Calculate component update duration statistics
        dsm_update_stats = DurationStats.from_values(list(self.dsm_update_durations))
        asm_update_stats = DurationStats.from_values(list(self.asm_update_durations))
        proxy_update_stats = DurationStats.from_values(
            list(self.proxy_update_durations)
        )
        node_update_stats = DurationStats.from_values(list(self.node_update_durations))

        # Get memory usage in MB
        # Note: ru_maxrss is in bytes on macOS but kilobytes on Linux
        # The resource module is Unix-only, so we handle Windows gracefully
        try:
            import resource

            rusage = resource.getrusage(resource.RUSAGE_SELF)
            process_memory_mb = (
                rusage.ru_maxrss / (1024 * 1024)  # Convert bytes to MB on macOS
                if sys.platform == "darwin"
                else rusage.ru_maxrss / 1024  # Convert KB to MB on Linux
            )
        except ImportError:
            # resource module not available on Windows
            process_memory_mb = 0.0

        return ControllerHealthMetrics(
            timestamp=now,
            controller_start_time=self.controller_start_time,
            uptime_s=uptime,
            num_control_loops=self.num_control_loops,
            loop_duration_s=loop_duration_stats,
            loops_per_second=loops_per_second,
            last_sleep_duration_s=self.last_sleep_duration_s,
            expected_sleep_duration_s=CONTROL_LOOP_INTERVAL_S,
            event_loop_delay_s=event_loop_delay,
            num_asyncio_tasks=num_asyncio_tasks,
            deployment_state_update_duration_s=dsm_update_stats,
            application_state_update_duration_s=asm_update_stats,
            proxy_state_update_duration_s=proxy_update_stats,
            node_update_duration_s=node_update_stats,
            handle_metrics_delay_ms=handle_delay_stats,
            replica_metrics_delay_ms=replica_delay_stats,
            process_memory_mb=process_memory_mb,
        )
