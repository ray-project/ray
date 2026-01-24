import asyncio
import resource
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from ray._common.pydantic_compat import BaseModel
from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S

# Number of recent loop iterations to track for rolling averages
_HEALTH_METRICS_HISTORY_SIZE = 100


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
    last_loop_duration_s: float = 0.0  # Duration of the last control loop iteration
    avg_loop_duration_s: float = 0.0  # Average loop duration (rolling window)
    max_loop_duration_s: float = 0.0  # Max loop duration in recent history
    min_loop_duration_s: float = 0.0  # Min loop duration in recent history
    loops_per_second: float = 0.0  # Control loop iterations per second

    # Sleep/scheduling metrics
    last_sleep_duration_s: float = 0.0  # Actual sleep duration of last iteration
    expected_sleep_duration_s: float = 0.0  # Expected sleep (CONTROL_LOOP_INTERVAL_S)
    event_loop_delay_s: float = 0.0  # Delay = actual - expected (positive = overloaded)

    # Event loop health
    num_asyncio_tasks: int = 0  # Number of pending asyncio tasks

    # Component update durations (from last iteration)
    deployment_state_update_duration_s: float = 0.0
    application_state_update_duration_s: float = 0.0
    proxy_state_update_duration_s: float = 0.0
    node_update_duration_s: float = 0.0

    # Autoscaling metrics latency tracking
    # These track the delay between when metrics are generated and when they reach controller
    last_handle_metrics_delay_ms: float = 0.0
    last_replica_metrics_delay_ms: float = 0.0
    avg_handle_metrics_delay_ms: float = 0.0
    avg_replica_metrics_delay_ms: float = 0.0
    max_handle_metrics_delay_ms: float = 0.0
    max_replica_metrics_delay_ms: float = 0.0

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

    # Latest values
    last_loop_duration_s: float = 0.0
    last_sleep_duration_s: float = 0.0
    last_dsm_update_duration_s: float = 0.0
    last_asm_update_duration_s: float = 0.0
    last_proxy_update_duration_s: float = 0.0
    last_node_update_duration_s: float = 0.0
    last_handle_metrics_delay_ms: float = 0.0
    last_replica_metrics_delay_ms: float = 0.0
    num_control_loops: int = 0

    def record_loop_duration(self, duration: float):
        self.last_loop_duration_s = duration
        self.loop_durations.append(duration)

    def record_handle_metrics_delay(self, delay_ms: float):
        self.last_handle_metrics_delay_ms = delay_ms
        self.handle_metrics_delays.append(delay_ms)

    def record_replica_metrics_delay(self, delay_ms: float):
        self.last_replica_metrics_delay_ms = delay_ms
        self.replica_metrics_delays.append(delay_ms)

    def collect_metrics(self) -> ControllerHealthMetrics:
        """Collect and return current health metrics."""
        now = time.time()

        # Calculate loop statistics from rolling history
        loop_durations = list(self.loop_durations)
        avg_loop_duration = (
            sum(loop_durations) / len(loop_durations) if loop_durations else 0.0
        )
        max_loop_duration = max(loop_durations) if loop_durations else 0.0
        min_loop_duration = min(loop_durations) if loop_durations else 0.0

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
        handle_delays = list(self.handle_metrics_delays)
        replica_delays = list(self.replica_metrics_delays)

        avg_handle_delay = (
            sum(handle_delays) / len(handle_delays) if handle_delays else 0.0
        )
        avg_replica_delay = (
            sum(replica_delays) / len(replica_delays) if replica_delays else 0.0
        )
        max_handle_delay = max(handle_delays) if handle_delays else 0.0
        max_replica_delay = max(replica_delays) if replica_delays else 0.0

        # Get memory usage in MB
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        process_memory_mb = rusage.ru_maxrss / 1024  # Convert KB to MB on Linux

        return ControllerHealthMetrics(
            timestamp=now,
            controller_start_time=self.controller_start_time,
            uptime_s=uptime,
            num_control_loops=self.num_control_loops,
            last_loop_duration_s=self.last_loop_duration_s,
            avg_loop_duration_s=avg_loop_duration,
            max_loop_duration_s=max_loop_duration,
            min_loop_duration_s=min_loop_duration,
            loops_per_second=loops_per_second,
            last_sleep_duration_s=self.last_sleep_duration_s,
            expected_sleep_duration_s=CONTROL_LOOP_INTERVAL_S,
            event_loop_delay_s=event_loop_delay,
            num_asyncio_tasks=num_asyncio_tasks,
            deployment_state_update_duration_s=self.last_dsm_update_duration_s,
            application_state_update_duration_s=self.last_asm_update_duration_s,
            proxy_state_update_duration_s=self.last_proxy_update_duration_s,
            node_update_duration_s=self.last_node_update_duration_s,
            last_handle_metrics_delay_ms=self.last_handle_metrics_delay_ms,
            last_replica_metrics_delay_ms=self.last_replica_metrics_delay_ms,
            avg_handle_metrics_delay_ms=avg_handle_delay,
            avg_replica_metrics_delay_ms=avg_replica_delay,
            max_handle_metrics_delay_ms=max_handle_delay,
            max_replica_metrics_delay_ms=max_replica_delay,
            process_memory_mb=process_memory_mb,
        )
