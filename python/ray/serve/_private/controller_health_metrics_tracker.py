import asyncio
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S
from ray.serve.schema import ControllerHealthMetrics, DurationStats

# Number of recent loop iterations to track for rolling averages
_HEALTH_METRICS_HISTORY_SIZE = 100

# Larger window for ingestion samples: at high fan-in there are many more
# record_* calls than control loops, so we keep more samples to get a stable
# picture of per-call cost.
_INGEST_METRICS_HISTORY_SIZE = 2000


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

    # --- Ingestion-path instrumentation (autoscaling metrics fan-in) ---
    # Rolling history of per-call ingestion processing time (ms): the wall time
    # spent inside record_autoscaling_metrics_from_{handle,replica}, i.e. the
    # decompress + deserialize + state-write work that shares the controller's
    # single event loop with the control loop.
    handle_ingest_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_INGEST_METRICS_HISTORY_SIZE)
    )
    replica_ingest_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_INGEST_METRICS_HISTORY_SIZE)
    )
    # Decompress-only time (ms), to split transport cost from state-write cost.
    decompress_durations: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_INGEST_METRICS_HISTORY_SIZE)
    )

    # Monotonic counters since controller start.
    handle_reports_received: int = 0
    replica_reports_received: int = 0
    # Cumulative wall-seconds spent on the ingestion path since start. Divided
    # by uptime this yields the average "CPU-second per wall-second" the
    # ingestion path consumes on the single event loop (the §2.3 saturation
    # signal: approaches 1.0 as the loop is monopolized by ingestion).
    total_ingest_seconds: float = 0.0

    # Latest values (used in collect_metrics)
    last_sleep_duration_s: float = 0.0
    num_control_loops: int = 0
    last_control_loop_time: float = 0.0

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

    def record_handle_ingest(self, duration_ms: float):
        self.handle_ingest_durations.append(duration_ms)
        self.handle_reports_received += 1
        self.total_ingest_seconds += duration_ms / 1000.0

    def record_replica_ingest(self, duration_ms: float):
        self.replica_ingest_durations.append(duration_ms)
        self.replica_reports_received += 1
        self.total_ingest_seconds += duration_ms / 1000.0

    def record_decompress(self, duration_ms: float):
        self.decompress_durations.append(duration_ms)

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

        # Ingestion-path statistics
        handle_ingest_stats = DurationStats.from_values(
            list(self.handle_ingest_durations)
        )
        replica_ingest_stats = DurationStats.from_values(
            list(self.replica_ingest_durations)
        )
        decompress_stats = DurationStats.from_values(list(self.decompress_durations))
        ingest_reports_received = (
            self.handle_reports_received + self.replica_reports_received
        )
        # Average fraction of one event-loop core consumed by ingestion.
        ingest_cpu_fraction = self.total_ingest_seconds / uptime if uptime > 0 else 0.0

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
            last_control_loop_time=self.last_control_loop_time,
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
            handle_ingest_duration_ms=handle_ingest_stats,
            replica_ingest_duration_ms=replica_ingest_stats,
            metrics_decompress_duration_ms=decompress_stats,
            handle_reports_received=self.handle_reports_received,
            replica_reports_received=self.replica_reports_received,
            ingest_reports_received=ingest_reports_received,
            ingest_cpu_fraction=ingest_cpu_fraction,
            process_memory_mb=process_memory_mb,
        )
