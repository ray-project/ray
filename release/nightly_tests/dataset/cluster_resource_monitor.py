import time
import ray
from ray.data._internal.execution.interfaces import ExecutionResources
from typing import Tuple
import threading


class ClusterResourceMonitor:
    """Monitor and validate cluster resources during benchmark execution.

    This class tracks the peak number of cluster resources during execution.

    This can be used to validate that the autoscaler behaves well.
    """

    def __init__(self):
        self._background_thread = None
        self._stop_background_thread_event = None

        self._peak_cpu_count = 0
        self._peak_gpu_count = 0

    def __repr__(self):
        return "ClusterResourceMonitor()"

    def __enter__(self):
        (
            self._background_thread,
            self._stop_background_thread_event,
        ) = self._start_background_thread()
        return self

    def get_peak_cluster_resources(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._peak_cpu_count, gpu=self._peak_gpu_count)

    def _start_background_thread(
        self, interval_s: float = 5.0
    ) -> Tuple[threading.Thread, threading.Event]:
        stop_event = threading.Event()

        def monitor_cluster_resources():
            while not stop_event.is_set():
                resources = ray.cluster_resources()
                self._peak_cpu_count = max(
                    self._peak_cpu_count, resources.get("CPU", 0)
                )
                self._peak_gpu_count = max(
                    self._peak_gpu_count, resources.get("GPU", 0)
                )
                time.sleep(interval_s)

        thread = threading.Thread(target=monitor_cluster_resources, daemon=True)
        thread.start()

        return thread, stop_event

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._background_thread is not None:
            self._stop_background_thread_event.set()
            self._background_thread.join()
