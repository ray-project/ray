import logging
import time
import threading
from typing import NamedTuple, Tuple, Optional

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.data._internal.execution.interfaces import ExecutionResources

logger = logging.getLogger(__name__)


class NodeCounts(NamedTuple):
    """The number of alive worker nodes, by whether they have a GPU."""

    cpu: int
    gpu: int


def _count_worker_nodes() -> NodeCounts:
    """Count the alive worker nodes, excluding the head node.

    A node counts as a GPU node if it has any GPU resource.
    """
    cpu_nodes = 0
    gpu_nodes = 0
    for node in ray.nodes():
        if not node.get("Alive", False):
            continue
        resources = node.get("Resources", {})
        if HEAD_NODE_RESOURCE_NAME in resources:
            continue
        if resources.get("GPU", 0) > 0:
            gpu_nodes += 1
        else:
            cpu_nodes += 1
    return NodeCounts(cpu=cpu_nodes, gpu=gpu_nodes)


class ClusterResourceMonitor:
    """Monitor and validate cluster resources during benchmark execution.

    This can be used to validate that the autoscaler behaves well.
    """

    def __init__(self):
        if not ray.is_initialized():
            raise RuntimeError("You must start Ray before using this monitor")

        self._background_thread: Optional[threading.Thread] = None
        self._stop_background_thread_event: Optional[threading.Event] = None

        self._peak_cpu_count: float = 0
        self._peak_gpu_count: float = 0
        self._peak_node_counts = NodeCounts(cpu=0, gpu=0)

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

    def get_peak_node_counts(self) -> NodeCounts:
        """Get the peak number of alive worker nodes, excluding the head node."""
        return self._peak_node_counts

    def _start_background_thread(
        self, interval_s: float = 5.0
    ) -> Tuple[threading.Thread, threading.Event]:
        stop_event = threading.Event()

        def monitor_cluster_resources():
            while not stop_event.is_set():
                # These query the GCS, so a transient failure shouldn't kill the
                # thread and leave the peaks frozen for the rest of the run.
                try:
                    resources = ray.cluster_resources()
                    self._peak_cpu_count = max(
                        self._peak_cpu_count, resources.get("CPU", 0)
                    )
                    self._peak_gpu_count = max(
                        self._peak_gpu_count, resources.get("GPU", 0)
                    )

                    node_counts = _count_worker_nodes()
                    self._peak_node_counts = NodeCounts(
                        cpu=max(self._peak_node_counts.cpu, node_counts.cpu),
                        gpu=max(self._peak_node_counts.gpu, node_counts.gpu),
                    )
                except Exception:
                    logger.warning("Failed to sample cluster state.", exc_info=True)

                time.sleep(interval_s)

        thread = threading.Thread(target=monitor_cluster_resources, daemon=True)
        thread.start()

        return thread, stop_event

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._background_thread is not None:
            self._stop_background_thread_event.set()
            self._background_thread.join()
