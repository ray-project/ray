from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ray.runtime_context import RuntimeContext
    from ray import JobID
    from ray import NodeID


class ClientWorkerPropertyAPI:
    """Emulates the properties of the ray.worker object for the client"""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    def build_runtime_context(self) -> "RuntimeContext":
        """Creates a RuntimeContext backed by the properites of this API"""
        # Defer the import of RuntimeContext until needed to avoid cycles
        from ray.runtime_context import RuntimeContext
        return RuntimeContext(self)

    def _fetch_runtime_context(self):
        import ray.core.generated.ray_client_pb2 as ray_client_pb2
        return self.worker.get_cluster_info(
            ray_client_pb2.ClusterInfoType.RUNTIME_CONTEXT)

    @property
    def mode(self):
        from ray.worker import SCRIPT_MODE
        return SCRIPT_MODE

    @property
    def current_job_id(self) -> "JobID":
        from ray import JobID
        return JobID(self._fetch_runtime_context().job_id)

    @property
    def current_node_id(self) -> "NodeID":
        from ray import NodeID
        return NodeID(self._fetch_runtime_context().node_id)

    @property
    def should_capture_child_tasks_in_placement_group(self) -> bool:
        return self._fetch_runtime_context().capture_client_tasks
