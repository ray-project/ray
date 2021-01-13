from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ray.runtime_context import RuntimeContext

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

    @property
    def mode(self):
        from ray.worker import SCRIPT_MODE
        return SCRIPT_MODE

    @property
    def current_job_id(self):
        pass

    @property
    def current_node_id(self):
        pass

    @property
    def current_task_id(self):
        pass

    @property
    def placement_group_id(self):
        pass

    @property
    def should_capture_child_tasks_in_placement_group(self):
        pass
