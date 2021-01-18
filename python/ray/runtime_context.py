import ray.worker
import logging

logger = logging.getLogger(__name__)


class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    def get(self):
        """Get a dictionary of the current context.

        Fields that are not available (e.g., actor ID inside a task) won't be
        included in the field.

        Returns:
            dict: Dictionary of the current context.
        """
        context = {
            "job_id": self.job_id,
            "node_id": self.node_id,
        }
        if self.worker.mode == ray.worker.WORKER_MODE:
            if self.task_id is not None:
                context["task_id"] = self.task_id
            if self.actor_id is not None:
                context["actor_id"] = self.actor_id

        return context

    @property
    def job_id(self):
        """Get current job ID for this worker or driver.

        Job ID is the id of your Ray drivers that create tasks or actors.

        Returns:
            If called by a driver, this returns the job ID. If called in
                a task, return the job ID of the associated driver.
        """
        job_id = self.worker.current_job_id
        assert not job_id.is_nil()
        return job_id

    @property
    def node_id(self):
        """Get current node ID for this worker or driver.

        Node ID is the id of a node that your driver, task, or actor runs.

        Returns:
            a node id for this worker or driver.
        """
        node_id = self.worker.current_node_id
        assert not node_id.is_nil()
        return node_id

    @property
    def task_id(self):
        """Get current task ID for this worker or driver.

        Task ID is the id of a Ray task.
        This shouldn't be used in a driver process.

        Example:

            >>> @ray.remote
            >>> class Actor:
            >>>     def ready(self):
            >>>         return True
            >>>
            >>> @ray.remote
            >>> def f():
            >>>     return True
            >>>
            >>> # All the below code will generate different task ids.
            >>> # Task ids are available for actor creation.
            >>> a = Actor.remote()
            >>> # Task ids are available for actor tasks.
            >>> a.ready.remote()
            >>> # Task ids are available for normal tasks.
            >>> f.remote()

        Returns:
            The current worker's task id. None if there's no task id.
        """
        # only worker mode has actor_id
        assert self.worker.mode == ray.worker.WORKER_MODE, (
            f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}")
        task_id = self.worker.current_task_id
        return task_id if not task_id.is_nil() else None

    @property
    def actor_id(self):
        """Get the current actor ID in this worker.

        ID of the actor of the current process.
        This shouldn't be used in a driver process.

        Returns:
            The current actor id in this worker. None if there's no actor id.
        """
        # only worker mode has actor_id
        assert self.worker.mode == ray.worker.WORKER_MODE, (
            f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}")
        actor_id = self.worker.actor_id
        return actor_id if not actor_id.is_nil() else None

    @property
    def was_current_actor_reconstructed(self):
        """Check whether this actor has been restarted

        Returns:
            Whether this actor has been ever restarted.
        """
        assert not self.actor_id.is_nil(), (
            "This method should't be called inside Ray tasks.")
        actor_info = ray.state.actors(self.actor_id.hex())
        return actor_info and actor_info["NumRestarts"] != 0

    @property
    def current_placement_group_id(self):
        """Get the current Placement group ID of this worker.

        Returns:
            The current placement group id of this worker.
        """
        return self.worker.placement_group_id

    @property
    def should_capture_child_tasks_in_placement_group(self):
        """Get if the current task should capture parent's placement group.

        This returns True if it is called inside a driver.

        Returns:
            Return True if the current task should implicitly
                capture the parent placement group.
        """
        return self.worker.should_capture_child_tasks_in_placement_group


_runtime_context = None


def get_runtime_context():
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray.worker.global_worker)

    return _runtime_context
