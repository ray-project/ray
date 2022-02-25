import ray.worker
import logging
from ray._private.client_mode_hook import client_mode_hook
from ray._private.runtime_env.validation import ParsedRuntimeEnv
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    def get(self):
        """Get a dictionary of the current context.

        Returns:
            dict: Dictionary of the current context.
        """
        context = {
            "job_id": self.job_id,
            "node_id": self.node_id,
            "namespace": self.namespace,
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
        assert (
            self.worker.mode == ray.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"
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
        assert (
            self.worker.mode == ray.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"
        actor_id = self.worker.actor_id
        return actor_id if not actor_id.is_nil() else None

    @property
    def namespace(self):
        return self.worker.namespace

    @property
    def was_current_actor_reconstructed(self):
        """Check whether this actor has been restarted

        Returns:
            Whether this actor has been ever restarted.
        """
        assert (
            not self.actor_id.is_nil()
        ), "This method should't be called inside Ray tasks."
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

    def get_runtime_env_string(self):
        """Get the runtime env string used for the current driver or worker.

        Returns:
            The runtime env string currently using by this worker.
        """
        return self.worker.runtime_env

    @property
    def runtime_env(self):
        """Get the runtime env dict used for the current driver or worker.

        Returns:
            The runtime env dict currently using by this worker.
        """

        return ParsedRuntimeEnv.deserialize(self.get_runtime_env_string())

    @property
    def current_actor(self):
        """Get the current actor handle of this actor itsself.

        Returns:
            The handle of current actor.
        """
        if self.actor_id is None:
            raise RuntimeError("This method is only available in an actor.")
        worker = self.worker
        worker.check_connected()
        return worker.core_worker.get_actor_handle(self.actor_id)

    def _get_actor_call_stats(self):
        """Get the current worker's task counters.

        Returns:
            A dictionary keyed by the function name. The values are
            dictionaries with form ``{"pending": 0, "running": 1,
            "finished": 2}``.
        """
        worker = self.worker
        worker.check_connected()
        return worker.core_worker.get_actor_call_stats()


_runtime_context = None


@PublicAPI(stability="beta")
@client_mode_hook(auto_init=False)
def get_runtime_context():
    """Get the runtime context of the current driver/worker.

    Example:

    >>> ray.get_runtime_context().job_id # Get the job id.
    >>> ray.get_runtime_context().get() # Get all the metadata.
    """
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray.worker.global_worker)

    return _runtime_context
