import logging
from typing import Any, Dict, Optional

import ray._private.worker
from ray._private.client_mode_hook import client_mode_hook
from ray.runtime_env import RuntimeEnv
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    def get(self) -> Dict[str, Any]:
        """Get a dictionary of the current context.

        Returns:
            dict: Dictionary of the current context.
        """
        context = {
            "job_id": self.job_id,
            "node_id": self.node_id,
            "namespace": self.namespace,
        }
        if self.worker.mode == ray._private.worker.WORKER_MODE:
            if self.task_id is not None:
                context["task_id"] = self.task_id
            if self.actor_id is not None:
                context["actor_id"] = self.actor_id

        return context

    @Deprecated(message="Use get_job_id() instead")
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

    def get_job_id(self) -> str:
        """Get current job ID for this worker or driver.

        Job ID is the id of your Ray drivers that create tasks or actors.

        Returns:
            If called by a driver, this returns the job ID. If called in
                a task, return the job ID of the associated driver. The
                job ID will be hex format.
        """
        job_id = self.worker.current_job_id
        assert not job_id.is_nil()
        return job_id.hex()

    @Deprecated(message="Use get_node_id() instead")
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

    def get_node_id(self) -> str:
        """Get current node ID for this worker or driver.

        Node ID is the id of a node that your driver, task, or actor runs.
        The ID will be in hex format.

        Returns:
            A node id in hex format for this worker or driver.
        """
        node_id = self.worker.current_node_id
        assert not node_id.is_nil()
        return node_id.hex()

    @Deprecated(message="Use get_task_id() instead")
    @property
    def task_id(self):
        """Get current task ID for this worker or driver.

        Task ID is the id of a Ray task.
        This shouldn't be used in a driver process.

        Example:

            >>> import ray
            >>> @ray.remote
            ... class Actor:
            ...     def ready(self):
            ...         return True
            >>>
            >>> @ray.remote # doctest: +SKIP
            ... def f():
            ...     return True
            >>> # All the below code will generate different task ids.
            >>> # Task ids are available for actor creation.
            >>> a = Actor.remote() # doctest: +SKIP
            >>> # Task ids are available for actor tasks.
            >>> a.ready.remote() # doctest: +SKIP
            >>> # Task ids are available for normal tasks.
            >>> f.remote() # doctest: +SKIP

        Returns:
            The current worker's task id. None if there's no task id.
        """
        # only worker mode has actor_id
        assert (
            self.worker.mode == ray._private.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"
        task_id = self.worker.current_task_id
        return task_id if not task_id.is_nil() else None

    def get_task_id(self) -> Optional[str]:
        """Get current task ID for this worker or driver.

        Task ID is the id of a Ray task. The ID will be in hex format.
        This shouldn't be used in a driver process.

        Example:

            >>> import ray
            >>> @ray.remote
            ... class Actor:
            ...     def ready(self):
            ...         return True
            >>>
            >>> @ray.remote # doctest: +SKIP
            ... def f():
            ...     return True
            >>> # All the below code will generate different task ids.
            >>> # Task ids are available for actor creation.
            >>> a = Actor.remote() # doctest: +SKIP
            >>> # Task ids are available for actor tasks.
            >>> a.ready.remote() # doctest: +SKIP
            >>> # Task ids are available for normal tasks.
            >>> f.remote() # doctest: +SKIP

        Returns:
            The current worker's task id in hex. None if there's no task id.
        """
        # only worker mode has actor_id
        if self.worker.mode != ray._private.worker.WORKER_MODE:
            logger.warning(
                "This method is only available when the process is a "
                f"worker. Current mode: {self.worker.mode}"
            )
            return None
        task_id = self.worker.current_task_id
        return task_id.hex() if not task_id.is_nil() else None

    @Deprecated(message="Use get_actor_id() instead")
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
            self.worker.mode == ray._private.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"
        actor_id = self.worker.actor_id
        return actor_id if not actor_id.is_nil() else None

    def get_actor_id(self) -> Optional[str]:
        """Get the current actor ID in this worker.

        ID of the actor of the current process.
        This shouldn't be used in a driver process.
        The ID will be in hex format.

        Returns:
            The current actor id in hex format in this worker. None if there's no
            actor id.
        """
        # only worker mode has actor_id
        if self.worker.mode != ray._private.worker.WORKER_MODE:
            logger.warning(
                "This method is only available when the process is a "
                "worker. Current mode: {self.worker.mode}"
            )
            return None
        actor_id = self.worker.actor_id
        return actor_id.hex() if not actor_id.is_nil() else None

    @property
    def namespace(self):
        """Get the current namespace of this worker.

        Returns:
            The current namespace of this worker.
        """
        return self.worker.namespace

    @property
    def was_current_actor_reconstructed(self):
        """Check whether this actor has been restarted.

        Returns:
            Whether this actor has been ever restarted.
        """
        assert (
            not self.actor_id.is_nil()
        ), "This method should't be called inside Ray tasks."
        actor_info = ray._private.state.actors(self.actor_id.hex())
        return actor_info and actor_info["NumRestarts"] != 0

    @Deprecated(message="Use get_placement_group_id() instead")
    @property
    def current_placement_group_id(self):
        """Get the current Placement group ID of this worker.

        Returns:
            The current placement group id of this worker.
        """
        return self.worker.placement_group_id

    def get_placement_group_id(self) -> Optional[str]:
        """Get the current Placement group ID of this worker.

        Returns:
            The current placement group id in hex format of this worker.
        """
        pg_id = self.worker.placement_group_id
        return pg_id.hex() if not pg_id.is_nil() else None

    @property
    def should_capture_child_tasks_in_placement_group(self):
        """Get if the current task should capture parent's placement group.

        This returns True if it is called inside a driver.

        Returns:
            Return True if the current task should implicitly
                capture the parent placement group.
        """
        return self.worker.should_capture_child_tasks_in_placement_group

    def get_assigned_resources(self):
        """Get the assigned resources to this worker.

        By default for tasks, this will return {"CPU": 1}.
        By default for actors, this will return {}. This is because
        actors do not have CPUs assigned to them by default.

        Returns:
            A dictionary mapping the name of a resource to a float, where
            the float represents the amount of that resource reserved
            for this worker.
        """
        assert (
            self.worker.mode == ray._private.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"
        self.worker.check_connected()
        resource_id_map = self.worker.core_worker.resource_ids()
        resource_map = {
            res: sum(amt for _, amt in mapping)
            for res, mapping in resource_id_map.items()
        }
        return resource_map

    def get_runtime_env_string(self):
        """Get the runtime env string used for the current driver or worker.

        Returns:
            The runtime env string currently using by this worker.
        """
        return self.worker.runtime_env

    @property
    def runtime_env(self):
        """Get the runtime env used for the current driver or worker.

        Returns:
            The runtime env currently using by this worker. The type of
                return value is ray.runtime_env.RuntimeEnv.
        """

        return RuntimeEnv.deserialize(self.get_runtime_env_string())

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

    @property
    def gcs_address(self):
        """Get the GCS address of the ray cluster.
        Returns:
            The GCS address of the cluster.
        """
        self.worker.check_connected()
        return self.worker.gcs_client.address

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


@PublicAPI
@client_mode_hook(auto_init=False)
def get_runtime_context():
    """Get the runtime context of the current driver/worker.

    Example:

        >>> import ray
        >>> # Get the job id.
        >>> ray.get_runtime_context().job_id # doctest: +SKIP
        >>> # Get all the metadata.
        >>> ray.get_runtime_context().get() # doctest: +SKIP

    """
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray._private.worker.global_worker)

    return _runtime_context
