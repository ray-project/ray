import logging
from typing import Any, Dict, List, Optional

import ray._private.worker
from ray._private import ray_constants
from ray._private.client_mode_hook import client_mode_hook
from ray._private.utils import pasre_pg_formatted_resources_to_original
from ray._raylet import TaskID
from ray.runtime_env import RuntimeEnv
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    @Deprecated(
        message="Use get_xxx_id() methods to get relevant ids instead", warning=True
    )
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

    @property
    @Deprecated(message="Use get_job_id() instead", warning=True)
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

        Raises:
            AssertionError: If not called in a driver or worker. Generally,
                this means that ray.init() was not called.
        """
        assert ray.is_initialized(), (
            "Job ID is not available because " "Ray has not been initialized."
        )
        job_id = self.worker.current_job_id
        return job_id.hex()

    @property
    @Deprecated(message="Use get_node_id() instead", warning=True)
    def node_id(self):
        """Get current node ID for this worker or driver.

        Node ID is the id of a node that your driver, task, or actor runs.

        Returns:
            A node id for this worker or driver.
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

        Raises:
            AssertionError: If not called in a driver or worker. Generally,
                this means that ray.init() was not called.
        """
        assert ray.is_initialized(), (
            "Node ID is not available because " "Ray has not been initialized."
        )
        node_id = self.worker.current_node_id
        return node_id.hex()

    def get_worker_id(self) -> str:
        """Get current worker ID for this worker or driver process.

        Returns:
            A worker id in hex format for this worker or driver process.
        """
        assert (
            ray.is_initialized()
        ), "Worker ID is not available because Ray has not been initialized."
        return self.worker.worker_id.hex()

    @property
    @Deprecated(message="Use get_task_id() instead", warning=True)
    def task_id(self):
        """Get current task ID for this worker or driver.

        Task ID is the id of a Ray task.
        This shouldn't be used in a driver process.

        Example:

            .. testcode::

                import ray

                @ray.remote
                class Actor:
                    def ready(self):
                        return True

                @ray.remote
                def f():
                    return True

                # All the below code generates different task ids.
                # Task ids are available for actor creation.
                a = Actor.remote()
                # Task ids are available for actor tasks.
                a.ready.remote()
                # Task ids are available for normal tasks.
                f.remote()

        Returns:
            The current worker's task id. None if there's no task id.
        """
        # only worker mode has actor_id
        assert (
            self.worker.mode == ray._private.worker.WORKER_MODE
        ), f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}"

        task_id = self._get_current_task_id()
        return task_id if not task_id.is_nil() else None

    def get_task_id(self) -> Optional[str]:
        """Get current task ID for this worker or driver.

        Task ID is the id of a Ray task. The ID will be in hex format.
        This shouldn't be used in a driver process.

        Example:

            .. testcode::

                import ray

                @ray.remote
                class Actor:
                    def get_task_id(self):
                        return ray.get_runtime_context().get_task_id()

                @ray.remote
                def get_task_id():
                    return ray.get_runtime_context().get_task_id()

                # All the below code generates different task ids.
                a = Actor.remote()
                # Task ids are available for actor tasks.
                print(ray.get(a.get_task_id.remote()))
                # Task ids are available for normal tasks.
                print(ray.get(get_task_id.remote()))

            .. testoutput::
                :options: +MOCK

                16310a0f0a45af5c2746a0e6efb235c0962896a201000000
                c2668a65bda616c1ffffffffffffffffffffffff01000000

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
        task_id = self._get_current_task_id()
        return task_id.hex() if not task_id.is_nil() else None

    def _get_current_task_id(self) -> TaskID:
        async_task_id = ray._raylet.async_task_id.get()
        if async_task_id is None:
            task_id = self.worker.current_task_id
        else:
            task_id = async_task_id
        return task_id

    @property
    @Deprecated(message="Use get_actor_id() instead", warning=True)
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

    @property
    @Deprecated(message="Use get_placement_group_id() instead", warning=True)
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
        return pasre_pg_formatted_resources_to_original(resource_map)

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
        worker = self.worker
        worker.check_connected()
        actor_id = worker.actor_id
        if actor_id.is_nil():
            raise RuntimeError("This method is only available in an actor.")

        return worker.core_worker.get_actor_handle(actor_id)

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

    def get_resource_ids(self) -> Dict[str, List[str]]:
        """
        Get the current worker's GPU, accelerator and TPU ids.

        Returns:
            A dictionary keyed by the resource name. The values are lists of ids.
            Example:
                {'GPU': ['0', '1'], 'neuron_cores': ['0', '1'], 'TPU': ['0', '1']}.
        """
        worker = self.worker
        worker.check_connected()
        ids_dict: Dict[str, List[str]] = {}
        for name in [ray_constants.GPU, ray_constants.NEURON_CORES, ray_constants.TPU]:
            resource_ids = worker.get_resource_ids_for_resource(
                name, f"^{name}_group_[0-9A-Za-z]+$"
            )
            # Convert resource_ids to strings as they can be user-configured
            # or system-generated.
            resource_ids = [str(i) for i in resource_ids]
            ids_dict[name] = resource_ids
        return ids_dict


_runtime_context = None


@PublicAPI
@client_mode_hook
def get_runtime_context() -> RuntimeContext:
    """Get the runtime context of the current driver/worker.

    The obtained runtime context can be used to get the metadata
    of the current task and actor.

    Example:

        .. testcode::

            import ray
            # Get the job id.
            ray.get_runtime_context().get_job_id()
            # Get the actor id.
            ray.get_runtime_context().get_actor_id()
            # Get the task id.
            ray.get_runtime_context().get_task_id()

    """
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray._private.worker.global_worker)

    return _runtime_context
