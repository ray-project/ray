"""This file defines the interface between the ray client worker
and the overall ray module API.
"""
from concurrent.futures import Future
import json
import logging

from ray.util.client.runtime_context import ClientWorkerPropertyAPI
from typing import Any, Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.actor import ActorClass
    from ray.remote_function import RemoteFunction
    from ray.util.client.common import ClientStub
    from ray.util.client.common import ClientActorHandle
    from ray.util.client.common import ClientObjectRef
    from ray.core.generated.ray_client_pb2 import DataResponse

logger = logging.getLogger(__name__)


def as_bytes(value):
    if isinstance(value, str):
        return value.encode("utf-8")
    return value


class ClientAPI:
    """The Client-side methods corresponding to the ray API. Delegates
    to the Client Worker that contains the connection to the ClientServer.
    """

    def __init__(self, worker=None):
        self.worker = worker

    def get(self, vals, *, timeout=None):
        """get is the hook stub passed on to replace `ray.get`

        Args:
            vals: [Client]ObjectRef or list of these refs to retrieve.
            timeout: Optional timeout in milliseconds
        """
        return self.worker.get(vals, timeout=timeout)

    def put(self, *args, **kwargs):
        """put is the hook stub passed on to replace `ray.put`

        Args:
            val: The value to `put`.
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        return self.worker.put(*args, **kwargs)

    def wait(self, *args, **kwargs):
        """wait is the hook stub passed on to replace `ray.wait`

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        return self.worker.wait(*args, **kwargs)

    def remote(self, *args, **kwargs):
        """remote is the hook stub passed on to replace `ray.remote`.

        This sets up remote functions or actors, as the decorator,
        but does not execute them.

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        # Delayed import to avoid a cyclic import
        from ray.util.client.common import remote_decorator

        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # This is the case where the decorator is just @ray.remote.
            return remote_decorator(options=None)(args[0])
        error_string = (
            "The @ray.remote decorator must be applied either "
            "with no arguments and no parentheses, for example "
            "'@ray.remote', or it must be applied using some of "
            "the arguments 'num_returns', 'num_cpus', 'num_gpus', "
            "'memory', 'object_store_memory', 'resources', "
            "'max_calls', or 'max_restarts', like "
            "'@ray.remote(num_returns=2, "
            'resources={"CustomResource": 1})\'.'
        )
        assert len(args) == 0 and len(kwargs) > 0, error_string
        return remote_decorator(options=kwargs)

    # TODO(mwtian): consider adding _internal_ prefix to call_remote /
    # call_release / call_retain.
    def call_remote(self, instance: "ClientStub", *args, **kwargs) -> List[Future]:
        """call_remote is called by stub objects to execute them remotely.

        This is used by stub objects in situations where they're called
        with .remote, eg, `f.remote()` or `actor_cls.remote()`.
        This allows the client stub objects to delegate execution to be
        implemented in the most effective way whether it's in the client,
        clientserver, or raylet worker.

        Args:
            instance: The Client-side stub reference to a remote object
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        return self.worker.call_remote(instance, *args, **kwargs)

    def call_release(self, id: bytes) -> None:
        """Attempts to release an object reference.

        When client references are destructed, they release their reference,
        which can opportunistically send a notification through the datachannel
        to release the reference being held for that object on the server.

        Args:
            id: The id of the reference to release on the server side.
        """
        return self.worker.call_release(id)

    def call_retain(self, id: bytes) -> None:
        """Attempts to retain a client object reference.

        Increments the reference count on the client side, to prevent
        the client worker from attempting to release the server reference.

        Args:
            id: The id of the reference to retain on the client side.
        """
        return self.worker.call_retain(id)

    def close(self) -> None:
        """close cleans up an API connection by closing any channels or
        shutting down any servers gracefully.
        """
        return self.worker.close()

    def get_actor(
        self, name: str, namespace: Optional[str] = None
    ) -> "ClientActorHandle":
        """Returns a handle to an actor by name.

        Args:
            name: The name passed to this actor by
              Actor.options(name="name").remote()
        """
        return self.worker.get_actor(name, namespace)

    def list_named_actors(self, all_namespaces: bool = False) -> List[str]:
        """List all named actors in the system.

        Actors must have been created with Actor.options(name="name").remote().
        This works for both detached & non-detached actors.

        By default, only actors in the current namespace will be returned
        and the returned entries will simply be their name.

        If `all_namespaces` is set to True, all actors in the cluster will be
        returned regardless of namespace, and the retunred entries will be of
        the form '<namespace>/<name>'.
        """
        return self.worker.list_named_actors(all_namespaces)

    def kill(self, actor: "ClientActorHandle", *, no_restart=True):
        """kill forcibly stops an actor running in the cluster

        Args:
            no_restart: Whether this actor should be restarted if it's a
              restartable actor.
        """
        return self.worker.terminate_actor(actor, no_restart)

    def cancel(self, obj: "ClientObjectRef", *, force=False, recursive=True):
        """Cancels a task on the cluster.

        If the specified task is pending execution, it will not be executed. If
        the task is currently executing, the behavior depends on the ``force``
        flag, as per `ray.cancel()`

        Only non-actor tasks can be canceled. Canceled tasks will not be
        retried (max_retries will not be respected).

        Args:
            object_ref (ObjectRef): ObjectRef returned by the task
                that should be canceled.
            force (boolean): Whether to force-kill a running task by killing
                the worker that is running the task.
            recursive (boolean): Whether to try to cancel tasks submitted by
                the task specified.
        """
        return self.worker.terminate_task(obj, force, recursive)

    # Various metadata methods for the client that are defined in the protocol.
    def is_initialized(self) -> bool:
        """True if our client is connected, and if the server is initialized.
        Returns:
            A boolean determining if the client is connected and
            server initialized.
        """
        return self.worker.is_initialized()

    def nodes(self):
        """Get a list of the nodes in the cluster (for debugging only).

        Returns:
            Information about the Ray clients in the cluster.
        """
        # This should be imported here, otherwise, it will error doc build.
        import ray.core.generated.ray_client_pb2 as ray_client_pb2

        return self.worker.get_cluster_info(ray_client_pb2.ClusterInfoType.NODES)

    def method(self, *args, **kwargs):
        """Annotate an actor method

        Args:
            num_returns: The number of object refs that should be returned by
                invocations of this actor method.
        """

        # NOTE: So this follows the same logic as in ray/actor.py::method()
        # The reason to duplicate it here is to simplify the client mode
        # redirection logic. As the annotated method gets pickled and sent to
        # the server from the client it carries this private variable, it
        # activates the same logic on the server side; so there's no need to
        # pass anything else. It's inside the class definition that becomes an
        # actor. Similar annotations would follow the same way.
        assert len(args) == 0
        assert len(kwargs) == 1

        assert "num_returns" in kwargs or "concurrency_group" in kwargs

        def annotate_method(method):
            if "num_returns" in kwargs:
                method.__ray_num_returns__ = kwargs["num_returns"]
            if "concurrency_group" in kwargs:
                method.__ray_concurrency_group__ = kwargs["concurrency_group"]
            return method

        return annotate_method

    def cluster_resources(self):
        """Get the current total cluster resources.

        Note that this information can grow stale as nodes are added to or
        removed from the cluster.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        # This should be imported here, otherwise, it will error doc build.
        import ray.core.generated.ray_client_pb2 as ray_client_pb2

        return self.worker.get_cluster_info(
            ray_client_pb2.ClusterInfoType.CLUSTER_RESOURCES
        )

    def available_resources(self):
        """Get the current available cluster resources.

        This is different from `cluster_resources` in that this will return
        idle (available) resources rather than total resources.

        Note that this information can grow stale as tasks start and finish.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        # This should be imported here, otherwise, it will error doc build.
        import ray.core.generated.ray_client_pb2 as ray_client_pb2

        return self.worker.get_cluster_info(
            ray_client_pb2.ClusterInfoType.AVAILABLE_RESOURCES
        )

    def get_runtime_context(self):
        """Return a Ray RuntimeContext describing the state on the server

        Returns:
            A RuntimeContext wrapping a client making get_cluster_info calls.
        """
        return ClientWorkerPropertyAPI(self.worker).build_runtime_context()

    # Client process isn't assigned any GPUs.
    def get_gpu_ids(self) -> list:
        return []

    def timeline(self, filename: Optional[str] = None) -> Optional[List[Any]]:
        logger.warning(
            "Timeline will include events from other clients using this server."
        )
        # This should be imported here, otherwise, it will error doc build.
        import ray.core.generated.ray_client_pb2 as ray_client_pb2

        all_events = self.worker.get_cluster_info(
            ray_client_pb2.ClusterInfoType.TIMELINE
        )
        if filename is not None:
            with open(filename, "w") as outfile:
                json.dump(all_events, outfile)
        else:
            return all_events

    def _internal_kv_initialized(self) -> bool:
        """Hook for internal_kv._internal_kv_initialized."""
        # NOTE(edoakes): the kv is always initialized because we initialize it
        # manually in the proxier with a GCS client if Ray hasn't been
        # initialized yet.
        return True

    def _internal_kv_exists(self, key: bytes) -> bool:
        """Hook for internal_kv._internal_kv_exists."""
        return self.worker.internal_kv_exists(as_bytes(key))

    def _internal_kv_get(self, key: bytes) -> bytes:
        """Hook for internal_kv._internal_kv_get."""
        return self.worker.internal_kv_get(as_bytes(key))

    def _internal_kv_put(
        self, key: bytes, value: bytes, overwrite: bool = False
    ) -> bool:
        """Hook for internal_kv._internal_kv_put."""
        return self.worker.internal_kv_put(as_bytes(key), as_bytes(value), overwrite)

    def _internal_kv_del(self, key: bytes) -> None:
        """Hook for internal_kv._internal_kv_del."""
        return self.worker.internal_kv_del(as_bytes(key))

    def _internal_kv_list(self, prefix: bytes) -> bytes:
        """Hook for internal_kv._internal_kv_list."""
        return self.worker.internal_kv_list(as_bytes(prefix))

    def _convert_actor(self, actor: "ActorClass") -> str:
        """Register a ClientActorClass for the ActorClass and return a UUID"""
        return self.worker._convert_actor(actor)

    def _convert_function(self, func: "RemoteFunction") -> str:
        """Register a ClientRemoteFunc for the ActorClass and return a UUID"""
        return self.worker._convert_function(func)

    def _get_converted(self, key: str) -> "ClientStub":
        """Given a UUID, return the converted object"""
        return self.worker._get_converted(key)

    def _converted_key_exists(self, key: str) -> bool:
        """Check if a key UUID is present in the store of converted objects."""
        return self.worker._converted_key_exists(key)

    def __getattr__(self, key: str):
        if not key.startswith("_"):
            raise NotImplementedError(
                "Not available in Ray client: `ray.{}`. This method is only "
                "available within Ray remote functions and is not yet "
                "implemented in the client API.".format(key)
            )
        return self.__getattribute__(key)

    def _register_callback(
        self, ref: "ClientObjectRef", callback: Callable[["DataResponse"], None]
    ) -> None:
        self.worker.register_callback(ref, callback)
