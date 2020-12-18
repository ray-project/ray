import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray.experimental.client import ray

import json
from typing import Any
from typing import Dict
from typing import Optional


class ClientBaseRef:
    def __init__(self, id: bytes):
        self.id: bytes = id
        ray.call_retain(id)

    def binary(self):
        return self.id

    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self):
        return "%s(%s)" % (
            type(self).__name__,
            self.id.hex(),
        )

    def __hash__(self):
        return hash(self.id)

    def __del__(self):
        if ray.is_connected():
            ray.call_release(self.id)


class ClientObjectRef(ClientBaseRef):
    pass


class ClientActorRef(ClientBaseRef):
    pass


class ClientStub:
    pass


class ClientRemoteFunc(ClientStub):
    """
    A stub created on the Ray Client to represent a remote
    function that can be exectued on the cluster.

    This class is allowed to be passed around between remote functions.

    Args:
        _func: The actual function to execute remotely
        _name: The original name of the function
        _ref: The ClientObjectRef of the pickled code of the function, _func
    """

    def __init__(self, f, options = None):
        self._func = f
        self._name = f.__name__
        self._ref = None
        self.options = options

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote function cannot be called directly. "
                        "Use {self._name}.remote method instead")

    def remote(self, *args, **kwargs):
        return ClientObjectRef(ray.call_remote(self, *args, **kwargs))

    def __repr__(self):
        return "ClientRemoteFunc(%s, %s)" % (self._name, self._ref)

    def _ensure_ref(self):
        if self._ref is None:
            # While calling ray.put() on our function, if
            # our function is recursive, it will attempt to
            # encode the ClientRemoteFunc -- itself -- and
            # infinitely recurse on _ensure_ref.
            #
            # So we set the state of the reference to be an
            # in-progress self reference value, which
            # the encoding can detect and handle correctly.
            self._ref = SelfReferenceSentinel()
            self._ref = ray.put(self._func)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        self._ensure_ref()
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.FUNCTION
        task.name = self._name
        task.payload_id = self._ref.id
        return task


class ClientActorClass(ClientStub):
    """ A stub created on the Ray Client to represent an actor class.

    It is wrapped by ray.remote and can be executed on the cluster.

    Args:
        actor_cls: The actual class to execute remotely
        _name: The original name of the class
        _ref: The ClientObjectRef of the pickled `actor_cls`
    """

    def __init__(self, actor_cls, options=None):
        self.actor_cls = actor_cls
        self._name = actor_cls.__name__
        self._ref = None
        self.options = options

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote actor cannot be instantiated directly. "
                        "Use {self._name}.remote() instead")

    def _ensure_ref(self):
        if self._ref is None:
            # As before, set the state of the reference to be an
            # in-progress self reference value, which
            # the encoding can detect and handle correctly.
            self._ref = SelfReferenceSentinel()
            self._ref = ray.put(self.actor_cls)

    def remote(self, *args, **kwargs) -> "ClientActorHandle":
        # Actually instantiate the actor
        ref_id = ray.call_remote(self, *args, **kwargs)
        return ClientActorHandle(ClientActorRef(ref_id), self)

    def __repr__(self):
        return "ClientActorClass(%s, %s)" % (self._name, self._ref)

    def __getattr__(self, key):
        if key not in self.__dict__:
            raise AttributeError("Not a class attribute")
        raise NotImplementedError("static methods")

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        self._ensure_ref()
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.ACTOR
        task.name = self._name
        task.payload_id = self._ref.id
        return task


class ClientActorHandle(ClientStub):
    """Client-side stub for instantiated actor.

    A stub created on the Ray Client to represent a remote actor that
    has been started on the cluster.  This class is allowed to be passed
    around between remote functions.

    Args:
        actor_ref: A reference to the running actor given to the client. This
          is a serialized version of the actual handle as an opaque token.
        actor_class: A reference to the ClientActorClass that this actor was
          instantiated from.
        _real_actor_handle: Cached copy of the Raylet-side
          ray.actor.ActorHandle contained in the actor_id ref.
    """

    def __init__(self, actor_ref: ClientActorRef,
                 actor_class: ClientActorClass):
        self.actor_ref = actor_ref

    def __del__(self) -> None:
        ray.call_release(self.actor_ref.id)

    @property
    def _actor_id(self):
        return self.actor_ref.id

    def __getattr__(self, key):
        return ClientRemoteMethod(self, key)

    def __repr__(self):
        return "ClientActorHandle(%s)" % (self.actor_ref.id.hex())


class ClientRemoteMethod(ClientStub):
    """A stub for a method on a remote actor.

    Can be annotated with exection options.

    Args:
        actor_handle: A reference to the ClientActorHandle that generated
          this method and will have this method called upon it.
        method_name: The name of this method
    """

    def __init__(self, actor_handle: ClientActorHandle, method_name: str, options = None):
        self.actor_handle = actor_handle
        self.method_name = method_name
        self.options = options

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote method cannot be called directly. "
                        f"Use {self._name}.remote() instead")

    def remote(self, *args, **kwargs):
        return ClientObjectRef(ray.call_remote(self, *args, **kwargs))

    def __repr__(self):
        return "ClientRemoteMethod(%s, %s)" % (self.method_name,
                                               self.actor_handle)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.METHOD
        task.name = self.method_name
        task.payload_id = self.actor_handle.actor_ref.id
        return task


_valid_options = [
    "num_cpus",
    "num_gpus",
    "resources",
    "accelerator_type",
    "max_calls",
    "max_restarts",
    "max_task_retries",
    "max_retries",
    "max_concurrency",
    "name",
    "lifetime",
    "memory",
    "object_store_memory",
    "placement_group",
    "placement_group_bundle_index",
    "placement_group_capture_child_tasks",
    "override_environment_variables",
]


def validate_options(kwargs_dict: Dict[str, Any], strict: bool = True) -> Dict[str, Any]:
    out = {}
    for k, v in kwargs_dict.items():
        if k not in _valid_options:
            if strict:
                raise TypeError(f"Invalid option passed to remote(): {k}")
            else:
                continue
        out[k] = v
    return out


class OptionWrapper:
    def __init__(self, stub: ClientStub, options: Dict[str, Any]):
        self.remote_stub = stub
        self.options = validate_options(options)

    def __getattr__(self, key):
        return getattr(self.remote_stub, key)

    def _prepare_client_task(self):
        task = self.remote_stub._prepare_client_task()
        set_task_options(task, self.options)
        return task


def set_task_options(
    task: ray_client_pb2.ClientTask,
    options: Optional[Dict[str, Any]]) -> None:
    if options is None:
        task.ClearField("options")
        return
    options_str = json.dumps(options)
    task.options = options_str

class DataEncodingSentinel:
    def __repr__(self) -> str:
        return self.__class__.__name__


class SelfReferenceSentinel(DataEncodingSentinel):
    pass
