import ray._raylet as raylet
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client import ray
from ray.util.client.options import validate_options
from ray._private.utils import check_oversized_function

import concurrent
from dataclasses import dataclass
import grpc
import os
import uuid
import inspect
from ray.util.inspect import is_cython, is_function_or_method
import json
import threading
from typing import Any
from typing import List
from typing import Dict
from typing import Optional
from typing import Union

# TODO: Instead of just making the max message size large, the right thing to
# do is to split up the bytes representation of serialized data into multiple
# messages and reconstruct them on either end. That said, since clients are
# drivers and really just feed initial things in and final results out, (when
# not going to S3 or similar) then a large limit will suffice for many use
# cases.
#
# Currently, this is 2GiB, the max for a signed int.
GRPC_MAX_MESSAGE_SIZE = (2 * 1024 * 1024 * 1024) - 1

# 30 seconds because ELB timeout is 60 seconds
GRPC_KEEPALIVE_TIME_MS = 1000 * 30

# Long timeout because we do not want gRPC ending a connection.
GRPC_KEEPALIVE_TIMEOUT_MS = 1000 * 600

GRPC_OPTIONS = [
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_SIZE),
    ("grpc.keepalive_time_ms", GRPC_KEEPALIVE_TIME_MS),
    ("grpc.keepalive_timeout_ms", GRPC_KEEPALIVE_TIMEOUT_MS),
    ("grpc.keepalive_permit_without_calls", 1),
    # Send an infinite number of pings
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.http2.min_ping_interval_without_data_ms",
     GRPC_KEEPALIVE_TIME_MS - 50),
    # Allow many strikes
    ("grpc.http2.max_ping_strikes", 0)
]

CLIENT_SERVER_MAX_THREADS = float(
    os.getenv("RAY_CLIENT_SERVER_MAX_THREADS", 100))

# Aliases for compatibility.
ClientObjectRef = raylet.ClientObjectRef
ClientActorRef = raylet.ClientActorRef


class ClientStub:
    pass


class ClientRemoteFunc(ClientStub):
    """A stub created on the Ray Client to represent a remote
    function that can be exectued on the cluster.

    This class is allowed to be passed around between remote functions.

    Args:
        _func: The actual function to execute remotely
        _name: The original name of the function
        _ref: The ClientObjectRef of the pickled code of the function, _func
    """

    def __init__(self, f, options=None):
        self._lock = threading.Lock()
        self._func = f
        self._name = f.__name__
        self._ref = None
        self._client_side_ref = ClientSideRefID.generate_id()
        self._options = validate_options(options)

    def __call__(self, *args, **kwargs):
        raise TypeError("Remote function cannot be called directly. "
                        f"Use {self._name}.remote method instead")

    def remote(self, *args, **kwargs):
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def options(self, **kwargs):
        return OptionWrapper(self, kwargs)

    def _remote(self, args=None, kwargs=None, **option_args):
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        return self.options(**option_args).remote(*args, **kwargs)

    def __repr__(self):
        return "ClientRemoteFunc(%s, %s)" % (self._name, self._ref)

    def _ensure_ref(self):
        with self._lock:
            if self._ref is None:
                # While calling ray.put() on our function, if
                # our function is recursive, it will attempt to
                # encode the ClientRemoteFunc -- itself -- and
                # infinitely recurse on _ensure_ref.
                #
                # So we set the state of the reference to be an
                # in-progress self reference value, which
                # the encoding can detect and handle correctly.
                self._ref = InProgressSentinel()
                self._ref = ray.worker._put_pickled(
                    self._pickle(), client_ref_id=self._client_side_ref.id)

    def _pickle(self):
        data = ray.worker._dumps_from_client(self._func)
        # Check pickled size before sending to server, which is more efficient
        # and can be done synchronously inside remote() calls.
        check_oversized_function(data, self._name, "function", None)
        return data

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        self._ensure_ref()
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.FUNCTION
        task.name = self._name
        task.payload_id = self._ref.id
        set_task_options(task, self._options, "baseline_options")
        return task

    def _num_returns(self) -> int:
        if not self._options:
            return 1
        return self._options.get("num_returns", 1)


class ClientActorClass(ClientStub):
    """A stub created on the Ray Client to represent an actor class.

    It is wrapped by ray.remote and can be executed on the cluster.

    Args:
        actor_cls: The actual class to execute remotely
        _name: The original name of the class
        _ref: The ClientObjectRef of the pickled `actor_cls`
    """

    def __init__(self, actor_cls, options=None):
        self.actor_cls = actor_cls
        self._lock = threading.Lock()
        self._name = actor_cls.__name__
        self._ref = None
        self._client_side_ref = ClientSideRefID.generate_id()
        self._options = validate_options(options)

    def __call__(self, *args, **kwargs):
        raise TypeError("Remote actor cannot be instantiated directly. "
                        f"Use {self._name}.remote() instead")

    def _ensure_ref(self):
        with self._lock:
            if self._ref is None:
                # As before, set the state of the reference to be an
                # in-progress self reference value, which
                # the encoding can detect and handle correctly.
                self._ref = InProgressSentinel()
                self._ref = ray.worker._put_pickled(
                    self._pickle(), client_ref_id=self._client_side_ref.id)

    def _pickle(self):
        data = ray.worker._dumps_from_client(self.actor_cls)
        # Check pickled size before sending to server, which is more efficient
        # and can be done synchronously inside remote() calls.
        check_oversized_function(data, self._name, "actor", None)
        return data

    def remote(self, *args, **kwargs) -> "ClientActorHandle":
        # Actually instantiate the actor
        futures = ray.call_remote(self, *args, **kwargs)
        assert len(futures) == 1
        return ClientActorHandle(ClientActorRef(futures[0]), actor_class=self)

    def options(self, **kwargs):
        return ActorOptionWrapper(self, kwargs)

    def _remote(self, args=None, kwargs=None, **option_args):
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        return self.options(**option_args).remote(*args, **kwargs)

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
        set_task_options(task, self._options, "baseline_options")
        return task

    @staticmethod
    def _num_returns() -> int:
        return 1


class ClientActorHandle(ClientStub):
    """Client-side stub for instantiated actor.

    A stub created on the Ray Client to represent a remote actor that
    has been started on the cluster.  This class is allowed to be passed
    around between remote functions.

    Args:
        actor_ref: A reference to the running actor given to the client. This
          is a serialized version of the actual handle as an opaque token.
    """

    def __init__(self,
                 actor_ref: ClientActorRef,
                 actor_class: Optional[ClientActorClass] = None):
        self.actor_ref = actor_ref
        self._dir: Optional[List[str]] = None
        if actor_class is not None:
            self._method_num_returns = {}
            for method_name, method_obj in inspect.getmembers(
                    actor_class.actor_cls, is_function_or_method):
                self._method_num_returns[method_name] = getattr(
                    method_obj, "__ray_num_returns__", 1)
        else:
            self._method_num_returns = None

    def __dir__(self) -> List[str]:
        if self._method_num_returns is not None:
            return self._method_num_returns.keys()
        if ray.is_connected():
            self._init_class_info()
            return self._method_num_returns.keys()
        return super().__dir__()

    # For compatibility with core worker ActorHandle._actor_id -> ActorID
    @property
    def _actor_id(self) -> ClientActorRef:
        return self.actor_ref

    def __getattr__(self, key):
        if self._method_num_returns is None:
            self._init_class_info()
        return ClientRemoteMethod(self, key,
                                  self._method_num_returns.get(key, 1))

    def __repr__(self):
        return "ClientActorHandle(%s)" % (self.actor_ref.id.hex())

    def _init_class_info(self):
        @ray.remote(num_cpus=0)
        def get_class_info(x):
            return x._ray_method_num_returns

        self._method_num_returns = ray.get(get_class_info.remote(self))


class ClientRemoteMethod(ClientStub):
    """A stub for a method on a remote actor.

    Can be annotated with execution options.

    Args:
        actor_handle: A reference to the ClientActorHandle that generated
          this method and will have this method called upon it.
        method_name: The name of this method
    """

    def __init__(self,
                 actor_handle: ClientActorHandle,
                 method_name: str,
                 num_returns: int = 1):
        self._actor_handle = actor_handle
        self._method_name = method_name
        self._method_num_returns = num_returns

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        f"of running 'object.{self._method_name}()', try "
                        f"'object.{self._method_name}.remote()'.")

    def remote(self, *args, **kwargs):
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def __repr__(self):
        return "ClientRemoteMethod(%s, %s, %s)" % (
            self._method_name, self._actor_handle, self._method_num_returns)

    def options(self, **kwargs):
        return OptionWrapper(self, kwargs)

    def _remote(self, args=None, kwargs=None, **option_args):
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        return self.options(**option_args).remote(*args, **kwargs)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.METHOD
        task.name = self._method_name
        task.payload_id = self._actor_handle.actor_ref.id
        return task

    def _num_returns(self) -> int:
        return self._method_num_returns


class OptionWrapper:
    def __init__(self, stub: ClientStub, options: Optional[Dict[str, Any]]):
        self.remote_stub = stub
        self.options = validate_options(options)

    def remote(self, *args, **kwargs):
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def __getattr__(self, key):
        return getattr(self.remote_stub, key)

    def _prepare_client_task(self):
        task = self.remote_stub._prepare_client_task()
        set_task_options(task, self.options)
        return task

    def _num_returns(self) -> int:
        if self.options:
            num = self.options.get("num_returns")
            if num is not None:
                return num
        return self.remote_stub._num_returns()


class ActorOptionWrapper(OptionWrapper):
    def remote(self, *args, **kwargs):
        futures = ray.call_remote(self, *args, **kwargs)
        assert len(futures) == 1
        actor_class = None
        if isinstance(self.remote_stub, ClientActorClass):
            actor_class = self.remote_stub
        return ClientActorHandle(
            ClientActorRef(futures[0]), actor_class=actor_class)


def set_task_options(task: ray_client_pb2.ClientTask,
                     options: Optional[Dict[str, Any]],
                     field: str = "options") -> None:
    if options is None:
        task.ClearField(field)
        return

    # If there's a non-null "placement_group" in `options`, convert the
    # placement group to a dict so that `options` can be passed to json.dumps.
    pg = options.get("placement_group", None)
    if pg and pg != "default":
        options["placement_group"] = options["placement_group"].to_dict()

    options_str = json.dumps(options)
    getattr(task, field).json_options = options_str


def return_refs(futures: List[concurrent.futures.Future]
                ) -> Union[None, ClientObjectRef, List[ClientObjectRef]]:
    if not futures:
        return None
    if len(futures) == 1:
        return ClientObjectRef(futures[0])
    return [ClientObjectRef(fut) for fut in futures]


class InProgressSentinel:
    def __repr__(self) -> str:
        return self.__class__.__name__


class ClientSideRefID:
    """An ID generated by the client for objects not yet given an ObjectRef"""

    def __init__(self, id: bytes):
        assert len(id) != 0
        self.id = id

    @staticmethod
    def generate_id() -> "ClientSideRefID":
        tid = uuid.uuid4()
        return ClientSideRefID(b"\xcc" + tid.bytes)


def remote_decorator(options: Optional[Dict[str, Any]]):
    def decorator(function_or_class) -> ClientStub:
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            return ClientRemoteFunc(function_or_class, options=options)
        elif inspect.isclass(function_or_class):
            return ClientActorClass(function_or_class, options=options)
        else:
            raise TypeError("The @ray.remote decorator must be applied to "
                            "either a function or to a class.")

    return decorator


@dataclass
class ClientServerHandle:
    """Holds the handles to the registered gRPC servicers and their server."""
    task_servicer: ray_client_pb2_grpc.RayletDriverServicer
    data_servicer: ray_client_pb2_grpc.RayletDataStreamerServicer
    logs_servicer: ray_client_pb2_grpc.RayletLogStreamerServicer
    grpc_server: grpc.Server

    # Add a hook for all the cases that previously
    # expected simply a gRPC server
    def __getattr__(self, attr):
        return getattr(self.grpc_server, attr)
