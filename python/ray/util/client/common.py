import inspect
import logging
import os
import pickle
import threading
import uuid
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import grpc

import ray._raylet as raylet
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray._private import ray_constants
from ray._private.inspect_util import (
    is_class_method,
    is_cython,
    is_function_or_method,
    is_static_method,
)
from ray._private.signature import extract_signature, get_signature
from ray._private.utils import check_oversized_function
from ray.util.client import ray
from ray.util.client.options import validate_options
from ray.util.common import INT32_MAX

logger = logging.getLogger(__name__)

# gRPC status codes that the client shouldn't attempt to recover from
# Resource exhausted: Server is low on resources, or has hit the max number
#   of client connections
# Invalid argument: Reserved for application errors
# Not found: Set if the client is attempting to reconnect to a session that
#   does not exist
# Failed precondition: Reserverd for application errors
# Aborted: Set when an error is serialized into the details of the context,
#   signals that error should be deserialized on the client side
GRPC_UNRECOVERABLE_ERRORS = (
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.ABORTED,
)

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
    *ray_constants.GLOBAL_GRPC_OPTIONS,
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_SIZE),
    ("grpc.keepalive_time_ms", GRPC_KEEPALIVE_TIME_MS),
    ("grpc.keepalive_timeout_ms", GRPC_KEEPALIVE_TIMEOUT_MS),
    ("grpc.keepalive_permit_without_calls", 1),
    # Send an infinite number of pings
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.http2.min_ping_interval_without_data_ms", GRPC_KEEPALIVE_TIME_MS - 50),
    # Allow many strikes
    ("grpc.http2.max_ping_strikes", 0),
]

CLIENT_SERVER_MAX_THREADS = float(os.getenv("RAY_CLIENT_SERVER_MAX_THREADS", 100))

# Large objects are chunked into 5 MiB messages, ref PR #35025
OBJECT_TRANSFER_CHUNK_SIZE = 5 * 2**20

# Warn the user if the object being transferred is larger than 2 GiB
OBJECT_TRANSFER_WARNING_SIZE = 2 * 2**30


class ClientObjectRef(raylet.ObjectRef):
    def __init__(self, id: Union[bytes, Future]):
        self._mutex = threading.Lock()
        self._worker = ray.get_context().client_worker
        self._id_future = None
        if isinstance(id, bytes):
            self._set_id(id)
        elif isinstance(id, Future):
            self._id_future = id
        else:
            raise TypeError("Unexpected type for id {}".format(id))

    def __del__(self):
        if self._worker is not None and self._worker.is_connected():
            try:
                if not self.is_nil():
                    self._worker.call_release(self.id)
            except Exception:
                logger.info(
                    "Exception in ObjectRef is ignored in destructor. "
                    "To receive this exception in application code, call "
                    "a method on the actor reference before its destructor "
                    "is run."
                )

    def binary(self):
        self._wait_for_id()
        return super().binary()

    def hex(self):
        self._wait_for_id()
        return super().hex()

    def is_nil(self):
        self._wait_for_id()
        return super().is_nil()

    def __hash__(self):
        self._wait_for_id()
        return hash(self.id)

    def task_id(self):
        self._wait_for_id()
        return super().task_id()

    @property
    def id(self):
        return self.binary()

    def future(self) -> Future:
        fut = Future()

        def set_future(data: Any) -> None:
            """Schedules a callback to set the exception or result
            in the Future."""

            if isinstance(data, Exception):
                fut.set_exception(data)
            else:
                fut.set_result(data)

        self._on_completed(set_future)

        # Prevent this object ref from being released.
        fut.object_ref = self
        return fut

    def _on_completed(self, py_callback: Callable[[Any], None]) -> None:
        """Register a callback that will be called after Object is ready.
        If the ObjectRef is already ready, the callback will be called soon.
        The callback should take the result as the only argument. The result
        can be an exception object in case of task error.
        """

        def deserialize_obj(
            resp: Union[ray_client_pb2.DataResponse, Exception]
        ) -> None:
            from ray.util.client.client_pickler import loads_from_server

            if isinstance(resp, Exception):
                data = resp
            elif isinstance(resp, bytearray):
                data = loads_from_server(resp)
            else:
                obj = resp.get
                data = None
                if not obj.valid:
                    data = loads_from_server(resp.get.error)
                else:
                    data = loads_from_server(resp.get.data)

            py_callback(data)

        self._worker.register_callback(self, deserialize_obj)

    def _set_id(self, id):
        super()._set_id(id)
        self._worker.call_retain(id)

    def _wait_for_id(self, timeout=None):
        if self._id_future:
            with self._mutex:
                if self._id_future:
                    self._set_id(self._id_future.result(timeout=timeout))
                    self._id_future = None


class ClientActorRef(raylet.ActorID):
    def __init__(
        self,
        id: Union[bytes, Future],
        weak_ref: Optional[bool] = False,
    ):
        self._weak_ref = weak_ref
        self._mutex = threading.Lock()
        self._worker = ray.get_context().client_worker
        if isinstance(id, bytes):
            self._set_id(id)
            self._id_future = None
        elif isinstance(id, Future):
            self._id_future = id
        else:
            raise TypeError("Unexpected type for id {}".format(id))

    def __del__(self):
        if self._weak_ref:
            return

        if self._worker is not None and self._worker.is_connected():
            try:
                if not self.is_nil():
                    self._worker.call_release(self.id)
            except Exception:
                logger.debug(
                    "Exception from actor creation is ignored in destructor. "
                    "To receive this exception in application code, call "
                    "a method on the actor reference before its destructor "
                    "is run."
                )

    def binary(self):
        self._wait_for_id()
        return super().binary()

    def hex(self):
        self._wait_for_id()
        return super().hex()

    def is_nil(self):
        self._wait_for_id()
        return super().is_nil()

    def __hash__(self):
        self._wait_for_id()
        return hash(self.id)

    @property
    def id(self):
        return self.binary()

    def _set_id(self, id):
        super()._set_id(id)
        self._worker.call_retain(id)

    def _wait_for_id(self, timeout=None):
        if self._id_future:
            with self._mutex:
                if self._id_future:
                    self._set_id(self._id_future.result(timeout=timeout))
                    self._id_future = None


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
        self._signature = get_signature(f)
        self._ref = None
        self._client_side_ref = ClientSideRefID.generate_id()
        self._options = validate_options(options)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote function cannot be called directly. "
            f"Use {self._name}.remote method instead"
        )

    def remote(self, *args, **kwargs):
        # Check if supplied parameters match the function signature. Same case
        # at the other callsites.
        self._signature.bind(*args, **kwargs)
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
                data = ray.worker._dumps_from_client(self._func)
                # Check pickled size before sending it to server, which is more
                # efficient and can be done synchronously inside remote() call.
                check_oversized_function(data, self._name, "remote function", None)
                self._ref = ray.worker._put_pickled(
                    data, client_ref_id=self._client_side_ref.id
                )

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
            return None
        return self._options.get("num_returns")


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
        self._init_signature = inspect.Signature(
            parameters=extract_signature(actor_cls.__init__, ignore_first=True)
        )
        self._ref = None
        self._client_side_ref = ClientSideRefID.generate_id()
        self._options = validate_options(options)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote actor cannot be instantiated directly. "
            f"Use {self._name}.remote() instead"
        )

    def _ensure_ref(self):
        with self._lock:
            if self._ref is None:
                # As before, set the state of the reference to be an
                # in-progress self reference value, which
                # the encoding can detect and handle correctly.
                self._ref = InProgressSentinel()
                data = ray.worker._dumps_from_client(self.actor_cls)
                # Check pickled size before sending it to server, which is more
                # efficient and can be done synchronously inside remote() call.
                check_oversized_function(data, self._name, "actor", None)
                self._ref = ray.worker._put_pickled(
                    data, client_ref_id=self._client_side_ref.id
                )

    def remote(self, *args, **kwargs) -> "ClientActorHandle":
        self._init_signature.bind(*args, **kwargs)
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

    def __init__(
        self,
        actor_ref: ClientActorRef,
        actor_class: Optional[ClientActorClass] = None,
    ):
        self.actor_ref = actor_ref
        self._dir: Optional[List[str]] = None
        if actor_class is not None:
            self._method_num_returns = {}
            self._method_signatures = {}
            for method_name, method_obj in inspect.getmembers(
                actor_class.actor_cls, is_function_or_method
            ):
                self._method_num_returns[method_name] = getattr(
                    method_obj, "__ray_num_returns__", None
                )
                self._method_signatures[method_name] = inspect.Signature(
                    parameters=extract_signature(
                        method_obj,
                        ignore_first=(
                            not (
                                is_class_method(method_obj)
                                or is_static_method(actor_class.actor_cls, method_name)
                            )
                        ),
                    )
                )
        else:
            self._method_num_returns = None
            self._method_signatures = None

    def __dir__(self) -> List[str]:
        if self._method_num_returns is not None:
            return self._method_num_returns.keys()
        if ray.is_connected():
            self._init_class_info()
            return self._method_num_returns.keys()
        return super().__dir__()

    # For compatibility with core worker ActorHandle._actor_id which returns
    # ActorID
    @property
    def _actor_id(self) -> ClientActorRef:
        return self.actor_ref

    def __hash__(self) -> int:
        return hash(self._actor_id)

    def __eq__(self, __value) -> bool:
        return hash(self) == hash(__value)

    def __getattr__(self, key):
        if key == "_method_num_returns":
            # We need to explicitly handle this value since it is used below,
            # otherwise we may end up infinitely recursing when deserializing.
            # This can happen after unpickling an object but before
            # _method_num_returns is correctly populated.
            raise AttributeError(f"ClientActorRef has no attribute '{key}'")

        if self._method_num_returns is None:
            self._init_class_info()
        if key not in self._method_signatures:
            raise AttributeError(f"ClientActorRef has no attribute '{key}'")
        return ClientRemoteMethod(
            self,
            key,
            self._method_num_returns.get(key),
            self._method_signatures.get(key),
        )

    def __repr__(self):
        return "ClientActorHandle(%s)" % (self.actor_ref.id.hex())

    def _init_class_info(self):
        # TODO: fetch Ray method decorators
        @ray.remote(num_cpus=0)
        def get_class_info(x):
            return x._ray_method_num_returns, x._ray_method_signatures

        self._method_num_returns, method_parameters = ray.get(
            get_class_info.remote(self)
        )

        self._method_signatures = {}
        for method, parameters in method_parameters.items():
            self._method_signatures[method] = inspect.Signature(parameters=parameters)


class ClientRemoteMethod(ClientStub):
    """A stub for a method on a remote actor.

    Can be annotated with execution options.

    Args:
        actor_handle: A reference to the ClientActorHandle that generated
          this method and will have this method called upon it.
        method_name: The name of this method
    """

    def __init__(
        self,
        actor_handle: ClientActorHandle,
        method_name: str,
        num_returns: int,
        signature: inspect.Signature,
    ):
        self._actor_handle = actor_handle
        self._method_name = method_name
        self._method_num_returns = num_returns
        self._signature = signature

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Actor methods cannot be called directly. Instead "
            f"of running 'object.{self._method_name}()', try "
            f"'object.{self._method_name}.remote()'."
        )

    def remote(self, *args, **kwargs):
        self._signature.bind(*args, **kwargs)
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def __repr__(self):
        return "ClientRemoteMethod(%s, %s, %s)" % (
            self._method_name,
            self._actor_handle,
            self._method_num_returns,
        )

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
        self._remote_stub = stub
        self._options = validate_options(options)

    def remote(self, *args, **kwargs):
        self._remote_stub._signature.bind(*args, **kwargs)
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def __getattr__(self, key):
        return getattr(self._remote_stub, key)

    def _prepare_client_task(self):
        task = self._remote_stub._prepare_client_task()
        set_task_options(task, self._options)
        return task

    def _num_returns(self) -> int:
        if self._options:
            num = self._options.get("num_returns")
            if num is not None:
                return num
        return self._remote_stub._num_returns()


class ActorOptionWrapper(OptionWrapper):
    def remote(self, *args, **kwargs):
        self._remote_stub._init_signature.bind(*args, **kwargs)
        futures = ray.call_remote(self, *args, **kwargs)
        assert len(futures) == 1
        actor_class = None
        if isinstance(self._remote_stub, ClientActorClass):
            actor_class = self._remote_stub
        return ClientActorHandle(ClientActorRef(futures[0]), actor_class=actor_class)


def set_task_options(
    task: ray_client_pb2.ClientTask,
    options: Optional[Dict[str, Any]],
    field: str = "options",
) -> None:
    if options is None:
        task.ClearField(field)
        return

    getattr(task, field).pickled_options = pickle.dumps(options)


def return_refs(
    futures: List[Future],
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
        if inspect.isfunction(function_or_class) or is_cython(function_or_class):
            return ClientRemoteFunc(function_or_class, options=options)
        elif inspect.isclass(function_or_class):
            return ClientActorClass(function_or_class, options=options)
        else:
            raise TypeError(
                "The @ray.remote decorator must be applied to "
                "either a function or to a class."
            )

    return decorator


@dataclass
class ClientServerHandle:
    """Holds the handles to the registered gRPC servicers and their server."""

    task_servicer: ray_client_pb2_grpc.RayletDriverServicer
    data_servicer: ray_client_pb2_grpc.RayletDataStreamerServicer
    logs_servicer: ray_client_pb2_grpc.RayletLogStreamerServicer
    grpc_server: grpc.Server

    def stop(self, grace: int) -> None:
        # The data servicer might be sleeping while waiting for clients to
        # reconnect. Signal that they no longer have to sleep and can exit
        # immediately, since the RPC server is stopped.
        self.grpc_server.stop(grace)
        self.data_servicer.stopped.set()

    # Add a hook for all the cases that previously
    # expected simply a gRPC server
    def __getattr__(self, attr):
        return getattr(self.grpc_server, attr)


def _get_client_id_from_context(context: Any) -> str:
    """
    Get `client_id` from gRPC metadata. If the `client_id` is not present,
    this function logs an error and sets the status_code.
    """
    metadata = dict(context.invocation_metadata())
    client_id = metadata.get("client_id") or ""
    if client_id == "":
        logger.error("Client connecting with no client_id")
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
    return client_id


def _propagate_error_in_context(e: Exception, context: Any) -> bool:
    """
    Encode an error into the context of an RPC response. Returns True
    if the error can be recovered from, false otherwise
    """
    try:
        if isinstance(e, grpc.RpcError):
            # RPC error, propagate directly by copying details into context
            context.set_code(e.code())
            context.set_details(e.details())
            return e.code() not in GRPC_UNRECOVERABLE_ERRORS
    except Exception:
        # Extra precaution -- if encoding the RPC directly fails fallback
        # to treating it as a regular error
        pass
    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
    context.set_details(str(e))
    return False


def _id_is_newer(id1: int, id2: int) -> bool:
    """
    We should only replace cache entries with the responses for newer IDs.
    Most of the time newer IDs will be the ones with higher value, except when
    the req_id counter rolls over. We check for this case by checking the
    distance between the two IDs. If the distance is significant, then it's
    likely that the req_id counter rolled over, and the smaller id should
    still be used to replace the one in cache.
    """
    diff = abs(id2 - id1)
    # Int32 max is also the maximum number of simultaneous in-flight requests.
    if diff > (INT32_MAX // 2):
        # Rollover likely occurred. In this case the smaller ID is newer
        return id1 < id2
    return id1 > id2


class ResponseCache:
    """
    Cache for blocking method calls. Needed to prevent retried requests from
    being applied multiple times on the server, for example when the client
    disconnects. This is used to cache requests/responses sent through
    unary-unary RPCs to the RayletServicer.

    Note that no clean up logic is used, the last response for each thread
    will always be remembered, so at most the cache will hold N entries,
    where N is the number of threads on the client side. This relies on the
    assumption that a thread will not make a new blocking request until it has
    received a response for a previous one, at which point it's safe to
    overwrite the old response.

    The high level logic is:

    1. Before making a call, check the cache for the current thread.
    2. If present in the cache, check the request id of the cached
        response.
        a. If it matches the current request_id, then the request has been
            received before and we shouldn't re-attempt the logic. Wait for
            the response to become available in the cache, and then return it
        b. If it doesn't match, then this is a new request and we can
            proceed with calling the real stub. While the response is still
            being generated, temporarily keep (req_id, None) in the cache.
            Once the call is finished, update the cache entry with the
            new (req_id, response) pair. Notify other threads that may
            have been waiting for the response to be prepared.
    """

    def __init__(self):
        self.cv = threading.Condition()
        self.cache: Dict[int, Tuple[int, Any]] = {}

    def check_cache(self, thread_id: int, request_id: int) -> Optional[Any]:
        """
        Check the cache for a given thread, and see if the entry in the cache
        matches the current request_id. Returns None if the request_id has
        not been seen yet, otherwise returns the cached result.

        Throws an error if the placeholder in the cache doesn't match the
        request_id -- this means that a new request evicted the old value in
        the cache, and that the RPC for `request_id` is redundant and the
        result can be discarded, i.e.:

        1. Request A is sent (A1)
        2. Channel disconnects
        3. Request A is resent (A2)
        4. A1 is received
        5. A2 is received, waits for A1 to finish
        6. A1 finishes and is sent back to client
        7. Request B is sent
        8. Request B overwrites cache entry
        9. A2 wakes up extremely late, but cache is now invalid

        In practice this is VERY unlikely to happen, but the error can at
        least serve as a sanity check or catch invalid request id's.
        """
        with self.cv:
            if thread_id in self.cache:
                cached_request_id, cached_resp = self.cache[thread_id]
                if cached_request_id == request_id:
                    while cached_resp is None:
                        # The call was started, but the response hasn't yet
                        # been added to the cache. Let go of the lock and
                        # wait until the response is ready.
                        self.cv.wait()
                        cached_request_id, cached_resp = self.cache[thread_id]
                        if cached_request_id != request_id:
                            raise RuntimeError(
                                "Cached response doesn't match the id of the "
                                "original request. This might happen if this "
                                "request was received out of order. The "
                                "result of the caller is no longer needed. "
                                f"({request_id} != {cached_request_id})"
                            )
                    return cached_resp
                if not _id_is_newer(request_id, cached_request_id):
                    raise RuntimeError(
                        "Attempting to replace newer cache entry with older "
                        "one. This might happen if this request was received "
                        "out of order. The result of the caller is no "
                        f"longer needed. ({request_id} != {cached_request_id}"
                    )
            self.cache[thread_id] = (request_id, None)
        return None

    def update_cache(self, thread_id: int, request_id: int, response: Any) -> None:
        """
        Inserts `response` into the cache for `request_id`.
        """
        with self.cv:
            cached_request_id, cached_resp = self.cache[thread_id]
            if cached_request_id != request_id or cached_resp is not None:
                # The cache was overwritten by a newer requester between
                # our call to check_cache and our call to update it.
                # This can't happen if the assumption that the cached requests
                # are all blocking on the client side, so if you encounter
                # this, check if any async requests are being cached.
                raise RuntimeError(
                    "Attempting to update the cache, but placeholder's "
                    "do not match the current request_id. This might happen "
                    "if this request was received out of order. The result "
                    f"of the caller is no longer needed. ({request_id} != "
                    f"{cached_request_id})"
                )
            self.cache[thread_id] = (request_id, response)
            self.cv.notify_all()


class OrderedResponseCache:
    """
    Cache for streaming RPCs, i.e. the DataServicer. Relies on explicit
    ack's from the client to determine when it can clean up cache entries.
    """

    def __init__(self):
        self.last_received = 0
        self.cv = threading.Condition()
        self.cache: Dict[int, Any] = OrderedDict()

    def check_cache(self, req_id: int) -> Optional[Any]:
        """
        Check the cache for a given thread, and see if the entry in the cache
        matches the current request_id. Returns None if the request_id has
        not been seen yet, otherwise returns the cached result.
        """
        with self.cv:
            if _id_is_newer(self.last_received, req_id) or self.last_received == req_id:
                # Request is for an id that has already been cleared from
                # cache/acknowledged.
                raise RuntimeError(
                    "Attempting to accesss a cache entry that has already "
                    "cleaned up. The client has already acknowledged "
                    f"receiving this response. ({req_id}, "
                    f"{self.last_received})"
                )
            if req_id in self.cache:
                cached_resp = self.cache[req_id]
                while cached_resp is None:
                    # The call was started, but the response hasn't yet been
                    # added to the cache. Let go of the lock and wait until
                    # the response is ready
                    self.cv.wait()
                    if req_id not in self.cache:
                        raise RuntimeError(
                            "Cache entry was removed. This likely means that "
                            "the result of this call is no longer needed."
                        )
                    cached_resp = self.cache[req_id]
                return cached_resp
            self.cache[req_id] = None
        return None

    def update_cache(self, req_id: int, resp: Any) -> None:
        """
        Inserts `response` into the cache for `request_id`.
        """
        with self.cv:
            self.cv.notify_all()
            if req_id not in self.cache:
                raise RuntimeError(
                    "Attempting to update the cache, but placeholder is "
                    "missing. This might happen on a redundant call to "
                    f"update_cache. ({req_id})"
                )
            self.cache[req_id] = resp

    def invalidate(self, e: Exception) -> bool:
        """
        Invalidate any partially populated cache entries, replacing their
        placeholders with the passed in exception. Useful to prevent a thread
        from waiting indefinitely on a failed call.

        Returns True if the cache contains an error, False otherwise
        """
        with self.cv:
            invalid = False
            for req_id in self.cache:
                if self.cache[req_id] is None:
                    self.cache[req_id] = e
                if isinstance(self.cache[req_id], Exception):
                    invalid = True
            self.cv.notify_all()
        return invalid

    def cleanup(self, last_received: int) -> None:
        """
        Cleanup all of the cached requests up to last_received. Assumes that
        the cache entries were inserted in ascending order.
        """
        with self.cv:
            if _id_is_newer(last_received, self.last_received):
                self.last_received = last_received
            to_remove = []
            for req_id in self.cache:
                if _id_is_newer(last_received, req_id) or last_received == req_id:
                    to_remove.append(req_id)
                else:
                    break
            for req_id in to_remove:
                del self.cache[req_id]
            self.cv.notify_all()
