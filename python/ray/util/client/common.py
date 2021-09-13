import logging
import ray._raylet as raylet
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client import ray
from ray.util.client.options import validate_options

from dataclasses import dataclass
import grpc
import os
import uuid
import inspect
from ray.util.inspect import is_cython, is_function_or_method
import json
import threading
from collections import OrderedDict
from typing import Any
from typing import List
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

# The maximum field value for int32 id's -- which is also the maximum
# number of simultaneous in-flight requests.
INT32_MAX = (2**31) - 1

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
                self._ref = ray.put(
                    self._func, client_ref_id=self._client_side_ref.id)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        self._ensure_ref()
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.FUNCTION
        task.name = self._name
        task.payload_id = self._ref.id
        set_task_options(task, self._options, "baseline_options")
        return task


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
                self._ref = ray.put(
                    self.actor_cls, client_ref_id=self._client_side_ref.id)

    def remote(self, *args, **kwargs) -> "ClientActorHandle":
        # Actually instantiate the actor
        ref_ids = ray.call_remote(self, *args, **kwargs)
        assert len(ref_ids) == 1
        return ClientActorHandle(ClientActorRef(ref_ids[0]), actor_class=self)

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
            self._dir = list(
                dict(
                    inspect.getmembers(actor_class.actor_cls,
                                       is_function_or_method)).keys())

    def __del__(self) -> None:
        if ray is None:
            # The ray API stub might be set to None when the script exits.
            # Should be safe to skip call_release in this case, since the
            # client should have already disconnected at this point.
            return
        if ray.is_connected():
            ray.call_release(self.actor_ref.id)

    def __dir__(self) -> List[str]:
        if self._dir is not None:
            return self._dir
        if ray.is_connected():

            @ray.remote(num_cpus=0)
            def get_dir(x):
                return dir(x)

            self._dir = ray.get(get_dir.remote(self))
            return self._dir
        return super().__dir__()

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

    def __init__(self, actor_handle: ClientActorHandle, method_name: str):
        self._actor_handle = actor_handle
        self._method_name = method_name

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        f"of running 'object.{self._method_name}()', try "
                        f"'object.{self._method_name}.remote()'.")

    def remote(self, *args, **kwargs):
        return return_refs(ray.call_remote(self, *args, **kwargs))

    def __repr__(self):
        return "ClientRemoteMethod(%s, %s)" % (self._method_name,
                                               self._actor_handle)

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


class ActorOptionWrapper(OptionWrapper):
    def remote(self, *args, **kwargs):
        ref_ids = ray.call_remote(self, *args, **kwargs)
        assert len(ref_ids) == 1
        actor_class = None
        if isinstance(self.remote_stub, ClientActorClass):
            actor_class = self.remote_stub
        return ClientActorHandle(
            ClientActorRef(ref_ids[0]), actor_class=actor_class)


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


def return_refs(ids: List[bytes]
                ) -> Union[None, ClientObjectRef, List[ClientObjectRef]]:
    if len(ids) == 1:
        return ClientObjectRef(ids[0])
    if len(ids) == 0:
        return None
    return [ClientObjectRef(id) for id in ids]


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

    def stop(self, grace: int) -> None:
        self.grpc_server.stop(grace)
        self.data_servicer.stopped.set()

    # Add a hook for all the cases that previously
    # expected simply a gRPC server
    def __getattr__(self, attr):
        return getattr(self.grpc_server, attr)


def _get_client_id_from_context(context: Any, logger: logging.Logger) -> str:
    """
    Get `client_id` from gRPC metadata. If the `client_id` is not present,
    this function logs an error and sets the status_code.
    """
    metadata = {k: v for k, v in context.invocation_metadata()}
    client_id = metadata.get("client_id") or ""
    if client_id == "":
        logger.error("Client connecting with no client_id")
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
    return client_id


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
    if diff > (INT32_MAX // 2):
        # Rollover likely occurred. In this case the smaller ID is newer
        return id1 < id2
    return id1 > id2


class ReplayCache:
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
                                f"({request_id} != {cached_request_id})")
                    return cached_resp
                if not _id_is_newer(request_id, cached_request_id):
                    raise RuntimeError(
                        "Attempting to replace newer cache entry with older "
                        "one. This might happen if this request was received "
                        "out of order. The result of the caller is no "
                        f"longer needed. ({request_id} != {cached_request_id}")
            self.cache[thread_id] = (request_id, None)
        return None

    def update_cache(self, thread_id: int, request_id: int,
                     response: Any) -> None:
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
                    f"{cached_request_id})")
            self.cache[thread_id] = (request_id, response)
            self.cv.notify_all()


class OrderedReplayCache:
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
            if _id_is_newer(self.last_received,
                            req_id) or self.last_received == req_id:
                # Request is for an id that has already been cleared from
                # cache/acknowledged.
                raise RuntimeError(
                    "Attempting to accesss a cache entry that has already "
                    "cleaned up. The client has already acknowledged "
                    f"receiving this response. ({req_id}, "
                    f"{self.last_received})")
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
                            "the result of this call is no longer needed.")
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
                    f"update_cache. ({req_id})")
            self.cache[req_id] = resp

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
                if _id_is_newer(last_received,
                                req_id) or last_received == req_id:
                    to_remove.append(req_id)
                else:
                    break
            for req_id in to_remove:
                del self.cache[req_id]
            self.cv.notify_all()
