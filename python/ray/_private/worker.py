import atexit
import faulthandler
import functools
import hashlib
import inspect
import io
import json
import logging
import os
import sys
import threading
import time
import traceback
import urllib
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)
from urllib.parse import urlparse

import colorama
import setproctitle

if sys.version_info >= (3, 8):
    from typing import Literal, Protocol
else:
    from typing_extensions import Literal, Protocol

import ray
import ray._private.gcs_utils as gcs_utils
import ray._private.import_thread as import_thread
import ray._private.memory_monitor as memory_monitor
import ray._private.node
import ray._private.parameter
import ray._private.profiling as profiling
import ray._private.ray_constants as ray_constants
import ray._private.serialization as serialization
import ray._private.services as services
import ray._private.state
import ray._private.storage as storage

# Ray modules
import ray.actor
import ray.cloudpickle as pickle
import ray.job_config
import ray.remote_function
from ray import ActorID, JobID, Language, ObjectRef
from ray._private import ray_option_utils
from ray._private.client_mode_hook import client_mode_hook
from ray._private.function_manager import FunctionActorManager, make_function_table_key
from ray._private.gcs_pubsub import (
    GcsErrorSubscriber,
    GcsFunctionKeySubscriber,
    GcsLogSubscriber,
    GcsPublisher,
)
from ray._private.inspect_util import is_cython
from ray._private.ray_logging import global_worker_stdstream_dispatcher, setup_logger
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray._private.runtime_env.py_modules import upload_py_modules_if_needed
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.storage import _load_class
from ray._private.utils import check_oversized_function, get_ray_doc_version
from ray.exceptions import ObjectStoreFullError, RayError, RaySystemError, RayTaskError
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_reset,
)
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray.util.debug import log_once
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tracing.tracing_helper import _import_from_string
from ray.widgets import Template

SCRIPT_MODE = 0
WORKER_MODE = 1
LOCAL_MODE = 2
SPILL_WORKER_MODE = 3
RESTORE_WORKER_MODE = 4

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


T0 = TypeVar("T0")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")
R = TypeVar("R")

DAGNode = TypeVar("DAGNode")


class RemoteFunctionNoArgs(Generic[R]):
    def __init__(self, function: Callable[[], R]) -> None:
        pass

    def remote(
        self,
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
    ) -> "DAGNode[R]":
        ...


class RemoteFunction0(Generic[R, T0]):
    def __init__(self, function: Callable[[T0], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction1(Generic[R, T0, T1]):
    def __init__(self, function: Callable[[T0, T1], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction2(Generic[R, T0, T1, T2]):
    def __init__(self, function: Callable[[T0, T1, T2], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction3(Generic[R, T0, T1, T2, T3]):
    def __init__(self, function: Callable[[T0, T1, T2, T3], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction4(Generic[R, T0, T1, T2, T3, T4]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction5(Generic[R, T0, T1, T2, T3, T4, T5]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction6(Generic[R, T0, T1, T2, T3, T4, T5, T6]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction7(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction8(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]):
    def __init__(
        self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
    ) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
        __arg8: "Union[T8, ObjectRef[T8]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
        __arg8: "Union[T8, DAGNode[T8]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction9(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]):
    def __init__(
        self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
    ) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
        __arg8: "Union[T8, ObjectRef[T8]]",
        __arg9: "Union[T9, ObjectRef[T9]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
        __arg8: "Union[T8, DAGNode[T8]]",
        __arg9: "Union[T9, DAGNode[T9]]",
    ) -> "DAGNode[R]":
        ...


# Visible for testing.
def _unhandled_error_handler(e: Exception):
    logger.error(
        f"Unhandled error (suppress with 'RAY_IGNORE_UNHANDLED_ERRORS=1'): {e}"
    )


class Worker:
    """A class used to define the control flow of a worker process.

    Note:
        The methods in this class are considered unexposed to the user. The
        functions outside of this class are considered exposed.

    Attributes:
        node (ray._private.node.Node): The node this worker is attached to.
        mode: The mode of the worker. One of SCRIPT_MODE, LOCAL_MODE, and
            WORKER_MODE.
        cached_functions_to_run: A list of functions to run on all of
            the workers that should be exported as soon as connect is called.
    """

    def __init__(self):
        """Initialize a Worker object."""
        self.node = None
        self.mode = None
        self.cached_functions_to_run: list = []
        self.actors = {}
        # When the worker is constructed. Record the original value of the
        # CUDA_VISIBLE_DEVICES environment variable.
        self.original_gpu_ids = ray._private.utils.get_cuda_visible_devices()
        self.memory_monitor = memory_monitor.MemoryMonitor()
        # A dictionary that maps from driver id to SerializationContext
        # TODO: clean up the SerializationContext once the job finished.
        self.serialization_context_map = {}
        self.function_actor_manager = FunctionActorManager(self)
        # This event is checked regularly by all of the threads so that they
        # know when to exit.
        self.threads_stopped = threading.Event()
        # Index of the current session. This number will
        # increment every time when `ray.shutdown` is called.
        self._session_index = 0
        # If this is set, the next .remote call should drop into the
        # debugger, at the specified breakpoint ID.
        self.debugger_breakpoint = b""
        # If this is set, ray.get calls invoked on the object ID returned
        # by the worker should drop into the debugger at the specified
        # breakpoint ID.
        self.debugger_get_breakpoint = b""
        # If True, make the debugger external to the node this worker is
        # running on.
        self.ray_debugger_external = False
        self._load_code_from_local = False
        # Create the lock here because the serializer will use it before
        # initializing Ray.
        self.lock = threading.RLock()

    @property
    def connected(self):
        """bool: True if Ray has been started and False otherwise."""
        return self.node is not None

    @property
    def node_ip_address(self):
        self.check_connected()
        return self.node.node_ip_address

    @property
    def load_code_from_local(self):
        self.check_connected()
        return self._load_code_from_local

    @property
    def current_job_id(self):
        if hasattr(self, "core_worker"):
            return self.core_worker.get_current_job_id()
        return JobID.nil()

    @property
    def actor_id(self):
        if hasattr(self, "core_worker"):
            return self.core_worker.get_actor_id()
        return ActorID.nil()

    @property
    def current_task_id(self):
        return self.core_worker.get_current_task_id()

    @property
    def current_node_id(self):
        return self.core_worker.get_current_node_id()

    @property
    def task_depth(self):
        return self.core_worker.get_task_depth()

    @property
    def namespace(self):
        return self.core_worker.get_job_config().ray_namespace

    @property
    def placement_group_id(self):
        return self.core_worker.get_placement_group_id()

    @property
    def worker_id(self):
        return self.core_worker.get_worker_id().binary()

    @property
    def should_capture_child_tasks_in_placement_group(self):
        return self.core_worker.should_capture_child_tasks_in_placement_group()

    @property
    def current_session_and_job(self):
        """Get the current session index and job id as pair."""
        assert isinstance(self._session_index, int)
        assert isinstance(self.current_job_id, ray.JobID)
        return self._session_index, self.current_job_id

    @property
    def runtime_env(self):
        """Get the runtime env in json format"""
        return self.core_worker.get_current_runtime_env()

    def get_serialization_context(self):
        """Get the SerializationContext of the job that this worker is processing.

        Returns:
            The serialization context of the given job.
        """
        # This function needs to be protected by a lock, because it will be
        # called by`register_class_for_serialization`, as well as the import
        # thread, from different threads. Also, this function will recursively
        # call itself, so we use RLock here.
        job_id = self.current_job_id
        context_map = self.serialization_context_map
        with self.lock:
            if job_id not in context_map:
                # The job ID is nil before initializing Ray.
                if JobID.nil() in context_map:
                    # Transfer the serializer context used before initializing Ray.
                    context_map[job_id] = context_map.pop(JobID.nil())
                else:
                    context_map[job_id] = serialization.SerializationContext(self)
            return context_map[job_id]

    def check_connected(self):
        """Check if the worker is connected.

        Raises:
          Exception: An exception is raised if the worker is not connected.
        """
        if not self.connected:
            raise RaySystemError(
                "Ray has not been started yet. You can start Ray with 'ray.init()'."
            )

    def set_mode(self, mode):
        """Set the mode of the worker.

        The mode SCRIPT_MODE should be used if this Worker is a driver that is
        being run as a Python script or interactively in a shell. It will print
        information about task failures.

        The mode WORKER_MODE should be used if this Worker is not a driver. It
        will not print information about tasks.

        The mode LOCAL_MODE should be used if this Worker is a driver and if
        you want to run the driver in a manner equivalent to serial Python for
        debugging purposes. It will not send remote function calls to the
        scheduler and will instead execute them in a blocking fashion.

        Args:
            mode: One of SCRIPT_MODE, WORKER_MODE, and LOCAL_MODE.
        """
        self.mode = mode

    def set_load_code_from_local(self, load_code_from_local):
        self._load_code_from_local = load_code_from_local

    def put_object(self, value, object_ref=None, owner_address=None):
        """Put value in the local object store with object reference `object_ref`.

        This assumes that the value for `object_ref` has not yet been placed in
        the local object store. If the plasma store is full, the worker will
        automatically retry up to DEFAULT_PUT_OBJECT_RETRIES times. Each
        retry will delay for an exponentially doubling amount of time,
        starting with DEFAULT_PUT_OBJECT_DELAY. After this, exception
        will be raised.

        Args:
            value: The value to put in the object store.
            object_ref: The object ref of the value to be
                put. If None, one will be generated.
            owner_address: The serialized address of object's owner.

        Returns:
            ObjectRef: The object ref the object was put under.

        Raises:
            ray.exceptions.ObjectStoreFullError: This is raised if the attempt
                to store the object fails because the object store is full even
                after multiple retries.
        """
        # Make sure that the value is not an object ref.
        if isinstance(value, ObjectRef):
            raise TypeError(
                "Calling 'put' on an ray.ObjectRef is not allowed. "
                "If you really want to do this, you can wrap the "
                "ray.ObjectRef in a list and call 'put' on it."
            )

        if self.mode == LOCAL_MODE:
            assert (
                object_ref is None
            ), "Local Mode does not support inserting with an ObjectRef"

        serialized_value = self.get_serialization_context().serialize(value)
        # This *must* be the first place that we construct this python
        # ObjectRef because an entry with 0 local references is created when
        # the object is Put() in the core worker, expecting that this python
        # reference will be created. If another reference is created and
        # removed before this one, it will corrupt the state in the
        # reference counter.
        return ray.ObjectRef(
            self.core_worker.put_serialized_object_and_increment_local_ref(
                serialized_value, object_ref=object_ref, owner_address=owner_address
            ),
            # The initial local reference is already acquired internally.
            skip_adding_local_ref=True,
        )

    def raise_errors(self, data_metadata_pairs, object_refs):
        out = self.deserialize_objects(data_metadata_pairs, object_refs)
        if "RAY_IGNORE_UNHANDLED_ERRORS" in os.environ:
            return
        for e in out:
            _unhandled_error_handler(e)

    def deserialize_objects(self, data_metadata_pairs, object_refs):
        # Function actor manager or the import thread may call pickle.loads
        # at the same time which can lead to failed imports
        # TODO: We may be better off locking on all imports or injecting a lock
        # into pickle.loads (https://github.com/ray-project/ray/issues/16304)
        with self.function_actor_manager.lock:
            context = self.get_serialization_context()
            return context.deserialize_objects(data_metadata_pairs, object_refs)

    def get_objects(self, object_refs: list, timeout: Optional[float] = None):
        """Get the values in the object store associated with the IDs.

        Return the values from the local object store for object_refs. This
        will block until all the values for object_refs have been written to
        the local object store.

        Args:
            object_refs: A list of the object refs
                whose values should be retrieved.
            timeout: The maximum amount of time in
                seconds to wait before returning.
        Returns:
            list: List of deserialized objects
            bytes: UUID of the debugger breakpoint we should drop
                into or b"" if there is no breakpoint.
        """
        # Make sure that the values are object refs.
        for object_ref in object_refs:
            if not isinstance(object_ref, ObjectRef):
                raise TypeError(
                    f"Attempting to call `get` on the value {object_ref}, "
                    "which is not an ray.ObjectRef."
                )

        timeout_ms = int(timeout * 1000) if timeout else -1
        data_metadata_pairs = self.core_worker.get_objects(
            object_refs, self.current_task_id, timeout_ms
        )
        debugger_breakpoint = b""
        for (data, metadata) in data_metadata_pairs:
            if metadata:
                metadata_fields = metadata.split(b",")
                if len(metadata_fields) >= 2 and metadata_fields[1].startswith(
                    ray_constants.OBJECT_METADATA_DEBUG_PREFIX
                ):
                    debugger_breakpoint = metadata_fields[1][
                        len(ray_constants.OBJECT_METADATA_DEBUG_PREFIX) :
                    ]
        return (
            self.deserialize_objects(data_metadata_pairs, object_refs),
            debugger_breakpoint,
        )

    @Deprecated(
        message="This function is deprecated and will be removed by Ray 2.4. "
        "Please use Runtime Environments "
        f"https://docs.ray.io/en/{get_ray_doc_version()}/ray-core"
        "/handling-dependencies.html "
        "to manage dependencies in workers.",
        warning=True,
    )
    def run_function_on_all_workers(self, function: callable):
        """This function has been deprecated given the following issues:
            - no guarantee that the function run before the remote function run.
            - pubsub signal might be lost in some failure cases.

        This API will be deleted once we move the working dir init away.
        NO NEW CODE SHOULD USE THIS API.

        Run arbitrary code on all of the workers.

        This function will first be run on the driver, and then it will be
        exported to all of the workers to be run. It will also be run on any
        new workers that register later. If ray.init has not been called yet,
        then cache the function and export it later.

        Args:
            function: The function to run on all of the workers. It
                takes only one argument, a worker info dict. If it returns
                anything, its return values will not be used.
        """
        # If ray.init has not been called yet, then cache the function and
        # export it when connect is called. Otherwise, run the function on all
        # workers.
        if self.mode is None:
            self.cached_functions_to_run.append(function)
        else:
            # Attempt to pickle the function before we need it. This could
            # fail, and it is more convenient if the failure happens before we
            # actually run the function locally.
            pickled_function = pickle.dumps(function)

            function_to_run_id = hashlib.shake_128(pickled_function).digest(
                ray_constants.ID_SIZE
            )
            key = make_function_table_key(
                b"FunctionsToRun", self.current_job_id, function_to_run_id
            )
            # First run the function on the driver.
            # We always run the task locally.
            function({"worker": self})

            check_oversized_function(
                pickled_function, function.__name__, "function", self
            )

            # Run the function on all workers.
            if (
                self.gcs_client.internal_kv_put(
                    key,
                    pickle.dumps(
                        {
                            "job_id": self.current_job_id.binary(),
                            "function_id": function_to_run_id,
                            "function": pickled_function,
                        }
                    ),
                    True,
                    ray_constants.KV_NAMESPACE_FUNCTION_TABLE,
                )
                != 0
            ):
                self.function_actor_manager.export_key(key)
            # TODO(rkn): If the worker fails after it calls setnx and before it
            # successfully completes the hset and rpush, then the program will
            # most likely hang. This could be fixed by making these three
            # operations into a transaction (or by implementing a custom
            # command that does all three things).

    def main_loop(self):
        """The main loop a worker runs to receive and execute tasks."""

        def sigterm_handler(signum, frame):
            shutdown(True)
            sys.exit(1)

        ray._private.utils.set_sigterm_handler(sigterm_handler)
        self.core_worker.run_task_loop()
        sys.exit(0)

    def print_logs(self):
        """Prints log messages from workers on all nodes in the same job."""
        import grpc

        subscriber = self.gcs_log_subscriber
        subscriber.subscribe()
        exception_type = grpc.RpcError
        localhost = services.get_node_ip_address()
        try:
            # Number of messages received from the last polling. When the batch
            # size exceeds 100 and keeps increasing, the worker and the user
            # probably will not be able to consume the log messages as rapidly
            # as they are coming in.
            # This is meaningful only for GCS subscriber.
            last_polling_batch_size = 0
            job_id_hex = self.current_job_id.hex()
            while True:
                # Exit if we received a signal that we should stop.
                if self.threads_stopped.is_set():
                    return

                data = subscriber.poll()
                # GCS subscriber only returns None on unavailability.
                if data is None:
                    last_polling_batch_size = 0
                    continue

                # Don't show logs from other drivers.
                if data["job"] and data["job"] != job_id_hex:
                    last_polling_batch_size = 0
                    continue

                data["localhost"] = localhost
                global_worker_stdstream_dispatcher.emit(data)

                lagging = 100 <= last_polling_batch_size < subscriber.last_batch_size
                if lagging:
                    logger.warning(
                        "The driver may not be able to keep up with the "
                        "stdout/stderr of the workers. To avoid forwarding "
                        "logs to the driver, use "
                        "'ray.init(log_to_driver=False)'."
                    )

                last_polling_batch_size = subscriber.last_batch_size

        except (OSError, exception_type) as e:
            logger.error(f"print_logs: {e}")
        finally:
            # Close the pubsub client to avoid leaking file descriptors.
            subscriber.close()


@PublicAPI
@client_mode_hook(auto_init=True)
def get_gpu_ids():
    """Get the IDs of the GPUs that are available to the worker.

    If the CUDA_VISIBLE_DEVICES environment variable was set when the worker
    started up, then the IDs returned by this method will be a subset of the
    IDs in CUDA_VISIBLE_DEVICES. If not, the IDs will fall in the range
    [0, NUM_GPUS - 1], where NUM_GPUS is the number of GPUs that the node has.

    Returns:
        A list of GPU IDs.
    """
    worker = global_worker
    worker.check_connected()

    if worker.mode != WORKER_MODE:
        if log_once("worker_get_gpu_ids_empty_from_driver"):
            logger.warning(
                "`ray.get_gpu_ids()` will always return the empty list when "
                "called from the driver. This is because Ray does not manage "
                "GPU allocations to the driver process."
            )

    # TODO(ilr) Handle inserting resources in local mode
    all_resource_ids = global_worker.core_worker.resource_ids()
    assigned_ids = set()
    for resource, assignment in all_resource_ids.items():
        # Handle both normal and placement group GPU resources.
        # Note: We should only get the GPU ids from the placement
        # group resource that does not contain the bundle index!
        import re

        if resource == "GPU" or re.match(r"^GPU_group_[0-9A-Za-z]+$", resource):
            for resource_id, _ in assignment:
                assigned_ids.add(resource_id)

    assigned_ids = list(assigned_ids)
    # If the user had already set CUDA_VISIBLE_DEVICES, then respect that (in
    # the sense that only GPU IDs that appear in CUDA_VISIBLE_DEVICES should be
    # returned).
    if global_worker.original_gpu_ids is not None:
        assigned_ids = [
            global_worker.original_gpu_ids[gpu_id] for gpu_id in assigned_ids
        ]
        # Give all GPUs in local_mode.
        if global_worker.mode == LOCAL_MODE:
            max_gpus = global_worker.node.get_resource_spec().num_gpus
            assigned_ids = global_worker.original_gpu_ids[:max_gpus]

    return assigned_ids


@Deprecated(
    message="Use ray.get_runtime_context().get_assigned_resources() instead.",
    warning=True,
)
def get_resource_ids():
    """Get the IDs of the resources that are available to the worker.

    Returns:
        A dictionary mapping the name of a resource to a list of pairs, where
        each pair consists of the ID of a resource and the fraction of that
        resource reserved for this worker.
    """
    worker = global_worker
    worker.check_connected()

    if _mode() == LOCAL_MODE:
        raise RuntimeError(
            "ray._private.worker.get_resource_ids() does not work in local_mode."
        )

    return global_worker.core_worker.resource_ids()


@Deprecated(message="Use ray.init().address_info['webui_url'] instead.")
def get_dashboard_url():
    """Get the URL to access the Ray dashboard.

    Note that the URL does not specify which node the dashboard is on.

    Returns:
        The URL of the dashboard as a string.
    """
    if ray_constants.RAY_OVERRIDE_DASHBOARD_URL in os.environ:
        return _remove_protocol_from_url(
            os.environ.get(ray_constants.RAY_OVERRIDE_DASHBOARD_URL)
        )
    else:
        worker = global_worker
        worker.check_connected()
        return _global_node.webui_url


def _remove_protocol_from_url(url: Optional[str]) -> str:
    """
    Helper function to remove protocol from URL if it exists.
    """
    if not url:
        return url
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme:
        # Construct URL without protocol
        scheme = f"{parsed_url.scheme}://"
        return parsed_url.geturl().replace(scheme, "", 1)
    return url


class BaseContext(metaclass=ABCMeta):
    """
    Base class for RayContext and ClientContext
    """

    @abstractmethod
    def disconnect(self):
        """
        If this context is for directly attaching to a cluster, disconnect
        will call ray.shutdown(). Otherwise, if the context is for a ray
        client connection, the client will be disconnected.
        """
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self):
        pass


@dataclass
class RayContext(BaseContext, Mapping):
    """
    Context manager for attached drivers.
    """

    dashboard_url: Optional[str]
    python_version: str
    ray_version: str
    ray_commit: str
    protocol_version = Optional[str]
    address_info: Dict[str, Optional[str]]

    def __init__(self, address_info: Dict[str, Optional[str]]):
        self.dashboard_url = get_dashboard_url()
        self.python_version = "{}.{}.{}".format(*sys.version_info[:3])
        self.ray_version = ray.__version__
        self.ray_commit = ray.__commit__
        # No client protocol version since this driver was intiialized
        # directly
        self.protocol_version = None
        self.address_info = address_info

    def __getitem__(self, key):
        if log_once("ray_context_getitem"):
            warnings.warn(
                f'Accessing values through ctx["{key}"] is deprecated. '
                f'Use ctx.address_info["{key}"] instead.',
                DeprecationWarning,
                stacklevel=2,
            )
        return self.address_info[key]

    def __len__(self):
        if log_once("ray_context_len"):
            warnings.warn("len(ctx) is deprecated. Use len(ctx.address_info) instead.")
        return len(self.address_info)

    def __iter__(self):
        if log_once("ray_context_len"):
            warnings.warn(
                "iter(ctx) is deprecated. Use iter(ctx.address_info) instead."
            )
        return iter(self.address_info)

    def __enter__(self) -> "RayContext":
        return self

    def __exit__(self, *exc):
        ray.shutdown()

    def disconnect(self):
        # Include disconnect() to stay consistent with ClientContext
        ray.shutdown()

    def _repr_html_(self):
        if self.dashboard_url:
            dashboard_row = Template("context_dashrow.html.j2").render(
                dashboard_url="http://" + self.dashboard_url
            )
        else:
            dashboard_row = None

        return Template("context.html.j2").render(
            python_version=self.python_version,
            ray_version=self.ray_version,
            dashboard_row=dashboard_row,
        )


global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

_global_node = None
"""ray._private.node.Node: The global node object that is created by ray.init()."""


@PublicAPI
@client_mode_hook(auto_init=False)
def init(
    address: Optional[str] = None,
    *,
    num_cpus: Optional[int] = None,
    num_gpus: Optional[int] = None,
    resources: Optional[Dict[str, float]] = None,
    object_store_memory: Optional[int] = None,
    local_mode: bool = False,
    ignore_reinit_error: bool = False,
    include_dashboard: Optional[bool] = None,
    dashboard_host: str = ray_constants.DEFAULT_DASHBOARD_IP,
    dashboard_port: Optional[int] = None,
    job_config: "ray.job_config.JobConfig" = None,
    configure_logging: bool = True,
    logging_level: int = ray_constants.LOGGER_LEVEL,
    logging_format: Optional[str] = None,
    log_to_driver: bool = True,
    namespace: Optional[str] = None,
    runtime_env: Optional[Union[Dict[str, Any], "RuntimeEnv"]] = None,  # noqa: F821
    storage: Optional[str] = None,
    **kwargs,
) -> BaseContext:
    """
    Connect to an existing Ray cluster or start one and connect to it.

    This method handles two cases; either a Ray cluster already exists and we
    just attach this driver to it or we start all of the processes associated
    with a Ray cluster and attach to the newly started cluster.

    In most cases, it is enough to just call this method with no arguments.
    This will autodetect an existing Ray cluster or start a new Ray instance if
    no existing cluster is found:

    .. code-block:: python

        ray.init()

    To explicitly connect to an existing local cluster, use this as follows. A
    ConnectionError will be thrown if no existing local cluster is found.

    .. code-block:: python

        ray.init(address="auto")

    To connect to an existing remote cluster, use this as follows (substituting
    in the appropriate address). Note the addition of "ray://" at the beginning
    of the address.

    .. code-block:: python

        ray.init(address="ray://123.45.67.89:10001")

    More details for starting and connecting to a remote cluster can be found
    here: https://docs.ray.io/en/master/cluster/getting-started.html

    You can also define an environment variable called `RAY_ADDRESS` in
    the same format as the `address` parameter to connect to an existing
    cluster with ray.init() or ray.init(address="auto").

    Args:
        address: The address of the Ray cluster to connect to. The provided
            address is resolved as follows:
            1. If a concrete address (e.g., localhost:<port>) is provided, try to
            connect to it. Concrete addresses can be prefixed with "ray://" to
            connect to a remote cluster. For example, passing in the address
            "ray://123.45.67.89:50005" will connect to the cluster at the given
            address.
            2. If no address is provided, try to find an existing Ray instance
            to connect to. This is done by first checking the environment
            variable `RAY_ADDRESS`. If this is not defined, check the address
            of the latest cluster started (found in
            /tmp/ray/ray_current_cluster) if available. If this is also empty,
            then start a new local Ray instance.
            3. If the provided address is "auto", then follow the same process
            as above. However, if there is no existing cluster found, this will
            throw a ConnectionError instead of starting a new local Ray
            instance.
            4. If the provided address is "local", start a new local Ray
            instance, even if there is already an existing local Ray instance.
        num_cpus: Number of CPUs the user wishes to assign to each
            raylet. By default, this is set based on virtual cores.
        num_gpus: Number of GPUs the user wishes to assign to each
            raylet. By default, this is set based on detected GPUs.
        resources: A dictionary mapping the names of custom resources to the
            quantities for them available.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with. By default, this is automatically set based on
            available system memory.
        local_mode: Deprecated: consider using the Ray Debugger instead.
        ignore_reinit_error: If true, Ray suppresses errors from calling
            ray.init() a second time. Ray won't be restarted.
        include_dashboard: Boolean flag indicating whether or not to start the
            Ray dashboard, which displays the status of the Ray
            cluster. If this argument is None, then the UI will be started if
            the relevant dependencies are present.
        dashboard_host: The host to bind the dashboard server to. Can either be
            localhost (127.0.0.1) or 0.0.0.0 (available from all interfaces).
            By default, this is set to localhost to prevent access from
            external machines.
        dashboard_port(int, None): The port to bind the dashboard server to.
            Defaults to 8265 and Ray will automatically find a free port if
            8265 is not available.
        job_config (ray.job_config.JobConfig): The job configuration.
        configure_logging: True (default) if configuration of logging is
            allowed here. Otherwise, the user may want to configure it
            separately.
        logging_level: Logging level, defaults to logging.INFO. Ignored unless
            "configure_logging" is true.
        logging_format: Logging format, defaults to string containing a
            timestamp, filename, line number, and message. See the source file
            ray_constants.py for details. Ignored unless "configure_logging"
            is true.
        log_to_driver: If true, the output from all of the worker
            processes on all nodes will be directed to the driver.
        namespace: A namespace is a logical grouping of jobs and named actors.
        runtime_env: The runtime environment to use
            for this job (see :ref:`runtime-environments` for details).
        storage: [Experimental] Specify a URI for persistent cluster-wide storage.
            This storage path must be accessible by all nodes of the cluster, otherwise
            an error will be raised. This option can also be specified as the
            RAY_STORAGE env var.
        _enable_object_reconstruction: If True, when an object stored in
            the distributed plasma store is lost due to node failure, Ray will
            attempt to reconstruct the object by re-executing the task that
            created the object. Arguments to the task will be recursively
            reconstructed. If False, then ray.ObjectLostError will be
            thrown.
        _redis_max_memory: Redis max memory.
        _plasma_directory: Override the plasma mmap file directory.
        _node_ip_address: The IP address of the node that we are on.
        _driver_object_store_memory: Deprecated.
        _memory: Amount of reservable memory resource in bytes rounded
            down to the nearest integer.
        _redis_password: Prevents external clients without the password
            from connecting to Redis if provided.
        _temp_dir: If provided, specifies the root temporary
            directory for the Ray process. Defaults to an OS-specific
            conventional location, e.g., "/tmp/ray".
        _metrics_export_port: Port number Ray exposes system metrics
            through a Prometheus endpoint. It is currently under active
            development, and the API is subject to change.
        _system_config: Configuration for overriding
            RayConfig defaults. For testing purposes ONLY.
        _tracing_startup_hook: If provided, turns on and sets up tracing
            for Ray. Must be the name of a function that takes no arguments and
            sets up a Tracer Provider, Remote Span Processors, and
            (optional) additional instruments. See more at
            docs.ray.io/tracing.html. It is currently under active development,
            and the API is subject to change.
        _node_name: User-provided node name or identifier. Defaults to
            the node IP address.

    Returns:
        If the provided address includes a protocol, for example by prepending
        "ray://" to the address to get "ray://1.2.3.4:10001", then a
        ClientContext is returned with information such as settings, server
        versions for ray and python, and the dashboard_url. Otherwise,
        a RayContext is returned with ray and python versions, and address
        information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """
    if configure_logging:
        setup_logger(logging_level, logging_format or ray_constants.LOGGER_FORMAT)

    # Parse the hidden options:
    _enable_object_reconstruction: bool = kwargs.pop(
        "_enable_object_reconstruction", False
    )
    _redis_max_memory: Optional[int] = kwargs.pop("_redis_max_memory", None)
    _plasma_directory: Optional[str] = kwargs.pop("_plasma_directory", None)
    _node_ip_address: str = kwargs.pop(
        "_node_ip_address", ray_constants.NODE_DEFAULT_IP
    )
    _driver_object_store_memory: Optional[int] = kwargs.pop(
        "_driver_object_store_memory", None
    )
    _memory: Optional[int] = kwargs.pop("_memory", None)
    _redis_password: str = kwargs.pop(
        "_redis_password", ray_constants.REDIS_DEFAULT_PASSWORD
    )
    _temp_dir: Optional[str] = kwargs.pop("_temp_dir", None)
    _metrics_export_port: Optional[int] = kwargs.pop("_metrics_export_port", None)
    _system_config: Optional[Dict[str, str]] = kwargs.pop("_system_config", None)
    _tracing_startup_hook: Optional[Callable] = kwargs.pop(
        "_tracing_startup_hook", None
    )
    _node_name: str = kwargs.pop("_node_name", None)
    # Fix for https://github.com/ray-project/ray/issues/26729
    _skip_env_hook: bool = kwargs.pop("_skip_env_hook", False)

    # If available, use RAY_ADDRESS to override if the address was left
    # unspecified, or set to "auto" in the call to init
    address_env_var = os.environ.get(ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if address_env_var:
        if address is None or address == "auto":
            address = address_env_var
            logger.info(
                f"Using address {address_env_var} set in the environment "
                f"variable {ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE}"
            )

    if address is not None and "://" in address:
        # Address specified a protocol, use ray client
        builder = ray.client(address, _deprecation_warn_enabled=False)

        # Forward any keyword arguments that were changed from their default
        # values to the builder
        init_sig = inspect.signature(init)
        passed_kwargs = {}
        for argument_name, param_obj in init_sig.parameters.items():
            if argument_name in {"kwargs", "address"}:
                # kwargs and address are handled separately
                continue
            default_value = param_obj.default
            passed_value = locals()[argument_name]
            if passed_value != default_value:
                # passed value is different than default, pass to the client
                # builder
                passed_kwargs[argument_name] = passed_value
        passed_kwargs.update(kwargs)
        builder._init_args(**passed_kwargs)
        ctx = builder.connect()
        from ray._private.usage import usage_lib

        if passed_kwargs.get("allow_multiple") is True:
            with ctx:
                usage_lib.put_pre_init_usage_stats()
        else:
            usage_lib.put_pre_init_usage_stats()

        usage_lib.record_library_usage("client")
        return ctx

    if kwargs.get("allow_multiple"):
        raise RuntimeError(
            "`allow_multiple` argument is passed to `ray.init` when the "
            "ray client is not used ("
            f"https://docs.ray.io/en/{get_ray_doc_version()}/cluster"
            "/running-applications/job-submission"
            "/ray-client.html#connect-to-multiple-ray-clusters-experimental). "
            "Do not pass the `allow_multiple` to `ray.init` to fix the issue."
        )

    if kwargs:
        # User passed in extra keyword arguments but isn't connecting through
        # ray client. Raise an error, since most likely a typo in keyword
        unknown = ", ".join(kwargs)
        raise RuntimeError(f"Unknown keyword argument(s): {unknown}")

    # Try to increase the file descriptor limit, which is too low by
    # default for Ray: https://github.com/ray-project/ray/issues/11239
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < hard:
            # https://github.com/ray-project/ray/issues/12059
            soft = max(soft, min(hard, 65536))
            logger.debug(
                f"Automatically increasing RLIMIT_NOFILE to max value of {hard}"
            )
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
            except ValueError:
                logger.debug("Failed to raise limit.")
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 4096:
            logger.warning(
                "File descriptor limit {} is too low for production "
                "servers and may result in connection errors. "
                "At least 8192 is recommended. --- "
                "Fix with 'ulimit -n 8192'".format(soft)
            )
    except ImportError:
        logger.debug("Could not import resource module (on Windows)")
        pass

    if RAY_JOB_CONFIG_JSON_ENV_VAR in os.environ:
        if runtime_env:
            logger.warning(
                "Both RAY_JOB_CONFIG_JSON_ENV_VAR and ray.init(runtime_env) "
                "are provided, only using JSON_ENV_VAR to construct "
                "job_config. Please ensure no runtime_env is used in driver "
                "script's ray.init() when using job submission API."
            )
        # Set runtime_env in job_config if passed as env variable, such as
        # ray job submission with driver script executed in subprocess
        job_config_json = json.loads(os.environ.get(RAY_JOB_CONFIG_JSON_ENV_VAR))
        job_config = ray.job_config.JobConfig.from_json(job_config_json)

        if ray_constants.RAY_RUNTIME_ENV_HOOK in os.environ and not _skip_env_hook:
            runtime_env = _load_class(os.environ[ray_constants.RAY_RUNTIME_ENV_HOOK])(
                job_config.runtime_env
            )
            job_config.set_runtime_env(runtime_env)

    # RAY_JOB_CONFIG_JSON_ENV_VAR is only set at ray job manager level and has
    # higher priority in case user also provided runtime_env for ray.init()
    else:
        if ray_constants.RAY_RUNTIME_ENV_HOOK in os.environ and not _skip_env_hook:
            runtime_env = _load_class(os.environ[ray_constants.RAY_RUNTIME_ENV_HOOK])(
                runtime_env
            )

        if runtime_env:
            # Set runtime_env in job_config if passed in as part of ray.init()
            if job_config is None:
                job_config = ray.job_config.JobConfig()
            job_config.set_runtime_env(runtime_env)

    if _node_ip_address is not None:
        node_ip_address = services.resolve_ip_for_localhost(_node_ip_address)
    raylet_ip_address = node_ip_address

    redis_address, gcs_address = None, None
    bootstrap_address = services.canonicalize_bootstrap_address(address, _temp_dir)
    if bootstrap_address is not None:
        gcs_address = bootstrap_address
        logger.info("Connecting to existing Ray cluster at address: %s...", gcs_address)

    if local_mode:
        driver_mode = LOCAL_MODE
        warnings.warn(
            "DeprecationWarning: local mode is an experimental feature that is no "
            "longer maintained and will be removed in the future."
            "For debugging consider using Ray debugger. ",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        driver_mode = SCRIPT_MODE

    global _global_node

    if global_worker.connected:
        if ignore_reinit_error:
            logger.info("Calling ray.init() again after it has already been called.")
            node_id = global_worker.core_worker.get_current_node_id()
            return RayContext(dict(_global_node.address_info, node_id=node_id.hex()))
        else:
            raise RuntimeError(
                "Maybe you called ray.init twice by accident? "
                "This error can be suppressed by passing in "
                "'ignore_reinit_error=True' or by calling "
                "'ray.shutdown()' prior to 'ray.init()'."
            )

    _system_config = _system_config or {}
    if not isinstance(_system_config, dict):
        raise TypeError("The _system_config must be a dict.")

    if bootstrap_address is None:
        # In this case, we need to start a new cluster.

        # Don't collect usage stats in ray.init() unless it's a nightly wheel.
        from ray._private.usage import usage_lib

        if usage_lib.is_nightly_wheel():
            usage_lib.show_usage_stats_prompt(cli=False)
        else:
            usage_lib.set_usage_stats_enabled_via_env_var(False)

        # Use a random port by not specifying Redis port / GCS server port.
        ray_params = ray._private.parameter.RayParams(
            node_ip_address=node_ip_address,
            raylet_ip_address=raylet_ip_address,
            object_ref_seed=None,
            driver_mode=driver_mode,
            redirect_output=None,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources,
            num_redis_shards=None,
            redis_max_clients=None,
            redis_password=_redis_password,
            plasma_directory=_plasma_directory,
            huge_pages=None,
            include_dashboard=include_dashboard,
            dashboard_host=dashboard_host,
            dashboard_port=dashboard_port,
            memory=_memory,
            object_store_memory=object_store_memory,
            redis_max_memory=_redis_max_memory,
            plasma_store_socket_name=None,
            temp_dir=_temp_dir,
            storage=storage,
            # We need to disable it if runtime env is not set.
            # Uploading happens after core worker is created. And we should
            # prevent default worker being created before uploading.
            # TODO (yic): Have a separate connection to gcs client when
            # removal redis is done. The uploading should happen before this
            # one.
            start_initial_python_workers_for_first_job=(
                job_config is None or job_config.runtime_env is None
            ),
            _system_config=_system_config,
            enable_object_reconstruction=_enable_object_reconstruction,
            metrics_export_port=_metrics_export_port,
            tracing_startup_hook=_tracing_startup_hook,
            node_name=_node_name,
        )
        # Start the Ray processes. We set shutdown_at_exit=False because we
        # shutdown the node in the ray.shutdown call that happens in the atexit
        # handler. We still spawn a reaper process in case the atexit handler
        # isn't called.
        _global_node = ray._private.node.Node(
            head=True,
            shutdown_at_exit=False,
            spawn_reaper=True,
            ray_params=ray_params,
        )
    else:
        # In this case, we are connecting to an existing cluster.
        if num_cpus is not None or num_gpus is not None:
            raise ValueError(
                "When connecting to an existing cluster, num_cpus "
                "and num_gpus must not be provided."
            )
        if resources is not None:
            raise ValueError(
                "When connecting to an existing cluster, "
                "resources must not be provided."
            )
        if object_store_memory is not None:
            raise ValueError(
                "When connecting to an existing cluster, "
                "object_store_memory must not be provided."
            )
        if storage is not None:
            raise ValueError(
                "When connecting to an existing cluster, "
                "storage must not be provided."
            )
        if _system_config is not None and len(_system_config) != 0:
            raise ValueError(
                "When connecting to an existing cluster, "
                "_system_config must not be provided."
            )
        if _enable_object_reconstruction:
            raise ValueError(
                "When connecting to an existing cluster, "
                "_enable_object_reconstruction must not be provided."
            )
        if _node_name is not None:
            raise ValueError(
                "_node_name cannot be configured when connecting to "
                "an existing cluster."
            )

        # In this case, we only need to connect the node.
        ray_params = ray._private.parameter.RayParams(
            node_ip_address=node_ip_address,
            raylet_ip_address=raylet_ip_address,
            gcs_address=gcs_address,
            redis_address=redis_address,
            redis_password=_redis_password,
            object_ref_seed=None,
            temp_dir=_temp_dir,
            _system_config=_system_config,
            enable_object_reconstruction=_enable_object_reconstruction,
            metrics_export_port=_metrics_export_port,
        )
        try:
            _global_node = ray._private.node.Node(
                ray_params,
                head=False,
                shutdown_at_exit=False,
                spawn_reaper=False,
                connect_only=True,
            )
        except ConnectionError:
            if gcs_address == ray._private.utils.read_ray_address(_temp_dir):
                logger.info(
                    "Failed to connect to the default Ray cluster address at "
                    f"{gcs_address}. This is most likely due to a previous Ray "
                    "instance that has since crashed. To reset the default "
                    "address to connect to, run `ray stop` or restart Ray with "
                    "`ray start`."
                )
            raise

    # Log a message to find the Ray address that we connected to and the
    # dashboard URL.
    if ray_constants.RAY_OVERRIDE_DASHBOARD_URL in os.environ:
        dashboard_url = os.environ.get(ray_constants.RAY_OVERRIDE_DASHBOARD_URL)
    else:
        dashboard_url = _global_node.webui_url
    # Add http protocol to dashboard URL if it doesn't
    # already contain a protocol.
    if dashboard_url and not urlparse(dashboard_url).scheme:
        dashboard_url = "http://" + dashboard_url

    # We logged the address before attempting the connection, so we don't need
    # to log it again.
    info_str = "Connected to Ray cluster."
    if gcs_address is None:
        info_str = "Started a local Ray instance."
    if dashboard_url:
        logger.info(
            info_str + " View the dashboard at %s%s%s %s%s",
            colorama.Style.BRIGHT,
            colorama.Fore.GREEN,
            dashboard_url,
            colorama.Fore.RESET,
            colorama.Style.NORMAL,
        )
    else:
        logger.info(info_str)

    connect(
        _global_node,
        _global_node.session_name,
        mode=driver_mode,
        log_to_driver=log_to_driver,
        worker=global_worker,
        driver_object_store_memory=_driver_object_store_memory,
        job_id=None,
        namespace=namespace,
        job_config=job_config,
        entrypoint=ray._private.utils.get_entrypoint_name(),
    )
    if job_config and job_config.code_search_path:
        global_worker.set_load_code_from_local(True)
    else:
        # Because `ray.shutdown()` doesn't reset this flag, for multiple
        # sessions in one process, the 2nd `ray.init()` will reuse the
        # flag of last session. For example:
        #     ray.init(load_code_from_local=True)
        #     ray.shutdown()
        #     ray.init()
        #     # Here the flag `load_code_from_local` is still True if we
        #     # doesn't have this `else` branch.
        #     ray.shutdown()
        global_worker.set_load_code_from_local(False)

    for hook in _post_init_hooks:
        hook()

    node_id = global_worker.core_worker.get_current_node_id()
    global_node_address_info = _global_node.address_info.copy()
    global_node_address_info["webui_url"] = _remove_protocol_from_url(dashboard_url)
    return RayContext(dict(global_node_address_info, node_id=node_id.hex()))


# Functions to run as callback after a successful ray init.
_post_init_hooks = []


@PublicAPI
@client_mode_hook(auto_init=False)
def shutdown(_exiting_interpreter: bool = False):
    """Disconnect the worker, and terminate processes started by ray.init().

    This will automatically run at the end when a Python process that uses Ray
    exits. It is ok to run this twice in a row. The primary use case for this
    function is to cleanup state between tests.

    Note that this will clear any remote function definitions, actor
    definitions, and existing actors, so if you wish to use any previously
    defined remote functions or actors after calling ray.shutdown(), then you
    need to redefine them. If they were defined in an imported module, then you
    will need to reload the module.

    Args:
        _exiting_interpreter: True if this is called by the atexit hook
            and false otherwise. If we are exiting the interpreter, we will
            wait a little while to print any extra error messages.
    """
    if _exiting_interpreter and global_worker.mode == SCRIPT_MODE:
        # This is a duration to sleep before shutting down everything in order
        # to make sure that log messages finish printing.
        time.sleep(0.5)
    disconnect(_exiting_interpreter)

    # disconnect internal kv
    if hasattr(global_worker, "gcs_client"):
        del global_worker.gcs_client
    _internal_kv_reset()

    # We need to destruct the core worker here because after this function,
    # we will tear down any processes spawned by ray.init() and the background
    # IO thread in the core worker doesn't currently handle that gracefully.
    if hasattr(global_worker, "core_worker"):
        global_worker.core_worker.shutdown()
        del global_worker.core_worker
    # We need to reset function actor manager to clear the context
    global_worker.function_actor_manager = FunctionActorManager(global_worker)
    # Disconnect global state from GCS.
    ray._private.state.state.disconnect()

    # Shut down the Ray processes.
    global _global_node
    if _global_node is not None:
        if _global_node.is_head():
            _global_node.destroy_external_storage()
        _global_node.kill_all_processes(check_alive=False, allow_graceful=True)
        _global_node = None
    storage._reset()

    # TODO(rkn): Instead of manually resetting some of the worker fields, we
    # should simply set "global_worker" to equal "None" or something like that.
    global_worker.set_mode(None)


atexit.register(shutdown, True)


# TODO(edoakes): this should only be set in the driver.
def sigterm_handler(signum, frame):
    sys.exit(signum)


try:
    ray._private.utils.set_sigterm_handler(sigterm_handler)
except ValueError:
    logger.warning(
        "Failed to set SIGTERM handler, processes might"
        "not be cleaned up properly on exit."
    )

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
    # If this is a driver, push the exception to GCS worker table.
    if global_worker.mode == SCRIPT_MODE and hasattr(global_worker, "worker_id"):
        error_message = "".join(traceback.format_tb(tb))
        worker_id = global_worker.worker_id
        worker_type = gcs_utils.DRIVER
        worker_info = {"exception": error_message}

        ray._private.state.state._check_connected()
        ray._private.state.state.add_worker(worker_id, worker_type, worker_info)
    # Call the normal excepthook.
    normal_excepthook(type, value, tb)


sys.excepthook = custom_excepthook


def print_to_stdstream(data):
    print_file = sys.stderr if data["is_err"] else sys.stdout
    print_worker_logs(data, print_file)


# Start time of this process, used for relative time logs.
t0 = time.time()
autoscaler_log_fyi_printed = False


def filter_autoscaler_events(lines: List[str]) -> Iterator[str]:
    """Given raw log lines from the monitor, return only autoscaler events.

    Autoscaler events are denoted by the ":event_summary:" magic token.
    """
    global autoscaler_log_fyi_printed

    if not ray_constants.AUTOSCALER_EVENTS:
        return

    # Print out autoscaler events only, ignoring other messages.
    for line in lines:
        if ray_constants.LOG_PREFIX_EVENT_SUMMARY in line:
            if not autoscaler_log_fyi_printed:
                yield (
                    "Tip: use `ray status` to view detailed "
                    "cluster status. To disable these "
                    "messages, set RAY_SCHEDULER_EVENTS=0."
                )
                autoscaler_log_fyi_printed = True
            # The event text immediately follows the ":event_summary:"
            # magic token.
            yield line.split(ray_constants.LOG_PREFIX_EVENT_SUMMARY)[1]


def time_string() -> str:
    """Return the relative time from the start of this job.

    For example, 15m30s.
    """
    delta = time.time() - t0
    hours = 0
    minutes = 0
    while delta > 3600:
        hours += 1
        delta -= 3600
    while delta > 60:
        minutes += 1
        delta -= 60
    output = ""
    if hours:
        output += f"{hours}h"
    if minutes:
        output += f"{minutes}m"
    output += f"{int(delta)}s"
    return output


# When we enter a breakpoint, worker logs are automatically disabled via this.
_worker_logs_enabled = True


def print_worker_logs(data: Dict[str, str], print_file: Any):
    if not _worker_logs_enabled:
        return

    def prefix_for(data: Dict[str, str]) -> str:
        """The PID prefix for this log line."""
        if data.get("pid") in ["autoscaler", "raylet"]:
            return ""
        else:
            res = "pid="
            if data.get("actor_name"):
                res = data["actor_name"] + " " + res
            elif data.get("task_name"):
                res = data["task_name"] + " " + res
            return res

    def message_for(data: Dict[str, str], line: str) -> str:
        """The printed message of this log line."""
        if ray_constants.LOG_PREFIX_INFO_MESSAGE in line:
            return line.split(ray_constants.LOG_PREFIX_INFO_MESSAGE)[1]
        return line

    def color_for(data: Dict[str, str], line: str) -> str:
        """The color for this log line."""
        if (
            data.get("pid") == "raylet"
            and ray_constants.LOG_PREFIX_INFO_MESSAGE not in line
        ):
            return colorama.Fore.YELLOW
        elif data.get("pid") == "autoscaler":
            if "Error:" in line or "Warning:" in line:
                return colorama.Style.BRIGHT + colorama.Fore.YELLOW
            else:
                return colorama.Style.BRIGHT + colorama.Fore.CYAN
        else:
            return colorama.Fore.CYAN

    if data.get("pid") == "autoscaler":
        pid = "autoscaler +{}".format(time_string())
        lines = filter_autoscaler_events(data.get("lines", []))
    else:
        pid = data.get("pid")
        lines = data.get("lines", [])

    if data.get("ip") == data.get("localhost"):
        for line in lines:
            print(
                "{}{}({}{}){} {}".format(
                    colorama.Style.DIM,
                    color_for(data, line),
                    prefix_for(data),
                    pid,
                    colorama.Style.RESET_ALL,
                    message_for(data, line),
                ),
                file=print_file,
            )
    else:
        for line in lines:
            print(
                "{}{}({}{}, ip={}){} {}".format(
                    colorama.Style.DIM,
                    color_for(data, line),
                    prefix_for(data),
                    pid,
                    data.get("ip"),
                    colorama.Style.RESET_ALL,
                    message_for(data, line),
                ),
                file=print_file,
            )


def listen_error_messages(worker, threads_stopped):
    """Listen to error messages in the background on the driver.

    This runs in a separate thread on the driver and pushes (error, time)
    tuples to be published.

    Args:
        worker: The worker class that this thread belongs to.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
    """

    # TODO: we should just subscribe to the errors for this specific job.
    worker.gcs_error_subscriber.subscribe()

    try:
        if _internal_kv_initialized():
            # Get any autoscaler errors that occurred before the call to
            # subscribe.
            error_message = _internal_kv_get(ray_constants.DEBUG_AUTOSCALING_ERROR)
            if error_message is not None:
                logger.warning(error_message.decode())

        while True:
            # Exit if received a signal that the thread should stop.
            if threads_stopped.is_set():
                return

            _, error_data = worker.gcs_error_subscriber.poll()
            if error_data is None:
                continue
            if error_data.job_id not in [
                worker.current_job_id.binary(),
                JobID.nil().binary(),
            ]:
                continue

            error_message = error_data.error_message
            if error_data.type == ray_constants.TASK_PUSH_ERROR:
                # TODO(ekl) remove task push errors entirely now that we have
                # the separate unhandled exception handler.
                pass
            else:
                logger.warning(error_message)
    except (OSError, ConnectionError) as e:
        logger.error(f"listen_error_messages: {e}")


@PublicAPI
@client_mode_hook(auto_init=False)
def is_initialized() -> bool:
    """Check if ray.init has been called yet.

    Returns:
        True if ray.init has already been called and false otherwise.
    """
    return ray._private.worker.global_worker.connected


def connect(
    node,
    session_name: str,
    mode=WORKER_MODE,
    log_to_driver: bool = False,
    worker=global_worker,
    driver_object_store_memory: Optional[int] = None,
    job_id=None,
    namespace: Optional[str] = None,
    job_config=None,
    runtime_env_hash: int = 0,
    startup_token: int = 0,
    ray_debugger_external: bool = False,
    entrypoint: str = "",
):
    """Connect this worker to the raylet, to Plasma, and to GCS.

    Args:
        node (ray._private.node.Node): The node to connect.
        session_name: The session name (cluster id) of this cluster.
        mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, and LOCAL_MODE.
        log_to_driver: If true, then output from all of the worker
            processes on all nodes will be directed to the driver.
        worker: The ray.Worker instance.
        driver_object_store_memory: Deprecated.
        job_id: The ID of job. If it's None, then we will generate one.
        namespace: Namespace to use.
        job_config (ray.job_config.JobConfig): The job configuration.
        runtime_env_hash: The hash of the runtime env for this worker.
        startup_token: The startup token of the process assigned to
            it during startup as a command line argument.
        ray_debugger_external: If True, make the debugger external to the
            node this worker is running on.
        entrypoint: The name of the entrypoint script. Ignored unless the
            mode != SCRIPT_MODE
    """
    # Do some basic checking to make sure we didn't call ray.init twice.
    error_message = "Perhaps you called ray.init twice by accident?"
    assert not worker.connected, error_message
    assert worker.cached_functions_to_run is not None, error_message

    # Enable nice stack traces on SIGSEGV etc.
    try:
        if not faulthandler.is_enabled():
            faulthandler.enable(all_threads=False)
    except io.UnsupportedOperation:
        pass  # ignore

    worker.gcs_client = node.get_gcs_client()
    assert worker.gcs_client is not None
    _initialize_internal_kv(worker.gcs_client)
    ray._private.state.state._initialize_global_state(
        ray._raylet.GcsClientOptions.from_gcs_address(node.gcs_address)
    )
    worker.gcs_publisher = GcsPublisher(address=worker.gcs_client.address)
    worker.gcs_error_subscriber = GcsErrorSubscriber(address=worker.gcs_client.address)
    worker.gcs_log_subscriber = GcsLogSubscriber(address=worker.gcs_client.address)
    worker.gcs_function_key_subscriber = GcsFunctionKeySubscriber(
        address=worker.gcs_client.address
    )

    # Initialize some fields.
    if mode in (WORKER_MODE, RESTORE_WORKER_MODE, SPILL_WORKER_MODE):
        # We should not specify the job_id if it's `WORKER_MODE`.
        assert job_id is None
        job_id = JobID.nil()
    else:
        # This is the code path of driver mode.
        if job_id is None:
            job_id = ray._private.state.next_job_id()

    if mode is not SCRIPT_MODE and mode is not LOCAL_MODE and setproctitle:
        process_name = ray_constants.WORKER_PROCESS_TYPE_IDLE_WORKER
        if mode is SPILL_WORKER_MODE:
            process_name = ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE
        elif mode is RESTORE_WORKER_MODE:
            process_name = ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE
        setproctitle.setproctitle(process_name)

    if not isinstance(job_id, JobID):
        raise TypeError("The type of given job id must be JobID.")

    # All workers start out as non-actors. A worker can be turned into an actor
    # after it is created.
    worker.node = node
    worker.set_mode(mode)

    # For driver's check that the version information matches the version
    # information that the Ray cluster was started with.
    try:
        node.check_version_info()
    except Exception as e:
        if mode == SCRIPT_MODE:
            raise e
        elif mode == WORKER_MODE:
            traceback_str = traceback.format_exc()
            ray._private.utils.publish_error_to_driver(
                ray_constants.VERSION_MISMATCH_PUSH_ERROR,
                traceback_str,
                gcs_publisher=worker.gcs_publisher,
                num_retries=1,
            )

    driver_name = ""
    log_stdout_file_path = ""
    log_stderr_file_path = ""
    interactive_mode = False
    if mode == SCRIPT_MODE:
        import __main__ as main

        if hasattr(main, "__file__"):
            driver_name = main.__file__
        else:
            interactive_mode = True
            driver_name = "INTERACTIVE MODE"
    elif not LOCAL_MODE:
        raise ValueError("Invalid worker mode. Expected DRIVER, WORKER or LOCAL.")

    gcs_options = ray._raylet.GcsClientOptions.from_gcs_address(node.gcs_address)
    if job_config is None:
        job_config = ray.job_config.JobConfig()

    if namespace is not None:
        ray._private.utils.validate_namespace(namespace)

        # The namespace field of job config may have already been set in code
        # paths such as the client.
        job_config.set_ray_namespace(namespace)

    # Make sure breakpoint() in the user's code will
    # invoke the Ray debugger if we are in a worker or actor process
    # (but not on the driver).
    if mode == WORKER_MODE:
        os.environ["PYTHONBREAKPOINT"] = "ray.util.rpdb.set_trace"
    else:
        # Add hook to suppress worker logs during breakpoint.
        os.environ["PYTHONBREAKPOINT"] = "ray.util.rpdb._driver_set_trace"

    worker.ray_debugger_external = ray_debugger_external

    # If it's a driver and it's not coming from ray client, we'll prepare the
    # environment here. If it's ray client, the environment will be prepared
    # at the server side.
    if mode == SCRIPT_MODE and not job_config.client_job and job_config.runtime_env:
        scratch_dir: str = worker.node.get_runtime_env_dir_path()
        runtime_env = job_config.runtime_env or {}
        runtime_env = upload_py_modules_if_needed(
            runtime_env, scratch_dir, logger=logger
        )
        runtime_env = upload_working_dir_if_needed(
            runtime_env, scratch_dir, logger=logger
        )
        # Remove excludes, it isn't relevant after the upload step.
        runtime_env.pop("excludes", None)
        job_config.set_runtime_env(runtime_env)

    if mode == SCRIPT_MODE:
        # Add the directory containing the script that is running to the Python
        # paths of the workers. Also add the current directory. Note that this
        # assumes that the directory structures on the machines in the clusters
        # are the same.
        # When using an interactive shell, there is no script directory.
        code_paths = []
        if not interactive_mode:
            script_directory = os.path.dirname(os.path.realpath(sys.argv[0]))
            # If driver's sys.path doesn't include the script directory
            # (e.g driver is started via `python -m`,
            # see https://peps.python.org/pep-0338/),
            # then we shouldn't add it to the workers.
            if script_directory in sys.path:
                code_paths.append(script_directory)
        # In client mode, if we use runtime envs with "working_dir", then
        # it'll be handled automatically.  Otherwise, add the current dir.
        if not job_config.client_job and not job_config.runtime_env_has_working_dir():
            current_directory = os.path.abspath(os.path.curdir)
            code_paths.append(current_directory)
        if len(code_paths) != 0:
            job_config.py_driver_sys_path.extend(code_paths)

    serialized_job_config = job_config.serialize()
    if not node.should_redirect_logs():
        # Logging to stderr, so give core worker empty logs directory.
        logs_dir = ""
    else:
        logs_dir = node.get_logs_dir_path()
    worker.core_worker = ray._raylet.CoreWorker(
        mode,
        node.plasma_store_socket_name,
        node.raylet_socket_name,
        job_id,
        gcs_options,
        logs_dir,
        node.node_ip_address,
        node.node_manager_port,
        node.raylet_ip_address,
        (mode == LOCAL_MODE),
        driver_name,
        log_stdout_file_path,
        log_stderr_file_path,
        serialized_job_config,
        node.metrics_agent_port,
        runtime_env_hash,
        startup_token,
        session_name,
        "" if mode != SCRIPT_MODE else entrypoint,
    )

    # Notify raylet that the core worker is ready.
    worker.core_worker.notify_raylet()

    if driver_object_store_memory is not None:
        logger.warning(
            "`driver_object_store_memory` is deprecated"
            " and will be removed in the future."
        )

    # Setup import thread, but defer the start up of
    # import thread until job_config is initialized.
    # (python/ray/_raylet.pyx maybe_initialize_job_config)
    if mode not in (RESTORE_WORKER_MODE, SPILL_WORKER_MODE):
        worker.import_thread = import_thread.ImportThread(
            worker, mode, worker.threads_stopped
        )

    # If this is a driver running in SCRIPT_MODE, start a thread to print error
    # messages asynchronously in the background. Ideally the scheduler would
    # push messages to the driver's worker service, but we ran into bugs when
    # trying to properly shutdown the driver's worker service, so we are
    # temporarily using this implementation which constantly queries the
    # scheduler for new error messages.
    if mode == SCRIPT_MODE:
        worker.listener_thread = threading.Thread(
            target=listen_error_messages,
            name="ray_listen_error_messages",
            args=(worker, worker.threads_stopped),
        )
        worker.listener_thread.daemon = True
        worker.listener_thread.start()
        if log_to_driver:
            global_worker_stdstream_dispatcher.add_handler(
                "ray_print_logs", print_to_stdstream
            )
            worker.logger_thread = threading.Thread(
                target=worker.print_logs, name="ray_print_logs"
            )
            worker.logger_thread.daemon = True
            worker.logger_thread.start()

    if mode == SCRIPT_MODE:
        # TODO(rkn): Here we first export functions to run, then remote
        # functions. The order matters. For example, one of the functions to
        # run may set the Python path, which is needed to import a module used
        # to define a remote function. We may want to change the order to
        # simply be the order in which the exports were defined on the driver.
        # In addition, we will need to retain the ability to decide what the
        # first few exports are (mostly to set the Python path). Additionally,
        # note that the first exports to be defined on the driver will be the
        # ones defined in separate modules that are imported by the driver.
        # Export cached functions_to_run.
        for function in worker.cached_functions_to_run:
            worker.run_function_on_all_workers(function)
    worker.cached_functions_to_run = None

    # Setup tracing here
    tracing_hook_val = worker.gcs_client.internal_kv_get(
        b"tracing_startup_hook", ray_constants.KV_NAMESPACE_TRACING
    )
    if tracing_hook_val is not None:
        ray.util.tracing.tracing_helper._global_is_tracing_enabled = True
        if not getattr(ray, "__traced__", False):
            _setup_tracing = _import_from_string(tracing_hook_val.decode("utf-8"))
            _setup_tracing()
            ray.__traced__ = True


def disconnect(exiting_interpreter=False):
    """Disconnect this worker from the raylet and object store."""
    # Reset the list of cached remote functions and actors so that if more
    # remote functions or actors are defined and then connect is called again,
    # the remote functions will be exported. This is mostly relevant for the
    # tests.
    worker = global_worker
    if worker.connected:
        # Shutdown all of the threads that we've started. TODO(rkn): This
        # should be handled cleanly in the worker object's destructor and not
        # in this disconnect method.
        worker.threads_stopped.set()
        worker.gcs_function_key_subscriber.close()
        worker.gcs_error_subscriber.close()
        worker.gcs_log_subscriber.close()
        if hasattr(worker, "import_thread"):
            worker.import_thread.join_import_thread()
        if hasattr(worker, "listener_thread"):
            worker.listener_thread.join()
        if hasattr(worker, "logger_thread"):
            worker.logger_thread.join()
        worker.threads_stopped.clear()

        worker._session_index += 1

        global_worker_stdstream_dispatcher.remove_handler("ray_print_logs")

    worker.node = None  # Disconnect the worker from the node.
    worker.cached_functions_to_run = []
    worker.serialization_context_map.clear()
    try:
        ray_actor = ray.actor
    except AttributeError:
        ray_actor = None  # This can occur during program termination
    if ray_actor is not None:
        ray_actor._ActorClassMethodMetadata.reset_cache()


def start_import_thread():
    """Start the import thread if the worker is connected."""
    worker = global_worker
    worker.check_connected()

    assert _mode() not in (
        RESTORE_WORKER_MODE,
        SPILL_WORKER_MODE,
    ), "import thread can not be used in IO workers."
    if worker.import_thread and ray._raylet.Config.start_python_importer_thread():
        worker.import_thread.start()


@contextmanager
def _changeproctitle(title, next_title):
    if _mode() is not LOCAL_MODE:
        setproctitle.setproctitle(title)
    try:
        yield
    finally:
        if _mode() is not LOCAL_MODE:
            setproctitle.setproctitle(next_title)


@DeveloperAPI
def show_in_dashboard(message: str, key: str = "", dtype: str = "text"):
    """Display message in dashboard.

    Display message for the current task or actor in the dashboard.
    For example, this can be used to display the status of a long-running
    computation.

    Args:
        message: Message to be displayed.
        key: The key name for the message. Multiple message under
            different keys will be displayed at the same time. Messages
            under the same key will be overridden.
        dtype: The type of message for rendering. One of the
            following: text, html.
    """
    worker = global_worker
    worker.check_connected()

    acceptable_dtypes = {"text", "html"}
    assert dtype in acceptable_dtypes, f"dtype accepts only: {acceptable_dtypes}"

    message_wrapped = {"message": message, "dtype": dtype}
    message_encoded = json.dumps(message_wrapped).encode()

    worker.core_worker.set_webui_display(key.encode(), message_encoded)


# Global variable to make sure we only send out the warning once.
blocking_get_inside_async_warned = False


@overload
def get(
    object_refs: "Sequence[ObjectRef[Any]]", *, timeout: Optional[float] = None
) -> List[Any]:
    ...


@overload
def get(
    object_refs: "Sequence[ObjectRef[R]]", *, timeout: Optional[float] = None
) -> List[R]:
    ...


@overload
def get(object_refs: "ObjectRef[R]", *, timeout: Optional[float] = None) -> R:
    ...


@PublicAPI
@client_mode_hook(auto_init=True)
def get(
    object_refs: Union[ray.ObjectRef, Sequence[ray.ObjectRef]],
    *,
    timeout: Optional[float] = None,
) -> Union[Any, List[Any]]:
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ref is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_refs is a list, then the objects
    corresponding to each object in the list will be returned.

    Ordering for an input list of object refs is preserved for each object
    returned. That is, if an object ref to A precedes an object ref to B in the
    input list, then A will precede B in the returned list.

    This method will issue a warning if it's running inside async context,
    you can use ``await object_ref`` instead of ``ray.get(object_ref)``. For
    a list of object refs, you can use ``await asyncio.gather(*object_refs)``.

    Related patterns and anti-patterns:

    - :doc:`/ray-core/patterns/ray-get-loop`
    - :doc:`/ray-core/patterns/unnecessary-ray-get`
    - :doc:`/ray-core/patterns/ray-get-submission-order`
    - :doc:`/ray-core/patterns/ray-get-too-many-objects`


    Args:
        object_refs: Object ref of the object to get or a list of object refs
            to get.
        timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning. Set this to None will block until the
            corresponding object becomes available.
            WARNING: In future ray releases ``timeout=0`` will return the object
            immediately if it's available, else raise GetTimeoutError in accordance with
            the above docstring. The current behavior of blocking until objects become
            available of ``timeout=0`` is considered to be a bug, see
            https://github.com/ray-project/ray/issues/28465.

    Returns:
        A Python object or a list of Python objects.

    Raises:
        GetTimeoutError: A GetTimeoutError is raised if a timeout is set and
            the get takes longer than timeout to return.
        Exception: An exception is raised if the task that created the object
            or that created one of the objects raised an exception.
    """
    if timeout == 0:
        if os.environ.get("RAY_WARN_RAY_GET_TIMEOUT_ZERO", "1") == "1":
            import warnings

            warnings.warn(
                (
                    "Please use timeout=None if you expect ray.get() to block. "
                    "Setting timeout=0 in future ray releases will raise "
                    "GetTimeoutError if the objects references are not available. "
                    "You could suppress this warning by setting "
                    "RAY_WARN_RAY_GET_TIMEOUT_ZERO=0."
                ),
                UserWarning,
            )

        # Record this usage in telemetry
        import ray._private.usage.usage_lib as usage_lib

        usage_lib.record_extra_usage_tag(usage_lib.TagKey.RAY_GET_TIMEOUT_ZERO, "True")

    worker = global_worker
    worker.check_connected()

    if hasattr(worker, "core_worker") and worker.core_worker.current_actor_is_asyncio():
        global blocking_get_inside_async_warned
        if not blocking_get_inside_async_warned:
            logger.warning(
                "Using blocking ray.get inside async actor. "
                "This blocks the event loop. Please use `await` "
                "on object ref with asyncio.gather if you want to "
                "yield execution to the event loop instead."
            )
            blocking_get_inside_async_warned = True

    with profiling.profile("ray.get"):
        is_individual_id = isinstance(object_refs, ray.ObjectRef)
        if is_individual_id:
            object_refs = [object_refs]

        if not isinstance(object_refs, list):
            raise ValueError(
                "'object_refs' must either be an ObjectRef or a list of ObjectRefs."
            )

        # TODO(ujvl): Consider how to allow user to retrieve the ready objects.
        values, debugger_breakpoint = worker.get_objects(object_refs, timeout=timeout)
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                if isinstance(value, ray.exceptions.ObjectLostError):
                    worker.core_worker.dump_object_store_memory_usage()
                if isinstance(value, RayTaskError):
                    raise value.as_instanceof_cause()
                else:
                    raise value

        if is_individual_id:
            values = values[0]

        if debugger_breakpoint != b"":
            frame = sys._getframe().f_back
            rdb = ray.util.pdb._connect_ray_pdb(
                host=None,
                port=None,
                patch_stdstreams=False,
                quiet=None,
                breakpoint_uuid=debugger_breakpoint.decode()
                if debugger_breakpoint
                else None,
                debugger_external=worker.ray_debugger_external,
            )
            rdb.set_trace(frame=frame)

        return values


@PublicAPI
@client_mode_hook(auto_init=True)
def put(
    value: Any, *, _owner: Optional["ray.actor.ActorHandle"] = None
) -> "ray.ObjectRef":
    """Store an object in the object store.

    The object may not be evicted while a reference to the returned ID exists.

    Related patterns and anti-patterns:

    - :doc:`/ray-core/patterns/return-ray-put`
    - :doc:`/ray-core/patterns/pass-large-arg-by-value`
    - :doc:`/ray-core/patterns/closure-capture-large-objects`

    Args:
        value: The Python object to be stored.
        _owner [Experimental]: The actor that should own this object. This
            allows creating objects with lifetimes decoupled from that of the
            creating process. The owner actor must be passed a reference to the
            object prior to the object creator exiting, otherwise the reference
            will still be lost. *Note that this argument is an experimental API
            and should be avoided if possible.*

    Returns:
        The object ref assigned to this value.
    """
    worker = global_worker
    worker.check_connected()

    if _owner is None:
        serialize_owner_address = None
    elif isinstance(_owner, ray.actor.ActorHandle):
        # Ensure `ray._private.state.state.global_state_accessor` is not None
        ray._private.state.state._check_connected()
        owner_address = gcs_utils.ActorTableData.FromString(
            ray._private.state.state.global_state_accessor.get_actor_info(
                _owner._actor_id
            )
        ).address
        if len(owner_address.worker_id) == 0:
            raise RuntimeError(f"{_owner} is not alive, it's worker_id is empty!")
        serialize_owner_address = owner_address.SerializeToString()
    else:
        raise TypeError(f"Expect an `ray.actor.ActorHandle`, but got: {type(_owner)}")

    with profiling.profile("ray.put"):
        try:
            object_ref = worker.put_object(value, owner_address=serialize_owner_address)
        except ObjectStoreFullError:
            logger.info(
                "Put failed since the value was either too large or the "
                "store was full of pinned objects."
            )
            raise
        return object_ref


# Global variable to make sure we only send out the warning once.
blocking_wait_inside_async_warned = False


@PublicAPI
@client_mode_hook(auto_init=True)
def wait(
    object_refs: List["ray.ObjectRef"],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
    fetch_local: bool = True,
) -> Tuple[List["ray.ObjectRef"], List["ray.ObjectRef"]]:
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object refs.

    This method returns two lists. The first list consists of object refs that
    correspond to objects that are available in the object store. The second
    list corresponds to the rest of the object refs (which may or may not be
    ready).

    Ordering of the input list of object refs is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    This method will issue a warning if it's running inside an async context.
    Instead of ``ray.wait(object_refs)``, you can use
    ``await asyncio.wait(object_refs)``.

    Related patterns and anti-patterns:

    - :doc:`/ray-core/patterns/limit-pending-tasks`
    - :doc:`/ray-core/patterns/ray-get-submission-order`

    Args:
        object_refs: List of object refs for objects that may
            or may not be ready. Note that these IDs must be unique.
        num_returns: The number of object refs that should be returned.
        timeout: The maximum amount of time in seconds to wait before
            returning.
        fetch_local: If True, wait for the object to be downloaded onto
            the local node before returning it as ready. If False, ray.wait()
            will not trigger fetching of objects to the local node and will
            return immediately once the object is available anywhere in the
            cluster.

    Returns:
        A list of object refs that are ready and a list of the remaining object
        IDs.
    """
    worker = global_worker
    worker.check_connected()

    if (
        hasattr(worker, "core_worker")
        and worker.core_worker.current_actor_is_asyncio()
        and timeout != 0
    ):
        global blocking_wait_inside_async_warned
        if not blocking_wait_inside_async_warned:
            logger.debug(
                "Using blocking ray.wait inside async method. "
                "This blocks the event loop. Please use `await` "
                "on object ref with asyncio.wait. "
            )
            blocking_wait_inside_async_warned = True

    if isinstance(object_refs, ObjectRef):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, got a single ray.ObjectRef"
        )

    if not isinstance(object_refs, list):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, " f"got {type(object_refs)}"
        )

    if timeout is not None and timeout < 0:
        raise ValueError(
            "The 'timeout' argument must be nonnegative. " f"Received {timeout}"
        )

    for object_ref in object_refs:
        if not isinstance(object_ref, ObjectRef):
            raise TypeError(
                "wait() expected a list of ray.ObjectRef, "
                f"got list containing {type(object_ref)}"
            )

    worker.check_connected()
    # TODO(swang): Check main thread.
    with profiling.profile("ray.wait"):

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_refs) == 0:
            return [], []

        if len(object_refs) != len(set(object_refs)):
            raise ValueError("Wait requires a list of unique object refs.")
        if num_returns <= 0:
            raise ValueError("Invalid number of objects to return %d." % num_returns)
        if num_returns > len(object_refs):
            raise ValueError(
                "num_returns cannot be greater than the number "
                "of objects provided to ray.wait."
            )

        timeout = timeout if timeout is not None else 10**6
        timeout_milliseconds = int(timeout * 1000)
        ready_ids, remaining_ids = worker.core_worker.wait(
            object_refs,
            num_returns,
            timeout_milliseconds,
            worker.current_task_id,
            fetch_local,
        )
        return ready_ids, remaining_ids


@PublicAPI
@client_mode_hook(auto_init=True)
def get_actor(name: str, namespace: Optional[str] = None) -> "ray.actor.ActorHandle":
    """Get a handle to a named actor.

    Gets a handle to an actor with the given name. The actor must
    have been created with Actor.options(name="name").remote(). This
    works for both detached & non-detached actors.

    This method is a sync call and it'll timeout after 60s. This can be modified
    by setting OS env RAY_gcs_server_request_timeout_seconds before starting
    the cluster.

    Args:
        name: The name of the actor.
        namespace: The namespace of the actor, or None to specify the current
            namespace.

    Returns:
        ActorHandle to the actor.

    Raises:
        ValueError if the named actor does not exist.
    """
    if not name:
        raise ValueError("Please supply a non-empty value to get_actor")

    if namespace is not None:
        ray._private.utils.validate_namespace(namespace)

    worker = global_worker
    worker.check_connected()
    return worker.core_worker.get_named_actor_handle(name, namespace or "")


@PublicAPI
@client_mode_hook(auto_init=True)
def kill(actor: "ray.actor.ActorHandle", *, no_restart: bool = True):
    """Kill an actor forcefully.

    This will interrupt any running tasks on the actor, causing them to fail
    immediately. ``atexit`` handlers installed in the actor will not be run.

    If you want to kill the actor but let pending tasks finish,
    you can call ``actor.__ray_terminate__.remote()`` instead to queue a
    termination task. Any ``atexit`` handlers installed in the actor *will*
    be run in this case.

    If the actor is a detached actor, subsequent calls to get its handle via
    ray.get_actor will fail.

    Args:
        actor: Handle to the actor to kill.
        no_restart: Whether or not this actor should be restarted if
            it's a restartable actor.
    """
    worker = global_worker
    worker.check_connected()
    if not isinstance(actor, ray.actor.ActorHandle):
        raise ValueError(
            "ray.kill() only supported for actors. Got: {}.".format(type(actor))
        )
    worker.core_worker.kill_actor(actor._ray_actor_id, no_restart)


@PublicAPI
@client_mode_hook(auto_init=True)
def cancel(object_ref: "ray.ObjectRef", *, force: bool = False, recursive: bool = True):
    """Cancels a task according to the following conditions.

    If the specified task is pending execution, it will not be executed. If
    the task is currently executing, the behavior depends on the ``force``
    flag. When ``force=False``, a KeyboardInterrupt will be raised in Python
    and when ``force=True``, the executing task will immediately exit.
    If the task is already finished, nothing will happen.

    Only non-actor tasks can be canceled. Canceled tasks will not be
    retried (max_retries will not be respected).

    Calling ray.get on a canceled task will raise a TaskCancelledError or a
    WorkerCrashedError if ``force=True``.

    Args:
        object_ref: ObjectRef returned by the task
            that should be canceled.
        force: Whether to force-kill a running task by killing
            the worker that is running the task.
        recursive: Whether to try to cancel tasks submitted by the
            task specified.
    Raises:
        TypeError: This is also raised for actor tasks.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    if not isinstance(object_ref, ray.ObjectRef):
        raise TypeError(
            "ray.cancel() only supported for non-actor object refs. "
            f"Got: {type(object_ref)}."
        )
    return worker.core_worker.cancel_task(object_ref, force, recursive)


def _mode(worker=global_worker):
    """This is a wrapper around worker.mode.

    We use this wrapper so that in the remote decorator, we can call _mode()
    instead of worker.mode. The difference is that when we attempt to
    serialize remote functions, we don't attempt to serialize the worker
    object, which cannot be serialized.
    """
    return worker.mode


def _make_remote(function_or_class, options):
    if not function_or_class.__module__:
        function_or_class.__module__ = "global"

    if inspect.isfunction(function_or_class) or is_cython(function_or_class):
        ray_option_utils.validate_task_options(options, in_options=False)
        return ray.remote_function.RemoteFunction(
            Language.PYTHON,
            function_or_class,
            None,
            options,
        )

    if inspect.isclass(function_or_class):
        ray_option_utils.validate_actor_options(options, in_options=False)
        return ray.actor._make_actor(function_or_class, options)

    raise TypeError(
        "The @ray.remote decorator must be applied to either a function or a class."
    )


class RemoteDecorator(Protocol):
    @overload
    def __call__(self, __function: Callable[[], R]) -> RemoteFunctionNoArgs[R]:
        ...

    @overload
    def __call__(self, __function: Callable[[T0], R]) -> RemoteFunction0[R, T0]:
        ...

    @overload
    def __call__(self, __function: Callable[[T0, T1], R]) -> RemoteFunction1[R, T0, T1]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2], R]
    ) -> RemoteFunction2[R, T0, T1, T2]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3], R]
    ) -> RemoteFunction3[R, T0, T1, T2, T3]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4], R]
    ) -> RemoteFunction4[R, T0, T1, T2, T3, T4]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5], R]
    ) -> RemoteFunction5[R, T0, T1, T2, T3, T4, T5]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]
    ) -> RemoteFunction6[R, T0, T1, T2, T3, T4, T5, T6]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]
    ) -> RemoteFunction7[R, T0, T1, T2, T3, T4, T5, T6, T7]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
    ) -> RemoteFunction8[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
    ) -> RemoteFunction9[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]:
        ...

    # Pass on typing actors for now. The following makes it so no type errors
    # are generated for actors.
    @overload
    def __call__(self, __t: type) -> Any:
        ...


# Only used for type annotations as a placeholder
Undefined: Any = object()


@overload
def remote(__function: Callable[[], R]) -> RemoteFunctionNoArgs[R]:
    ...


@overload
def remote(__function: Callable[[T0], R]) -> RemoteFunction0[R, T0]:
    ...


@overload
def remote(__function: Callable[[T0, T1], R]) -> RemoteFunction1[R, T0, T1]:
    ...


@overload
def remote(__function: Callable[[T0, T1, T2], R]) -> RemoteFunction2[R, T0, T1, T2]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3], R]
) -> RemoteFunction3[R, T0, T1, T2, T3]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4], R]
) -> RemoteFunction4[R, T0, T1, T2, T3, T4]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5], R]
) -> RemoteFunction5[R, T0, T1, T2, T3, T4, T5]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]
) -> RemoteFunction6[R, T0, T1, T2, T3, T4, T5, T6]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]
) -> RemoteFunction7[R, T0, T1, T2, T3, T4, T5, T6, T7]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
) -> RemoteFunction8[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
) -> RemoteFunction9[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]:
    ...


# Pass on typing actors for now. The following makes it so no type errors
# are generated for actors.
@overload
def remote(__t: type) -> Any:
    ...


# Passing options
@overload
def remote(
    *,
    num_returns: Union[int, float] = Undefined,
    num_cpus: Union[int, float] = Undefined,
    num_gpus: Union[int, float] = Undefined,
    resources: Dict[str, float] = Undefined,
    accelerator_type: str = Undefined,
    memory: Union[int, float] = Undefined,
    max_calls: int = Undefined,
    max_restarts: int = Undefined,
    max_task_retries: int = Undefined,
    max_retries: int = Undefined,
    runtime_env: Dict[str, Any] = Undefined,
    retry_exceptions: bool = Undefined,
    scheduling_strategy: Union[
        None, Literal["DEFAULT"], Literal["SPREAD"], PlacementGroupSchedulingStrategy
    ] = Undefined,
) -> RemoteDecorator:
    ...


@PublicAPI
def remote(
    *args, **kwargs
) -> Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]:
    """Defines a remote function or an actor class.

    This function can be used as a decorator with no arguments
    to define a remote function or actor as follows:

    >>> import ray
    >>>
    >>> @ray.remote
    ... def f(a, b, c):
    ...     return a + b + c
    >>>
    >>> object_ref = f.remote(1, 2, 3)
    >>> result = ray.get(object_ref)
    >>> assert result == (1 + 2 + 3)
    >>>
    >>> @ray.remote
    ... class Foo:
    ...     def __init__(self, arg):
    ...         self.x = arg
    ...
    ...     def method(self, a):
    ...         return self.x + a
    >>>
    >>> actor_handle = Foo.remote(123)
    >>> object_ref = actor_handle.method.remote(321)
    >>> result = ray.get(object_ref)
    >>> assert result == (123 + 321)

    Equivalently, use a function call to create a remote function or actor.

    >>> def g(a, b, c):
    ...     return a + b + c
    >>>
    >>> remote_g = ray.remote(g)
    >>> object_ref = remote_g.remote(1, 2, 3)
    >>> assert ray.get(object_ref) == (1 + 2 + 3)

    >>> class Bar:
    ...     def __init__(self, arg):
    ...         self.x = arg
    ...
    ...     def method(self, a):
    ...         return self.x + a
    >>>
    >>> RemoteBar = ray.remote(Bar)
    >>> actor_handle = RemoteBar.remote(123)
    >>> object_ref = actor_handle.method.remote(321)
    >>> result = ray.get(object_ref)
    >>> assert result == (123 + 321)


    It can also be used with specific keyword arguments as follows:

    >>> @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
    ... def f():
    ...     return 1, 2
    >>>
    >>> @ray.remote(num_cpus=2, resources={"CustomResource": 1})
    ... class Foo:
    ...     def method(self):
    ...         return 1

    Remote task and actor objects returned by @ray.remote can also be
    dynamically modified with the same arguments as above using
    ``.options()`` as follows:

    >>> @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
    ... def f():
    ...     return 1, 2
    >>>
    >>> f_with_2_gpus = f.options(num_gpus=2) # doctest: +SKIP
    >>> object_ref = f_with_2_gpus.remote() # doctest: +SKIP
    >>> assert ray.get(object_ref) == (1, 2) # doctest: +SKIP

    >>> @ray.remote(num_cpus=2, resources={"CustomResource": 1})
    ... class Foo:
    ...     def method(self):
    ...         return 1
    >>>
    >>> Foo_with_no_resources = Foo.options(num_cpus=1, resources=None)
    >>> foo_actor = Foo_with_no_resources.remote()
    >>> assert ray.get(foo_actor.method.remote()) == 1


    A remote actor will be terminated when all actor handle to it
    in Python is deleted, which will cause them to complete any outstanding
    work and then shut down. If you only have 1 reference to an actor handle,
    calling ``del actor`` *could* trigger actor deletion. Note that your program
    may have multiple references to the same ActorHandle, and actor termination
    will not occur until the reference count goes to 0. See the Python
    documentation for more context about object deletion.
    https://docs.python.org/3.9/reference/datamodel.html#object.__del__

    If you want to kill actors immediately, you can also call ``ray.kill(actor)``.

    .. tip::
        Avoid repeatedly passing in large arguments to remote task or method calls.

        Instead, use ray.put to create a copy of the object in the object store.

        See :ref:`more info here <ray-pass-large-arg-by-value>`.

    Args:
        num_returns: This is only for *remote functions*. It specifies
            the number of object refs returned by the remote function
            invocation. Pass "dynamic" to allow the task to decide how many
            return values to return during execution, and the caller will
            receive an ObjectRef[ObjectRefGenerator] (note, this setting is
            experimental).
            See :ref:`dynamic generators <dynamic-generators>` for more details.
        num_cpus: The quantity of CPU cores to reserve
            for this task or for the lifetime of the actor.
        num_gpus: The quantity of GPUs to reserve
            for this task or for the lifetime of the actor.
        resources (Dict[str, float]): The quantity of various custom resources
            to reserve for this task or for the lifetime of the actor.
            This is a dictionary mapping strings (resource names) to floats.
        accelerator_type: If specified, requires that the task or actor run
            on a node with the specified type of accelerator.
            See `ray.util.accelerators` for accelerator types.
        memory: The heap memory request in bytes for this task/actor,
            rounded down to the nearest integer.
        max_calls: Only for *remote functions*. This specifies the
            maximum number of times that a given worker can execute
            the given remote function before it must exit
            (this can be used to address memory leaks in third-party
            libraries or to reclaim resources that cannot easily be
            released, e.g., GPU memory that was acquired by TensorFlow).
            By default this is infinite for CPU tasks and 1 for GPU tasks
            (to force GPU tasks to release resources after finishing).
        max_restarts: Only for *actors*. This specifies the maximum
            number of times that the actor should be restarted when it dies
            unexpectedly. The minimum valid value is 0 (default),
            which indicates that the actor doesn't need to be restarted.
            A value of -1 indicates that an actor should be restarted
            indefinitely.
        max_task_retries: Only for *actors*. How many times to
            retry an actor task if the task fails due to a system error,
            e.g., the actor has died. If set to -1, the system will
            retry the failed task until the task succeeds, or the actor
            has reached its max_restarts limit. If set to `n > 0`, the
            system will retry the failed task up to n times, after which the
            task will throw a `RayActorError` exception upon :obj:`ray.get`.
            Note that Python exceptions are not considered system errors
            and will not trigger retries.
        max_retries: Only for *remote functions*. This specifies
            the maximum number of times that the remote function
            should be rerun when the worker process executing it
            crashes unexpectedly. The minimum valid value is 0,
            the default is 4 (default), and a value of -1 indicates
            infinite retries.
        runtime_env (Dict[str, Any]): Specifies the runtime environment for
            this actor or task and its children. See
            :ref:`runtime-environments` for detailed documentation.
        retry_exceptions: Only for *remote functions*. This specifies whether
            application-level errors should be retried up to max_retries times.
            This can be a boolean or a list of exceptions that should be retried.
        scheduling_strategy: Strategy about how to
            schedule a remote function or actor. Possible values are
            None: ray will figure out the scheduling strategy to use, it
            will either be the PlacementGroupSchedulingStrategy using parent's
            placement group if parent has one and has
            placement_group_capture_child_tasks set to true,
            or "DEFAULT";
            "DEFAULT": default hybrid scheduling;
            "SPREAD": best effort spread scheduling;
            `PlacementGroupSchedulingStrategy`:
            placement group based scheduling;
            `NodeAffinitySchedulingStrategy`:
            node id based affinity scheduling.
        _metadata: Extended options for Ray libraries. For example,
            _metadata={"workflows.io/options": <workflow options>} for Ray workflows.

    """
    # "callable" returns true for both function and class.
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        # "args[0]" is the class or function under the decorator.
        return _make_remote(args[0], {})
    assert len(args) == 0 and len(kwargs) > 0, ray_option_utils.remote_args_error_string
    return functools.partial(_make_remote, options=kwargs)
