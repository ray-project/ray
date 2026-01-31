# source: _raylet.pxi
import asyncio
import concurrent.futures
import contextvars
import threading
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

# for FunctionDescriptor matching
try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

import ray._private.ray_constants as ray_constants
import ray.cloudpickle as ray_pickle
import ray.core.generated.common_pb2 as common_pb2
from ray._common.utils import decode

## import everything from _private, generated, _utils, and anything aliased
from ray._private import external_storage
from ray._private.async_compat import (
    get_new_event_loop,
    has_async_methods,
    is_async_func,
    sync_to_async,
)
from ray._private.client_mode_hook import disable_client_hook
from ray._private.custom_types import TensorTransportEnum
from ray._private.object_ref_generator import (
    DynamicObjectRefGenerator,
    ObjectRefGenerator,
)
from ray._private.utils import DeferSigint
from ray.actor import ActorHandle
from ray.core.generated.common_pb2 import (
    ActorDiedErrorContext,
    JobConfig,
    LineageReconstructionTask,
)
from ray.core.generated.gcs_service_pb2 import GetAllResourceUsageReply
from ray.experimental.channel.shared_memory_channel import (
    ReaderRefInfo,  # circular reference - .pyi only
)
from ray.includes.buffer import Buffer
from ray.includes.common import (
    GCS_AUTOSCALER_CLUSTER_CONFIG_KEY,
    GCS_AUTOSCALER_STATE_NAMESPACE,
    GCS_AUTOSCALER_V2_ENABLED_KEY,
    GCS_PID_KEY,
    IMPLICIT_RESOURCE_PREFIX,
    RESOURCE_UNIT_SCALING,
    STREAMING_GENERATOR_RETURN,
    WORKER_PROCESS_SETUP_HOOK_KEY_NAME_GCS,
    GcsClientOptions,
)
from ray.includes.function_descriptor import (
    CppFunctionDescriptor,
    EmptyFunctionDescriptor,
    FunctionDescriptor,
    JavaFunctionDescriptor,
    PythonFunctionDescriptor,
)
from ray.includes.gcs_client import InnerGcsClient
from ray.includes.gcs_subscriber import (
    GcsErrorSubscriber,
    GcsLogSubscriber,
    _GcsSubscriber,
)
from ray.includes.global_state_accessor import GlobalStateAccessor
from ray.includes.libcoreworker import ProfileEvent
from ray.includes.metric import Count, Gauge, Histogram, Metric, Sum, TagKey
from ray.includes.object_ref import ObjectRef, _set_future_helper
from ray.includes.ray_config import Config
from ray.includes.raylet_client import RayletClient
from ray.includes.rpc_token_authentication import (
    AuthenticationMode,
    AuthenticationTokenLoader,
    get_authentication_mode,
    validate_authentication_token,
)
from ray.includes.serialization import (
    MessagePackSerializedObject,
    MessagePackSerializer,
    Pickle5SerializedObject,
    Pickle5Writer,
    RawSerializedObject,
    SerializedObject,
    SubBuffer,
    _temporarily_disable_gc,
    split_buffer,
    unpack_pickle5_buffers,
)
from ray.includes.setproctitle import (
    _current_proctitle,
    _current_proctitle_lock,
    getproctitle,
    setproctitle,
)
from ray.includes.unique_ids import (
    ActorClassID,
    ActorID,
    BaseID,
    ClusterID,
    FunctionID,
    JobID,
    NodeID,
    ObjectID,
    PlacementGroupID,
    TaskID,
    UniqueID,
    WorkerID,
    check_id,
)

__all__ = [
    # ray.includes.libcoreworker
    "ProfileEvent",

    # ray.includes.serialization
    "_temporarily_disable_gc",
    "SubBuffer",
    "RawSerializedObject",
    "SerializedObject",
    "Pickle5SerializedObject",
    "Pickle5Writer",
    "MessagePackSerializedObject",
    "MessagePackSerializer",
    "split_buffer",
    "unpack_pickle5_buffers",

    # ray.includes.metric
    "Count",
    "Sum",
    "TagKey",
    "Gauge",
    "Histogram",
    "Metric",

    # ray.includes.ray_config
    "Config",

    # ray.includes.gcs_client
    "InnerGcsClient",

    # ray.includes.function_descriptor
    "FunctionDescriptor",
    "PythonFunctionDescriptor",
    "CppFunctionDescriptor",
    "EmptyFunctionDescriptor",
    "JavaFunctionDescriptor",

    # ray.includes.global_state_accessor
    "GlobalStateAccessor",

    # ray.includes.common
    "GCS_AUTOSCALER_CLUSTER_CONFIG_KEY",
    "GCS_AUTOSCALER_STATE_NAMESPACE",
    "GCS_AUTOSCALER_V2_ENABLED_KEY",
    "GCS_PID_KEY",
    "GcsClientOptions",
    "IMPLICIT_RESOURCE_PREFIX",
    "STREAMING_GENERATOR_RETURN",
    "RESOURCE_UNIT_SCALING",
    "WORKER_PROCESS_SETUP_HOOK_KEY_NAME_GCS",

    # ray.includes.unique_ids
    "ActorClassID",
    "ActorID",
    "BaseID",
    "ClusterID",
    "FunctionID",
    "JobID",
    "NodeID",
    "ObjectID",
    "PlacementGroupID",
    "TaskID",
    "UniqueID",
    "WorkerID",
    "check_id",

    # ray.includes.setproctitle
    "_current_proctitle",
    "_current_proctitle_lock",
    "getproctitle",
    "setproctitle",

    # ray.includes.object_ref
    "_set_future_helper",
    "ObjectRef",

    # ray.includes.buffer
    "Buffer",

    # ray.includes.raylet_client
    "RayletClient",

    # ray.includes.gcs_subscriber
    "_GcsSubscriber",
    "GcsErrorSubscriber",
    "GcsLogSubscriber",

    # ray.includes.rpc_token_authentication
    "AuthenticationMode",
    "get_authentication_mode",
    "validate_authentication_token",
    "AuthenticationTokenLoader",

    # raylet constants
    "GRPC_STATUS_CODE_DEADLINE_EXCEEDED",
    "GRPC_STATUS_CODE_RESOURCE_EXHAUSTED",
    "GRPC_STATUS_CODE_UNAVAILABLE",
    "GRPC_STATUS_CODE_UNIMPLEMENTED",
    "GRPC_STATUS_CODE_UNKNOWN",
    "OPTIMIZED",
    "async_task_function_name",
    "async_task_id",
    "async_task_name",
    "current_task_id",
    "current_task_id_lock",
    "job_config_initialization_lock",
    "job_config_initialized",

    # raylet classes/functions
    "CoreWorker",
    "EmptyProfileEvent",
    "GcsClient",
    "Language",
    "ObjectRefGenerator",
    "StreamRedirector",
    "StreamingGeneratorExecutionContext",
    "StreamingObjectRefGenerator",
    "_call_actor_shutdown",
    "_get_actor_serialized_owner_address_or_none",
    "compute_task_id",
    "del_key_prefix_from_storage",
    "execute_streaming_generator_async",
    "get_session_key_from_storage",
    "maybe_initialize_job_config",
    "raise_sys_exit_with_custom_error_message",
    "serialize_retry_exception_allowlist",

    # protobuf imports
    "ActorDiedErrorContext",
    "GetAllResourceUsageReply",
    "common_pb2",

    # other ray imports
    "DeferSigint",
    "DynamicObjectRefGenerator",
    "TensorTransportEnum",
    "decode",
    "disable_client_hook",
    "external_storage",
    "get_new_event_loop",
    "has_async_methods",
    "is_async_func",
    "ray_constants",
    "ray_pickle",
    "sync_to_async",
]

## RAYLET IMPLEMENTATION

OPTIMIZED: int
GRPC_STATUS_CODE_UNAVAILABLE: int
GRPC_STATUS_CODE_UNKNOWN: int
GRPC_STATUS_CODE_DEADLINE_EXCEEDED: int
GRPC_STATUS_CODE_RESOURCE_EXHAUSTED: int
GRPC_STATUS_CODE_UNIMPLEMENTED: int

current_task_id: Union[TaskID, None]
current_task_id_lock: threading.Lock

job_config_initialized: bool
job_config_initialization_lock: threading.Lock

async_task_id: contextvars.ContextVar[Union[TaskID, None] ]
async_task_name: contextvars.ContextVar[Union[str, None] ]
async_task_function_name: contextvars.ContextVar[Union[str, None] ]

class NumReturnsWarning(UserWarning):
    """Warning when num_returns=0 but the task returns a non-None value."""
    pass

class HasReadInto(Protocol):
    def readinto(self,b: Union[bytearray, memoryview] ,/) -> None: ...

# update module. _raylet.pyx also updates the type name of ObjectRef using cython, but can't do that here
ObjectRefGenerator.__module__ = "ray"

# For backward compatibility
StreamingObjectRefGenerator = ObjectRefGenerator

class SerializedRayObject(NamedTuple):
    data: Optional[Buffer]
    metadata: Optional[Buffer]
    # If set to None, use the default object store transport. Data will be
    # either inlined in `data` or found in the plasma object store.
    tensor_transport: Optional[TensorTransportEnum]

class LocationPtrDict(TypedDict):
    node_ids: list[str]
    object_size: int
    did_spill: bool

class RefCountDict(TypedDict):
    local: int
    submitted: int

class FallbackStrategyDict(TypedDict,total=False):
    label_selector: Dict[str,str]


# An empty profile event context to be used when the timeline is disabled.
class EmptyProfileEvent:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


class EventLoopDict:

    thread: threading.Thread

# ObjectRef Type Vars
_R = TypeVar("_R")
_R2 = TypeVar("_R2")

# Function Descriptor Type Vars
_FDArgs = ParamSpec("_FDArgs")
_FDReturn = TypeVar("_FDReturn")

class CoreWorker:

    def __init__(self, worker_type: int, store_socket: str, raylet_socket: str,
                  job_id: JobID, gcs_options: GcsClientOptions, log_dir: str,
                  node_ip_address: str, node_manager_port: int,
                  local_mode: bool, driver_name: str,
                  serialized_job_config: str, metrics_agent_port: int, runtime_env_hash: int,
                  startup_token: int, session_name: str, cluster_id: str, entrypoint: str,
                  worker_launch_time_ms: int, worker_launched_time_ms: int, debug_source: str):
        ...

    def shutdown_driver(self) -> None: ...

    def run_task_loop(self) -> None: ...

    def drain_and_exit_worker(self, exit_type: str, detail: Union[str,bytes]) -> None:
        """
        Exit the current worker process. This API should only be used by
        a worker. If this API is called, the worker will wait to finish
        currently executing task, initiate the shutdown, and stop
        itself gracefully. The given exit_type and detail will be
        reported to GCS, and any worker failure error will contain them.

        The behavior of this API while a task is running is undefined.
        Avoid using the API when a task is still running.
        """
        ...


    def get_current_task_name(self) -> str:
        """Return the current task name.

        If it is a normal task, it returns the task name from the main thread.
        If it is a threaded actor, it returns the task name for the current thread.
        If it is async actor, it returns the task name stored in contextVar for
        the current asyncio task.
        """
        ...


    def get_current_task_function_name(self) -> str:
        """Return the current task function.

        If it is a normal task, it returns the task function from the main thread.
        If it is a threaded actor, it returns the task function for the current thread.
        If it is async actor, it returns the task function stored in contextVar for
        the current asyncio task.
        """
        ...


    def get_current_task_id(self) -> TaskID:
        """Return the current task ID.

        If it is a normal task, it returns the TaskID from the main thread.
        If it is a threaded actor, it returns the TaskID for the current thread.
        If it is async actor, it returns the TaskID stored in contextVar for
        the current asyncio task.
        """
        ...

    def get_current_task_attempt_number(self) -> int: ...

    def get_task_depth(self) -> int: ...

    def get_current_job_id(self) -> JobID: ...

    def get_current_node_id(self) -> NodeID: ...

    def get_actor_id(self) -> ActorID: ...

    def get_actor_name(self) -> bytes: ...

    def get_placement_group_id(self) -> PlacementGroupID: ...

    def get_worker_id(self) -> WorkerID: ...

    def should_capture_child_tasks_in_placement_group(self) -> bool: ...

    def update_task_is_debugger_paused(self, task_id: TaskID, is_debugger_paused: bool) -> None: ...

    def set_webui_display(self, key: Union[str,bytes], message: Union[str,bytes]) -> bool: ...

    def set_actor_repr_name(self, repr_name: Union[str,bytes]) -> None: ...

    def get_objects(self, object_refs: Iterable[ObjectRef], timeout_ms: int=-1) -> list[SerializedRayObject]: ...

    def get_if_local(self, object_refs: Iterable[ObjectRef], timeout_ms: int=-1) -> list[SerializedRayObject]:
        """Get objects from local plasma store directly
        without a fetch request to raylet."""
        ...

    def object_exists(self, object_ref: ObjectRef, memory_store_only: bool=False) -> bool: ...

    def put_file_like_object(
            self, metadata: bytes, data_size: int, file_like: HasReadInto, object_ref: ObjectRef,
            owner_address):
        """Directly create a new Plasma Store object from a file like
        object. This avoids extra memory copy.

        Args:
            metadata (bytes): The metadata of the object.
            data_size (int): The size of the data buffer.
            file_like: A python file object that provides the `readinto`
                interface.
            object_ref: The new ObjectRef.
            owner_address: Owner address for this object ref.
        """
        ...


    def experimental_channel_put_serialized(self, serialized_object: SerializedObject,
                                            object_ref: ObjectRef, num_readers: int,
                                            timeout_ms: int) -> None: ...


    def experimental_channel_set_error(self, object_ref: ObjectRef) -> None: ...


    def experimental_channel_register_writer(self,
                                             writer_ref: ObjectRef,
                                             remote_reader_ref_info: dict[str,ReaderRefInfo]) -> None: ...

    def experimental_channel_register_reader(self, object_ref: ObjectRef): ...

    def put_object(
            self,
            serialized_object: SerializedObject,
            *,
            pin_object: bool=True,
            owner_address: Optional[str]=None,
            inline_small_object: bool=True,
            _is_experimental_channel: bool=False,
            tensor_transport_val: int=0,
    ) -> ObjectRef:
        """Create an object reference with the current worker as the owner."""

    def put_serialized_object_and_increment_local_ref(
            self,
            serialized_object: SerializedObject,
            pin_object: bool=True,
            owner_address: Optional[str]=None,
            inline_small_object: bool=True,
            _is_experimental_channel: bool=False,
            tensor_transport_val: int=0,
            ) -> bytes: ...


    def wait(self,  # TODO: possible to type-check the generics here properly? At least overloads
             object_refs_or_generators: Iterable[Union[ObjectRef[_R],ObjectRefGenerator[_R2]]],
             num_returns: int,
             timeout_ms: int,
             fetch_local: bool) -> tuple[list[Union[ObjectRef[_R],ObjectRefGenerator[_R2]]],list[Union[ObjectRef[_R],ObjectRefGenerator[_R2]]]]:
        ...

    def free_objects(self, object_refs: Iterable[ObjectRef], local_only: bool) -> None: ...

    def get_local_ongoing_lineage_reconstruction_tasks(self) -> list[tuple[LineageReconstructionTask,int]]: ...


    def get_local_object_locations(self, object_refs: Iterable[ObjectRef[_R]]) -> dict[ObjectRef[_R],LocationPtrDict]: ...

    def get_object_locations(self, object_refs: Iterable[ObjectRef[_R]], timeout_ms: int) -> dict[ObjectRef[_R],LocationPtrDict]: ...

    def global_gc(self) -> None: ...

    def log_plasma_usage(self) -> None: ...

    def get_memory_store_size(self) -> int: ...

    def submit_task(self,
                    language: Language,
                    function_descriptor: FunctionDescriptor[_FDArgs,_FDReturn],
                    args: Iterable[Union[ObjectRef, Any] ],
                    name: Union[str, bytes] ,
                    num_returns: int,
                    resources: dict[str,Union[int, float] ],
                    max_retries: int,
                    retry_exceptions: bool,
                    retry_exception_allowlist: tuple[type[Exception],...]|None,
                    scheduling_strategy: str,
                    debugger_breakpoint: Union[str, bytes] ,
                    serialized_runtime_env_info: Union[str, bytes] ,
                    generator_backpressure_num_objects: int,
                    enable_task_events: bool,
                    labels: dict[str,str],
                    label_selector: dict[str,str],
                    fallback_strategy: Optional[List[FallbackStrategyDict]]) -> list[ObjectRef[_FDReturn]]: ...

    def create_actor(self,
                     language: Language,
                     function_descriptor: FunctionDescriptor,
                     args: Iterable[Union[ObjectRef, Any] ],
                     max_restarts: int,
                     max_task_retries: int,
                     resources: dict[str,Union[int, float] ],
                     placement_resources: dict[str,Union[int, float] ],
                     max_concurrency: int,
                     is_detached: Union[Any, None] ,
                     name: Union[str, bytes] ,
                     ray_namespace: Union[str, bytes] ,
                     is_asyncio: bool,
                     extension_data: Union[str, bytes] ,
                     serialized_runtime_env_info: Union[str, bytes] ,
                     concurrency_groups_dict: dict[str,Any],
                     max_pending_calls: int,
                     scheduling_strategy: str,
                     enable_task_events: bool,
                     labels: dict[str,str],
                     label_selector: dict[str,str],
                     allow_out_of_order_execution: bool,
                     enable_tensor_transport: bool,
                     fallback_strategy: Optional[List[FallbackStrategyDict]],
                     ) -> ActorID: ...

    def create_placement_group(
                            self,
                            name: Union[str, bytes] ,
                            bundles: list[dict[Union[str, bytes] , float]],
                            strategy: Union[str, bytes] ,
                            is_detached: bool,
                            soft_target_node_id: Union[str, bytes] |None,
                            bundle_label_selector: list[dict[Union[str, bytes] , Union[str, bytes] ]]) -> PlacementGroupID: ...

    def remove_placement_group(self, placement_group_id: PlacementGroupID): ...

    def wait_placement_group_ready(self,
                                   placement_group_id: PlacementGroupID,
                                   timeout_seconds: int) -> bool: ...

    def submit_actor_task(self,
                          language: Language,
                          actor_id: ActorID,
                          function_descriptor: FunctionDescriptor[_FDArgs,_FDReturn],
                          args: Iterable[Union[ObjectRef, Any] ],
                          name: Union[str, bytes] ,
                          num_returns: int,
                          max_retries: int,
                          retry_exceptions: bool,
                          retry_exception_allowlist: tuple[type[Exception],...]|None,
                          num_method_cpus: float,
                          concurrency_group_name: Union[str, bytes] ,
                          generator_backpressure_num_objects: int,
                          enable_task_events: bool,
                          py_tensor_transport: int) -> list[ObjectRef[_FDReturn]]: ...


    def kill_actor(self, actor_id: ActorID, no_restart: bool) -> None: ...

    def cancel_task(self, object_ref: ObjectRef, force_kill: bool,
                    recursive: bool) -> None: ...

    def resource_ids(self) -> dict[str,list[tuple[int,float]]]: ...

    def profile_event(self, event_type: Union[str, bytes] , extra_data=None) -> Union[ProfileEvent, EmptyProfileEvent] : ...

    def remove_actor_handle_reference(self, actor_id: ActorID) -> None: ...

    def get_local_actor_state(self, actor_id: ActorID) -> Union[int, None] : ...

    def deserialize_and_register_actor_handle(self, bytes: bytes,
                                              outer_object_ref: ObjectRef[ActorHandle[_R]],
                                              weak_ref: bool) -> ActorHandle[_R]: ...

    def get_named_actor_handle(self, name: bytes,
                               ray_namespace: bytes) -> ActorHandle: ...

    def get_actor_handle(self, actor_id: ActorID) -> ActorHandle: ...

    def list_named_actors(self, all_namespaces: bool) -> list[tuple[str,str]]:
        """Returns (namespace, name) for named actors in the system.

        If all_namespaces is True, returns all actors in all namespaces,
        else returns only the actors in the current namespace.
        """
        ...

    def serialize_actor_handle(self, actor_id: ActorID) -> tuple[bytes,ObjectRef[ActorHandle]]: ... # TODO: generic ActorID?

    def add_object_ref_reference(self, object_ref: ObjectRef) -> None: ...

    def remove_object_ref_reference(self, object_ref: ObjectRef) -> None: ...

    def get_owner_address(self, object_ref: ObjectRef) -> bytes: ...

    def serialize_object_ref(self, object_ref: ObjectRef[_R]) -> tuple[ObjectRef[_R],bytes,bytes]: ...

    def deserialize_and_register_object_ref(
            self, object_ref_binary: bytes,
            outer_object_ref: ObjectRef,
            serialized_owner_address: bytes,
            serialized_object_status: bytes,
    ) -> None: ...

    def get_event_loop_executor(self) -> concurrent.futures.ThreadPoolExecutor: ...

    def reset_event_loop_executor(self, executor: concurrent.futures.ThreadPoolExecutor) -> None: ...

    def get_event_loop(self, function_descriptor: PythonFunctionDescriptor, specified_cgname: str) -> tuple[Union[asyncio.AbstractEventLoop, None] ,threading.Thread]: ...

    def run_async_func_or_coro_in_event_loop(
          self,
          func_or_coro: Union[Callable[..., Awaitable[_R]], Awaitable[_R]],
          function_descriptor: FunctionDescriptor,
          specified_cgname: str,
          *,
          task_id: Optional[TaskID] = None,
          task_name: Optional[str] = None,
          func_args: Optional[Tuple] = None,
          func_kwargs: Optional[Dict] = None,
    ) -> _R:
        """Run the async function or coroutine to the event loop.

        The event loop is running in a separate thread.

        Args:
            func_or_coro: Async function (not a generator) or awaitable objects.
            function_descriptor: The function descriptor.
            specified_cgname: The name of a concurrent group.
            task_id: The task ID to track the future. If None is provided
                the future is not tracked with a task ID.
                (e.g., When we deserialize the arguments, we don't want to
                track the task_id -> future mapping).
            func_args: The arguments for the async function.
            func_kwargs: The keyword arguments for the async function.

        NOTE: func_args and func_kwargs are intentionally passed as a tuple/dict and
        not unpacked to avoid collisions between system arguments and user-provided
        arguments. See https://github.com/ray-project/ray/issues/41272.
        """
        ...

    def stop_and_join_asyncio_threads_if_exist(self) -> None: ...

    def current_actor_is_asyncio(self) -> bool: ...

    def set_current_actor_should_exit(self) -> None: ...

    def get_current_actor_should_exit(self) -> bool: ...

    def current_actor_max_concurrency(self) -> int: ...

    def get_current_root_detached_actor_id(self) -> ActorID: ...

    def get_future_for_running_task(self, task_id: Optional[TaskID]) -> Optional[concurrent.futures.Future]:
        """Get the future corresponding to a running task (or None).

        The underyling asyncio task might be queued, running, or completed.
        """
        ...

    def get_current_runtime_env(self) -> str: ...

    def trigger_gc(self): ...

    def get_pending_children_task_ids(self, parent_task_id: TaskID) -> list[TaskID]: ...

    def get_all_reference_counts(self) -> dict[ObjectRef,RefCountDict]: ...

    def set_get_async_callback(self, object_ref: ObjectRef[_R], user_callback: Callable[[_R],Any]) -> None: ...

    def push_error(self, job_id: JobID, error_type: str, error_message: str,
                   timestamp: float) -> None: ...

    def get_job_config(self) -> JobConfig: ...

    def get_task_submission_stats(self) -> tuple[int,int]: ...

    def get_local_memory_store_bytes_used(self) -> int: ...

    def record_task_log_start(
            self, task_id: TaskID, attempt_number: int,
            stdout_path: str, stderr_path: str,
            out_start_offset: int, err_start_offset: int) -> None: ...

    def record_task_log_end(
            self, task_id: TaskID, attempt_number: int,
            out_end_offset: int, err_end_offset: int) -> None: ...

    def async_delete_object_ref_stream(self, generator_id: ObjectRef[None]) -> None: ...

    # unfortunately the generator type information is in the ObjectRefGenerator, not the underlying reference,
    # so this function can't be properly typed. oh well
    def try_read_next_object_ref_stream(self, generator_id: ObjectRef[None]) -> ObjectRef: ...

    def is_object_ref_stream_finished(self, generator_id: ObjectRef[None]) -> bool: ...

    # like try_read_next_object_ref_stream, can't be properly typed
    def peek_object_ref_stream(self, generator_id: ObjectRef[None]) -> tuple[ObjectRef,bool]: ...

def _call_actor_shutdown() -> None:
    """Internal function that calls actor's __ray_shutdown__ method."""
    ...

class StreamRedirector:
    @staticmethod
    def redirect_stdout(file_path: Union[str,bytes], rotation_max_size: int, rotation_max_file_count: int, tee_to_stdout: bool, tee_to_stderr: bool) -> None: ...

    @staticmethod
    def redirect_stderr(file_path: Union[str,bytes], rotation_max_size: int, rotation_max_file_count: int, tee_to_stdout: bool, tee_to_stderr: bool) -> None: ...

_L = TypeVar("_L",bound=Language)
class Language:

    def __init__(self, lang: int) -> None: ... # from __cinit__

    def value(self) -> int: ...

    def __eq__(self, other: object) -> bool: ...

    def __repr__(self) -> str: ...

    def __reduce__(self:_L) -> tuple[type[_L],tuple[int]]: ...

    PYTHON: Language
    CPP: Language
    JAVA: Language


class GcsClient:
    """
    Client to the GCS server.

    This is a thin wrapper around InnerGcsClient with only call frequency collection.
    """

    def __init__(self, address: str, # from __cinit__
                  cluster_id: Optional[str] = None) -> None: ...
        # For timeout (DEADLINE_EXCEEDED): retries once with timeout_ms.
        #
        # For other RpcError (UNAVAILABLE, UNKNOWN): retries indefinitely until it
        # thinks GCS is down and kills the whole process.

    def __getattr__(self, name: str) -> Any: ...
        # We collect the frequency of each method call.


# Note this deletes keys with prefix `RAY{key_prefix}@`
# Example: with key_prefix = `default`, we remove all `RAYdefault@...` keys.
def del_key_prefix_from_storage(host: Union[str,bytes], port: int, username: Union[str,bytes], password: Union[str,bytes], use_ssl: bool, key_prefix: Union[str,bytes]) -> bool: ...

def get_session_key_from_storage(host: Union[str,bytes], port: int, username: Union[str,bytes], password: Union[str,bytes], use_ssl: bool, config: Union[str,bytes], key: Union[str,bytes]) -> bool:
    """
    Get the session key from the storage.
    Intended to be used for session_name only.
    Args:
        host: The address of the owner (caller) of the
            generator task.
        port: The task ID of the generator task.
        username: The Redis username.
        password: The Redis password.
        use_ssl: Whether to use SSL.
        config: The Ray config. Used to get storage namespace.
        key: The key to retrieve.
    """
    ...

def raise_sys_exit_with_custom_error_message(
        ray_terminate_msg: str,
        exit_code: int = 0) -> None:
    """It is equivalent to sys.exit, but it can contain
    a custom message. Custom message is reported to
    raylet and is accessible via GCS (from `ray get workers`).

    Note that sys.exit == raise SystemExit. I.e., this API
    simply raises SystemExit with a custom error message accessible
    via `e.ray_terminate_msg`.

    Args:
        ray_terminate_msg: The error message to propagate to GCS.
        exit_code: The exit code. If it is not 0, it is considered
            as a system error.
    """
    ...


class StreamingGeneratorExecutionContext:
    """The context to run a streaming generator function.

    Make sure you always call `initialize` API before
    accessing any fields.

    Args:
        generator: The generator to run.
        generator_id: The object ref id of the generator task.
        task_type: The type of the task. E.g., actor task, normal task.
        caller_address: The address of the caller. By our protocol,
            the caller of the streaming generator task is always
            the owner, so we can also call it "owner address".
        task_id: The task ID of the generator task.
        serialized_retry_exception_allowlist: A list of
            exceptions that are allowed to retry this generator task.
        function_name: The name of the generator function. Used for
            writing an error message.
        function_descriptor: The function descriptor of
            the generator function. Used for writing an error message.
        title: The process title of the generator task. Used for
            writing an error message.
        actor: The instance of the actor created in this worker.
            It is used to write an error message.
        actor_id: The ID of the actor. It is used to write an error message.
        return_size: The number of static returns.
        attempt_number: The number of times the current task is retried.
            0 means it is the first execution of the task.
        should_retry_exceptions: True if the task should be
            retried upon exceptions.
        streaming_generator_returns(out): A list of a pair of (ObjectID,
        is_plasma_object) that are generated by a streaming generator
        task.
        is_retryable_error(out): It is set to True if the generator
            raises an exception, and the error is retryable.
        application_error(out): It is set if the generator raises an
            application error.
        generator_backpressure_num_objects: The backpressure threshold
            for streaming generator. The stremaing generator pauses if
            total number of unconsumed objects exceed this threshold.
    """
    # TODO: can the context's various cdef'd attributes actually be accessed outside of Cpython?

    def initialize(self, generator: Union[Generator, AsyncGenerator]) -> None: ...

    def is_initialized(self) -> bool: ...

def _get_actor_serialized_owner_address_or_none(actor_table_data: bytes) -> Union[bytes, None] : ...

def compute_task_id(object_ref: ObjectRef) -> TaskID: ...

async def execute_streaming_generator_async(
        context: StreamingGeneratorExecutionContext) -> None:
    """Execute a given generator and report the
        result to the given caller_address in a streaming (ie as
        soon as become available) fashion.

    This method is same as `execute_streaming_generator_sync`,
    but it should be used inside an async event loop.

    NOTE: since this function runs inside an event loop thread,
    some of core worker APIs will be executed inside
    the event loop thread as well.

    E.g., core_worker.SealOwned can be called.

    At this time, if we access worker_context_ API from core worker,
    it can cause problems because worker_context_ is configured
    per thread (it is a bug & tech debt).

    Args:
        context: The context to execute streaming generator.
    """
    ...


def maybe_initialize_job_config() -> None: ...

def serialize_retry_exception_allowlist(retry_exception_allowlist: tuple[type[Exception],...], function_descriptor: FunctionDescriptor) -> bytes: ...
