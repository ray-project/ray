import asyncio
import concurrent.futures
import threading
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    NamedTuple,
    NoReturn,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

from typing_extensions import NotRequired, ParamSpec

from python.ray.includes.buffer import Buffer
from python.ray.includes.function_descriptor import (
    FunctionDescriptor,
    PythonFunctionDescriptor,
)
from python.ray.includes.libcoreworker import ProfileEvent
from python.ray.includes.serialization import SerializedObject

from ray._private.custom_types import TensorTransportEnum
from ray._private.worker import Worker
from ray.actor import ActorHandle
from ray.core.generated.common_pb2 import (
    JobConfig,
    LineageReconstructionTask,
)
from ray.experimental.channel.shared_memory_channel import (
    ReaderRefInfo,  # circular reference - .pyi only
)
from ray.includes.common import GcsClientOptions
from ray.includes.object_ref import ObjectRef, _set_future_helper
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

    # ray.includes.object_ref
    "_set_future_helper",
    "ObjectRef",
]

_R = TypeVar("_R") # for ObjectRefs
_R2 = TypeVar("_R2")


_ORG = TypeVar("_ORG",bound=ObjectRefGenerator)
class ObjectRefGenerator(Generic[_R],Generator[ObjectRef[_R],None,None],AsyncGenerator[ObjectRef[_R],None]):
    """A generator to obtain object references
    from a task in a streaming manner.

    The class is compatible with generator and
    async generator interface.

    The class is not thread-safe.

    Do not initialize the class and create an instance directly.
    The instance should be created by `.remote`.

    >>> gen = generator_task.remote()
    >>> next(gen)
    >>> await gen.__anext__()
    """

    _generator_ref: ObjectRef
    _generator_task_exception: Union[Exception, None]
    worker: Worker



    def __init__(self, generator_ref: ObjectRef[None], worker: "Worker") -> None: ...

    # Public APIs

    def __iter__(self:_ORG) -> _ORG: ...

    def __next__(self) -> ObjectRef[_R]:
        """Waits until a next ref is available and returns the object ref.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).
        """
        ...

    def send(self, value) -> NoReturn: ...

    def throw(self, value, val=None, tb=None) -> NoReturn: ...

    def close(self) -> NoReturn: ...

    def __aiter__(self:_ORG) -> _ORG: ...

    async def __anext__(self) -> ObjectRef[_R]: ...

    async def asend(self, value) -> NoReturn: ...

    async def athrow(self, value, val=None, tb=None) -> NoReturn: ...

    async def aclose(self) -> NoReturn: ...

    def completed(self) -> ObjectRef[None]:
        """Returns an object ref that is ready when
        a generator task completes.

        If the task is failed unexpectedly (e.g., worker failure),
        the `ray.get(gen.completed())` raises an exception.

        The function returns immediately.

        >>> ray.get(gen.completed())
        """
        ...

    def next_ready(self) -> bool:
        """If True, it means the output of next(gen) is ready and
        ray.get(next(gen)) returns immediately. False otherwise.

        It returns False when next(gen) raises a StopIteration
        (this condition should be checked using is_finished).

        The function returns immediately.
        """
        ...


    def is_finished(self) -> bool:
        """If True, it means the generator is finished
        and all output is taken. False otherwise.

        When True, if next(gen) is called, it will raise StopIteration
        or StopAsyncIteration

        The function returns immediately.
        """
        ...

    # Private APIs

    def _get_next_ref(self) -> ObjectRef[_R]:
        """Return the next reference from a generator.

        Note that the ObjectID generated from a generator
        is always deterministic.
        """
        ...


    def _next_sync(
        self,
        timeout_s: Optional[Union[int, float]] = None
    ) -> ObjectRef[_R]: # ObjectRef could be nil
        """Waits for timeout_s and returns the object ref if available.

        If an object is not available within the given timeout, it
        returns a nil object reference.

        If -1 timeout is provided, it means it waits infinitely.

        Waiting is implemented as busy waiting.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).

        Args:
            timeout_s: If the next object is not ready within
                this timeout, it returns the nil object ref.
        """
        ...

    async def _suppress_exceptions(self, ref: ObjectRef) -> None: ...

    async def _next_async(
            self,
            timeout_s: Optional[Union[int, float]] = None
    ) -> ObjectRef[_R]: # ObjectRef could be nil
        """Same API as _next_sync, but it is for async context."""
        ...

    def __del__(self) -> None: ...
        # if hasattr(self.worker, "core_worker"):
        #     # The stream is created when a task is first submitted.
        #     # NOTE: This can be called multiple times
        #     # because python doesn't guarantee __del__ is called
        #     # only once.
        #     self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)

    def __getstate__(self) -> NoReturn: ...
        # raise TypeError(
        #     "You cannot return or pass a generator to other task. "
        #     "Serializing a ObjectRefGenerator is not allowed.")

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

class FallbackStrategyDict(TypedDict):
    label_selector: NotRequired[Dict[str,str]]

class HasReadInto(Protocol):
    def readinto(self,b: Union[bytearray, memoryview],/) -> None: ...

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

# An empty profile event context to be used when the timeline is disabled.
class EmptyProfileEvent:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

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

    def drain_and_exit_worker(self, exit_type: str, detail: Union[str, bytes]) -> None:
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
                    args: Iterable[Union[ObjectRef, Any]],
                    name: Union[str, bytes],
                    num_returns: int,
                    resources: dict[str,Union[int, float]],
                    max_retries: int,
                    retry_exceptions: bool,
                    retry_exception_allowlist: tuple[type[Exception],...]|None,
                    scheduling_strategy: str,
                    debugger_breakpoint: Union[str, bytes],
                    serialized_runtime_env_info: Union[str, bytes],
                    generator_backpressure_num_objects: int,
                    enable_task_events: bool,
                    labels: dict[str,str],
                    label_selector: dict[str,str],
                    fallback_strategy: Optional[List[FallbackStrategyDict]]) -> list[ObjectRef[_FDReturn]]: ...

    def create_actor(self,
                     language: Language,
                     function_descriptor: FunctionDescriptor,
                     args: Iterable[Union[ObjectRef, Any]],
                     max_restarts: int,
                     max_task_retries: int,
                     resources: dict[str,Union[int, float]],
                     placement_resources: dict[str,Union[int, float]],
                     max_concurrency: int,
                     is_detached: Union[Any, None],
                     name: Union[str, bytes],
                     ray_namespace: Union[str, bytes],
                     is_asyncio: bool,
                     extension_data: Union[str, bytes],
                     serialized_runtime_env_info: Union[str, bytes],
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
                            name: Union[str, bytes],
                            bundles: list[dict[Union[str, bytes], float]],
                            strategy: Union[str, bytes],
                            is_detached: bool,
                            soft_target_node_id: Union[str, bytes]|None,
                            bundle_label_selector: list[dict[Union[str, bytes], Union[str, bytes]]]) -> PlacementGroupID: ...

    def remove_placement_group(self, placement_group_id: PlacementGroupID): ...

    def wait_placement_group_ready(self,
                                   placement_group_id: PlacementGroupID,
                                   timeout_seconds: int) -> bool: ...

    def submit_actor_task(self,
                          language: Language,
                          actor_id: ActorID,
                          function_descriptor: FunctionDescriptor[_FDArgs,_FDReturn],
                          args: Iterable[Union[ObjectRef, Any]],
                          name: Union[str, bytes],
                          num_returns: int,
                          max_retries: int,
                          retry_exceptions: bool,
                          retry_exception_allowlist: tuple[type[Exception],...]|None,
                          num_method_cpus: float,
                          concurrency_group_name: Union[str, bytes],
                          generator_backpressure_num_objects: int,
                          enable_task_events: bool,
                          py_tensor_transport: int) -> list[ObjectRef[_FDReturn]]: ...


    def kill_actor(self, actor_id: ActorID, no_restart: bool) -> None: ...

    def cancel_task(self, object_ref: ObjectRef, force_kill: bool,
                    recursive: bool) -> None: ...

    def resource_ids(self) -> dict[str,list[tuple[int,float]]]: ...

    def profile_event(self, event_type: Union[str, bytes], extra_data=None) -> Union[ProfileEvent, EmptyProfileEvent]: ...

    def remove_actor_handle_reference(self, actor_id: ActorID) -> None: ...

    def get_local_actor_state(self, actor_id: ActorID) -> Union[int, None]: ...

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

    def get_event_loop(self, function_descriptor: PythonFunctionDescriptor, specified_cgname: str) -> tuple[Union[asyncio.AbstractEventLoop, None],threading.Thread]: ...

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
