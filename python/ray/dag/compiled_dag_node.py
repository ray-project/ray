import weakref
import asyncio
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass, asdict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    List,
    Tuple,
    Union,
    Optional,
    Set,
)
import logging
import threading
import time
import uuid
import traceback

import ray.exceptions
from ray.dag.dag_operation_future import GPUFuture, DAGOperationFuture, ResolvedFuture
from ray.experimental.channel.cached_channel import CachedChannel
from ray.experimental.channel.gpu_communicator import GPUCommunicator
from ray.dag.constants import RAY_ADAG_VISUALIZE_SCHEDULE
import ray
from ray.exceptions import RayTaskError, RayChannelError
from ray.experimental.compiled_dag_ref import (
    CompiledDAGRef,
    CompiledDAGFuture,
    _process_return_vals,
)
from ray.experimental.channel import (
    ChannelContext,
    ChannelInterface,
    ChannelOutputType,
    ReaderInterface,
    SynchronousReader,
    WriterInterface,
    SynchronousWriter,
    AwaitableBackgroundReader,
    AwaitableBackgroundWriter,
    RayDAGArgs,
    CompositeChannel,
    IntraProcessChannel,
)
from ray.util.annotations import DeveloperAPI

from ray.experimental.channel.shared_memory_channel import (
    SharedMemoryType,
)
from ray.experimental.channel.torch_tensor_type import TorchTensorType

from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
    _destroy_nccl_group,
)

from ray.dag.dag_node_operation import (
    _DAGNodeOperation,
    _DAGNodeOperationType,
    _DAGOperationGraphNode,
    _build_dag_node_operation_graph,
    _extract_execution_schedule,
    _generate_actor_to_execution_schedule,
    _generate_overlapped_execution_schedule,
    _visualize_execution_schedule,
)

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import cupy as cp

logger = logging.getLogger(__name__)

# Keep tracking of every compiled dag created during the lifetime of
# this process. It tracks them as weakref meaning when the compiled dag
# is GC'ed, it is automatically removed from here. It is used to teardown
# compiled dags at interpreter shutdown time.
_compiled_dags = weakref.WeakValueDictionary()


# Relying on __del__ doesn't work well upon shutdown because
# the destructor order is not guaranteed. We call this function
# upon `ray.worker.shutdown` which is registered to atexit handler
# so that teardown is properly called before objects are destructed.
def _shutdown_all_compiled_dags():
    global _compiled_dags
    for _, compiled_dag in _compiled_dags.items():
        # Kill DAG actors to avoid hanging during shutdown if the actor tasks
        # cannot be cancelled.
        compiled_dag.teardown(kill_actors=True)
    _compiled_dags = weakref.WeakValueDictionary()


@DeveloperAPI
def do_allocate_channel(
    self,
    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
    typ: ChannelOutputType,
    driver_actor_id: Optional[str] = None,
) -> ChannelInterface:
    """Generic actor method to allocate an output channel.

    Args:
        reader_and_node_list: A list of tuples, where each tuple contains a reader
            actor handle and the node ID where the actor is located.
        typ: The output type hint for the channel.
        driver_actor_id: If this channel is read by a driver and that driver is an
            actual actor, this will be the actor ID of that driver actor.

    Returns:
        The allocated channel.
    """
    # None means it is called from a driver.
    writer: Optional["ray.actor.ActorHandle"] = None
    try:
        writer = ray.get_runtime_context().current_actor
    except RuntimeError:
        # This is the driver so there is no current actor handle.
        pass

    output_channel = typ.create_channel(
        writer,
        reader_and_node_list,
        driver_actor_id,
    )
    return output_channel


@DeveloperAPI
def do_exec_tasks(
    self,
    tasks: List["ExecutableTask"],
    schedule: List[_DAGNodeOperation],
    overlap_gpu_communication: bool = False,
) -> None:
    """A generic actor method to begin executing the operations belonging to an
    actor. This runs an infinite loop to execute each _DAGNodeOperation in the
    order specified by the schedule. It exits only if the actor dies or an
    exception is thrown.

    Args:
        tasks: the executable tasks corresponding to the actor methods.
        schedule: A list of _DAGNodeOperation that should be executed in order.
        overlap_gpu_communication: Whether to overlap GPU communication with
            computation during DAG execution to improve performance.
    """
    try:
        for task in tasks:
            task.prepare(overlap_gpu_communication=overlap_gpu_communication)

        done = False
        while True:
            if done:
                break
            for operation in schedule:
                done = tasks[operation.exec_task_idx].exec_operation(
                    self, operation.type, overlap_gpu_communication
                )
                if done:
                    break
    except Exception:
        logging.exception("Compiled DAG task exited with exception")
        raise


@DeveloperAPI
def do_profile_tasks(
    self,
    tasks: List["ExecutableTask"],
    schedule: List[_DAGNodeOperation],
    overlap_gpu_communication: bool = False,
) -> None:
    """A generic actor method similar to `do_exec_tasks`, but with profiling enabled.

    Args:
        tasks: the executable tasks corresponding to the actor methods.
        schedule: A list of _DAGNodeOperation that should be executed in order.
        overlap_gpu_communication: Whether to overlap GPU communication with
            computation during DAG execution to improve performance.
    """
    try:
        for task in tasks:
            task.prepare(overlap_gpu_communication=overlap_gpu_communication)

        if not hasattr(self, "__ray_adag_events"):
            self.__ray_adag_events = []

        done = False
        while True:
            if done:
                break
            for operation in schedule:
                start_t = time.perf_counter()
                task = tasks[operation.exec_task_idx]
                done = tasks[operation.exec_task_idx].exec_operation(
                    self, operation.type, overlap_gpu_communication
                )
                end_t = time.perf_counter()

                self.__ray_adag_events.append(
                    _ExecutableTaskRecord(
                        actor_classname=self.__class__.__name__,
                        actor_name=ray.get_runtime_context().get_actor_name(),
                        actor_id=ray.get_runtime_context().get_actor_id(),
                        method_name=task.method_name,
                        bind_index=task.bind_index,
                        operation=operation.type.value,
                        start_t=start_t,
                        end_t=end_t,
                    )
                )

                if done:
                    break
    except Exception:
        logging.exception("Compiled DAG task exited with exception")
        raise


@DeveloperAPI
def do_cancel_executable_tasks(self, tasks: List["ExecutableTask"]) -> None:
    for task in tasks:
        task.cancel()


def _wrap_exception(exc):
    backtrace = ray._private.utils.format_error_message(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        task_exception=True,
    )
    wrapped = RayTaskError(
        function_name="do_exec_tasks",
        traceback_str=backtrace,
        cause=exc,
    )
    return wrapped


def _get_nccl_group_id(type_hint: ChannelOutputType) -> Optional[str]:
    """
    Get the NCCL group ID from the type hint. If the type hint does not
    require NCCL, return None.

    Args:
        type_hint: The type hint of the channel.

    Returns:
        The NCCL group ID if the type hint requires NCCL, otherwise None.
    """
    if type_hint.requires_nccl():
        assert isinstance(type_hint, TorchTensorType)
        return type_hint.nccl_group_id
    return None


def _device_context_manager():
    """
    Return a context manager for executing communication operations
    (i.e., READ and WRITE). For NCCL operations, the context manager
    uses the proper cuda device from channel context, otherwise,
    nullcontext will be returned.
    """
    if not ChannelContext.get_current().torch_available:
        return nullcontext()

    import torch

    device = ChannelContext.get_current().torch_device

    if device.type == "cuda" and torch.cuda.is_available():
        # In the case of mocked NCCL, we may get a device with type "cuda"
        # but CUDA is not available. We return nullcontext() in that case,
        # otherwise torch raises a runtime error if the cuda device context
        # manager is used.
        # TODO(rui): consider better mocking NCCL to support device context.
        return torch.cuda.device(device)
    return nullcontext()


@DeveloperAPI
class CompiledTask:
    """Wraps the normal Ray DAGNode with some metadata."""

    def __init__(self, idx: int, dag_node: "ray.dag.DAGNode"):
        """
        Args:
            idx: A unique index into the original DAG.
            dag_node: The original DAG node created by the user.
        """
        self.idx = idx
        self.dag_node = dag_node

        # Dict from task index to actor handle for immediate downstream tasks.
        self.downstream_task_idxs: Dict[int, "ray.actor.ActorHandle"] = {}
        # Case 1: The task represents a ClassMethodNode.
        #
        # Multiple return values are written to separate `output_channels`.
        # `output_idxs` represents the tuple index of the output value for
        # multiple returns in a tuple. If an output index is None, it means
        # the complete return value is written to the output channel.
        # Otherwise, the return value is a tuple and the index is used
        # to extract the value to be written to the output channel.
        #
        # Case 2: The task represents an InputNode.
        #
        # `output_idxs` can be an integer or a string to retrieve the
        # corresponding value from `args` or `kwargs` in the DAG's input.
        self.output_channels: List[ChannelInterface] = []
        self.output_idxs: List[Optional[Union[int, str]]] = []
        self.arg_type_hints: List["ChannelOutputType"] = []
        # idxs of possible ClassMethodOutputNodes if they exist, used for visualization
        self.output_node_idxs: List[int] = []

    @property
    def args(self) -> Tuple[Any]:
        return self.dag_node.get_args()

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.dag_node.get_kwargs()

    @property
    def num_readers(self) -> int:
        return len(self.downstream_task_idxs)

    def __str__(self) -> str:
        return f"""
            Node: {self.dag_node}
            Arguments: {self.args}
            Output: {self.output_channels}
            """


class _ExecutableTaskInput:
    """Represents an input to an ExecutableTask.

    Args:
        input_variant: either an unresolved input (when type is ChannelInterface)
            , or a resolved input value (when type is Any)
        channel_idx: if input_variant is an unresolved input, this is the index
            into the input channels list.
    """

    def __init__(
        self,
        input_variant: Union[ChannelInterface, Any],
        channel_idx: Optional[int],
    ):
        self.input_variant = input_variant
        self.channel_idx = channel_idx

    def resolve(self, channel_results: Any) -> Any:
        """
        Resolve the input value from the channel results.

        Args:
            channel_results: The results from reading the input channels.
        """

        if isinstance(self.input_variant, ChannelInterface):
            value = channel_results[self.channel_idx]
        else:
            value = self.input_variant
        return value


@DeveloperAPI
class ExecutableTask:
    """A task that can be executed in a compiled DAG, and it
    corresponds to an actor method.
    """

    def __init__(
        self,
        task: "CompiledTask",
        resolved_args: List[Any],
        resolved_kwargs: Dict[str, Any],
    ):
        """
        Args:
            task: The CompiledTask that this ExecutableTask corresponds to.
            resolved_args: The arguments to the method. Arguments that are
                not Channels will get passed through to the actor method.
                If the argument is a channel, it will be replaced by the
                value read from the channel before the method executes.
            resolved_kwargs: The keyword arguments to the method. Currently, we
                do not support binding kwargs to other DAG nodes, so the values
                of the dictionary cannot be Channels.
        """
        from ray.dag import CollectiveOutputNode

        self.method_name = task.dag_node.get_method_name()
        self.bind_index = task.dag_node._get_bind_index()
        self.output_channels = task.output_channels
        self.output_idxs = task.output_idxs
        self.input_type_hints: List[ChannelOutputType] = task.arg_type_hints
        self.output_type_hint: ChannelOutputType = task.dag_node.type_hint

        # The NCCL collective operation.
        self.collective_op: Optional["ray.dag.CollectiveOperation"] = None
        if isinstance(task.dag_node, CollectiveOutputNode):
            self.collective_op = task.dag_node.collective_op

        self.input_channels: List[ChannelInterface] = []
        self.task_inputs: List[_ExecutableTaskInput] = []
        self.resolved_kwargs: Dict[str, Any] = resolved_kwargs
        # A unique index which can be used to index into `idx_to_task` to get
        # the corresponding task.
        self.task_idx = task.idx

        # Reverse map for input_channels: maps an input channel to
        # its index in input_channels.
        input_channel_to_idx: dict[ChannelInterface, int] = {}

        for arg in resolved_args:
            if isinstance(arg, ChannelInterface):
                if isinstance(arg, ChannelInterface):
                    channel = arg
                else:
                    adapter = arg
                    channel = adapter.get_dag_input_channel()

                if channel in input_channel_to_idx:
                    # The same channel was added before, so reuse the index.
                    channel_idx = input_channel_to_idx[channel]
                else:
                    # Add a new channel to the list of input channels.
                    self.input_channels.append(channel)
                    channel_idx = len(self.input_channels) - 1
                    input_channel_to_idx[channel] = channel_idx

                task_input = _ExecutableTaskInput(arg, channel_idx)
            else:
                task_input = _ExecutableTaskInput(arg, None)
            self.task_inputs.append(task_input)

        # Currently DAGs do not support binding kwargs to other DAG nodes.
        for val in self.resolved_kwargs.values():
            assert not isinstance(val, ChannelInterface)

        # Input reader to read input data from upstream DAG nodes.
        self.input_reader: ReaderInterface = SynchronousReader(self.input_channels)
        # Output writer to write output data to downstream DAG nodes.
        self.output_writer: WriterInterface = SynchronousWriter(
            self.output_channels, self.output_idxs
        )
        # The intermediate future for a READ or COMPUTE operation,
        # and `wait()` must be called to get the actual result of the operation.
        # The result of a READ operation will be used by a COMPUTE operation,
        # and the result of a COMPUTE operation will be used by a WRITE operation.
        self._intermediate_future: Optional[DAGOperationFuture] = None

    def cancel(self):
        """
        Close all the input channels and the output channel. The exact behavior
        depends on the type of channel. Typically, it will release the resources
        used by the channels.
        """
        self.input_reader.close()
        self.output_writer.close()

    def prepare(self, overlap_gpu_communication: bool = False):
        """
        Prepare the task for execution. The `exec_operation` function can only
        be called after `prepare` has been called.

        Args:
            overlap_gpu_communication: Whether to overlap GPU communication with
                computation during DAG execution to improve performance
        """
        for typ_hint in self.input_type_hints:
            typ_hint.register_custom_serializer()
        self.output_type_hint.register_custom_serializer()
        self.input_reader.start()
        self.output_writer.start()

        self._send_stream: Union["cp.cuda.Stream", nullcontext] = nullcontext()
        self._recv_stream: Union["cp.cuda.Stream", nullcontext] = nullcontext()
        if not overlap_gpu_communication:
            return

        # Set up send_stream and recv_stream when overlap_gpu_communication
        # is configured
        if self.output_type_hint.requires_nccl():
            nccl_group_id = _get_nccl_group_id(self.output_type_hint)
            nccl_group = ChannelContext.get_current().nccl_groups.get(nccl_group_id)
            assert nccl_group is not None
            self._send_stream = nccl_group.send_stream
        if self.input_type_hints:
            for type_hint in self.input_type_hints:
                if type_hint.requires_nccl():
                    nccl_group_id = _get_nccl_group_id(type_hint)
                    nccl_group = ChannelContext.get_current().nccl_groups.get(
                        nccl_group_id
                    )
                    assert nccl_group is not None
                    if not isinstance(self._recv_stream, nullcontext):
                        assert self._recv_stream == nccl_group.recv_stream, (
                            "Currently all torch tensor input channels of a "
                            "Compiled Graph task should use the same recv cuda stream."
                        )
                    self._recv_stream = nccl_group.recv_stream

    def wrap_and_set_intermediate_future(
        self, val: Any, wrap_in_gpu_future: bool
    ) -> None:
        """
        Wrap the value in a `DAGOperationFuture` and store to the intermediate future.
        The value corresponds to result of a READ or COMPUTE operation.

        If wrap_in_gpu_future is True, the value will be wrapped in a GPUFuture,
        Otherwise, the future will be a ResolvedFuture.

        Args:
            val: The value to wrap in a future.
            wrap_in_gpu_future: Whether to wrap the value in a GPUFuture.
        """
        assert self._intermediate_future is None

        if wrap_in_gpu_future:
            future = GPUFuture(val)
        else:
            future = ResolvedFuture(val)
        self._intermediate_future = future

    def reset_and_wait_intermediate_future(self) -> Any:
        """
        Reset the intermediate future and wait for the result.

        The wait does not block the CPU because:
        - If the future is a ResolvedFuture, the result is immediately returned.
        - If the future is a GPUFuture, the result is only waited by the current
            CUDA stream, and the CPU is not blocked.

        Returns:
            The result of a READ or COMPUTE operation from the intermediate future.
        """
        future = self._intermediate_future
        self._intermediate_future = None
        return future.wait()

    def _read(self, overlap_gpu_communication: bool) -> bool:
        """
        Read input data from upstream DAG nodes and cache the intermediate result.

        Args:
            overlap_gpu_communication: Whether to overlap GPU communication with
                computation during DAG execution to improve performance.

        Returns:
            True if system error occurs and exit the loop; otherwise, False.
        """
        assert self._intermediate_future is None
        exit = False
        try:
            input_data = self.input_reader.read()
            # When overlap_gpu_communication is enabled, wrap the result in
            # a GPUFuture so that this read operation (communication) can
            # be overlapped with computation.
            self.wrap_and_set_intermediate_future(
                input_data, wrap_in_gpu_future=overlap_gpu_communication
            )
        except RayChannelError:
            # Channel closed. Exit the loop.
            exit = True
        return exit

    def _compute(
        self,
        overlap_gpu_communication: bool,
        class_handle,
    ) -> bool:
        """
        Retrieve the intermediate result from the READ operation and perform the
        computation. Then, cache the new intermediate result. The caller must ensure
        that the last operation executed is READ so that the function retrieves the
        correct intermediate result.

        Args:
            overlap_gpu_communication: Whether to overlap GPU communication with
                computation during DAG execution to improve performance.
            class_handle: An instance of the class to which the actor belongs. For
                example, the type of `class_handle` is <class 'xxxx.Worker'> if the
                actor belongs to the `class Worker` class.
        Returns:
            True if system error occurs and exit the loop; otherwise, False.
        """
        input_data = self.reset_and_wait_intermediate_future()
        try:
            _process_return_vals(input_data, return_single_output=False)
        except Exception as exc:
            # Previous task raised an application-level exception.
            # Propagate it and skip the actual task. We don't need to wrap the
            # exception in a RayTaskError here because it has already been wrapped
            # by the previous task.
            self.wrap_and_set_intermediate_future(
                exc, wrap_in_gpu_future=overlap_gpu_communication
            )
            return False

        resolved_inputs = []
        for task_input in self.task_inputs:
            resolved_inputs.append(task_input.resolve(input_data))

        if self.collective_op is not None:
            # Run a NCCL collective operation.
            method = self.collective_op.execute
        else:
            # Run an actor method.
            method = getattr(class_handle, self.method_name)
        try:
            output_val = method(*resolved_inputs, **self.resolved_kwargs)
        except Exception as exc:
            output_val = _wrap_exception(exc)

        # When overlap_gpu_communication is enabled, wrap the result in a GPUFuture
        # so that this compute operation can be overlapped with communication.
        self.wrap_and_set_intermediate_future(
            output_val, wrap_in_gpu_future=overlap_gpu_communication
        )
        return False

    def _write(self) -> bool:
        """
        Retrieve the intermediate result from the COMPUTE operation and write to its
        downstream DAG nodes. The caller must ensure that the last operation executed
        is COMPUTE so that the function retrieves the correct intermediate result.

        Returns:
            True if system error occurs and exit the loop; otherwise, False.
        """
        output_val = self.reset_and_wait_intermediate_future()
        exit = False
        try:
            self.output_writer.write(output_val)
        except RayChannelError:
            # Channel closed. Exit the loop.
            exit = True
        return exit

    def exec_operation(
        self,
        class_handle,
        op_type: _DAGNodeOperationType,
        overlap_gpu_communication: bool = False,
    ) -> bool:
        """
        An ExecutableTask corresponds to a DAGNode. It consists of three
        operations: READ, COMPUTE, and WRITE, which should be executed in
        order to ensure that each operation can read the correct intermediate
        result.
        Args:
            class_handle: The handle of the class to which the actor belongs.
            op_type: The type of the operation. Possible types are READ,
                COMPUTE, and WRITE.
            overlap_gpu_communication: Whether to overlap GPU communication with
                computation during DAG execution to improve performance.
        Returns:
            True if the next operation should not be executed; otherwise, False.
        """
        if op_type == _DAGNodeOperationType.READ:
            with _device_context_manager():
                with self._recv_stream:
                    return self._read(overlap_gpu_communication)
        elif op_type == _DAGNodeOperationType.COMPUTE:
            return self._compute(overlap_gpu_communication, class_handle)
        elif op_type == _DAGNodeOperationType.WRITE:
            with _device_context_manager():
                with self._send_stream:
                    return self._write()


@dataclass
class _ExecutableTaskRecord:
    actor_classname: str
    actor_name: str
    actor_id: str
    method_name: str
    bind_index: int
    operation: str
    start_t: float
    end_t: float

    def to_dict(self):
        return asdict(self)


@DeveloperAPI
class CompiledDAG:
    """Experimental class for accelerated execution.

    This class should not be called directly. Instead, create
    a ray.dag and call experimental_compile().

    See REP https://github.com/ray-project/enhancements/pull/48 for more
    information.
    """

    @ray.remote(num_cpus=0)
    class DAGDriverProxyActor:
        """
        To support the driver as a reader, the output writer needs to be able to invoke
        remote functions on the driver. This is necessary so that the output writer can
        create a reader ref on the driver node, and later potentially create a larger
        reader ref on the driver node if the channel backing store needs to be resized.
        However, remote functions cannot be invoked on the driver.

        An accelerated DAG creates an actor from this class when the DAG is intialized.
        The actor is on the same node as the driver. This class has an empty
        implementation, though it serves as a way for the output writer to invoke remote
        functions on the driver node.
        """

        pass

    def __init__(
        self,
        execution_timeout: Optional[float] = None,
        buffer_size_bytes: Optional[int] = None,
        enable_asyncio: bool = False,
        asyncio_max_queue_size: Optional[int] = None,
        max_buffered_results: Optional[int] = None,
        max_inflight_executions: Optional[int] = None,
        overlap_gpu_communication: Optional[bool] = None,
    ):
        """
        Args:
            execution_timeout: The maximum time in seconds to wait for execute() calls.
                None means using default timeout (DAGContext.execution_timeout),
                0 means immediate timeout (immediate success or timeout without
                blocking), -1 means infinite timeout (block indefinitely).
            buffer_size_bytes: The number of bytes to allocate for object data and
                metadata. Each argument passed to a task in the DAG must be
                less than or equal to this value when serialized.
            enable_asyncio: Whether to enable asyncio. If enabled, caller must
                be running in an event loop and must use `execute_async` to
                invoke the DAG. Otherwise, the caller should use `execute` to
                invoke the DAG.
            asyncio_max_queue_size: Optional parameter to limit how many DAG
                inputs can be queued at a time. The actual number of concurrent
                DAG invocations may be higher than this, if there are already
                inputs being processed by the DAG executors. If used, the
                caller is responsible for preventing deadlock, i.e. if the
                input queue is full, another asyncio task is reading from the
                DAG output. It is only used when enable_asyncio=True.
            max_buffered_results: The maximum number of execution results that
                are allowed to be buffered. Setting a higher value allows more
                DAGs to be executed before `ray.get()` must be called but also
                increases the memory usage. Note that if the number of ongoing
                executions is beyond the DAG capacity, the new execution would
                be blocked in the first place; therefore, this limit is only
                enforced when it is smaller than the DAG capacity.
            max_inflight_executions: The maximum number of in-flight executions that
                are allowed to be sent to this DAG. Before submitting more requests,
                the caller is responsible for calling ray.get to get the result,
                otherwise, RayAdagCapacityExceeded is raised.
            overlap_gpu_communication: Whether to overlap GPU communication with
                computation during DAG execution. If True, the communication
                and computation can be overlapped, which can improve the
                performance of the DAG execution. If None, the default value
                will be used.

        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        from ray.dag import DAGContext

        ctx = DAGContext.get_current()

        self._enable_asyncio: bool = enable_asyncio
        self._fut_queue = asyncio.Queue()
        self._asyncio_max_queue_size: Optional[int] = asyncio_max_queue_size
        # TODO(rui): consider unify it with asyncio_max_queue_size
        self._max_buffered_results: Optional[int] = max_buffered_results
        if self._max_buffered_results is None:
            self._max_buffered_results = ctx.max_buffered_results
        self._max_inflight_executions = max_inflight_executions
        if self._max_inflight_executions is None:
            self._max_inflight_executions = ctx.max_inflight_executions
        self._dag_id = uuid.uuid4().hex
        self._execution_timeout: Optional[float] = execution_timeout
        if self._execution_timeout is None:
            self._execution_timeout = ctx.execution_timeout
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = ctx.buffer_size_bytes
        self._overlap_gpu_communication: Optional[bool] = overlap_gpu_communication
        if self._overlap_gpu_communication is None:
            self._overlap_gpu_communication = ctx.overlap_gpu_communication

        self._default_type_hint: ChannelOutputType = SharedMemoryType(
            buffer_size_bytes=self._buffer_size_bytes,
            # We conservatively set num_shm_buffers to _max_inflight_executions.
            # It means that the DAG can be underutilized, but it guarantees there's
            # no false positive timeouts.
            num_shm_buffers=1,
        )
        if not isinstance(self._buffer_size_bytes, int) or self._buffer_size_bytes <= 0:
            raise ValueError(
                "`buffer_size_bytes` must be a positive integer, found "
                f"{self._buffer_size_bytes}"
            )

        # Used to ensure that the future returned to the
        # caller corresponds to the correct DAG output. I.e.
        # order of futures added to fut_queue should match the
        # order of inputs written to the DAG.
        self._dag_submission_lock = asyncio.Lock()

        # idx -> CompiledTask.
        self.idx_to_task: Dict[int, "CompiledTask"] = {}
        # DAGNode -> idx.
        self.dag_node_to_idx: Dict["ray.dag.DAGNode", int] = {}
        # idx counter.
        self.counter: int = 0

        # Attributes that are set during preprocessing.
        # Preprocessing identifies the input node and output node.
        self.input_task_idx: Optional[int] = None
        self.output_task_idx: Optional[int] = None
        # Denotes whether execute/execute_async returns a list of refs/futures.
        self._returns_list: bool = False
        # Number of expected positional args and kwargs that may be passed to
        # dag.execute.
        self._input_num_positional_args: Optional[int] = None
        self._input_kwargs: Tuple[str, ...] = None

        # Cached attributes that are set during compilation.
        self.dag_input_channels: Optional[List[ChannelInterface]] = None
        self.dag_output_channels: Optional[List[ChannelInterface]] = None
        self._dag_submitter: Optional[WriterInterface] = None
        self._dag_output_fetcher: Optional[ReaderInterface] = None

        # ObjectRef for each worker's task. The task is an infinite loop that
        # repeatedly executes the method specified in the DAG.
        self.worker_task_refs: Dict["ray.actor.ActorHandle", "ray.ObjectRef"] = {}
        # Set of actors present in the DAG.
        self.actor_refs = set()
        self.actor_to_tasks: Dict[
            "ray.actor.ActorHandle", List["CompiledTask"]
        ] = defaultdict(list)
        self.actor_to_executable_tasks: Dict[
            "ray.actor.ActorHandle", List["ExecutableTask"]
        ] = {}
        # Mapping from the actor handle to the execution schedule which is a list
        # of operations to be executed.
        self.actor_to_execution_schedule: Dict[
            "ray.actor.ActorHandle", List[_DAGNodeOperation]
        ] = defaultdict(list)
        # Mapping from the actor handle to the node ID that the actor is on.
        self.actor_to_node_id: Dict["ray.actor.ActorHandle", str] = {}

        # This is set to true when type hint of `transport="nccl"` is used.
        self._use_default_nccl_group = False
        # This is set to the specified custom nccl group
        # if there exists a type hint of `transport=nccl_group`.
        self._custom_nccl_group_p2p: Optional[GPUCommunicator] = None
        # The NCCL group ID for P2P send/recv operations.
        self._nccl_group_id_p2p: Optional[str] = None
        # All the NCCL group IDs for P2P send/recv and collective operations.
        self._nccl_group_ids: Set[str] = set()
        # The index of the current execution. It is incremented each time
        # the DAG is executed.
        self._execution_index: int = 0
        # The maximum index of finished executions.
        # All results with higher indexes have not been generated yet.
        self._max_finished_execution_index: int = -1
        # execution_index -> {channel_index -> result}
        self._result_buffer: Dict[int, Dict[int, Any]] = defaultdict(dict)
        # channel to possible inner channel
        self._channel_dict: Dict[ChannelInterface, ChannelInterface] = {}

        def _create_proxy_actor() -> "ray.actor.ActorHandle":
            # Creates the driver actor on the same node as the driver.
            #
            # To support the driver as a reader, the output writer needs to be able to
            # invoke remote functions on the driver (e.g., to create the reader ref, to
            # create a reader ref for a larger object when the channel backing store is
            # resized, etc.). The driver actor serves as a way for the output writer
            # to invoke remote functions on the driver node.
            return CompiledDAG.DAGDriverProxyActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(), soft=False
                )
            ).remote()

        self._proxy_actor = _create_proxy_actor()
        # Set to True when `teardown` API is called.
        self._is_teardown = False

    @property
    def nccl_group_id_p2p(self) -> Optional[str]:
        return self._nccl_group_id_p2p

    @property
    def is_teardown(self) -> bool:
        return self._is_teardown

    @property
    def nccl_group_ids(self) -> Set[str]:
        return self._nccl_group_ids

    def increment_max_finished_execution_index(self) -> None:
        """Increment the max finished execution index. It is used to
        figure out the max number of in-flight requests to the DAG
        """
        self._max_finished_execution_index += 1

    def get_id(self) -> str:
        """
        Get the unique ID of the compiled DAG.
        """
        return self._dag_id

    def __str__(self) -> str:
        return f"CompiledDAG({self._dag_id})"

    def _add_node(self, node: "ray.dag.DAGNode") -> None:
        idx = self.counter
        self.idx_to_task[idx] = CompiledTask(idx, node)
        self.dag_node_to_idx[node] = idx
        self.counter += 1

    def _preprocess(self) -> None:
        """Before compiling, preprocess the DAG to build an index from task to
        upstream and downstream tasks, and to set the input and output node(s)
        of the DAG.

        This function is idempotent.
        """
        from ray.dag import (
            DAGNode,
            ClassMethodNode,
            CollectiveOutputNode,
            FunctionNode,
            InputAttributeNode,
            InputNode,
            MultiOutputNode,
        )
        from ray.dag.collective_node import _CollectiveOperation

        self.input_task_idx, self.output_task_idx = None, None

        nccl_actors_p2p: Set["ray.actor.ActorHandle"] = set()
        nccl_collective_ops: Set[_CollectiveOperation] = set()

        # Find the input node to the DAG.
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "More than one InputNode found"
                self.input_task_idx = idx

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
            if idx == self.input_task_idx or isinstance(
                task.dag_node, InputAttributeNode
            ):
                continue
            if (
                len(task.downstream_task_idxs) == 0
                and task.dag_node.is_adag_output_node
            ):
                assert self.output_task_idx is None, "More than one output node found"
                self.output_task_idx = idx

        assert self.output_task_idx is not None
        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # Add an MultiOutputNode to the end of the DAG if it's not already there.
        if not isinstance(output_node, MultiOutputNode):
            output_node = MultiOutputNode([output_node])
            self._add_node(output_node)
            self.output_task_idx = self.dag_node_to_idx[output_node]
        else:
            self._returns_list = True

        # TODO: Support no-input DAGs (use an empty object to signal).
        if self.input_task_idx is None:
            raise NotImplementedError(
                "Compiled DAGs currently require exactly one InputNode"
            )

        # Whether the DAG binds directly to the InputNode(), versus binding to
        # a positional arg or kwarg of the input. For example, a.foo.bind(inp)
        # instead of a.foo.bind(inp[0]) or a.foo.bind(inp.key).
        direct_input: Optional[bool] = None
        # Collect the set of InputNode keys bound to DAG node args.
        input_positional_args: Set[int] = set()
        input_kwargs: Set[str] = set()

        # For each task node, set its upstream and downstream task nodes.
        # Also collect the set of tasks that produce torch.tensors.
        for task_idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            if not (
                isinstance(dag_node, InputNode)
                or isinstance(dag_node, InputAttributeNode)
                or isinstance(dag_node, MultiOutputNode)
                or isinstance(dag_node, ClassMethodNode)
            ):
                if isinstance(dag_node, FunctionNode):
                    # TODO(swang): Support non-actor tasks.
                    raise NotImplementedError(
                        "Compiled DAGs currently only support actor method nodes"
                    )
                else:
                    raise ValueError(f"Found unsupported node of type {type(dag_node)}")

            if isinstance(dag_node, ClassMethodNode) and dag_node.is_class_method_call:
                actor_handle = dag_node._get_actor_handle()
                if actor_handle is None:
                    raise ValueError(
                        "Compiled DAGs can only bind methods to an actor "
                        "that is already created with Actor.remote()"
                    )

                # Collect actors for NCCL P2P methods.
                if dag_node.type_hint.requires_nccl():
                    nccl_actors_p2p.add(actor_handle)
                    custom_nccl_group = dag_node.type_hint.get_custom_nccl_group()
                    mixed_nccl_group_error_message = (
                        "Accelerated DAGs do not support mixed usage of "
                        "type hints of default NCCL group "
                        '(i.e., TorchTensor(transport="nccl"))'
                        "and custom NCCL group "
                        "(i.e., TorchTensor(transport=nccl_group)). "
                        "Please check all the TorchTensor type hints and "
                        "make sure only one type of NCCL transport is specified."
                    )
                    if custom_nccl_group is None:
                        if self._custom_nccl_group_p2p is not None:
                            raise ValueError(mixed_nccl_group_error_message)
                        self._use_default_nccl_group = True
                    else:
                        if self._use_default_nccl_group:
                            raise ValueError(mixed_nccl_group_error_message)
                        if self._custom_nccl_group_p2p is not None:
                            if self._custom_nccl_group_p2p != custom_nccl_group:
                                raise ValueError(
                                    "Accelerated DAGs currently only support "
                                    "a single custom NCCL group, but multiple "
                                    "have been specified. Check all the "
                                    "TorchTensor(transport=nccl_group) type hints "
                                    "to make sure only one NCCL group is used."
                                )
                        self._custom_nccl_group_p2p = custom_nccl_group

                # Collect NCCL collective operations.
                if isinstance(dag_node, CollectiveOutputNode):
                    nccl_collective_ops.add(dag_node.collective_op)
                    assert not self._overlap_gpu_communication, (
                        "Currently, the overlap_gpu_communication option is not "
                        "supported for NCCL collective operations. Please set "
                        "overlap_gpu_communication=False."
                    )
            elif isinstance(dag_node, InputNode):
                if dag_node.type_hint.requires_nccl():
                    raise ValueError(
                        "DAG inputs cannot be transferred via NCCL because "
                        "the driver cannot participate in the NCCL group"
                    )

            if type(dag_node.type_hint) == ChannelOutputType:
                # No type hint specified by the user. Replace
                # with the default type hint for this DAG.
                dag_node.with_type_hint(self._default_type_hint)

            for _, val in task.kwargs.items():
                if isinstance(val, DAGNode):
                    raise ValueError(
                        "Compiled DAG currently does not support binding to "
                        "other DAG nodes as kwargs"
                    )

            for _, arg in enumerate(task.args):
                if not isinstance(arg, DAGNode):
                    continue
                upstream_node_idx = self.dag_node_to_idx[arg]
                upstream_task = self.idx_to_task[upstream_node_idx]
                downstream_actor_handle = None
                if (
                    isinstance(dag_node, ClassMethodNode)
                    and dag_node.is_class_method_call
                ):
                    downstream_actor_handle = dag_node._get_actor_handle()

                if isinstance(upstream_task.dag_node, InputAttributeNode):
                    # Record all of the keys used to index the InputNode.
                    # During execution, we will check that the user provides
                    # the same args and kwargs.
                    if isinstance(upstream_task.dag_node.key, int):
                        input_positional_args.add(upstream_task.dag_node.key)
                    elif isinstance(upstream_task.dag_node.key, str):
                        input_kwargs.add(upstream_task.dag_node.key)
                    else:
                        raise ValueError(
                            "InputNode() can only be indexed using int "
                            "for positional args or str for kwargs."
                        )

                    if direct_input is not None and direct_input:
                        raise ValueError(
                            "All tasks must either use InputNode() "
                            "directly, or they must index to specific args or "
                            "kwargs."
                        )
                    direct_input = False

                    # If the upstream node is an InputAttributeNode, treat the
                    # DAG's input node as the actual upstream node
                    upstream_task = self.idx_to_task[self.input_task_idx]

                elif isinstance(upstream_task.dag_node, InputNode):
                    if direct_input is not None and not direct_input:
                        raise ValueError(
                            "All tasks must either use InputNode() directly, "
                            "or they must index to specific args or kwargs."
                        )
                    direct_input = True

                upstream_task.downstream_task_idxs[task_idx] = downstream_actor_handle
                task.arg_type_hints.append(upstream_task.dag_node.type_hint)

                if upstream_task.dag_node.type_hint.requires_nccl():
                    # Add all readers to the NCCL actors of P2P.
                    nccl_actors_p2p.add(downstream_actor_handle)

        # Collect all leaf nodes.
        leaf_nodes: DAGNode = []
        for idx, task in self.idx_to_task.items():
            if not isinstance(task.dag_node, ClassMethodNode):
                continue
            if (
                len(task.downstream_task_idxs) == 0
                and not task.dag_node.is_adag_output_node
            ):
                leaf_nodes.append(task.dag_node)
        # Leaf nodes are not allowed because the exception thrown by the leaf
        # node will not be propagated to the driver.
        if len(leaf_nodes) != 0:
            raise ValueError(
                "Compiled DAG doesn't support leaf nodes, i.e., nodes that don't have "
                "downstream nodes and are not output nodes. There are "
                f"{len(leaf_nodes)} leaf nodes in the DAG. Please add the outputs of "
                f"{[leaf_node.get_method_name() for leaf_node in leaf_nodes]} to the "
                f"the MultiOutputNode."
            )

        nccl_actors_p2p = list(nccl_actors_p2p)
        if None in nccl_actors_p2p:
            raise ValueError("Driver cannot participate in the NCCL group.")

        # Initialize and cache a NCCL group for each custom NCCL group. All the
        # custom NCCL groups are initialized before the default NCCL groups.
        custom_nccl_group_to_id: Dict[GPUCommunicator, str] = {}
        # Initialize and cache a NCCL group for each set of actors. A set of actors
        # can perform P2P send/recv and collective operations. If there are multiple
        # custom NCCL groups for a set of actors, only one is cached.
        actors_to_nccl_group_id: Dict[FrozenSet["ray.actor.ActorHandle"], str] = {}

        # If a custom NCCL group is specified for P2P actors, initialize and cache
        # the NCCL group ID.
        if nccl_actors_p2p and self._custom_nccl_group_p2p:
            if not set(nccl_actors_p2p).issubset(
                set(self._custom_nccl_group_p2p.get_actor_handles())
            ):
                raise ValueError(
                    "Expected P2P actor handles to be a subset of the custom NCCL group"
                )
            self._nccl_group_id_p2p = _init_nccl_group(
                nccl_actors_p2p,
                self._custom_nccl_group_p2p,
                self._overlap_gpu_communication,
            )
            custom_nccl_group_to_id[
                self._custom_nccl_group_p2p
            ] = self._nccl_group_id_p2p
            actors = frozenset(nccl_actors_p2p)
            actors_to_nccl_group_id[actors] = self._nccl_group_id_p2p

        # If a custom NCCL group is specified for collective actors, initialize and
        # cache the NCCL group ID.
        for collective_op in nccl_collective_ops:
            type_hint = collective_op.type_hint
            custom_nccl_group = type_hint.get_custom_nccl_group()
            if custom_nccl_group:
                nccl_group_id = collective_op.init_nccl_group(
                    custom_nccl_group_to_id.get(custom_nccl_group, None)
                )
                custom_nccl_group_to_id[custom_nccl_group] = nccl_group_id
                actors = frozenset(collective_op.actor_handles)
                if actors not in actors_to_nccl_group_id:
                    actors_to_nccl_group_id[actors] = nccl_group_id

        # If a NCCL group for P2P actors is not initialized, initialize and cache
        # the NCCL group ID.
        if nccl_actors_p2p and self._nccl_group_id_p2p is None:
            actors = frozenset(nccl_actors_p2p)
            if actors in actors_to_nccl_group_id:
                self._nccl_group_id_p2p = actors_to_nccl_group_id[actors]
            else:
                self._nccl_group_id_p2p = _init_nccl_group(
                    nccl_actors_p2p,
                    self._custom_nccl_group_p2p,
                    self._overlap_gpu_communication,
                )
                actors_to_nccl_group_id[actors] = self._nccl_group_id_p2p

        # If a NCCL group for collective actors is not initialized, initialize and
        # cache the NCCL group ID.
        for collective_op in nccl_collective_ops:
            if collective_op.type_hint.nccl_group_id is None:
                actors = frozenset(collective_op.actor_handles)
                nccl_group_id = collective_op.init_nccl_group(
                    actors_to_nccl_group_id.get(actors, None)
                )
                if actors not in actors_to_nccl_group_id:
                    actors_to_nccl_group_id[actors] = nccl_group_id

        # Store all the NCCL group IDs for P2P send/recv and collective operations.
        self._nccl_group_ids = set(actors_to_nccl_group_id.values()).union(
            set(custom_nccl_group_to_id.values())
        )

        if direct_input:
            self._input_num_positional_args = 1
        elif not input_positional_args:
            self._input_num_positional_args = 0
        else:
            self._input_num_positional_args = max(input_positional_args) + 1
        self._input_kwargs = tuple(input_kwargs)

    def _get_node_id(self, actor_handle: "ray.actor.ActorHandle") -> str:
        """
        Get the node ID of an actor handle and cache it.

        Args:
            actor_handle: The actor handle.
        Returns:
            The node ID of the actor handle.
        """
        if actor_handle in self.actor_to_node_id:
            return self.actor_to_node_id[actor_handle]
        node_id = None
        if actor_handle == self._proxy_actor:
            node_id = ray.get_runtime_context().get_node_id()
        else:
            node_id = ray.get(
                actor_handle.__ray_call__.remote(
                    lambda self: ray.get_runtime_context().get_node_id()
                )
            )
        assert node_id is not None
        self.actor_to_node_id[actor_handle] = node_id
        return node_id

    def _get_or_compile(
        self,
    ) -> None:
        """Compile an execution path. This allocates channels for adjacent
        tasks to send/receive values. An infinite task is submitted to each
        actor in the DAG that repeatedly receives from input channel(s) and
        sends to output channel(s).

        This function is idempotent and will cache the previously allocated
        channels. After calling this function, _dag_submitter and
        _dag_output_fetcher will be set and can be used to invoke and fetch
        outputs for the DAG.
        """
        from ray.dag import (
            DAGNode,
            InputNode,
            InputAttributeNode,
            MultiOutputNode,
            ClassMethodNode,
        )

        if self.input_task_idx is None:
            self._preprocess()
        assert self.input_task_idx is not None

        if self._dag_submitter is not None:
            assert self._dag_output_fetcher is not None
            return

        frontier = [self.input_task_idx]
        visited = set()
        # Create output buffers. This loop does a breadth-first search through the DAG.
        while frontier:
            cur_idx = frontier.pop(0)
            if cur_idx in visited:
                continue
            visited.add(cur_idx)

            task = self.idx_to_task[cur_idx]
            type_hint = task.dag_node.type_hint
            if type_hint.requires_nccl():
                type_hint.set_nccl_group_id(self._nccl_group_id_p2p)

            if (
                isinstance(task.dag_node, ClassMethodNode)
                and task.dag_node.is_class_method_call
            ):
                # Create output buffers for the actor method.
                assert len(task.output_channels) == 0
                # `output_to_readers` stores the reader tasks for each output of
                # the current node. If the current node returns one output, the
                # readers are the downstream nodes of the current node. If the
                # current node returns multiple outputs, the readers of each
                # output are the downstream nodes of the ClassMethodNode that
                # is a class method output.
                output_to_readers: Dict[CompiledTask, List[CompiledTask]] = defaultdict(
                    list
                )
                for idx in task.downstream_task_idxs:
                    downstream_task = self.idx_to_task[idx]
                    downstream_node = downstream_task.dag_node
                    if (
                        isinstance(downstream_node, ClassMethodNode)
                        and downstream_node.is_class_method_output
                    ):
                        output_to_readers[downstream_task] = [
                            self.idx_to_task[idx]
                            for idx in downstream_task.downstream_task_idxs
                        ]
                    else:
                        if task not in output_to_readers:
                            output_to_readers[task] = []
                        output_to_readers[task].append(downstream_task)
                fn = task.dag_node._get_remote_method("__ray_call__")
                for output, readers in output_to_readers.items():
                    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]] = []
                    # Use reader_handles_set to deduplicate readers on the
                    # same actor, because with CachedChannel each actor will
                    # only read from the upstream channel once.
                    reader_handles_set = set()
                    read_by_multi_output_node = False
                    for reader in readers:
                        if isinstance(reader.dag_node, MultiOutputNode):
                            read_by_multi_output_node = True
                            # inserting at 0 to make sure driver is first reader as
                            # expected by CompositeChannel read
                            reader_and_node_list.insert(
                                0,
                                (
                                    self._proxy_actor,
                                    self._get_node_id(self._proxy_actor),
                                ),
                            )
                        else:
                            reader_handle = reader.dag_node._get_actor_handle()
                            if reader_handle not in reader_handles_set:
                                reader_handle = reader.dag_node._get_actor_handle()
                                reader_and_node_list.append(
                                    (reader_handle, self._get_node_id(reader_handle))
                                )
                                reader_handles_set.add(reader_handle)

                    # if driver is an actual actor, gets driver actor id
                    driver_actor_id = (
                        ray.get_runtime_context().get_actor_id()
                        if read_by_multi_output_node
                        else None
                    )
                    # Create an output channel for each output of the current node.
                    output_channel = ray.get(
                        fn.remote(
                            do_allocate_channel,
                            reader_and_node_list,
                            type_hint,
                            driver_actor_id,
                        )
                    )
                    output_idx = None
                    downstream_node = output.dag_node
                    if (
                        isinstance(downstream_node, ClassMethodNode)
                        and downstream_node.is_class_method_output
                    ):
                        output_idx = downstream_node.output_idx
                    task.output_channels.append(output_channel)
                    task.output_idxs.append(output_idx)
                    task.output_node_idxs.append(self.dag_node_to_idx[downstream_node])
                actor_handle = task.dag_node._get_actor_handle()
                assert actor_handle is not None
                self.actor_refs.add(actor_handle)
                self.actor_to_tasks[actor_handle].append(task)
            elif (
                isinstance(task.dag_node, ClassMethodNode)
                and task.dag_node.is_class_method_output
            ):
                task_node = task.dag_node
                upstream_node = task_node.class_method_call
                assert upstream_node
                upstream_task = self.idx_to_task[self.dag_node_to_idx[upstream_node]]
                for i in range(len(upstream_task.output_channels)):
                    if upstream_task.output_idxs[i] == task_node.output_idx:
                        task.output_channels.append(upstream_task.output_channels[i])
                        task.output_idxs.append(upstream_task.output_idxs[i])
                assert len(task.output_channels) == 1
            elif isinstance(task.dag_node, InputNode):
                # A dictionary that maps an InputNode or InputAttributeNode to its
                # readers and the node on which the reader is running. Use `set` to
                # deduplicate readers on the same actor because with CachedChannel
                # each actor will only read from the shared memory once.
                input_node_to_reader_and_node_set: Dict[
                    Union[InputNode, InputAttributeNode],
                    Set[Tuple["ray.actor.ActorHandle", str]],
                ] = defaultdict(set)

                for idx in task.downstream_task_idxs:
                    reader_task = self.idx_to_task[idx]
                    assert isinstance(reader_task.dag_node, ClassMethodNode)
                    reader_handle = reader_task.dag_node._get_actor_handle()
                    reader_node_id = self._get_node_id(reader_handle)
                    for arg in reader_task.args:
                        if isinstance(arg, InputAttributeNode) or isinstance(
                            arg, InputNode
                        ):
                            input_node_to_reader_and_node_set[arg].add(
                                (reader_handle, reader_node_id)
                            )

                # A single channel is responsible for sending the same data to
                # corresponding consumers. Therefore, we create a channel for
                # each InputAttributeNode, or a single channel for the entire
                # input data if there are no InputAttributeNodes.
                task.output_channels = []
                for input_dag_node in input_node_to_reader_and_node_set:
                    reader_and_node_list = list(
                        input_node_to_reader_and_node_set[input_dag_node]
                    )
                    output_channel = do_allocate_channel(
                        self,
                        reader_and_node_list,
                        type_hint,
                        None,
                    )
                    task.output_channels.append(output_channel)
                    task.output_idxs.append(
                        None
                        if isinstance(input_dag_node, InputNode)
                        else input_dag_node.key
                    )

                    # Update the InputAttributeNode's `output_channels`, which is
                    # used to determine whether to create a CachedChannel.
                    if isinstance(input_dag_node, InputAttributeNode):
                        input_attr_idx = self.dag_node_to_idx[input_dag_node]
                        input_attr_task = self.idx_to_task[input_attr_idx]
                        input_attr_task.output_channels.append(output_channel)
                        assert len(input_attr_task.output_channels) == 1
            else:
                assert isinstance(task.dag_node, InputAttributeNode) or isinstance(
                    task.dag_node, MultiOutputNode
                )

            for idx in task.downstream_task_idxs:
                frontier.append(idx)

        # Validate input channels for tasks that have not been visited
        for node_idx, task in self.idx_to_task.items():
            if (
                node_idx == self.input_task_idx
                or node_idx == self.output_task_idx
                or isinstance(task.dag_node, InputAttributeNode)
            ):
                continue
            if node_idx not in visited:
                has_at_least_one_channel_input = False
                for arg in task.args:
                    if isinstance(arg, DAGNode):
                        has_at_least_one_channel_input = True
                if not has_at_least_one_channel_input:
                    raise ValueError(
                        "Compiled DAGs require each task to take a ray.dag.InputNode "
                        "or at least one other DAGNode as an input. "
                        "Invalid task node:\n"
                        f"{task.dag_node}\n"
                        "Please bind the task to proper DAG nodes."
                    )

        from ray.dag.constants import RAY_ADAG_ENABLE_DETECT_DEADLOCK

        if RAY_ADAG_ENABLE_DETECT_DEADLOCK and self._detect_deadlock():
            raise ValueError(
                "This DAG cannot be compiled because it will deadlock on NCCL "
                "calls. If you believe this is a false positive, please disable "
                "the graph verification by setting the environment variable "
                "RAY_ADAG_ENABLE_DETECT_DEADLOCK to 0 and file an issue at "
                "https://github.com/ray-project/ray/issues/new/."
            )

        input_task = self.idx_to_task[self.input_task_idx]
        # Register custom serializers for inputs provided to dag.execute().
        input_task.dag_node.type_hint.register_custom_serializer()
        self.dag_input_channels = input_task.output_channels
        assert self.dag_input_channels is not None

        # Create executable tasks for each actor
        for actor_handle, tasks in self.actor_to_tasks.items():
            # Dict from arg to the set of tasks that consume it.
            arg_to_consumers: Dict[DAGNode, Set[CompiledTask]] = defaultdict(set)

            # Step 1: populate `arg_to_consumers` and perform some validation.
            for task in tasks:
                has_at_least_one_channel_input = False
                for arg in task.args:
                    if isinstance(arg, DAGNode):
                        has_at_least_one_channel_input = True
                        arg_to_consumers[arg].add(task)
                        arg_idx = self.dag_node_to_idx[arg]
                        upstream_task = self.idx_to_task[arg_idx]
                        assert len(upstream_task.output_channels) == 1
                        arg_channel = upstream_task.output_channels[0]
                        assert arg_channel is not None
                # TODO: Support no-input DAGs (use an empty object to signal).
                if not has_at_least_one_channel_input:
                    raise ValueError(
                        "Compiled DAGs require each task to take a "
                        "ray.dag.InputNode or at least one other DAGNode as an "
                        "input"
                    )

            # Step 2: create cached channels if needed

            # Dict from original channel to the channel to be used in execution.
            # The value of this dict is either the original channel or a newly
            # created CachedChannel (if the original channel is read more than once).
            for arg, consumers in arg_to_consumers.items():
                arg_idx = self.dag_node_to_idx[arg]
                upstream_task = self.idx_to_task[arg_idx]
                assert len(upstream_task.output_channels) == 1
                arg_channel = upstream_task.output_channels[0]
                assert arg_channel is not None
                if len(consumers) > 1:
                    self._channel_dict[arg_channel] = CachedChannel(
                        len(consumers),
                        arg_channel,
                    )
                else:
                    self._channel_dict[arg_channel] = arg_channel

            # Step 3: create executable tasks for the actor
            executable_tasks = []
            for task in tasks:
                resolved_args: List[Any] = []
                for arg in task.args:
                    if isinstance(arg, DAGNode):
                        arg_idx = self.dag_node_to_idx[arg]
                        upstream_task = self.idx_to_task[arg_idx]
                        assert len(upstream_task.output_channels) == 1
                        arg_channel = upstream_task.output_channels[0]
                        assert arg_channel is not None
                        arg_channel = self._channel_dict[arg_channel]
                        resolved_args.append(arg_channel)
                    else:
                        # Constant arg
                        resolved_args.append(arg)
                executable_task = ExecutableTask(
                    task,
                    resolved_args,
                    task.kwargs,
                )
                executable_tasks.append(executable_task)
            # Sort executable tasks based on their bind index, i.e., submission order
            # so that they will be executed in that order.
            executable_tasks.sort(key=lambda task: task.bind_index)
            self.actor_to_executable_tasks[actor_handle] = executable_tasks

        # Build an execution schedule for each actor
        from ray.dag.constants import RAY_ADAG_ENABLE_PROFILING

        if RAY_ADAG_ENABLE_PROFILING:
            exec_task_func = do_profile_tasks
        else:
            exec_task_func = do_exec_tasks

        self.actor_to_execution_schedule = self._build_execution_schedule()
        for actor_handle, executable_tasks in self.actor_to_executable_tasks.items():
            self.worker_task_refs[actor_handle] = actor_handle.__ray_call__.options(
                concurrency_group="_ray_system"
            ).remote(
                exec_task_func,
                executable_tasks,
                self.actor_to_execution_schedule[actor_handle],
                self._overlap_gpu_communication,
            )

        assert self.output_task_idx is not None
        self.dag_output_channels = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            task = self.idx_to_task[output_idx]
            assert len(task.output_channels) == 1
            self.dag_output_channels.append(task.output_channels[0])
            # Register custom serializers for DAG outputs.
            output.type_hint.register_custom_serializer()

        assert self.dag_input_channels
        assert self.dag_output_channels
        assert [
            output_channel is not None for output_channel in self.dag_output_channels
        ]
        # If no MultiOutputNode was specified during the DAG creation, there is only
        # one output. Return a single output channel instead of a list of
        # channels.
        if not self._returns_list:
            assert len(self.dag_output_channels) == 1

        # Driver should ray.put on input, ray.get/release on output
        self._monitor = self._monitor_failures()
        input_task = self.idx_to_task[self.input_task_idx]
        if self._enable_asyncio:
            self._dag_submitter = AwaitableBackgroundWriter(
                self.dag_input_channels,
                input_task.output_idxs,
                self._asyncio_max_queue_size,
                is_input=True,
            )
            self._dag_output_fetcher = AwaitableBackgroundReader(
                self.dag_output_channels,
                self._fut_queue,
            )
        else:
            self._dag_submitter = SynchronousWriter(
                self.dag_input_channels, input_task.output_idxs, is_input=True
            )
            self._dag_output_fetcher = SynchronousReader(self.dag_output_channels)

        self._dag_submitter.start()
        self._dag_output_fetcher.start()

    def _generate_dag_operation_graph_node(
        self,
    ) -> Dict["ray.actor.ActorHandle", List[List[_DAGOperationGraphNode]]]:
        """
        Generate READ, COMPUTE, and WRITE operations for each DAG node.

        Returns:
            A dictionary that maps an actor handle to a list of lists of
            _DAGOperationGraphNode. For the same actor, the index of the
            outer list corresponds to the index of the ExecutableTask in
            the list of `executable_tasks` in `actor_to_executable_tasks`,
            i.e. `exec_task_idx`. In the inner list, the order of operations
            is READ, COMPUTE, and WRITE.

            Example:
            {
                actor1: [
                    [READ COMPUTE WRITE] # exec_task_idx 0
                    [READ COMPUTE WRITE] # exec_task_idx 1
                ]
            }
        """
        from ray.dag.collective_node import CollectiveOutputNode, _CollectiveOperation

        assert self.idx_to_task
        assert self.actor_to_executable_tasks

        actor_to_operation_nodes: Dict[
            "ray.actor.ActorHandle", List[List[_DAGOperationGraphNode]]
        ] = defaultdict(list)
        collective_op_to_nodes: Dict[
            _CollectiveOperation, Set[_DAGOperationGraphNode]
        ] = defaultdict(set)
        collective_op_to_idxs: Dict[
            _CollectiveOperation, Tuple[int, _DAGNodeOperationType]
        ] = defaultdict(set)

        for actor_handle, executable_tasks in self.actor_to_executable_tasks.items():
            for exec_task_idx, exec_task in enumerate(executable_tasks):
                # Divide a DAG node into three _DAGOperationGraphNodes: READ, COMPUTE,
                # and WRITE. Each _DAGOperationGraphNode has a _DAGNodeOperation.
                task_idx = exec_task.task_idx
                dag_node = self.idx_to_task[task_idx].dag_node
                method_name = exec_task.method_name
                actor_handle = dag_node._get_actor_handle()
                requires_nccl = dag_node.type_hint.requires_nccl()
                upstream_requires_nccl = False
                for upstream_node in dag_node._upstream_nodes:
                    if upstream_node.type_hint.requires_nccl():
                        upstream_requires_nccl = True
                        break

                read_node = _DAGOperationGraphNode(
                    _DAGNodeOperation(
                        exec_task_idx, _DAGNodeOperationType.READ, method_name
                    ),
                    task_idx,
                    actor_handle,
                    upstream_requires_nccl,
                )
                compute_node = _DAGOperationGraphNode(
                    _DAGNodeOperation(
                        exec_task_idx, _DAGNodeOperationType.COMPUTE, method_name
                    ),
                    task_idx,
                    actor_handle,
                    isinstance(dag_node, CollectiveOutputNode),
                )
                write_node = _DAGOperationGraphNode(
                    _DAGNodeOperation(
                        exec_task_idx, _DAGNodeOperationType.WRITE, method_name
                    ),
                    task_idx,
                    actor_handle,
                    requires_nccl,
                )

                actor_to_operation_nodes[actor_handle].append(
                    [read_node, compute_node, write_node]
                )
                if isinstance(dag_node, CollectiveOutputNode):
                    collective_op_to_nodes[dag_node.collective_op].add(compute_node)
                    collective_op_to_idxs[dag_node.collective_op].add(
                        (task_idx, _DAGNodeOperationType.COMPUTE)
                    )

        # Set collective nodes for all the NCCL collective operation nodes.
        for collective_op, nodes in collective_op_to_nodes.items():
            idxs = collective_op_to_idxs[collective_op]
            for node in nodes:
                node.collective_idxs = idxs

        return actor_to_operation_nodes

    def _build_execution_schedule(
        self,
    ) -> Dict["ray.actor.ActorHandle", List[_DAGNodeOperation]]:
        """
        Generate an execution schedule for each actor. The schedule is a list of
        _DAGNodeOperation.

        Step 1: Generate a DAG node operation graph. Refer to the functions
        `_generate_dag_operation_graph_node` and `_build_dag_node_operation_graph`
        for more details.

        Step 2: Topological sort

        It is possible to have multiple _DAGOperationGraphNodes with zero in-degree.
        Refer to the function `_select_next_nodes` for the logic of selecting nodes.

        Then, put the selected nodes into the corresponding actors' schedules.

        The schedule should be intuitive to users, meaning that the execution should
        perform operations in ascending order of `bind_index` as much as possible.

        [Example]:

        See `test_execution_schedule` for more examples.

        Returns:
            actor_to_execution_schedule: A dictionary that maps an actor handle to
                the execution schedule which is a list of operations to be executed.
        """
        # Step 1: Build a graph of _DAGOperationGraphNode
        actor_to_operation_nodes = self._generate_dag_operation_graph_node()
        graph = _build_dag_node_operation_graph(
            self.idx_to_task, actor_to_operation_nodes
        )
        # Step 2: Generate an execution schedule for each actor using topological sort
        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)

        # Step 3: Overlap GPU communication for the execution schedule if configured
        actor_to_overlapped_schedule = None
        if self._overlap_gpu_communication:
            actor_to_overlapped_schedule = _generate_overlapped_execution_schedule(
                actor_to_execution_schedule
            )

        if RAY_ADAG_VISUALIZE_SCHEDULE:
            _visualize_execution_schedule(
                actor_to_execution_schedule, actor_to_overlapped_schedule, graph
            )

        if actor_to_overlapped_schedule is not None:
            return _extract_execution_schedule(actor_to_overlapped_schedule)
        else:
            return _extract_execution_schedule(actor_to_execution_schedule)

    def _detect_deadlock(self) -> bool:
        """
        Check whether the DAG will deadlock on NCCL calls.
        There are no false positives in this deadlock detection,
        but there may be false negatives for now. For example,

        actor1.f1 ---> actor2.f1
                   |
        actor1.f2 --

        In this case, actor1.f1 and actor1.f2 have control dependencies
        between them. If actor2.f1 reads actor1.f2 first and then actor1.f1,
        there will be a deadlock. However, this deadlock is not detectable
        until we have a more granular execution schedule.

        TODO (kevin85421): Avoid false negatives

        Returns:
            True if deadlock is detected, otherwise False.
        """
        assert self.idx_to_task
        assert self.actor_to_tasks

        from ray.dag import ClassMethodNode

        def _is_same_actor(idx1: int, idx2: int) -> bool:
            """
            Args:
                idx1: A key in the idx_to_task dictionary.
                idx2: A key in the idx_to_task dictionary.

            Returns:
                True if both DAG nodes are on the same actor;
                otherwise, False.
            """
            task1 = self.idx_to_task[idx1]
            task2 = self.idx_to_task[idx2]
            if (
                not isinstance(task1.dag_node, ClassMethodNode)
                or task1.dag_node.is_class_method_output
            ):
                return False
            if (
                not isinstance(task2.dag_node, ClassMethodNode)
                or task2.dag_node.is_class_method_output
            ):
                return False
            actor_id_1 = task1.dag_node._get_actor_handle()._actor_id
            actor_id_2 = task2.dag_node._get_actor_handle()._actor_id
            return actor_id_1 == actor_id_2

        for idx, task in self.idx_to_task.items():
            for downstream_idx in task.downstream_task_idxs:
                if task.dag_node.type_hint.requires_nccl():
                    if _is_same_actor(idx, downstream_idx):
                        actor_handle = self.idx_to_task[
                            idx
                        ].dag_node._get_actor_handle()
                        method = self.idx_to_task[idx].dag_node.get_method_name()
                        downstream_method = self.idx_to_task[
                            downstream_idx
                        ].dag_node.get_method_name()
                        logger.error(
                            "Detected a deadlock caused by using NCCL channels to "
                            f"transfer data between the task `{method}` and "
                            f"its downstream method `{downstream_method}` on the same "
                            f"actor {actor_handle}. Please remove "
                            '`TorchTensorType(transport="nccl")` between '
                            "DAG nodes on the same actor."
                        )
                        return True
        return False

    def _monitor_failures(self):
        outer = weakref.proxy(self)

        class Monitor(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True)
                self.in_teardown = False
                # Lock to make sure that we only perform teardown for this DAG
                # once.
                self.in_teardown_lock = threading.Lock()
                self.name = "CompiledGraphMonitorThread"
                self._teardown_done = False

            def wait_teardown(self, kill_actors: bool = False):
                from ray.dag import DAGContext

                ctx = DAGContext.get_current()
                teardown_timeout = ctx.teardown_timeout

                for actor, ref in outer.worker_task_refs.items():
                    timeout = False
                    try:
                        ray.get(ref, timeout=teardown_timeout)
                    except ray.exceptions.GetTimeoutError:
                        msg = (
                            f"Compiled DAG actor {actor} is still running "
                            f"{teardown_timeout}s after teardown()."
                        )
                        if kill_actors:
                            msg += " Force-killing actor."
                            ray.kill(actor)
                        else:
                            msg += " Teardown may hang."

                        logger.warning(msg)
                        timeout = True
                    except Exception:
                        # We just want to check that the task has finished so
                        # we don't care if the actor task ended in an
                        # exception.
                        pass

                    if not timeout:
                        continue

                    try:
                        ray.get(ref)
                    except Exception:
                        pass

            def teardown(self, kill_actors: bool = False):
                do_teardown = False
                with self.in_teardown_lock:
                    if self._teardown_done:
                        return

                    if not self.in_teardown:
                        do_teardown = True
                        self.in_teardown = True

                if not do_teardown:
                    # Teardown is already being performed.
                    while True:
                        with self.in_teardown_lock:
                            if self._teardown_done:
                                return

                        time.sleep(0.1)

                logger.info("Tearing down compiled DAG")
                outer._dag_submitter.close()
                outer._dag_output_fetcher.close()

                for actor in outer.actor_refs:
                    logger.info(f"Cancelling compiled worker on actor: {actor}")
                # Cancel all actor loops in parallel.
                cancel_refs = [
                    actor.__ray_call__.remote(do_cancel_executable_tasks, tasks)
                    for actor, tasks in outer.actor_to_executable_tasks.items()
                ]
                for cancel_ref in cancel_refs:
                    try:
                        ray.get(cancel_ref, timeout=30)
                    except ray.exceptions.RayChannelError:
                        # Channel error happens when a channel is closed
                        # or timed out. In this case, do not log.
                        pass
                    except Exception:
                        logger.exception("Error cancelling worker task")
                        pass

                for nccl_group_id in outer._nccl_group_ids:
                    _destroy_nccl_group(nccl_group_id)

                logger.info("Waiting for worker tasks to exit")
                self.wait_teardown()
                logger.info("Teardown complete")

                with self.in_teardown_lock:
                    self._teardown_done = True

            def run(self):
                try:
                    ray.get(list(outer.worker_task_refs.values()))
                except Exception as e:
                    logger.debug(f"Handling exception from worker tasks: {e}")
                    self.teardown()

        monitor = Monitor()
        monitor.start()
        return monitor

    def raise_if_too_many_inflight_requests(self):
        num_in_flight_requests = (
            self._execution_index - self._max_finished_execution_index
        )
        if num_in_flight_requests > self._max_inflight_executions:
            raise ray.exceptions.RayAdagCapacityExceeded(
                f"There are {num_in_flight_requests} in-flight requests which "
                "is more than specified _max_inflight_executions of the dag: "
                f"{self._max_inflight_executions}. Retrieve the output using "
                "ray.get before submitting more requests or increase "
                "`max_inflight_executions`. "
                "`adag.experimental_compile(_max_inflight_executions=...)`"
            )

    def _has_execution_results(
        self,
        execution_index: int,
    ) -> bool:
        """Check whether there are results corresponding to the given execution
        index stored in self._result_buffer. This helps avoid fetching and
        caching results again.

        Args:
            execution_index: The execution index corresponding to the result.

        Returns:
            Whether the result for the given index has been fetched and cached.
        """
        return execution_index in self._result_buffer

    def _cache_execution_results(
        self,
        execution_index: int,
        result: Any,
    ):
        """Cache execution results in self._result_buffer. Results are converted
        to dictionary format to allow efficient element removal and calculation of
        the buffer size. This can only be called once per execution index.

        Args:
            execution_index: The execution index corresponding to the result.
            result: The results from all channels to be cached.
        """
        if not self._has_execution_results(execution_index):
            for chan_idx, res in enumerate(result):
                self._result_buffer[execution_index][chan_idx] = res

    def _get_execution_results(
        self, execution_index: int, channel_index: Optional[int]
    ) -> List[Any]:
        """Retrieve execution results from self._result_buffer and return the result.
        Results are converted back to original list format ordered by output channel
        index.

        Args:
            execution_index: The execution index to retrieve results from.
            channel_index: The index of the output channel corresponding to the result.
                Channel indexing is consistent with the order of
                self.dag_output_channels. None means that the result wraps outputs from
                all output channels.

        Returns:
            The execution result corresponding to the given execution index and channel
            index.
        """
        # Although CompiledDAGRef and CompiledDAGFuture guarantee that the same
        # execution index and channel index combination will not be requested multiple
        # times and therefore self._result_buffer will always have execution_index as
        # a key, we still do a sanity check to avoid misuses.
        assert execution_index in self._result_buffer

        if channel_index is None:
            # Convert results stored in self._result_buffer back to original
            # list representation
            result = [
                kv[1]
                for kv in sorted(
                    self._result_buffer.pop(execution_index).items(),
                    key=lambda kv: kv[0],
                )
            ]
        else:
            result = [self._result_buffer[execution_index].pop(channel_index)]
            if len(self._result_buffer[execution_index]) == 0:
                del self._result_buffer[execution_index]
        return result

    def _execute_until(
        self,
        execution_index: int,
        channel_index: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> List[Any]:
        """Repeatedly execute this DAG until the given execution index,
        and buffer all results up to that index. If the DAG has already
        been executed up to the given index, just return the result
        corresponding to the given index and channel.

        Args:
            execution_index: The execution index to execute until.
            channel_index: The index of the output channel to get the result from.
                Channel indexing is consistent with the order of
                self.dag_output_channels. None means wrapping results from all output
                channels into a single list.
            timeout: The maximum time in seconds to wait for the result.
                None means using default timeout (DAGContext.retrieval_timeout),
                0 means immediate timeout (immediate success or timeout without
                blocking), -1 means infinite timeout (block indefinitely).

        Returns:
            The execution result corresponding to the given execution index and
            channel index.

        TODO(rui): catch the case that user holds onto the CompiledDAGRefs
        """
        if self._max_finished_execution_index < execution_index:
            from ray.dag import DAGContext

            ctx = DAGContext.get_current()
            if timeout is None:
                timeout = ctx.retrieval_timeout

        while self._max_finished_execution_index < execution_index:
            if len(self._result_buffer) >= self._max_buffered_results:
                raise ValueError(
                    "Too many buffered results: the allowed max count for "
                    f"buffered results is {self._max_buffered_results}; call "
                    "ray.get() on previous CompiledDAGRefs to free them up "
                    "from buffer."
                )
            self.increment_max_finished_execution_index()
            start_time = time.monotonic()

            # Fetch results from each output channel up to execution_index and cache
            # them separately to enable individual retrieval
            self._cache_execution_results(
                self._max_finished_execution_index,
                self._dag_output_fetcher.read(timeout),
            )

            if timeout != -1:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)

        return self._get_execution_results(execution_index, channel_index)

    def execute(
        self,
        *args,
        **kwargs,
    ) -> Union[CompiledDAGRef, List[CompiledDAGRef]]:
        """Execute this DAG using the compiled execution path.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode

        Returns:
            A list of Channels that can be used to read the DAG result.

        Raises:
            RayChannelTimeoutError: If the execution does not complete within
                self._execution_timeout seconds.

        NOTE: Not thread-safe due to _execution_index etc.
        """
        if self._enable_asyncio:
            raise ValueError("Use execute_async if enable_asyncio=True")

        self._get_or_compile()

        self._check_inputs(args, kwargs)
        if len(args) == 1 and len(kwargs) == 0:
            # When serializing a tuple, the Ray serializer invokes pickle5, which adds
            # several microseconds of overhead. One common case for accelerated DAGs is
            # passing a single argument (oftentimes of of type `bytes`, which requires
            # no serialization). To avoid imposing this overhead on this common case, we
            # create a fast path for this case that avoids pickle5.
            inp = args[0]
        else:
            inp = RayDAGArgs(args=args, kwargs=kwargs)

        self.raise_if_too_many_inflight_requests()
        self._dag_submitter.write(inp, self._execution_timeout)

        if self._returns_list:
            ref = [
                CompiledDAGRef(self, self._execution_index, channel_index)
                for channel_index in range(len(self.dag_output_channels))
            ]
        else:
            ref = CompiledDAGRef(self, self._execution_index)

        self._execution_index += 1
        return ref

    def _check_inputs(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
        """
        Helper method to check that the DAG args provided by the user during
        execution are valid according to the defined DAG.
        """
        if len(args) != self._input_num_positional_args:
            raise ValueError(
                "dag.execute() or dag.execute_async() must be "
                f"called with {self._input_num_positional_args} positional args, got "
                f"{len(args)}"
            )

        for kwarg in self._input_kwargs:
            if kwarg not in kwargs:
                raise ValueError(
                    "dag.execute() or dag.execute_async() "
                    f"must be called with kwarg `{kwarg}`"
                )

    async def execute_async(
        self,
        *args,
        **kwargs,
    ) -> Union[CompiledDAGFuture, List[CompiledDAGFuture]]:
        """Execute this DAG using the compiled execution path.

        NOTE: Not thread-safe.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode.

        Returns:
            A list of Channels that can be used to read the DAG result.
        """
        if not self._enable_asyncio:
            raise ValueError("Use execute if enable_asyncio=False")

        self._get_or_compile()
        self._check_inputs(args, kwargs)
        async with self._dag_submission_lock:
            if len(args) == 1 and len(kwargs) == 0:
                # When serializing a tuple, the Ray serializer invokes pickle5, which
                # adds several microseconds of overhead. One common case for accelerated
                # DAGs is passing a single argument (oftentimes of of type `bytes`,
                # which requires no serialization). To avoid imposing this overhead on
                # this common case, we create a fast path for this case that avoids
                # pickle5.
                inp = args[0]
            else:
                inp = RayDAGArgs(args=args, kwargs=kwargs)

            self.raise_if_too_many_inflight_requests()
            await self._dag_submitter.write(inp)
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        if self._returns_list:
            fut = [
                CompiledDAGFuture(self, self._execution_index, fut, channel_index)
                for channel_index in range(len(self.dag_output_channels))
            ]
        else:
            fut = CompiledDAGFuture(self, self._execution_index, fut)

        self._execution_index += 1
        return fut

    def _visualize_ascii(self) -> str:
        """
        Visualize the compiled graph in
        ASCII format with directional markers.

        This function generates an ASCII visualization of a Compiled Graph,
        where each task node is labeled,
        and edges use `<` and `>` markers to show data flow direction.

        This method is called by:
            - `compiled_dag.visualize(format="ascii")`



        High-Level Algorithm:
        - Topological Sorting: Sort nodes topologically to organize
            them into layers based on dependencies.
        - Grid Initialization: Set up a 2D grid canvas with dimensions based
            on the number of layers and the maximum number of nodes per layer.
        - Node Placement: Position each node on the grid according to its
            layer and relative position within that layer.
            Spacing is added for readability, and directional markers (`<` and `>`)
            are added to edges to show input/output flow clearly.

        This method should be called
          **after** compiling the graph with `experimental_compile()`.

        Returns:
            ASCII representation of the CG with Nodes Information,
            Edges Information and Graph Built.

        Limitations:
        - Note: This is only used for quick visualization for small graphs.
            For complex graph (i.e. more than 20 tasks), please use graphviz.
        - Scale: Works best for smaller CGs (typically fewer than 20 tasks).
            Larger CGs may result in dense, less readable ASCII
            outputs due to limited space for node and edge rendering.
        - Shape: Ideal for relatively shallow CGs with clear dependency paths.
            For deep, highly branched or densely connected CGs,
            readability may suffer.
        - Edge Overlap: In cases with high fan-out (i.e., nodes with many children)
            or fan-in (nodes with many parents), edge lines may intersect or overlap
            in the ASCII visualization, potentially obscuring some connections.
        - Multi-output Tasks: Multi-output tasks can be visualized, but positioning
            may cause line breaks or overlap when a task has multiple outputs that
            feed into nodes at varying depths.

        Example:
            Basic Visualization:
            ```python
            # Print the CG structure in ASCII format
            print(compiled_dag.visualize(format="ascii"))
            ```

            Example of Ordered Visualization (task is build in order
                to reduce line intersection):
            ```python
            with InputNode() as i:
                o1, o2, o3 = a.return_three.bind(i)
                o4 = b.echo.bind(o1)
                o5 = b.echo.bind(o2)
                o6, o7 = b.return_two.bind(o3)
                dag = MultiOutputNode([o4, o5, o6, o7])

            compiled_dag = dag.experimental_compile()
            compiled_dag.visualize(format="ascii",view=True)


            # Output:
            # 0:InputNode
            # |
            # 1:Actor_54777d:return_three
            # |---------------------------->|---------------------------->|                                                  # noqa
            # 2:Output[0]                   3:Output[1]                   4:Output[2]                                        # noqa
            # |                             |                             |                                                  # noqa
            # 5:Actor_c927c9:echo           6:Actor_c927c9:echo           7:Actor_c927c9:return_two                          # noqa
            # |                             |                             |---------------------------->|                    # noqa
            # |                             |                             9:Output[0]                   10:Output[1]         # noqa
            # |<----------------------------|-----------------------------|-----------------------------|                    # noqa
            # 8:MultiOutputNode
            ```

            Example of Anti-pattern Visualization (There are intersections):
            # We can swtich the nodes ordering to reduce intersections, i.e. swap o2 and o3
            ```python
            with InputNode() as i:
                o1, o2, o3 = a.return_three.bind(i)
                o4 = b.echo.bind(o1)
                o5 = b.echo.bind(o3)
                o6, o7 = b.return_two.bind(o2)
                dag = MultiOutputNode([o4, o5, o6, o7])
            compiled_dag = dag.experimental_compile()
            compiled_dag.visualize(format="ascii",view=True)

            # Output (Nodes 5, 7, 9, 10 should connect to Node 8):
            # 0:InputNode
            # |
            # 1:Actor_84835a:return_three
            # |---------------------------->|---------------------------->|                            # noqa
            # 2:Output[0]                   3:Output[1]                   4:Output[2]                  # noqa
            # |                             |                             |                            # noqa
            # 5:Actor_02a6a1:echo           6:Actor_02a6a1:return_two     7:Actor_02a6a1:echo          # noqa
            # |                             |---------------------------->|                            # noqa
            # |                             9:Output[0]                   10:Output[1]                 # noqa
            # |<----------------------------------------------------------|                            # noqa
            # 8:MultiOutputNod
            ```
        """

        from ray.dag import (
            InputAttributeNode,
            InputNode,
            MultiOutputNode,
            ClassMethodNode,
            DAGNode,
        )

        # Check that the DAG has been compiled
        if not hasattr(self, "idx_to_task") or not self.idx_to_task:
            raise ValueError(
                "The DAG must be compiled before calling 'visualize()'. "
                "Please call 'experimental_compile()' first."
            )

        # Check that each CompiledTask has a valid dag_node
        for idx, task in self.idx_to_task.items():
            if not hasattr(task, "dag_node") or not isinstance(task.dag_node, DAGNode):
                raise ValueError(
                    f"Task at index {idx} does not have a valid 'dag_node'. "
                    "Ensure that 'experimental_compile()' completed successfully."
                )

        from collections import defaultdict, deque

        # Create adjacency list representation of the DAG
        # Adjacency list for DAG; maps a node index to its downstream nodes.
        adj_list: Dict[int, List[int]] = defaultdict(list)
        # Indegree count for topological sorting; maps a node index to its indegree.
        indegree: Dict[int, int] = defaultdict(int)

        # Tracks whether a node is a multi-output node.
        is_multi_output: Dict[int, bool] = defaultdict(bool)
        # Maps child node indices to their parent node indices.
        child2parent: Dict[int, int] = defaultdict(int)
        ascii_visualization = ""
        # Node information; maps a node index to its descriptive label.
        node_info: Dict[int, str] = {}
        # Edge information; tuples of (upstream_index, downstream_index, edge_label).
        edge_info: List[Tuple[int, int, str]] = []

        for idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            label = f"Task {idx}  "

            # Determine the type and label of the node
            if isinstance(dag_node, InputNode):
                label += "InputNode"
            elif isinstance(dag_node, InputAttributeNode):
                label += f"InputAttributeNode[{dag_node.key}]"
            elif isinstance(dag_node, MultiOutputNode):
                label += "MultiOutputNode"
            elif isinstance(dag_node, ClassMethodNode):
                if dag_node.is_class_method_call:
                    method_name = dag_node.get_method_name()
                    actor_handle = dag_node._get_actor_handle()
                    actor_id = (
                        actor_handle._actor_id.hex()[:6] if actor_handle else "unknown"
                    )
                    label += f"Actor: {actor_id}... Method: {method_name}"
                elif dag_node.is_class_method_output:
                    label += f"ClassMethodOutputNode[{dag_node.output_idx}]"
                else:
                    label += "ClassMethodNode"
            else:
                label += type(dag_node).__name__

            node_info[idx] = label

            for arg_index, arg in enumerate(dag_node.get_args()):
                if isinstance(arg, DAGNode):
                    upstream_task_idx = self.dag_node_to_idx[arg]

                    # Get the type hint for this argument
                    if arg_index < len(task.arg_type_hints):
                        if task.arg_type_hints[arg_index].requires_nccl():
                            type_hint = "Nccl"
                        else:
                            type_hint = type(task.arg_type_hints[arg_index]).__name__
                    else:
                        type_hint = "UnknownType"

                    adj_list[upstream_task_idx].append(idx)
                    indegree[idx] += 1
                    edge_info.append((upstream_task_idx, idx, type_hint))

        width_adjust = 0
        for upstream_task_idx, child_idx_list in adj_list.items():
            # Mark as multi-output if the node has more than one output path
            if len(child_idx_list) > 1:
                for child in child_idx_list:
                    is_multi_output[child] = True
                    child2parent[child] = upstream_task_idx
                width_adjust = max(width_adjust, len(child_idx_list))

        # Topological sort to determine layers
        layers = defaultdict(list)
        zero_indegree = deque([idx for idx in self.idx_to_task if indegree[idx] == 0])
        layer_index = 0

        while zero_indegree:
            next_layer = deque()
            while zero_indegree:
                task_idx = zero_indegree.popleft()
                layers[layer_index].append(task_idx)
                for downstream in adj_list[task_idx]:
                    indegree[downstream] -= 1
                    if indegree[downstream] == 0:
                        next_layer.append(downstream)
            zero_indegree = next_layer
            layer_index += 1

        # Print detailed node information
        ascii_visualization += "Nodes Information:\n"
        for idx, info in node_info.items():
            ascii_visualization += f'{idx} [label="{info}"] \n'

        # Print edges
        ascii_visualization += "\nEdges Information:\n"
        for upstream_task, downstream_task, type_hint in edge_info:
            if type_hint == "Nccl":
                edgs_channel = "+++"
            else:
                edgs_channel = "---"
            ascii_visualization += (
                f"{upstream_task} {edgs_channel}>" f" {downstream_task}\n"
            )

        # Add the legend to the output
        ascii_visualization += "\nLegend:\n"
        ascii_visualization += "+++> : Represents Nccl-type data channels\n"
        ascii_visualization += "---> : Represents Shared Memory data channels\n"

        # Find the maximum width (number of nodes in any layer)
        max_width = max(len(layer) for layer in layers.values()) + width_adjust
        height = len(layers)

        # Build grid for ASCII visualization
        grid = [[" " for _ in range(max_width * 20)] for _ in range(height * 2 - 1)]

        # Place nodes in the grid with more details
        task_to_pos = {}
        for layer_num, layer_tasks in layers.items():
            layer_y = layer_num * 2  # Every second row is for nodes
            for col_num, task_idx in enumerate(layer_tasks):
                task = self.idx_to_task[task_idx]
                task_info = f"{task_idx}:"

                # Determine if it's an actor method or a regular task
                if isinstance(task.dag_node, ClassMethodNode):
                    if task.dag_node.is_class_method_call:
                        method_name = task.dag_node.get_method_name()
                        actor_handle = task.dag_node._get_actor_handle()
                        actor_id = (
                            actor_handle._actor_id.hex()[:6]
                            if actor_handle
                            else "unknown"
                        )
                        task_info += f"Actor_{actor_id}:{method_name}"
                    elif task.dag_node.is_class_method_output:
                        task_info += f"Output[{task.dag_node.output_idx}]"
                    else:
                        task_info += "UnknownMethod"
                else:
                    task_info += type(task.dag_node).__name__

                adjust_col_num = 0
                if task_idx in is_multi_output:
                    adjust_col_num = layers[layer_num - 1].index(child2parent[task_idx])
                col_x = (col_num + adjust_col_num) * 30  # Every 30th column for spacing
                # Place the task information into the grid
                for i, char in enumerate(task_info):
                    if col_x + i < len(grid[0]):  # Ensure we don't overflow the grid
                        grid[layer_y][col_x + i] = char

                task_to_pos[task_idx] = (layer_y, col_x)

        # Connect the nodes with lines
        for upstream_task, downstream_tasks in adj_list.items():
            upstream_y, upstream_x = task_to_pos[upstream_task]
            for downstream_task in downstream_tasks:
                downstream_y, downstream_x = task_to_pos[downstream_task]

                # Draw vertical line
                for y in range(upstream_y + 1, downstream_y):
                    if grid[y][upstream_x] == " ":
                        grid[y][upstream_x] = "|"

                    # Draw horizontal line with directional arrows
                if upstream_x != downstream_x:
                    for x in range(
                        min(upstream_x, downstream_x) + 1,
                        max(upstream_x, downstream_x),
                    ):
                        grid[downstream_y - 1][x] = (
                            "-"
                            if grid[downstream_y - 1][x] == " "
                            else grid[downstream_y - 1][x]
                        )

                    # Add arrows to indicate flow direction
                    if downstream_x > upstream_x:
                        grid[downstream_y - 1][downstream_x - 1] = ">"
                    else:
                        grid[downstream_y - 1][downstream_x + 1] = "<"

                # Draw connection to the next task
                grid[downstream_y - 1][downstream_x] = "|"

        # Ensure proper multi-output task connection
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, MultiOutputNode):
                output_tasks = task.dag_node.get_args()
                for i, output_task in enumerate(output_tasks):
                    if isinstance(output_task, DAGNode):
                        output_task_idx = self.dag_node_to_idx[output_task]
                        if output_task_idx in task_to_pos:
                            output_y, output_x = task_to_pos[output_task_idx]
                            grid[output_y - 1][output_x] = "|"

        # Convert grid to string for printing
        ascii_visualization += "\nGraph Built:\n"
        ascii_visualization += "\n".join("".join(row) for row in grid)

        return ascii_visualization

    def get_channel_details(
        self, channel: ChannelInterface, downstream_actor_id: str
    ) -> str:
        """
        Get details about outer and inner channel types and channel ids
        based on the channel and the downstream actor ID.
        Used for graph visualization.
        Args:
            channel: The channel to get details for.
            downstream_actor_id: The downstream actor ID.
        Returns:
            A string with details about the channel based on its connection
            to the actor provided.
        """
        channel_details = type(channel).__name__
        # get outer channel
        if channel in self._channel_dict and self._channel_dict[channel] != channel:
            channel = self._channel_dict[channel]
            channel_details += f"\n{type(channel).__name__}"
            if type(channel) == CachedChannel:
                channel_details += f", {channel._channel_id[:6]}..."
        # get inner channel
        if (
            type(channel) == CompositeChannel
            and downstream_actor_id in channel._channel_dict
        ):
            inner_channel = channel._channel_dict[downstream_actor_id]
            channel_details += f"\n{type(inner_channel).__name__}"
            if type(inner_channel) == IntraProcessChannel:
                channel_details += f", {inner_channel._channel_id[:6]}..."
        return channel_details

    def visualize(
        self,
        filename="compiled_graph",
        format="png",
        view=False,
        channel_details=False,
    ) -> str:
        """
        Visualize the compiled graph using Graphviz.

        For non-ASCII formats, the visualization will be saved to a file specified
        by the `filename` argument.

        This method generates a graphical representation of the compiled graph,
        showing tasks and their dependencies.This method should be called
        **after** the graph has been compiled using `experimental_compile()`.

        Args:
            filename: The name of the output file (without extension).
            format: The format of the output file (e.g., 'png', 'pdf', 'ascii').
            view: For non-ascii: Whether to open the file with the default viewer.
                For ascii: Whether to print the visualization and return None
                    or return the ascii visualization string directly.
            channel_details: If True, adds channel details to edges.

        Returns:
            str:
                - For Graphviz-based formats (e.g., 'png', 'pdf', 'jpeg'), returns
                the Graphviz DOT string representation of the compiled graph.
                - For ASCII format, returns the ASCII string representation of the
                compiled graph.

        Raises:
            ValueError: If the graph is empty or not properly compiled.
            ImportError: If the `graphviz` package is not installed.

        """
        if format == "ascii":
            if channel_details:
                raise ValueError(
                    "Parameters 'channel_details' are"
                    " not compatible with 'ascii' format."
                )
            ascii_visualiztion_str = self._visualize_ascii()
            if view:
                print(ascii_visualiztion_str)
            return ascii_visualiztion_str
        try:
            import graphviz
        except ImportError:
            raise ImportError(
                "Please install graphviz to visualize the compiled graph. "
                "You can install it by running `pip install graphviz`."
            )
        from ray.dag import (
            InputAttributeNode,
            InputNode,
            MultiOutputNode,
            ClassMethodNode,
            DAGNode,
        )

        # Check that the DAG has been compiled
        if not hasattr(self, "idx_to_task") or not self.idx_to_task:
            raise ValueError(
                "The DAG must be compiled before calling 'visualize()'. "
                "Please call 'experimental_compile()' first."
            )

        # Check that each CompiledTask has a valid dag_node
        for idx, task in self.idx_to_task.items():
            if not hasattr(task, "dag_node") or not isinstance(task.dag_node, DAGNode):
                raise ValueError(
                    f"Task at index {idx} does not have a valid 'dag_node'. "
                    "Ensure that 'experimental_compile()' completed successfully."
                )

        # Dot file for debugging
        dot = graphviz.Digraph(name="compiled_graph", format=format)
        # Give every actor a unique color, colors between 24k -> 40k tested as readable
        # other colors may be too dark, especially when wrapping back around to 0
        actor_id_to_color = defaultdict(
            lambda: f"#{((len(actor_id_to_color) * 2000 + 24000) % 0xFFFFFF):06X}"
        )
        # Add nodes with task information
        for idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            # Initialize the label and attributes
            label = f"Task {idx}\n"
            shape = "oval"  # Default shape
            style = "filled"
            fillcolor = ""

            # Handle different types of dag_node
            if isinstance(dag_node, InputNode):
                label += "InputNode"
                shape = "rectangle"
                fillcolor = "lightblue"
            elif isinstance(dag_node, InputAttributeNode):
                label += f"InputAttributeNode[{dag_node.key}]"
                shape = "rectangle"
                fillcolor = "lightblue"
            elif isinstance(dag_node, MultiOutputNode):
                label += "MultiOutputNode"
                shape = "rectangle"
                fillcolor = "yellow"
            elif isinstance(dag_node, ClassMethodNode):
                if dag_node.is_class_method_call:
                    # Class Method Call Node
                    method_name = dag_node.get_method_name()
                    actor_handle = dag_node._get_actor_handle()
                    if actor_handle:
                        actor_id = actor_handle._actor_id.hex()
                        label += f"Actor: {actor_id[:6]}...\nMethod: {method_name}"
                        fillcolor = actor_id_to_color[actor_id]
                    else:
                        label += f"Method: {method_name}"
                        fillcolor = "lightgreen"
                    shape = "oval"
                elif dag_node.is_class_method_output:
                    # Class Method Output Node
                    label += f"ClassMethodOutputNode[{dag_node.output_idx}]"
                    shape = "rectangle"
                    fillcolor = "orange"
                else:
                    # Unexpected ClassMethodNode
                    label += "ClassMethodNode"
                    shape = "diamond"
                    fillcolor = "red"
            else:
                # Unexpected node type
                label += type(dag_node).__name__
                shape = "diamond"
                fillcolor = "red"

            # Add the node to the graph with attributes
            dot.node(str(idx), label, shape=shape, style=style, fillcolor=fillcolor)
            channel_type_str = (
                type(dag_node.type_hint).__name__
                if dag_node.type_hint
                else "UnknownType"
            ) + "\n"

            # This logic is built on the assumption that there will only be multiple
            # output channels if the task has multiple returns
            # case: task with one output
            if len(task.output_channels) == 1:
                for downstream_node in task.dag_node._downstream_nodes:
                    downstream_idx = self.dag_node_to_idx[downstream_node]
                    edge_label = channel_type_str
                    if channel_details:
                        edge_label += self.get_channel_details(
                            task.output_channels[0],
                            (
                                downstream_node._get_actor_handle()._actor_id.hex()
                                if type(downstream_node) == ClassMethodNode
                                else self._proxy_actor._actor_id.hex()
                            ),
                        )
                    dot.edge(str(idx), str(downstream_idx), label=edge_label)
            # case: multi return, output channels connect to class method output nodes
            elif len(task.output_channels) > 1:
                assert len(task.output_idxs) == len(task.output_channels)
                for output_channel, downstream_idx in zip(
                    task.output_channels, task.output_node_idxs
                ):
                    edge_label = channel_type_str
                    if channel_details:
                        edge_label += self.get_channel_details(
                            output_channel,
                            task.dag_node._get_actor_handle()._actor_id.hex(),
                        )
                    dot.edge(str(idx), str(downstream_idx), label=edge_label)
            if type(task.dag_node) == InputAttributeNode:
                # Add an edge from the InputAttributeNode to the InputNode
                dot.edge(str(self.input_task_idx), str(idx))
        dot.render(filename, view=view)
        return dot.source

    def teardown(self, kill_actors: bool = False):
        """Teardown and cancel all actor tasks for this DAG. After this
        function returns, the actors should be available to execute new tasks
        or compile a new DAG."""
        if self._is_teardown:
            return

        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            from ray.dag import DAGContext

            ctx = DAGContext.get_current()
            monitor.teardown(kill_actors=kill_actors)
            monitor.join(timeout=ctx.teardown_timeout)
            # We do not log a warning here if the thread is still alive because
            # wait_teardown already logs upon teardown_timeout.

        self._is_teardown = True

    def __del__(self):
        self.teardown()


@DeveloperAPI
def build_compiled_dag_from_ray_dag(
    dag: "ray.dag.DAGNode",
    execution_timeout: Optional[float] = None,
    buffer_size_bytes: Optional[int] = None,
    enable_asyncio: bool = False,
    asyncio_max_queue_size: Optional[int] = None,
    max_buffered_results: Optional[int] = None,
    max_inflight_executions: Optional[int] = None,
    overlap_gpu_communication: Optional[bool] = None,
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        execution_timeout,
        buffer_size_bytes,
        enable_asyncio,
        asyncio_max_queue_size,
        max_buffered_results,
        max_inflight_executions,
        overlap_gpu_communication,
    )

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    root = dag._find_root()
    root.traverse_and_apply(_build_compiled_dag)
    compiled_dag._get_or_compile()
    global _compiled_dags
    _compiled_dags[compiled_dag.get_id()] = compiled_dag
    return compiled_dag
