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

from ray.experimental.channel.auto_transport_type import (
    AutoTransportType,
    TypeHintResolver,
)
import ray.exceptions
from ray.dag.dag_operation_future import GPUFuture, DAGOperationFuture, ResolvedFuture
from ray.experimental.channel.cached_channel import CachedChannel
from ray.experimental.channel.communicator import Communicator
from ray.dag.constants import (
    RAY_CGRAPH_ENABLE_NVTX_PROFILING,
    RAY_CGRAPH_ENABLE_TORCH_PROFILING,
    RAY_CGRAPH_VISUALIZE_SCHEDULE,
)
import ray
from ray.exceptions import (
    RayCgraphCapacityExceeded,
    RayTaskError,
    RayChannelError,
    RayChannelTimeoutError,
)
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
    CompiledDAGArgs,
    CompositeChannel,
    IntraProcessChannel,
)
from ray.util.annotations import DeveloperAPI

from ray.experimental.channel.shared_memory_channel import (
    SharedMemoryType,
)
from ray.experimental.channel.torch_tensor_type import TorchTensorType

from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_communicator,
    _destroy_communicator,
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


def _check_unused_dag_input_attributes(
    output_node: "ray.dag.MultiOutputNode", input_attributes: Set[str]
) -> Set[str]:
    """
    Helper function to check that all input attributes are used in the DAG.
    For example, if the user creates an input attribute by calling
    InputNode()["x"], we ensure that there is a path from the
    InputAttributeNode corresponding to "x" to the DAG's output. If an
    input attribute is not used, throw an error.

    Args:
        output_node: The starting node for the traversal.
        input_attributes: A set of attributes accessed by the InputNode.
    """
    from ray.dag import InputAttributeNode

    used_attributes = set()
    visited_nodes = set()
    stack: List["ray.dag.DAGNode"] = [output_node]

    while stack:
        current_node = stack.pop()
        if current_node in visited_nodes:
            continue
        visited_nodes.add(current_node)

        if isinstance(current_node, InputAttributeNode):
            used_attributes.add(current_node.key)

        stack.extend(current_node._upstream_nodes)

    unused_attributes = input_attributes - used_attributes
    if unused_attributes:
        unused_attributes_str = ", ".join(str(key) for key in unused_attributes)
        input_attributes_str = ", ".join(str(key) for key in input_attributes)
        unused_phrase = "is unused" if len(unused_attributes) == 1 else "are unused"

        raise ValueError(
            "Compiled Graph expects input to be accessed "
            f"using all of attributes {input_attributes_str}, "
            f"but {unused_attributes_str} {unused_phrase}. "
            "Ensure all input attributes are used and contribute "
            "to the computation of the Compiled Graph output."
        )


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

        if RAY_CGRAPH_ENABLE_NVTX_PROFILING:
            assert (
                not RAY_CGRAPH_ENABLE_TORCH_PROFILING
            ), "NVTX and torch profiling cannot be enabled at the same time."
            try:
                import nvtx
            except ImportError:
                raise ImportError(
                    "Please install nvtx to enable nsight profiling. "
                    "You can install it by running `pip install nvtx`."
                )
            nvtx_profile = nvtx.Profile()
            nvtx_profile.enable()

        if RAY_CGRAPH_ENABLE_TORCH_PROFILING:
            assert (
                not RAY_CGRAPH_ENABLE_NVTX_PROFILING
            ), "NVTX and torch profiling cannot be enabled at the same time."

            import torch

            torch_profile = torch.profiler.profile(
                activities=[
                    torch.profiler.ProfilerActivity.CPU,
                    torch.profiler.ProfilerActivity.CUDA,
                ],
                with_stack=True,
                on_trace_ready=torch.profiler.tensorboard_trace_handler(
                    "compiled_graph_torch_profiles"
                ),
            )
            torch_profile.start()
            logger.info("Torch profiling started")

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

        if RAY_CGRAPH_ENABLE_NVTX_PROFILING:
            nvtx_profile.disable()

        if RAY_CGRAPH_ENABLE_TORCH_PROFILING:
            torch_profile.stop()
            logger.info("Torch profiling stopped")
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

        if not hasattr(self, "__ray_cgraph_events"):
            self.__ray_cgraph_events = []

        done = False
        while True:
            if done:
                break
            for operation in schedule:
                start_t = time.perf_counter()
                task = tasks[operation.exec_task_idx]
                done = task.exec_operation(
                    self, operation.type, overlap_gpu_communication
                )
                end_t = time.perf_counter()

                self.__ray_cgraph_events.append(
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
    # CUDA events should be destroyed before other CUDA resources.
    for task in tasks:
        task.destroy_cuda_event()
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
        return type_hint.communicator_id
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
        # The DAGNodes that are arguments to this task.
        # This is used for lazy resolution of the arguments' type hints.
        self.arg_nodes: List["ray.dag.DAGNode"] = []
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

    @property
    def arg_type_hints(self) -> List["ChannelOutputType"]:
        return [arg_node.type_hint for arg_node in self.arg_nodes]

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
                channel = arg
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

    def destroy_cuda_event(self):
        """
        If this executable task has created a GPU future that is not yet waited on,
        that future is in the channel context cache. Remove the future from the cache
        and destroy its CUDA event.
        """
        GPUFuture.remove_gpu_future(self.task_idx)

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
            nccl_group = ChannelContext.get_current().communicators.get(nccl_group_id)
            assert nccl_group is not None
            self._send_stream = nccl_group.send_stream
        if self.input_type_hints:
            for type_hint in self.input_type_hints:
                if type_hint.requires_nccl():
                    nccl_group_id = _get_nccl_group_id(type_hint)
                    nccl_group = ChannelContext.get_current().communicators.get(
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
            future = GPUFuture(val, self.task_idx)
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

        A Compiled Graph creates an actor from this class when the DAG is initialized.
        The actor is on the same node as the driver. This class has an empty
        implementation, though it serves as a way for the output writer to invoke remote
        functions on the driver node.
        """

        pass

    def __init__(
        self,
        submit_timeout: Optional[float] = None,
        buffer_size_bytes: Optional[int] = None,
        enable_asyncio: bool = False,
        max_inflight_executions: Optional[int] = None,
        max_buffered_results: Optional[int] = None,
        overlap_gpu_communication: Optional[bool] = None,
        default_communicator: Optional[Union[Communicator, str]] = "create",
    ):
        """
        Args:
            submit_timeout: The maximum time in seconds to wait for execute() calls.
                None means using default timeout (DAGContext.submit_timeout),
                0 means immediate timeout (immediate success or timeout without
                blocking), -1 means infinite timeout (block indefinitely).
            buffer_size_bytes: The initial buffer size in bytes for messages
                that can be passed between tasks in the DAG. The buffers will
                be automatically resized if larger messages are written to the
                channel.
            enable_asyncio: Whether to enable asyncio. If enabled, caller must
                be running in an event loop and must use `execute_async` to
                invoke the DAG. Otherwise, the caller should use `execute` to
                invoke the DAG.
            max_inflight_executions: The maximum number of in-flight executions that
                can be submitted via `execute` or `execute_async` before consuming
                the output using `ray.get()`. If the caller submits more executions,
                `RayCgraphCapacityExceeded` is raised.
            max_buffered_results: The maximum number of results that can be
                buffered at the driver. If more results are buffered,
                `RayCgraphCapacityExceeded` is raised. Note that
                when result corresponding to an execution is retrieved
                (by calling `ray.get()` on a `CompiledDAGRef` or
                `CompiledDAGRef` or await on a `CompiledDAGFuture), results
                corresponding to earlier executions that have not been retrieved
                yet are buffered.
            overlap_gpu_communication: (experimental) Whether to overlap GPU
                communication with computation during DAG execution. If True, the
                communication and computation can be overlapped, which can improve
                the performance of the DAG execution. If None, the default value
                will be used.
            _default_communicator: The default communicator to use to transfer
                tensors. Three types of values are valid. (1) Communicator:
                For p2p operations, this is the default communicator
                to use for nodes annotated with `with_tensor_transport()` and when
                shared memory is not the desired option (e.g., when transport="nccl",
                or when transport="auto" for communication between two different GPUs).
                For collective operations, this is the default communicator to use
                when a custom communicator is not specified.
                (2) "create": for each collective operation without a custom communicator
                specified, a communicator is created and initialized on its involved actors,
                or an already created communicator is reused if the set of actors is the same.
                For all p2p operations without a custom communicator specified, it reuses
                an already created collective communicator if the p2p actors are a subset.
                Otherwise, a new communicator is created.
                (3) None: a ValueError will be thrown if a custom communicator is not specified.

        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        from ray.dag import DAGContext

        ctx = DAGContext.get_current()

        self._enable_asyncio: bool = enable_asyncio
        self._fut_queue = asyncio.Queue()
        self._max_inflight_executions = max_inflight_executions
        if self._max_inflight_executions is None:
            self._max_inflight_executions = ctx.max_inflight_executions
        self._max_buffered_results = max_buffered_results
        if self._max_buffered_results is None:
            self._max_buffered_results = ctx.max_buffered_results
        self._dag_id = uuid.uuid4().hex
        self._submit_timeout: Optional[float] = submit_timeout
        if self._submit_timeout is None:
            self._submit_timeout = ctx.submit_timeout
        self._get_timeout: Optional[float] = ctx.get_timeout
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = ctx.buffer_size_bytes
        self._overlap_gpu_communication: Optional[bool] = overlap_gpu_communication
        if self._overlap_gpu_communication is None:
            self._overlap_gpu_communication = ctx.overlap_gpu_communication
        self._create_default_communicator = False
        if isinstance(default_communicator, str):
            if default_communicator == "create":
                self._create_default_communicator = True
                default_communicator = None
            else:
                raise ValueError(
                    "The only allowed string for default_communicator is 'create', "
                    f"got {default_communicator}"
                )
        elif default_communicator is not None and not isinstance(
            default_communicator, Communicator
        ):
            raise ValueError(
                "The default_communicator must be None, a string, or a Communicator, "
                f"got {type(default_communicator)}"
            )
        self._default_communicator: Optional[Communicator] = default_communicator

        # Dict from passed-in communicator to set of type hints that refer to it.
        self._communicator_to_type_hints: Dict[
            Communicator,
            Set["ray.experimental.channel.torch_tensor_type.TorchTensorType"],
        ] = defaultdict(set)
        # Dict from set of actors to created communicator ID.
        # These communicators are created by Compiled Graph, rather than passed in.
        # Communicators are only created when self._create_default_communicator is True.
        self._actors_to_created_communicator_id: Dict[
            FrozenSet["ray.actor.ActorHandle"], str
        ] = {}

        # Set of actors involved in P2P communication using an unresolved communicator.
        self._p2p_actors_with_unresolved_communicators: Set[
            "ray.actor.ActorHandle"
        ] = set()
        # Set of DAG nodes involved in P2P communication using an unresolved communicator.
        self._p2p_dag_nodes_with_unresolved_communicators: Set[
            "ray.dag.DAGNode"
        ] = set()
        # Set of collective operations using an unresolved communicator.
        self._collective_ops_with_unresolved_communicators: Set[
            "ray.dag.collective_node._CollectiveOperation"
        ] = set()

        self._default_type_hint: ChannelOutputType = SharedMemoryType(
            buffer_size_bytes=self._buffer_size_bytes,
            # We conservatively set num_shm_buffers to _max_inflight_executions.
            # It means that the DAG can be underutilized, but it guarantees there's
            # no false positive timeouts.
            num_shm_buffers=self._max_inflight_executions,
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
        # List of task indices that are input attribute nodes.
        self.input_attr_task_idxs: List[int] = []
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
        self.actor_to_tasks: Dict[
            "ray.actor.ActorHandle", List["CompiledTask"]
        ] = defaultdict(list)
        # Mapping from actor handle to its GPU IDs.
        # This is used for type hint resolution for with_tensor_transport("auto").
        self.actor_to_gpu_ids: Dict["ray.actor.ActorHandle", List[str]] = {}
        self.actor_to_executable_tasks: Dict[
            "ray.actor.ActorHandle", List["ExecutableTask"]
        ] = {}
        # Mapping from the actor handle to the execution schedule which is a list
        # of operations to be executed.
        self.actor_to_execution_schedule: Dict[
            "ray.actor.ActorHandle", List[_DAGNodeOperation]
        ] = defaultdict(list)
        # Mapping from the actor handle to the node ID that the actor is on.
        # A None actor handle means the actor is the driver.
        self.actor_to_node_id: Dict[Optional["ray.actor.ActorHandle"], str] = {}
        # The index of the current execution. It is incremented each time
        # the DAG is executed.
        self._execution_index: int = -1
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
        # Execution index to set of channel indices for CompiledDAGRefs
        # or CompiledDAGFuture whose destructor has been called. A "None"
        # channel index means there is only one channel, and its destructor
        # has been called.
        self._destructed_ref_idxs: Dict[int, Set[Optional[int]]] = dict()
        # Execution index to set of channel indices for CompiledDAGRefs
        # or CompiledDAGFuture whose get() has been called. A "None"
        # channel index means there is only one channel, and its get()
        # has been called.
        self._got_ref_idxs: Dict[int, Set[Optional[int]]] = dict()

    @property
    def is_teardown(self) -> bool:
        return self._is_teardown

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

        self.input_task_idx, self.output_task_idx = None, None

        input_attributes: Set[str] = set()
        # Find the input node and input attribute nodes in the DAG.
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "More than one InputNode found"
                self.input_task_idx = idx
                # handle_unused_attributes:
                # Save input attributes in a set.
                input_node = task.dag_node
                input_attributes.update(input_node.input_attribute_nodes.keys())
            elif isinstance(task.dag_node, InputAttributeNode):
                self.input_attr_task_idxs.append(idx)

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
            if idx == self.input_task_idx or isinstance(
                task.dag_node, InputAttributeNode
            ):
                continue
            if (
                len(task.downstream_task_idxs) == 0
                and task.dag_node.is_cgraph_output_node
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
        # Set of tasks with annotation of with_tensor_transport("auto").
        # These only correspond to ClassMethodNodes, but not InputNodes
        # or InputAttributeNodes.
        auto_transport_tasks: Set["CompiledTask"] = set()

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

                if actor_handle not in self.actor_to_gpu_ids:
                    self.actor_to_gpu_ids[actor_handle] = CompiledDAG._get_gpu_ids(
                        actor_handle
                    )

                if isinstance(dag_node.type_hint, AutoTransportType):
                    auto_transport_tasks.add(task)

                # Collect actors for NCCL P2P methods.
                if dag_node.type_hint.requires_nccl():
                    self._track_communicator_usage(dag_node, {actor_handle})
                # Collect NCCL collective operations.
                if isinstance(dag_node, CollectiveOutputNode):
                    self._track_communicator_usage(
                        dag_node,
                        set(dag_node._collective_op.actor_handles),
                        collective_op=True,
                    )
                    assert not self._overlap_gpu_communication, (
                        "Currently, the overlap_gpu_communication option is not "
                        "supported for NCCL collective operations. Please set "
                        "overlap_gpu_communication=False."
                    )
            elif isinstance(dag_node, InputNode) or isinstance(
                dag_node, InputAttributeNode
            ):
                if dag_node.type_hint.requires_nccl():
                    raise ValueError(
                        "DAG inputs cannot be transferred via NCCL because "
                        "the driver cannot participate in the NCCL group"
                    )
                if isinstance(dag_node.type_hint, AutoTransportType):
                    # Currently driver on GPU is not supported, so we always
                    # use shared memory to transfer tensors.
                    dag_node.type_hint = TorchTensorType(
                        device=dag_node.type_hint.device
                    )

            if type(dag_node.type_hint) is ChannelOutputType:
                # No type hint specified by the user. Replace
                # with the default type hint for this DAG.
                dag_node.type_hint = self._default_type_hint

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

                # Add upstream node as the argument nodes of this task, whose
                # type hints may be updated when resolved lazily.
                task.arg_nodes.append(upstream_task.dag_node)

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

                if upstream_task.dag_node.type_hint.requires_nccl():
                    # Here we are processing the args of the DAGNode, so track
                    # downstream actors only, upstream actor is already tracked
                    # when processing the DAGNode itself.
                    self._track_communicator_usage(
                        upstream_task.dag_node,
                        {downstream_actor_handle},
                    )
        # Check that all specified input attributes, e.g., InputNode()["x"],
        # are used in the DAG.
        _check_unused_dag_input_attributes(output_node, input_attributes)

        self._check_leaf_nodes()

        self._resolve_auto_transport(auto_transport_tasks)

        self._init_communicators()

        if direct_input:
            self._input_num_positional_args = 1
        elif not input_positional_args:
            self._input_num_positional_args = 0
        else:
            self._input_num_positional_args = max(input_positional_args) + 1
        self._input_kwargs = tuple(input_kwargs)

    def _init_communicators(self) -> None:
        """
        Initialize communicators for the DAG.
        """

        # First, initialize communicators that are passed in by the user.
        for communicator, type_hints in self._communicator_to_type_hints.items():
            communicator_id = _init_communicator(
                communicator.get_actor_handles(),
                communicator,
                self._overlap_gpu_communication,
            )
            for type_hint in type_hints:
                type_hint.set_communicator_id(communicator_id)

        # Then, create communicators for collective operations.
        # Reuse an already created communicator for the same set of actors.
        for collective_op in self._collective_ops_with_unresolved_communicators:
            if not self._create_default_communicator:
                raise ValueError(
                    "Communicator creation is not allowed for collective operations."
                )
            actors = frozenset(collective_op.actor_handles)
            if actors in self._actors_to_created_communicator_id:
                communicator_id = self._actors_to_created_communicator_id[actors]
            else:
                communicator_id = _init_communicator(
                    list(actors),
                    None,
                    self._overlap_gpu_communication,
                )
                self._actors_to_created_communicator_id[actors] = communicator_id
            collective_op.type_hint.set_communicator_id(communicator_id)

        # Finally, create a communicator for P2P operations.
        # Reuse an already created collective op communicator when p2p actors
        # are a subset of the actors in the collective op communicator.
        p2p_communicator_id = None
        if self._p2p_actors_with_unresolved_communicators:
            for (
                actors,
                communicator_id,
            ) in self._actors_to_created_communicator_id.items():
                if self._p2p_actors_with_unresolved_communicators.issubset(actors):
                    p2p_communicator_id = communicator_id
                    break
            if p2p_communicator_id is None:
                p2p_communicator_id = _init_communicator(
                    list(self._p2p_actors_with_unresolved_communicators),
                    None,
                    self._overlap_gpu_communication,
                )
            for dag_node in self._p2p_dag_nodes_with_unresolved_communicators:
                dag_node.type_hint.set_communicator_id(p2p_communicator_id)

    def _track_communicator_usage(
        self,
        dag_node: "ray.dag.DAGNode",
        actors: Set["ray.actor.ActorHandle"],
        collective_op: bool = False,
    ) -> None:
        """
        Track the usage of a communicator.

        This method first determines the communicator to use: if a custom
        communicator is specified, use it; if not and a default communicator
        is available, use it; otherwise, it records necessary information to
        create a new communicator later.

        This method also performs validation checks on the passed-in communicator.

        Args:
            dag_node: The DAG node that uses the communicator, this is the node
                that has the `with_tensor_transport()` type hint for p2p communication,
                or a `CollectiveOutputNode` for collective operations.
            actors: The full or partial set of actors that use the communicator.
                This method should be called one or multiple times so that all actors
                of the communicator are tracked.
            collective_op: Whether the communicator is used for a collective operation.
        """
        if None in actors:
            raise ValueError("Driver cannot participate in the NCCL group.")
        if collective_op:
            type_hint = dag_node._collective_op.type_hint
        else:
            type_hint = dag_node.type_hint
        communicator = type_hint.get_custom_communicator()

        if communicator is None:
            if (
                self._default_communicator is None
                and not self._create_default_communicator
            ):
                if dag_node._original_type_hint is not None:
                    assert isinstance(dag_node._original_type_hint, AutoTransportType)
                    raise ValueError(
                        f"with_tensor_transport(transport='auto') is used for DAGNode {dag_node}, "
                        "This requires specifying a default communicator or 'create' for "
                        "_default_communicator when calling experimental_compile()."
                    )
                raise ValueError(
                    f"DAGNode {dag_node} has no custom communicator specified. "
                    "Please specify a custom communicator for the DAGNode using "
                    "`with_tensor_transport()`, or specify a communicator or 'create' for "
                    "_default_communicator when calling experimental_compile()."
                )
            communicator = self._default_communicator

        if communicator is None:
            if collective_op:
                self._collective_ops_with_unresolved_communicators.add(
                    dag_node._collective_op
                )
            else:
                self._p2p_dag_nodes_with_unresolved_communicators.add(dag_node)
                self._p2p_actors_with_unresolved_communicators.update(actors)
        else:
            if collective_op:
                if set(communicator.get_actor_handles()) != actors:
                    raise ValueError(
                        "The passed-in communicator must have the same set "
                        "of actors as the collective operation. "
                        f"The passed-in communicator has actors {communicator.get_actor_handles()} "
                        f"while the collective operation has actors {actors}."
                    )
            else:
                if not actors.issubset(set(communicator.get_actor_handles())):
                    raise ValueError(
                        "The passed-in communicator must include all of the actors "
                        "used in the P2P operation. "
                        f"The passed-in communicator has actors {communicator.get_actor_handles()} "
                        f"while the P2P operation has actors {actors}."
                    )
            self._communicator_to_type_hints[communicator].add(type_hint)

    def _resolve_auto_transport(
        self,
        auto_transport_tasks: Set["CompiledTask"],
    ) -> None:
        """
        Resolve the auto transport type hint for the DAG.
        """
        type_hint_resolver = TypeHintResolver(self.actor_to_gpu_ids)
        # Resolve AutoChannelType type hints and track the actors that use NCCL.
        # This is needed so that the NCCL group can be initialized for these
        # actors that use NCCL.
        for task in auto_transport_tasks:
            writer = task.dag_node._get_actor_handle()
            readers = task.downstream_task_idxs.values()
            writer_and_node = (writer, self._get_node_id(writer))
            reader_and_node_list = [
                (reader, self._get_node_id(reader)) for reader in readers
            ]
            # Update the type hint to the resolved one. This is needed because
            # the resolved type hint's `register_custom_serializer` will be called
            # in preparation for channel I/O.
            task.dag_node.type_hint = type_hint_resolver.resolve(
                task.dag_node.type_hint,
                writer_and_node,
                reader_and_node_list,
            )
            if task.dag_node.type_hint.requires_nccl():
                self._track_communicator_usage(
                    task.dag_node,
                    set(readers).union({writer}),
                )

    def _check_leaf_nodes(self) -> None:
        """
        Check if there are leaf nodes in the DAG and raise an error if there are.
        """
        from ray.dag import (
            DAGNode,
            ClassMethodNode,
        )

        leaf_nodes: List[DAGNode] = []
        for _, task in self.idx_to_task.items():
            if not isinstance(task.dag_node, ClassMethodNode):
                continue
            if (
                len(task.downstream_task_idxs) == 0
                and not task.dag_node.is_cgraph_output_node
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

    @staticmethod
    def _get_gpu_ids(actor_handle: "ray.actor.ActorHandle") -> List[str]:
        """
        Get the GPU IDs of an actor handle.
        """
        accelerator_ids = ray.get(
            actor_handle.__ray_call__.remote(
                lambda self: ray.get_runtime_context().get_accelerator_ids()
            )
        )
        return accelerator_ids.get("GPU", [])

    def _get_node_id(self, actor_handle: Optional["ray.actor.ActorHandle"]) -> str:
        """
        Get the node ID of an actor handle and cache it.

        Args:
            actor_handle: The actor handle, or None if the actor handle is the
                driver.
        Returns:
            The node ID of the actor handle or driver.
        """
        if actor_handle in self.actor_to_node_id:
            return self.actor_to_node_id[actor_handle]
        node_id = None
        if actor_handle == self._proxy_actor or actor_handle is None:
            node_id = ray.get_runtime_context().get_node_id()
        else:
            node_id = ray.get(
                actor_handle.__ray_call__.remote(
                    lambda self: ray.get_runtime_context().get_node_id()
                )
            )
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
                            task.dag_node.type_hint,
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
                        input_dag_node.type_hint,
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

        from ray.dag.constants import RAY_CGRAPH_ENABLE_DETECT_DEADLOCK

        if RAY_CGRAPH_ENABLE_DETECT_DEADLOCK and self._detect_deadlock():
            raise ValueError(
                "This DAG cannot be compiled because it will deadlock on NCCL "
                "calls. If you believe this is a false positive, please disable "
                "the graph verification by setting the environment variable "
                "RAY_CGRAPH_ENABLE_DETECT_DEADLOCK to 0 and file an issue at "
                "https://github.com/ray-project/ray/issues/new/."
            )

        input_task = self.idx_to_task[self.input_task_idx]
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

        from ray.dag.constants import RAY_CGRAPH_ENABLE_PROFILING

        if RAY_CGRAPH_ENABLE_PROFILING:
            exec_task_func = do_profile_tasks
        else:
            exec_task_func = do_exec_tasks

        # Build an execution schedule for each actor
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

        # Register custom serializers for input, input attribute, and output nodes.
        self._register_input_output_custom_serializer()

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

        if RAY_CGRAPH_VISUALIZE_SCHEDULE:
            _visualize_execution_schedule(
                actor_to_execution_schedule, actor_to_overlapped_schedule, graph
            )

        if actor_to_overlapped_schedule is not None:
            return _extract_execution_schedule(actor_to_overlapped_schedule)
        else:
            return _extract_execution_schedule(actor_to_execution_schedule)

    def _detect_deadlock(self) -> bool:
        """
        TODO (kevin85421): Avoid false negatives.

        Currently, a compiled graph may deadlock if there are NCCL channels, and the
        readers have control dependencies on the same actor. For example:

        actor1.a ---> actor2.f1
                 |
                 ---> actor2.f2

        The control dependency between `actor2.f1` and `actor2.f2` is that `f1` should
        run before `f2`. If `actor1.a` writes to `actor2.f2` before `actor2.f1`, a
        deadlock will occur.

        Currently, the execution schedule is not granular enough to detect this
        deadlock.

        Returns:
            True if a deadlock is detected; otherwise, False.
        """
        logger.debug("Deadlock detection has not been implemented yet.")
        return False

    def _monitor_failures(self):
        get_outer = weakref.ref(self)

        class Monitor(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True)
                self.name = "CompiledGraphMonitorThread"
                # Lock to make sure that we only perform teardown for this DAG
                # once.
                self._in_teardown_lock = threading.Lock()
                self._teardown_done = False

            def _outer_ref_alive(self) -> bool:
                if get_outer() is None:
                    logger.error(
                        "CompiledDAG has been destructed before teardown. "
                        "This should not occur please report an issue at "
                        "https://github.com/ray-project/ray/issues/new/.",
                        stack_info=True,
                    )
                    return False
                return True

            def wait_teardown(self, kill_actors: bool = False):
                outer = get_outer()
                if not self._outer_ref_alive():
                    return

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
                            msg += (
                                " Force-killing actor. "
                                "Increase RAY_CGRAPH_teardown_timeout if you want "
                                "teardown to wait longer."
                            )
                            ray.kill(actor)
                        else:
                            msg += (
                                " Teardown may hang. "
                                "Call teardown with kill_actors=True if force kill "
                                "is desired."
                            )

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

                if kill_actors:
                    # In the previous loop, we allow the actor tasks to exit first.
                    # Now, we force kill the actors if not yet.
                    for actor in outer.worker_task_refs:
                        logger.info(f"Killing actor: {actor}")
                        ray.kill(actor)

            def teardown(self, kill_actors: bool = False):
                with self._in_teardown_lock:
                    if self._teardown_done:
                        return

                    outer = get_outer()
                    if not self._outer_ref_alive():
                        return

                    logger.info("Tearing down compiled DAG")
                    outer._dag_submitter.close()
                    outer._dag_output_fetcher.close()

                    for actor in outer.actor_to_executable_tasks.keys():
                        logger.info(f"Cancelling compiled worker on actor: {actor}")
                    # Cancel all actor loops in parallel.
                    cancel_refs = [
                        actor.__ray_call__.remote(do_cancel_executable_tasks, tasks)
                        for actor, tasks in outer.actor_to_executable_tasks.items()
                    ]
                    for cancel_ref in cancel_refs:
                        try:
                            ray.get(cancel_ref, timeout=30)
                        except RayChannelError:
                            # Channel error happens when a channel is closed
                            # or timed out. In this case, do not log.
                            pass
                        except Exception:
                            logger.exception("Error cancelling worker task")
                            pass

                    for (
                        communicator_id
                    ) in outer._actors_to_created_communicator_id.values():
                        _destroy_communicator(communicator_id)

                    logger.info("Waiting for worker tasks to exit")
                    self.wait_teardown(kill_actors=kill_actors)

                    logger.info("Teardown complete")
                    self._teardown_done = True

            def run(self):
                try:
                    outer = get_outer()
                    if not self._outer_ref_alive():
                        return
                    ray.get(list(outer.worker_task_refs.values()))
                except KeyboardInterrupt:
                    logger.info(
                        "Received KeyboardInterrupt, tearing down with kill_actors=True"
                    )
                    self.teardown(kill_actors=True)
                except Exception as e:
                    logger.debug(f"Handling exception from worker tasks: {e}")
                    self.teardown()

        monitor = Monitor()
        monitor.start()
        return monitor

    def _raise_if_too_many_inflight_executions(self):
        num_inflight_executions = (
            self._execution_index - self._max_finished_execution_index
        )
        if num_inflight_executions >= self._max_inflight_executions:
            raise ray.exceptions.RayCgraphCapacityExceeded(
                "The compiled graph can't have more than "
                f"{self._max_inflight_executions} in-flight executions, and you "
                f"currently have {num_inflight_executions} in-flight executions. "
                "Retrieve an output using ray.get before submitting more requests or "
                "increase `_max_inflight_executions`. "
                "`dag.experimental_compile(_max_inflight_executions=...)`"
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
                # avoid caching for any CompiledDAGRef that has already been destructed.
                if not (
                    execution_index in self._destructed_ref_idxs
                    and chan_idx in self._destructed_ref_idxs[execution_index]
                ):
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

        if execution_index not in self._got_ref_idxs:
            self._got_ref_idxs[execution_index] = set()
        self._got_ref_idxs[execution_index].add(channel_index)
        self._clean_up_buffers(execution_index)
        return result

    def _delete_execution_results(self, execution_index: int, channel_index: int):
        """
        Delete the execution results for the given execution index and channel index.
        This method should be called when a CompiledDAGRef or CompiledDAGFuture is
        destructed.

        Note that this method maintains metadata for the deleted execution results,
        and only actually deletes the buffers lazily when the buffer is not needed
        anymore.

        Args:
            execution_index: The execution index to destruct results from.
            channel_index: The index of the output channel corresponding to the result.
        """
        if execution_index not in self._destructed_ref_idxs:
            self._destructed_ref_idxs[execution_index] = set()
        self._destructed_ref_idxs[execution_index].add(channel_index)
        self._clean_up_buffers(execution_index)

    def _try_release_result_buffer(self, execution_index: int):
        """
        Try to release the result buffer for the given execution index.
        """

        should_release = False
        got_channel_idxs = self._got_ref_idxs.get(execution_index, set())
        if None in got_channel_idxs:
            assert len(got_channel_idxs) == 1, (
                "when None exists in got_channel_idxs, it means all channels, and "
                "it should be the only value in the set",
            )
            should_release = True
        else:
            destructed_channel_idxs = self._destructed_ref_idxs.get(
                execution_index, set()
            )
            processed_channel_idxs = got_channel_idxs.union(destructed_channel_idxs)
            # No more processing is needed for this execution index.
            should_release = processed_channel_idxs == set(
                range(len(self.dag_output_channels))
            )

        if not should_release:
            return False

        self._result_buffer.pop(execution_index, None)
        self._destructed_ref_idxs.pop(execution_index, None)
        self._got_ref_idxs.pop(execution_index, None)
        return True

    def _try_release_native_buffer(
        self, idx_to_release: int, timeout: Optional[float] = None
    ) -> bool:
        """
        Try to release the native buffer for the given execution index.

        Args:
            idx_to_release: The execution index to release buffers from.
            timeout: The maximum time in seconds to wait for the release.

        Returns:
            Whether the buffers have been released.
        """
        if idx_to_release != self._max_finished_execution_index + 1:
            # Native buffer can only be released for the next execution index.
            return False

        destructed_channel_idxs = self._destructed_ref_idxs.get(idx_to_release, set())
        should_release = False
        if None in destructed_channel_idxs:
            assert len(destructed_channel_idxs) == 1, (
                "when None exists in destructed_channel_idxs, it means all channels, "
                "and it should be the only value in the set",
            )
            should_release = True
        elif len(destructed_channel_idxs) == len(self.dag_output_channels):
            should_release = True

        if not should_release:
            return False

        # refs corresponding to idx_to_release are all destructed,
        # and they are never fetched or cached.
        assert idx_to_release not in self._result_buffer
        assert idx_to_release not in self._got_ref_idxs

        try:
            self._dag_output_fetcher.release_channel_buffers(timeout)
        except RayChannelTimeoutError as e:
            raise RayChannelTimeoutError(
                "Releasing native buffers corresponding to a stale CompiledDAGRef "
                "is taking a long time. If this is expected, increase "
                f"RAY_CGRAPH_get_timeout which is currently {self._get_timeout} "
                "seconds. Otherwise, this may indicate that the execution "
                "is hanging."
            ) from e
        self._destructed_ref_idxs.pop(idx_to_release)

        return True

    def _try_release_buffer(
        self, idx_to_release: int, timeout: Optional[float] = None
    ) -> bool:
        """
        Try to release the buffer for the given execution index.
        First try to release the native buffer, then try to release the result buffer.

        Args:
            idx_to_release: The execution index to release buffers from.
            timeout: The maximum time in seconds to wait for the release.

        Returns:
            Whether the native buffer or result buffer has been released.
        """
        if self._try_release_native_buffer(idx_to_release, timeout):
            # Releasing native buffer means the corresponding execution result
            # is consumed (and discarded).
            self._max_finished_execution_index += 1
            return True
        return self._try_release_result_buffer(idx_to_release)

    def _try_release_buffers(self):
        """
        Repeatedly release buffer if possible.

        This method starts from _max_finished_execution_index + 1 and tries to release
        as many buffers as possible. If a native buffer is released,
        _max_finished_execution_index will be incremented.
        """
        timeout = self._get_timeout
        while True:
            start_time = time.monotonic()
            if not self._try_release_buffer(
                self._max_finished_execution_index + 1, timeout
            ):
                break

            if timeout != -1:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)

    def _clean_up_buffers(self, idx_to_release: int):
        """
        Clean up native and result buffers.

        This method:
        1. Tries to release the buffer for the given execution index.
           This index is the specific one that requires a clean up,
           e.g., right after get() is called or a CompiledDAGRef/CompiledDAGFuture
           is destructed.
        2. Tries to release all buffers starting from _max_finished_execution_index + 1.
           This step is to clean up buffers that are no longer needed.

        Args:
            idx_to_release: The execution index that requires a clean up,
                e.g., right after get() is called or a CompiledDAGRef/CompiledDAGFuture
                is destructed.
        """
        self._try_release_buffer(idx_to_release)
        self._try_release_buffers()

    def _execute_until(
        self,
        execution_index: int,
        channel_index: Optional[int] = None,
        timeout: Optional[float] = None,
    ):
        """Repeatedly execute this DAG until the given execution index and
        buffer results for all CompiledDagRef's.
        If the DAG has already been executed up to the given index, it will do nothing.

        Note: If this comes across execution indices for which the corresponding
        CompiledDAGRef's have been destructed, it will release the buffer and not
        cache the result.

        Args:
            execution_index: The execution index to execute until.
            channel_index: The index of the output channel to get the result from.
                Channel indexing is consistent with the order of
                self.dag_output_channels. None means wrapping results from all output
                channels into a single list.
            timeout: The maximum time in seconds to wait for the execution.
                None means using default timeout (DAGContext.get_timeout),
                0 means immediate timeout (immediate success or timeout without
                blocking), -1 means infinite timeout (block indefinitely).

        TODO(rui): catch the case that user holds onto the CompiledDAGRefs
        """
        if timeout is None:
            timeout = self._get_timeout
        while self._max_finished_execution_index < execution_index:
            if len(self._result_buffer) >= self._max_buffered_results:
                raise RayCgraphCapacityExceeded(
                    "The compiled graph can't have more than "
                    f"{self._max_buffered_results} buffered results, and you "
                    f"currently have {len(self._result_buffer)} buffered results. "
                    "Call `ray.get()` on CompiledDAGRef's (or await on "
                    "CompiledDAGFuture's) to retrieve results, or increase "
                    f"`_max_buffered_results` if buffering is desired, note that "
                    "this will increase driver memory usage."
                )
            start_time = time.monotonic()

            # Fetch results from each output channel up to execution_index and cache
            # them separately to enable individual retrieval
            # If a CompiledDagRef for a specific execution index has been destructed,
            # release the channel buffers for that execution index instead of caching
            try:
                if not self._try_release_native_buffer(
                    self._max_finished_execution_index + 1, timeout
                ):
                    result = self._dag_output_fetcher.read(timeout)
                    self._cache_execution_results(
                        self._max_finished_execution_index + 1,
                        result,
                    )
                # We have either released the native buffer or fetched and
                # cached the result buffer, therefore we always increment
                # _max_finished_execution_index.
                self._max_finished_execution_index += 1
            except RayChannelTimeoutError as e:
                raise RayChannelTimeoutError(
                    "If the execution is expected to take a long time, increase "
                    f"RAY_CGRAPH_get_timeout which is currently {self._get_timeout} "
                    "seconds. Otherwise, this may indicate that the execution is "
                    "hanging."
                ) from e

            if timeout != -1:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)

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
                self._submit_timeout seconds.

        NOTE: Not thread-safe due to _execution_index etc.
        """
        if self._enable_asyncio:
            raise ValueError("Use execute_async if enable_asyncio=True")

        self._get_or_compile()

        self._check_inputs(args, kwargs)
        if len(args) == 1 and len(kwargs) == 0:
            # When serializing a tuple, the Ray serializer invokes pickle5, which adds
            # several microseconds of overhead. One common case for Compiled Graphs is
            # passing a single argument (oftentimes of of type `bytes`, which requires
            # no serialization). To avoid imposing this overhead on this common case, we
            # create a fast path for this case that avoids pickle5.
            inp = args[0]
        else:
            inp = CompiledDAGArgs(args=args, kwargs=kwargs)

        # We want to release any buffers we can at this point based on the
        # max_finished_execution_index so that the number of inflight executions
        # is up to date.
        self._try_release_buffers()
        self._raise_if_too_many_inflight_executions()
        try:
            self._dag_submitter.write(inp, self._submit_timeout)
        except RayChannelTimeoutError as e:
            raise RayChannelTimeoutError(
                "If the execution is expected to take a long time, increase "
                f"RAY_CGRAPH_submit_timeout which is currently {self._submit_timeout} "
                "seconds. Otherwise, this may indicate that execution is hanging."
            ) from e

        self._execution_index += 1

        if self._returns_list:
            ref = [
                CompiledDAGRef(self, self._execution_index, channel_index)
                for channel_index in range(len(self.dag_output_channels))
            ]
        else:
            ref = CompiledDAGRef(self, self._execution_index)

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
                inp = CompiledDAGArgs(args=args, kwargs=kwargs)

            self._raise_if_too_many_inflight_executions()
            await self._dag_submitter.write(inp)
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        self._execution_index += 1

        if self._returns_list:
            fut = [
                CompiledDAGFuture(self, self._execution_index, fut, channel_index)
                for channel_index in range(len(self.dag_output_channels))
            ]
        else:
            fut = CompiledDAGFuture(self, self._execution_index, fut)

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
            # 8:MultiOutputNode
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
            if type(channel) is CachedChannel:
                channel_details += f", {channel._channel_id[:6]}..."
        # get inner channel
        if (
            type(channel) is CompositeChannel
            and downstream_actor_id in channel._channel_dict
        ):
            inner_channel = channel._channel_dict[downstream_actor_id]
            channel_details += f"\n{type(inner_channel).__name__}"
            if type(inner_channel) is IntraProcessChannel:
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
        Visualize the compiled graph by showing tasks and their dependencies.
        This method should be called **after** the graph has been compiled using
        `experimental_compile()`.

        Args:
            filename: For non-ASCII formats, the output file name (without extension).
                For ASCII format, the visualization will be printed to the console,
                and this argument is ignored.
            format: The format of the output file (e.g., 'png', 'pdf', 'ascii').
            view: For non-ASCII formats: Whether to open the file with the default
                viewer. For ASCII format: Whether to print the visualization and return
                None or return the ascii visualization string directly.
            channel_details: If True, adds channel details to edges.

        Returns:
            The string representation of the compiled graph. For Graphviz-based formats
            (e.g., 'png', 'pdf', 'jpeg'), returns the Graphviz DOT string representation
            of the compiled graph. For ASCII format, returns the ASCII string
            representation of the compiled graph.

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
                    actor = dag_node._get_actor_handle()
                    if actor:
                        class_name = (
                            actor._ray_actor_creation_function_descriptor.class_name
                        )
                        actor_id = actor._actor_id.hex()
                        label += f"Actor: {class_name}\n"
                        label += f"ID: {actor_id[:6]}...\n"
                        label += f"Method: {method_name}"
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
                (
                    type(dag_node.type_hint).__name__
                    if dag_node.type_hint
                    else "UnknownType"
                )
                + "\n"
                if channel_details
                else None
            )

            # This logic is built on the assumption that there will only be multiple
            # output channels if the task has multiple returns
            # case: task with one output
            if len(task.output_channels) == 1:
                for downstream_node in task.dag_node._downstream_nodes:
                    downstream_idx = self.dag_node_to_idx[downstream_node]
                    edge_label = None
                    if channel_details:
                        edge_label = channel_type_str
                        edge_label += self.get_channel_details(
                            task.output_channels[0],
                            (
                                downstream_node._get_actor_handle()._actor_id.hex()
                                if type(downstream_node) is ClassMethodNode
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
                    edge_label = None
                    if channel_details:
                        edge_label = channel_type_str
                        edge_label += self.get_channel_details(
                            output_channel,
                            task.dag_node._get_actor_handle()._actor_id.hex(),
                        )
                    dot.edge(str(idx), str(downstream_idx), label=edge_label)
            if type(task.dag_node) is InputAttributeNode:
                # Add an edge from the InputAttributeNode to the InputNode
                dot.edge(str(self.input_task_idx), str(idx))
        dot.render(filename, view=view)
        return dot.source

    def _register_input_output_custom_serializer(self):
        """
        Register custom serializers for input, input attribute, and output nodes.
        """
        assert self.input_task_idx is not None
        assert self.output_task_idx is not None

        # Register custom serializers for input node.
        input_task = self.idx_to_task[self.input_task_idx]
        input_task.dag_node.type_hint.register_custom_serializer()

        # Register custom serializers for input attribute nodes.
        for input_attr_task_idx in self.input_attr_task_idxs:
            input_attr_task = self.idx_to_task[input_attr_task_idx]
            input_attr_task.dag_node.type_hint.register_custom_serializer()

        # Register custom serializers for output nodes.
        for output in self.idx_to_task[self.output_task_idx].args:
            output.type_hint.register_custom_serializer()

    def teardown(self, kill_actors: bool = False):
        """
        Teardown and cancel all actor tasks for this DAG. After this
        function returns, the actors should be available to execute new tasks
        or compile a new DAG.

        Note: This method is automatically called when the CompiledDAG is destructed
        or the script exits. However, this should be explicitly called before compiling
        another graph on the same actors. Python may not garbage collect the
        CompiledDAG object immediately when you may expect.
        """
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
    submit_timeout: Optional[float] = None,
    buffer_size_bytes: Optional[int] = None,
    enable_asyncio: bool = False,
    max_inflight_executions: Optional[int] = None,
    max_buffered_results: Optional[int] = None,
    overlap_gpu_communication: Optional[bool] = None,
    default_communicator: Optional[Union[Communicator, str]] = "create",
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        submit_timeout,
        buffer_size_bytes,
        enable_asyncio,
        max_inflight_executions,
        max_buffered_results,
        overlap_gpu_communication,
        default_communicator,
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
