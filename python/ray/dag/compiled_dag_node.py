import asyncio
from collections import defaultdict, deque
from typing import Any, Dict, List, Tuple, Union, Optional, Set
import logging
import threading
import time
import uuid
import traceback
from typing import NamedTuple

import ray
from ray.exceptions import RayTaskError, RayChannelError
from ray.util.annotations import PublicAPI
from ray.experimental.compiled_dag_ref import (
    CompiledDAGRef,
    CompiledDAGFuture,
    _process_return_vals,
)
from ray.experimental.channel import (
    ChannelInterface,
    ChannelOutputType,
    ReaderInterface,
    SynchronousReader,
    WriterInterface,
    SynchronousWriter,
    AwaitableBackgroundReader,
    AwaitableBackgroundWriter,
)
from ray.util.annotations import DeveloperAPI

from ray.experimental.channel.shared_memory_channel import (
    SharedMemoryType,
)

from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
    _destroy_nccl_group,
)

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


# Holds the input arguments for an accelerated DAG node.
@PublicAPI(stability="alpha")
class RayDAGArgs(NamedTuple):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


logger = logging.getLogger(__name__)


@DeveloperAPI
def do_allocate_channel(
    self,
    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
    typ: ChannelOutputType,
) -> ChannelInterface:
    """Generic actor method to allocate an output channel.

    Args:
        reader_and_node_list: A list of tuples, where each tuple contains a reader
            actor handle and the node ID where the actor is located.
        typ: The output type hint for the channel.

    Returns:
        The allocated channel.
    """
    self_actor = None
    try:
        self_actor = ray.get_runtime_context().current_actor
    except RuntimeError:
        # This is the driver so there is no current actor handle.
        pass

    output_channel = typ.create_channel(
        self_actor,
        reader_and_node_list,
    )
    return output_channel


@DeveloperAPI
def do_exec_tasks(
    self,
    tasks: List["ExecutableTask"],
) -> None:
    """Generic actor method to begin executing the tasks belonging to an actor.
    This runs an infinite loop to run each task in turn (following the order specified
    in the list): reading input channel(s), executing the given taks, and writing output
    channel(s). It only exits if the actor dies or an exception is thrown.

    Args:
        tasks: the executable tasks corresponding to the actor methods.
    """
    try:
        self._input_readers = []
        self._output_writers = []
        for task in tasks:
            _prep_task(self, task)

        done = False
        while True:
            if done:
                break
            for idx, task in enumerate(tasks):
                done = _exec_task(self, task, idx)
                if done:
                    break

    except Exception:
        logging.exception("Compiled DAG task exited with exception")
        raise


@DeveloperAPI
def do_cancel_executable_tasks(self, tasks: List["ExecutableTask"]) -> None:
    for idx in range(len(tasks)):
        self._input_readers[idx].close()
        self._output_writers[idx].close()


def _prep_task(self, task: "ExecutableTask") -> None:
    """
    Prepare the task for execution.
    """
    for typ_hint in task.input_type_hints:
        typ_hint.register_custom_serializer()
    task.output_type_hint.register_custom_serializer()

    input_reader: ReaderInterface = SynchronousReader(task.input_channels)
    output_writer: WriterInterface = SynchronousWriter(task.output_channel)
    self._input_readers.append(input_reader)
    self._output_writers.append(output_writer)

    input_reader.start()
    output_writer.start()


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


def _exec_task(self, task: "ExecutableTask", idx: int) -> bool:
    """
    Execute the task.
    Args:
        task: The task to execute.
        idx: The index of the task in the list of tasks of the actor.
    Returns:
        True if we are done executing all tasks of this actor, False otherwise.
    """
    # TODO: for cases where output is passed as input to a task on
    # the same actor, introduce a "IntraProcessChannel" to avoid the overhead
    # of serialization/deserialization and synchronization.
    method = getattr(self, task.method_name)
    input_reader = self._input_readers[idx]
    output_writer = self._output_writers[idx]
    res = None
    try:
        res = input_reader.read()
    except RayChannelError:
        # Channel closed. Exit the loop.
        return True

    try:
        _process_return_vals(res, return_single_output=False)
    except Exception as exc:
        # Previous task raised an application-level exception.
        # Propagate it and skip the actual task. We don't need to wrap the
        # exception in a RayTaskError here because it has already been wrapped
        # by the previous task.
        output_writer.write(exc)
        return False

    resolved_inputs = []
    for task_input in task.task_inputs:
        resolved_inputs.append(task_input.resolve(res))

    try:
        output_val = method(*resolved_inputs, **task.resolved_kwargs)
    except Exception as exc:
        output_val = _wrap_exception(exc)

    try:
        output_writer.write(output_val)
    except RayChannelError:
        # Channel closed. Exit the loop.
        return True

    return False


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

        self.downstream_node_idxs: Dict[int, "ray.actor.ActorHandle"] = {}
        self.output_channel = None
        self.arg_type_hints: List["ChannelOutputType"] = []

    @property
    def args(self) -> Tuple[Any]:
        return self.dag_node.get_args()

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.dag_node.get_kwargs()

    @property
    def num_readers(self) -> int:
        return len(self.downstream_node_idxs)

    def __str__(self) -> str:
        return f"""
Node: {self.dag_node}
Arguments: {self.args}
Output: {self.output_channel}
"""


@DeveloperAPI
class DAGInputAdapter:
    """Adapter to extract individual positional arguments and kwargs
    from objects read from DAG input channel."""

    def __init__(
        self,
        input_attr_node: Optional["ray.dag.InputAttributeNode"],
        dag_input_channel: "ray.experimental.channel.ChannelInterface",
    ):
        """
        Args:
            input_attr_node: The input attribute node that this adapter is
                created for. None should be used when creating an adapter for
                the DAG input node itself; in this case, the adapter will
                extract the 0th positional argument.
            dag_input_channel: The DAG input channel.
        """
        self._dag_input_channel = dag_input_channel

        def extractor(key: Union[int, str]):
            def extract_arg(raw_args):
                if not isinstance(raw_args, RayDAGArgs):
                    # Fast path for a single input.
                    return raw_args
                else:
                    assert isinstance(raw_args, RayDAGArgs)
                    args = raw_args.args
                    kwargs = raw_args.kwargs

                if isinstance(key, int):
                    return args[key]
                else:
                    return kwargs[key]

            return extract_arg

        if input_attr_node:
            key = input_attr_node.key
        else:
            key = 0
        self._adapt_method = extractor(key)

    def adapt(self, input):
        return self._adapt_method(input)

    def get_dag_input_channel(self):
        return self._dag_input_channel


class _ExecutableTaskInput:
    """Represents an input to an ExecutableTask.

    Args:
        input_variant: either an unresolved input (when type is ChannelInterface
            or DAGInputAdapter), or a resolved input value (when type is Any)
        channel_idx: if input_variant is an unresolved input, this is the index
            into the input channels list.
    """

    def __init__(
        self,
        input_variant: Union[ChannelInterface, DAGInputAdapter, Any],
        channel_idx: Optional[int],
    ):
        self.input_variant = input_variant
        self.channel_idx = channel_idx

    def resolve(self, channel_results: Any):
        """
        Resolve the input value from the channel results.

        Args:
            channel_results: The results from reading the input channels.
        """
        if isinstance(self.input_variant, ChannelInterface):
            value = channel_results[self.channel_idx]
        elif isinstance(self.input_variant, DAGInputAdapter):
            adapter = self.input_variant
            value = adapter.adapt(channel_results[self.channel_idx])
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
        self.method_name = task.dag_node.get_method_name()
        self.bind_index = task.dag_node._get_bind_index()
        self.output_channel = task.output_channel
        self.input_type_hints: List["ChannelOutputType"] = task.arg_type_hints
        self.output_type_hint: "ChannelOutputType" = task.dag_node.type_hint

        self.input_channels: List[ChannelInterface] = []
        self.task_inputs: List[_ExecutableTaskInput] = []
        self.resolved_kwargs: Dict[str, Any] = resolved_kwargs

        # Reverse map for input_channels: maps an input channel to
        # its index in input_channels.
        input_channel_to_idx: dict[ChannelInterface, int] = {}

        for arg in resolved_args:
            if isinstance(arg, ChannelInterface) or isinstance(arg, DAGInputAdapter):
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
            assert not isinstance(val, DAGInputAdapter)


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

        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        from ray.dag import DAGContext

        ctx = DAGContext.get_current()

        self._dag_id = uuid.uuid4().hex
        self._execution_timeout: Optional[float] = execution_timeout
        if self._execution_timeout is None:
            self._execution_timeout = ctx.execution_timeout
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = ctx.buffer_size_bytes
        self._default_type_hint: ChannelOutputType = SharedMemoryType(
            self._buffer_size_bytes
        )
        if not isinstance(self._buffer_size_bytes, int) or self._buffer_size_bytes <= 0:
            raise ValueError(
                "`buffer_size_bytes` must be a positive integer, found "
                f"{self._buffer_size_bytes}"
            )

        self._enable_asyncio: bool = enable_asyncio
        self._fut_queue = asyncio.Queue()
        self._asyncio_max_queue_size: Optional[int] = asyncio_max_queue_size
        # TODO(rui): consider unify it with asyncio_max_queue_size
        self._max_buffered_results: Optional[int] = max_buffered_results
        if self._max_buffered_results is None:
            self._max_buffered_results = ctx.max_buffered_results
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
        self._has_single_output: bool = False
        # Number of expected positional args and kwargs that may be passed to
        # dag.execute.
        self._input_num_positional_args: Optional[int] = None
        self._input_kwargs: Tuple[str, ...] = None
        self.actor_task_count: Dict["ray._raylet.ActorID", int] = defaultdict(int)

        # Cached attributes that are set during compilation.
        self.dag_input_channel: Optional[ChannelInterface] = None
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
        # Mapping from the actor handle to the node ID that the actor is on.
        self.actor_to_node_id: Dict["ray.actor.ActorHandle", str] = {}

        # Type hints specified by the user for DAG (intermediate) outputs.
        self._type_hints = []

        # Uniquely identifies the NCCL communicator that will be used within
        # this DAG, if any.
        self._nccl_group_id: Optional[str] = None
        # The index of the current execution. It is incremented each time
        # the DAG is executed.
        self._execution_index: int = 0
        # The maximum index of finished executions.
        # All results with higher indexes have not been generated yet.
        self._max_execution_index: int = -1
        self._result_buffer: Dict[int, Any] = {}

        def _get_or_create_local_actor_handle():
            """
            Get the actor handle of the current process. If the current process is
            the driver process, create a new actor handle DAGDriverProxyActor.

            Returns:
              Return the actor handle if the current task is an actor method.
              Create a DAGDriverProxyActor actor if the current process is the
              driver process.

            Raises:
                NotImplementedError: If the current process is a Ray task.
            """
            runtime_context = ray.get_runtime_context()
            if runtime_context.worker.mode == ray.WORKER_MODE:
                try:
                    return ray.get_runtime_context().current_actor
                except RuntimeError:
                    raise NotImplementedError(
                        "Compiled DAGs currently require the InputNode() to be the "
                        "driver process or an actor method. Ray task is not supported."
                    )
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

        self._dag_creator_actor = _get_or_create_local_actor_handle()

    @property
    def has_single_output(self):
        return self._has_single_output

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
            FunctionNode,
            InputAttributeNode,
            InputNode,
            MultiOutputNode,
        )

        self.input_task_idx, self.output_task_idx = None, None
        self.actor_task_count.clear()
        self._type_hints.clear()

        nccl_actors: Set["ray.actor.ActorHandle"] = set()

        # Find the input node to the DAG.
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "more than one InputNode found"
                self.input_task_idx = idx
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
        for node_idx, task in self.idx_to_task.items():
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
                    raise ValueError(
                        f"Found unsupported node of type {type(task.dag_node)}"
                    )

            if isinstance(dag_node, ClassMethodNode):
                actor_handle = dag_node._get_actor_handle()
                if actor_handle is None:
                    raise ValueError(
                        "Compiled DAGs can only bind methods to an actor "
                        "that is already created with Actor.remote()"
                    )

                if dag_node.num_returns != 1:
                    raise ValueError(
                        "Compiled DAGs only supports actor methods with "
                        "num_returns=1"
                    )

                self.actor_task_count[actor_handle._actor_id] += 1

                if dag_node.type_hint.requires_nccl():
                    # Add all writers to the NCCL group.
                    nccl_actors.add(actor_handle)
            elif isinstance(dag_node, InputNode):
                if dag_node.type_hint.requires_nccl():
                    raise ValueError(
                        "DAG inputs cannot be transferred via NCCL because "
                        "the driver cannot participate in the NCCL group"
                    )

            if type(task.dag_node.type_hint) == ChannelOutputType:
                # No type hint specified by the user. Replace
                # with the default type hint for this DAG.
                task.dag_node.with_type_hint(self._default_type_hint)

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
                upstream_node = self.idx_to_task[upstream_node_idx]
                downstream_actor_handle = None
                if isinstance(task.dag_node, ClassMethodNode):
                    downstream_actor_handle = task.dag_node._get_actor_handle()

                if isinstance(upstream_node.dag_node, InputAttributeNode):
                    # Record all of the keys used to index the InputNode.
                    # During execution, we will check that the user provides
                    # the same args and kwargs.
                    if isinstance(upstream_node.dag_node.key, int):
                        input_positional_args.add(upstream_node.dag_node.key)
                    elif isinstance(upstream_node.dag_node.key, str):
                        input_kwargs.add(upstream_node.dag_node.key)
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
                    upstream_node = self.idx_to_task[self.input_task_idx]

                elif isinstance(upstream_node.dag_node, InputNode):
                    if direct_input is not None and not direct_input:
                        raise ValueError(
                            "All tasks must either use InputNode() directly, "
                            "or they must index to specific args or kwargs."
                        )
                    direct_input = True

                upstream_node.downstream_node_idxs[node_idx] = downstream_actor_handle
                task.arg_type_hints.append(upstream_node.dag_node.type_hint)

                if upstream_node.dag_node.type_hint.requires_nccl():
                    # Add all readers to the NCCL group.
                    nccl_actors.add(downstream_actor_handle)

            if dag_node.type_hint is not None:
                self._type_hints.append(dag_node.type_hint)

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
            if idx == self.input_task_idx or isinstance(
                task.dag_node, InputAttributeNode
            ):
                continue
            if len(task.downstream_node_idxs) == 0:
                assert self.output_task_idx is None, "More than one output node found"
                self.output_task_idx = idx

        assert self.output_task_idx is not None
        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # Add an MultiOutputNode to the end of the DAG if it's not already there.
        if not isinstance(output_node, MultiOutputNode):
            self._has_single_output = True
            output_node = MultiOutputNode([output_node])
            self._add_node(output_node)
            self.output_task_idx = self.dag_node_to_idx[output_node]
            # Preprocess one more time so that we have the right output node
            # now.
            self._preprocess()

        # If there were type hints indicating transport via NCCL, initialize
        # the NCCL group on the participating actors.
        nccl_actors = list(nccl_actors)
        if None in nccl_actors:
            raise ValueError("Driver cannot participate in the NCCL group.")
        if nccl_actors and self._nccl_group_id is None:
            self._nccl_group_id = _init_nccl_group(nccl_actors)

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
        if actor_handle == self._dag_creator_actor:
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
            # Create an output buffer for the actor method.
            assert task.output_channel is None

            type_hint = task.dag_node.type_hint
            if type_hint.requires_nccl():
                type_hint.set_nccl_group_id(self._nccl_group_id)

            if isinstance(task.dag_node, ClassMethodNode):
                # `readers` is the nodes that are ordered after the current one (`task`)
                # in the DAG.
                readers = [self.idx_to_task[idx] for idx in task.downstream_node_idxs]
                reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]] = []
                dag_nodes = [reader.dag_node for reader in readers]
                read_by_multi_output_node = False
                for dag_node in dag_nodes:
                    if isinstance(dag_node, MultiOutputNode):
                        read_by_multi_output_node = True
                        break
                if read_by_multi_output_node:
                    if len(readers) != 1:
                        raise ValueError(
                            "DAG outputs currently can only be read by the driver or "
                            "the same actor that is also the InputNode, not by both "
                            "the driver and actors."
                        )
                    # This node is a multi-output node, which means it will only be
                    # read by the driver or the actor that is also the InputNode.

                    # TODO(jhumphri): Handle case where there is an actor, other than
                    # just the driver actor, also reading the output from the `task`
                    # node.
                    # For example, the following currently does not work:
                    # def test_blah(ray_start_regular):
                    #     a = Actor.remote(0)
                    #     b = Actor.remote(10)
                    #     with InputNode() as inp:
                    #         x = a.inc.bind(inp)
                    #         y = b.inc.bind(x)
                    #         dag = MultiOutputNode([x, y])

                    #     compiled_dag = dag.experimental_compile()
                    #     output_channel = compiled_dag.execute(1)
                    #     result = output_channel.read()
                    #     print(result)

                    #     compiled_dag.teardown()
                    assert self._dag_creator_actor is not None
                    reader_and_node_list.append(
                        (
                            self._dag_creator_actor,
                            self._get_node_id(self._dag_creator_actor),
                        )
                    )
                else:
                    for reader in readers:
                        reader_handle = reader.dag_node._get_actor_handle()
                        reader_and_node_list.append(
                            (reader_handle, self._get_node_id(reader_handle))
                        )

                fn = task.dag_node._get_remote_method("__ray_call__")
                task.output_channel = ray.get(
                    fn.remote(
                        do_allocate_channel,
                        reader_and_node_list,
                        typ=type_hint,
                    )
                )
                actor_handle = task.dag_node._get_actor_handle()
                self.actor_refs.add(actor_handle)
                self.actor_to_tasks[actor_handle].append(task)
            elif isinstance(task.dag_node, InputNode):
                reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]] = []
                # TODO (kevin85421): Currently, the shared memory channel doesn't
                # support multiple readers on the same actor. However, if the
                # InputNode is an actor instead of the driver process, we can support
                # multiple readers on the same actor if the readers are on the same
                # actor using IntraProcessChannel, which supports multiple readers
                # on the same actor. We need to remove reader_handles_set in the future
                # when we support multiple readers for both shared memory channel
                # and IntraProcessChannel.
                reader_handles_set = set()
                for idx in task.downstream_node_idxs:
                    reader_task = self.idx_to_task[idx]
                    assert isinstance(reader_task.dag_node, ClassMethodNode)
                    reader_handle = reader_task.dag_node._get_actor_handle()
                    if reader_handle not in reader_handles_set:
                        reader_and_node_list.append(
                            (reader_handle, self._get_node_id(reader_handle))
                        )
                    reader_handles_set.add(reader_handle)
                task.output_channel = do_allocate_channel(
                    self,
                    reader_and_node_list,
                    typ=type_hint,
                )
            else:
                assert isinstance(task.dag_node, InputAttributeNode) or isinstance(
                    task.dag_node, MultiOutputNode
                )

            for idx in task.downstream_node_idxs:
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
                        "or at least one other DAGNode as an input"
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
        self.dag_input_channel = input_task.output_channel

        # Create executable tasks for each actor
        for actor_handle, tasks in self.actor_to_tasks.items():
            executable_tasks = []
            worker_fn = None
            for task in tasks:
                resolved_args = []
                has_at_least_one_channel_input = False
                for arg in task.args:
                    if isinstance(arg, InputNode):
                        input_adapter = DAGInputAdapter(None, self.dag_input_channel)
                        resolved_args.append(input_adapter)
                        has_at_least_one_channel_input = True
                    elif isinstance(arg, InputAttributeNode):
                        input_adapter = DAGInputAdapter(arg, self.dag_input_channel)
                        resolved_args.append(input_adapter)
                        has_at_least_one_channel_input = True
                    elif isinstance(arg, DAGNode):  # Other DAGNodes
                        arg_idx = self.dag_node_to_idx[arg]
                        arg_channel = self.idx_to_task[arg_idx].output_channel
                        assert arg_channel is not None
                        resolved_args.append(arg_channel)
                        has_at_least_one_channel_input = True
                    else:
                        resolved_args.append(arg)
                # TODO: Support no-input DAGs (use an empty object to signal).
                if not has_at_least_one_channel_input:
                    raise ValueError(
                        "Compiled DAGs require each task to take a "
                        "ray.dag.InputNode or at least one other DAGNode as an "
                        "input"
                    )
                executable_task = ExecutableTask(
                    task,
                    resolved_args,
                    task.kwargs,
                )
                executable_tasks.append(executable_task)
                if worker_fn is None:
                    worker_fn = task.dag_node._get_remote_method("__ray_call__")
            # Sort executable tasks based on their bind index, i.e., submission order
            # so that they will be executed in that order.
            executable_tasks.sort(key=lambda task: task.bind_index)

            self.actor_to_executable_tasks[actor_handle] = executable_tasks
            # Assign the task with the correct input and output buffers.
            self.worker_task_refs[
                task.dag_node._get_actor_handle()
            ] = worker_fn.options(concurrency_group="_ray_system").remote(
                do_exec_tasks,
                executable_tasks,
            )

        self.dag_output_channels = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            self.dag_output_channels.append(self.idx_to_task[output_idx].output_channel)
            # Register custom serializers for DAG outputs.
            output.type_hint.register_custom_serializer()

        assert self.dag_input_channel
        assert self.dag_output_channels
        assert [
            output_channel is not None for output_channel in self.dag_output_channels
        ]
        # If no MultiOutputNode was specified during the DAG creation, there is only
        # one output. Return a single output channel instead of a list of
        # channels.
        if self._has_single_output:
            assert len(self.dag_output_channels) == 1

        # Driver should ray.put on input, ray.get/release on output
        self._monitor = self._monitor_failures()
        if self._enable_asyncio:
            self._dag_submitter = AwaitableBackgroundWriter(
                self.dag_input_channel, self._asyncio_max_queue_size
            )
            self._dag_output_fetcher = AwaitableBackgroundReader(
                self.dag_output_channels,
                self._fut_queue,
            )
        else:
            self._dag_submitter = SynchronousWriter(self.dag_input_channel)
            self._dag_output_fetcher = SynchronousReader(self.dag_output_channels)

        self._dag_submitter.start()
        self._dag_output_fetcher.start()

    def _detect_deadlock(self) -> bool:
        """
        Create a graph with the following 3 rules, and then use
        topological sort to verify whether the graph is a DAG.
        If not, the DAG will result in a deadlock due to a cycle.

        We need to check whether there is a cycle in a “happens-before”
        graph, where A -> B means that B happens before A.

        #1: Add an edge from task.{bind_index} to task.{bind_index+1}
            on the same actor.

        Reason: Each actor executes tasks in the order that they are
                bound in. Therefore task.{bind_index+1} happens before
                task.{bind_index}.

        #2: Add an edge from the writer to the reader

        Reason: Channels represent data dependencies. In order to read
                data, the writer must have written the data first.

        #3: Add an edge from the reader of an NCCL channel to the node
            that has the next bind index on the same actor as the writer.

        Reason: NCCL channels are blocking, meaning that both the writer
                and reader must reach the send/recv call before either can
                proceed. Therefore, the next task on the writer cannot be
                executed until the reader of the NCCL channel has started.

        With rules #1 and #2 alone, it is not possible to create cycles,
        because when the DAG is created, new tasks can only depend on tasks
        that have already been created.

        With rule #3, it is possible to create a cycle where two actors will
        block waiting for the other to begin reading from an NCCL channel.

        [Example]

        # data flow: driver -> a.no_op -> a.no_op -> driver
        with InputNode() as inp:
            dag = a.no_op.bind(inp)
            dag.with_type_hint(TorchTensorType(transport="nccl"))
            dag = a.no_op.bind(dag)
        dag.experimental_compile()

        In the above example, communication between a.no_op occurs via an NCCL
        channel, while communication between the driver process and a.no_op occurs
        via shared memory channels. The example experiences a deadlock because the
        completion of the write function in the first a.no_op requires the second
        a.no_op to simultaneously call the read function. However, each actor has
        a list of tasks, each assigned a bind index, and these tasks are executed
        sequentially in ascending order of their bind index on the actor. Therefore,
        it’s impossible for both writer and reader on the same actor to write and
        read simultaneously.

        We can create a happens-before graph based on the above rules. Then, the
        graph will look like this:

                              |---|
                              |   v
        driver -> a.no_op -> a.no_op -> driver

        Then, we use topological sort to verify whether the graph has a cycle.

        If you are interested in the detailed explanation, please refer to
        https://github.com/ray-project/ray/pull/45960.

        Returns:
            True if deadlock is detected, otherwise False.
        """
        assert self.idx_to_task
        assert self.actor_to_tasks

        class GraphNode:
            def __init__(self):
                self.in_edges = set()
                self.out_edges = set()

            @property
            def in_degree(self) -> int:
                return len(self.in_edges)

        from ray.dag import ClassMethodNode

        def _get_next_task_idx(task: "CompiledTask") -> Optional[int]:
            if not isinstance(task.dag_node, ClassMethodNode):
                return None
            actor_handle = task.dag_node._get_actor_handle()
            bind_index = task.dag_node._get_bind_index()
            for same_node_task in self.actor_to_tasks[actor_handle]:
                if same_node_task.dag_node._get_bind_index() == bind_index + 1:
                    return same_node_task.idx
            return None

        def _add_edge(
            graph: Dict[int, GraphNode], from_idx: int, to_idx: Optional[int]
        ):
            if to_idx is None:
                return
            graph[from_idx].out_edges.add(to_idx)
            graph[to_idx].in_edges.add(from_idx)

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
            if not isinstance(task1.dag_node, ClassMethodNode):
                return False
            if not isinstance(task2.dag_node, ClassMethodNode):
                return False
            actor_id_1 = task1.dag_node._get_actor_handle()._actor_id
            actor_id_2 = task2.dag_node._get_actor_handle()._actor_id
            return actor_id_1 == actor_id_2

        graph = defaultdict(GraphNode)
        for idx, task in self.idx_to_task.items():
            # Add an edge from task_{bind_index} to task_{bind_index+1}
            # on the same actor.
            next_task_idx = _get_next_task_idx(task)
            _add_edge(graph, idx, next_task_idx)
            for downstream_idx in task.downstream_node_idxs:
                # Add an edge from the writer to the reader.
                _add_edge(graph, idx, downstream_idx)
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
                    # Add an edge from the reader of an NCCL channel to the node
                    # that has the next bind index on the same actor as the writer.
                    _add_edge(graph, downstream_idx, next_task_idx)
        num_total_nodes = len(graph)

        # A list of nodes with in-degree 0, including (1) InputNode and
        # (2) the nodes that only read from NCCL channels and are the first
        # node on the actor.
        zero_in_degree_nodes = deque()
        for idx, node in graph.items():
            if node.in_degree == 0:
                zero_in_degree_nodes.append(idx)
        visited_nodes = set()

        # Perform topological sort to find a topological order of the graph.
        # If topological order exists, the graph is a DAG. Otherwise, it has
        # a cycle.
        while zero_in_degree_nodes:
            node = zero_in_degree_nodes.popleft()
            visited_nodes.add(node)
            for out_node in graph[node].out_edges:
                graph[out_node].in_edges.remove(node)
                if graph[out_node].in_degree == 0:
                    zero_in_degree_nodes.append(out_node)

        # Remove visited nodes from the graph.
        for node in visited_nodes:
            del graph[node]

        topological_order_exists = len(visited_nodes) == num_total_nodes
        if not topological_order_exists:
            logger.error(
                "The compiled DAG may hang due to blocking NCCL calls. If you "
                "believe this is a false positive, please file an issue at "
                "https://github.com/ray-project/ray/issues/new/."
            )

        return not topological_order_exists

    def _monitor_failures(self):
        outer = self

        class Monitor(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True)
                self.in_teardown = False
                # Lock to make sure that we only perform teardown for this DAG
                # once.
                self.in_teardown_lock = threading.Lock()

            def wait_teardown(self):
                for actor, ref in outer.worker_task_refs.items():
                    timeout = False
                    try:
                        ray.get(ref, timeout=10)
                    except ray.exceptions.GetTimeoutError:
                        logger.warn(
                            f"Compiled DAG actor {actor} is still running 10s "
                            "after teardown(). Teardown may hang."
                        )
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

            def teardown(self, wait: bool):
                do_teardown = False
                with self.in_teardown_lock:
                    if not self.in_teardown:
                        do_teardown = True
                        self.in_teardown = True

                if not do_teardown:
                    # Teardown is already being performed.
                    if wait:
                        self.wait_teardown()
                    return

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
                        # TODO(swang): Suppress exceptions from actors trying to
                        # read closed channels when DAG is being torn down.
                        ray.get(cancel_ref, timeout=30)
                    except Exception:
                        logger.exception("Error cancelling worker task")
                        pass

                if outer._nccl_group_id is not None:
                    _destroy_nccl_group(outer._nccl_group_id)

                if wait:
                    logger.info("Waiting for worker tasks to exit")
                    self.wait_teardown()
                    logger.info("Teardown complete")

            def run(self):
                try:
                    ray.get(list(outer.worker_task_refs.values()))
                except Exception as e:
                    logger.debug(f"Handling exception from worker tasks: {e}")
                    self.teardown(wait=True)

        monitor = Monitor()
        monitor.start()
        return monitor

    def _execute_until(
        self,
        execution_index: int,
        timeout: Optional[float] = None,
    ) -> Any:
        """Repeatedly execute this DAG until the given execution index,
        and buffer all results up to that index. If the DAG has already
        been executed up to the given index, just return the result
        corresponding to the given index.

        Args:
            execution_index: The execution index to execute until.
            timeout: The maximum time in seconds to wait for the result.
                None means using default timeout (DAGContext.retrieval_timeout),
                0 means immediate timeout (immediate success or timeout without
                blocking), -1 means infinite timeout (block indefinitely).

        Returns:
            The execution result corresponding to the given execution index.

        TODO(rui): catch the case that user holds onto the CompiledDAGRefs
        """
        from ray.dag import DAGContext

        ctx = DAGContext.get_current()
        if timeout is None:
            timeout = ctx.retrieval_timeout

        while self._max_execution_index < execution_index:
            if self._max_execution_index + 1 == execution_index:
                # Directly fetch and return without buffering
                self._max_execution_index += 1
                return self._dag_output_fetcher.read(timeout)
            # Otherwise, buffer the result
            if len(self._result_buffer) >= self._max_buffered_results:
                raise ValueError(
                    "Too many buffered results: the allowed max count for "
                    f"buffered results is {self._max_buffered_results}; call ray.get() "
                    "on previous CompiledDAGRefs to free them up from buffer."
                )
            self._max_execution_index += 1
            start_time = time.monotonic()
            self._result_buffer[
                self._max_execution_index
            ] = self._dag_output_fetcher.read(timeout)
            if timeout != -1:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)

        # CompiledDAGRef guarantees that the same execution index will not
        # be requested multiple times
        return self._result_buffer.pop(execution_index)

    def execute(
        self,
        *args,
        **kwargs,
    ) -> CompiledDAGRef:
        """Execute this DAG using the compiled execution path.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode

        Returns:
            A list of Channels that can be used to read the DAG result.

        Raises:
            RayChannelTimeoutError: If the execution does not complete within
                self._execution_timeout seconds.

        NOTE: Not threadsafe due to _execution_index etc.
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

        self._dag_submitter.write(inp, self._execution_timeout)

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
    ) -> CompiledDAGFuture:
        """Execute this DAG using the compiled execution path.

        NOTE: Not threadsafe.

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

            await self._dag_submitter.write(inp)
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        return CompiledDAGFuture(self, self._execution_index, fut)

    def teardown(self):
        """Teardown and cancel all actor tasks for this DAG. After this
        function returns, the actors should be available to execute new tasks
        or compile a new DAG."""
        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            monitor.teardown(wait=True)

    def __del__(self):
        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            # Teardown asynchronously.
            # NOTE(swang): Somehow, this can get called after the CoreWorker
            # has already been destructed, so it is not safe to block in
            # ray.get.
            monitor.teardown(wait=False)


@DeveloperAPI
def build_compiled_dag_from_ray_dag(
    dag: "ray.dag.DAGNode",
    execution_timeout: Optional[float] = None,
    buffer_size_bytes: Optional[int] = None,
    enable_asyncio: bool = False,
    asyncio_max_queue_size: Optional[int] = None,
    max_buffered_results: Optional[int] = None,
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        execution_timeout,
        buffer_size_bytes,
        enable_asyncio,
        asyncio_max_queue_size,
        max_buffered_results,
    )

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag._get_or_compile()
    return compiled_dag
