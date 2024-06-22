import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Union, Optional, Set
import logging
import threading
import uuid

import ray
from ray.experimental.compiled_dag_ref import CompiledDAGRef, RayDAGTaskError
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
from ray.util.annotations import DeveloperAPI, PublicAPI

from ray.experimental.channel.shared_memory_channel import (
    SharedMemoryType,
)

from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
    _destroy_nccl_group,
)

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB

# The maximum total memory that can be used to buffer DAG execution results.
MAX_BUFFER_TOTAL_MEMORY = int(10 * 1e9)  # 10GB

MAX_BUFFER_COUNT = MAX_BUFFER_TOTAL_MEMORY // MAX_BUFFER_SIZE

logger = logging.getLogger(__name__)


@DeveloperAPI
def do_allocate_channel(
    self,
    readers: List[Optional["ray.actor.ActorHandle"]],
    typ: ChannelOutputType,
) -> ChannelInterface:
    """Generic actor method to allocate an output channel.

    Args:
        readers: The actor handles of the readers.
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
        readers,
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
    except IOError:
        # Channel closed. Exit the loop.
        return True
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
        output_val = method(*resolved_inputs)
        output_writer.write(output_val)
    except IOError:
        # Channel closed. Exit the loop.
        return True
    except Exception as exc:
        # TODO(rui): consider different ways of passing down the exception,
        # e.g., wrapping with RayTaskError.
        output_writer.write(RayDAGTaskError(exc))

    return False


@PublicAPI(stability="alpha")
class AwaitableDAGOutput:
    def __init__(self, fut: asyncio.Future, ReaderInterface: ReaderInterface):
        self._fut = fut
        self._reader = ReaderInterface

    async def get(self):
        ret = await self._fut
        if isinstance(ret, Exception):
            raise ret
        return ret


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
            def extract_arg(args_tuple):
                positional_args, kwargs = args_tuple
                if isinstance(key, int):
                    return positional_args[key]
                else:
                    return kwargs[key]

            return extract_arg

        if input_attr_node:
            key = input_attr_node.get_other_args_to_resolve()["key"]
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
    ):
        """
        Args:
            task: The CompiledTask that this ExecutableTask corresponds to.
            resolved_args: The arguments to the method. Arguments that are
                not Channels will get passed through to the actor method.
                If the argument is a channel, it will be replaced by the
                value read from the channel before the method executes.
        """
        self.method_name = task.dag_node.get_method_name()
        self.bind_index = task.dag_node._get_bind_index()
        self.output_channel = task.output_channel
        self.input_type_hints: List["ChannelOutputType"] = task.arg_type_hints
        self.output_type_hint: "ChannelOutputType" = task.dag_node.type_hint

        self.input_channels: List[ChannelInterface] = []
        self.task_inputs: List[_ExecutableTaskInput] = []

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
        buffer_size_bytes: Optional[int],
        enable_asyncio: bool = False,
        async_max_queue_size: Optional[int] = None,
        max_buffered_results: Optional[int] = None,
    ):
        """
        Args:
            buffer_size_bytes: The number of bytes to allocate for object data and
                metadata. Each argument passed to a task in the DAG must be
                less than or equal to this value when serialized.
            enable_asyncio: Whether to enable asyncio. If enabled, caller must
                be running in an event loop and must use `execute_async` to
                invoke the DAG. Otherwise, the caller should use `execute` to
                invoke the DAG.
            async_max_queue_size: Optional parameter to limit how many DAG
                inputs can be queued at a time. The actual number of concurrent
                DAG invocations may be higher than this, if there are already
                inputs being processed by the DAG executors. If used, the
                caller is responsible for preventing deadlock, i.e. if the
                input queue is full, another asyncio task is reading from the
                DAG output.
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
        self._dag_id = uuid.uuid4().hex
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = MAX_BUFFER_SIZE
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
        self._async_max_queue_size: Optional[int] = async_max_queue_size
        # TODO(rui): consider unify it with async_max_queue_size
        self._max_buffered_results: Optional[int] = max_buffered_results
        if self._max_buffered_results is None:
            self._max_buffered_results = MAX_BUFFER_COUNT
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
        self.has_single_output: bool = False
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

        # Creates the driver actor on the same node as the driver.
        #
        # To support the driver as a reader, the output writer needs to be able to
        # invoke remote functions on the driver (e.g., to create the reader ref, to
        # create a reader ref for a larger object when the channel backing store is
        # resized, etc.). The `_driver_actor` serves as a way for the output writer to
        # invoke remote functions on the driver node.
        self._driver_actor = CompiledDAG.DAGDriverProxyActor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            )
        ).remote()

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

            for arg_idx, arg in enumerate(task.args):
                if not isinstance(arg, DAGNode):
                    continue

                upstream_node_idx = self.dag_node_to_idx[arg]
                upstream_node = self.idx_to_task[upstream_node_idx]
                downstream_actor_handle = None
                if isinstance(task.dag_node, ClassMethodNode):
                    downstream_actor_handle = task.dag_node._get_actor_handle()

                # If the upstream node is an InputAttributeNode, treat the
                # DAG's input node as the actual upstream node
                if isinstance(upstream_node.dag_node, InputAttributeNode):
                    upstream_node = self.idx_to_task[self.input_task_idx]

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
            self.has_single_output = True
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

                def _get_node_id(self):
                    return ray.get_runtime_context().get_node_id()

                if isinstance(readers[0].dag_node, MultiOutputNode):
                    assert len(readers) == 1
                    # This node is a multi-output node, which means that it will only be
                    # read by the driver, not an actor. Thus, we handle this case by
                    # setting `reader_handles` to `[self._driver_actor]`.

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
                    reader_handles = [self._driver_actor]
                else:
                    reader_handles = [
                        reader.dag_node._get_actor_handle() for reader in readers
                    ]
                fn = task.dag_node._get_remote_method("__ray_call__")
                task.output_channel = ray.get(
                    fn.remote(
                        do_allocate_channel,
                        reader_handles,
                        typ=type_hint,
                    )
                )
                actor_handle = task.dag_node._get_actor_handle()
                self.actor_refs.add(actor_handle)
                self.actor_to_tasks[actor_handle].append(task)
            elif isinstance(task.dag_node, InputNode):
                reader_handles_set = set()
                for idx in task.downstream_node_idxs:
                    reader_task = self.idx_to_task[idx]
                    assert isinstance(reader_task.dag_node, ClassMethodNode)
                    reader_handle = reader_task.dag_node._get_actor_handle()
                    reader_handles_set.add(reader_handle)
                task.output_channel = do_allocate_channel(
                    self,
                    list(reader_handles_set),
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
        if self.has_single_output:
            assert len(self.dag_output_channels) == 1
            self.dag_output_channels = self.dag_output_channels[0]

        # Driver should ray.put on input, ray.get/release on output
        self._monitor = self._monitor_failures()
        if self._enable_asyncio:
            self._dag_submitter = AwaitableBackgroundWriter(
                self.dag_input_channel, self._async_max_queue_size
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
    ) -> Any:
        """Repeatedly execute this DAG until the given execution index,
        and buffer all results up to that index. If the DAG has already
        been executed up to the given index, just return the result
        corresponding to the given index.

        Args:
            execution_index: The execution index to execute until.

        Returns:
            The execution result corresponding to the given execution index.

        TODO(rui): catch the case that user holds onto the CompiledDAGRefs
        """
        while self._max_execution_index < execution_index:
            if self._max_execution_index + 1 == execution_index:
                # Directly fetch and return without buffering
                self._max_execution_index += 1
                return self._dag_output_fetcher.read()
            # Otherwise, buffer the result
            if len(self._result_buffer) >= self._max_buffered_results:
                raise ValueError(
                    "Too many buffered results: the allowed max count for "
                    f"buffered results is {self._max_buffered_results}; call ray.get() "
                    "on previous CompiledDAGRefs to free them up from buffer."
                )
            self._max_execution_index += 1
            self._result_buffer[
                self._max_execution_index
            ] = self._dag_output_fetcher.read()

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

        NOTE: Not threadsafe due to _execution_index etc.
        """
        if self._enable_asyncio:
            raise ValueError("Use execute_async if enable_asyncio=True")

        self._get_or_compile()

        inp = (args, kwargs)
        self._dag_submitter.write(inp)

        ref = CompiledDAGRef(self, self._execution_index)
        self._execution_index += 1
        return ref

    async def execute_async(
        self,
        *args,
        **kwargs,
    ) -> AwaitableDAGOutput:
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
        async with self._dag_submission_lock:
            inp = (args, kwargs)
            await self._dag_submitter.write(inp)
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        return AwaitableDAGOutput(fut, self._dag_output_fetcher)

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
    buffer_size_bytes: Optional[int],
    enable_asyncio: bool = False,
    async_max_queue_size: Optional[int] = None,
    max_buffered_results: Optional[int] = None,
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        buffer_size_bytes,
        enable_asyncio,
        async_max_queue_size,
        max_buffered_results,
    )

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag._get_or_compile()
    return compiled_dag
