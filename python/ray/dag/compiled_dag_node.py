import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Union, Optional
import logging
import traceback
import threading

import ray
from ray.exceptions import RayTaskError
from ray.experimental.channel import (
    Channel,
    ReaderInterface,
    SynchronousReader,
    WriterInterface,
    SynchronousWriter,
    AwaitableBackgroundReader,
    AwaitableBackgroundWriter,
)
from ray.util.annotations import DeveloperAPI, PublicAPI


MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB

logger = logging.getLogger(__name__)


@DeveloperAPI
def do_allocate_channel(self, buffer_size_bytes: int, num_readers: int = 1) -> Channel:
    """Generic actor method to allocate an output channel.

    Args:
        buffer_size_bytes: The maximum size of messages in the channel.
        num_readers: The number of readers per message.

    Returns:
        The allocated channel.
    """
    self._output_channel = Channel(buffer_size_bytes, num_readers)
    return self._output_channel


@DeveloperAPI
def do_exec_compiled_task(
    self,
    inputs: List[Union[Any, Channel]],
    actor_method_name: str,
) -> None:
    """Generic actor method to begin executing a compiled DAG. This runs an
    infinite loop to repeatedly read input channel(s), execute the given
    method, and write output channel(s). It only exits if the actor dies or an
    exception is thrown.

    Args:
        inputs: The arguments to the task. Arguments that are not Channels will
            get passed through to the actor method. If the argument is a channel,
            it will be replaced by the value read from the channel before the
            method execute.
        actor_method_name: The name of the actual actor method to execute in
            the loop.
    """
    try:
        method = getattr(self, actor_method_name)

        resolved_inputs = []
        input_channels = []
        input_channel_idxs = []
        # Add placeholders for input channels.
        for idx, inp in enumerate(inputs):
            if isinstance(inp, Channel):
                input_channels.append(inp)
                input_channel_idxs.append(idx)
                resolved_inputs.append(None)
            else:
                resolved_inputs.append(inp)

        self._input_reader: ReaderInterface = SynchronousReader(input_channels)
        self._output_writer: WriterInterface = SynchronousWriter(self._output_channel)
        self._input_reader.start()
        self._output_writer.start()

        while True:
            res = self._input_reader.begin_read()

            for idx, output in zip(input_channel_idxs, res):
                resolved_inputs[idx] = output

            try:
                output_val = method(*resolved_inputs)
            except Exception as exc:
                backtrace = ray._private.utils.format_error_message(
                    "".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    ),
                    task_exception=True,
                )
                wrapped = RayTaskError(
                    function_name="do_exec_compiled_task",
                    traceback_str=backtrace,
                    cause=exc,
                )
                self._output_writer.write(wrapped)
            else:
                self._output_writer.write(output_val)
            finally:
                self._input_reader.end_read()

    except Exception:
        logging.exception("Compiled DAG task exited with exception")
        raise


@DeveloperAPI
def do_cancel_compiled_task(self):
    self._input_reader.close()
    self._output_writer.close()


@PublicAPI(stability="alpha")
class AwaitableDAGOutput:
    def __init__(self, fut: asyncio.Future, ReaderInterface: ReaderInterface):
        self._fut = fut
        self._reader = ReaderInterface

    async def begin_read(self):
        ret = await self._fut
        if isinstance(ret, Exception):
            raise ret
        return ret

    def end_read(self):
        self._reader.end_read()

    async def __aenter__(self):
        ret = await self._fut
        return ret

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.end_read()


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

        self.downstream_node_idxs = set()
        self.output_channel = None

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
class CompiledDAG:
    """Experimental class for accelerated execution.

    This class should not be called directly. Instead, create
    a ray.dag and call experimental_compile().

    See REP https://github.com/ray-project/enhancements/pull/48 for more
    information.
    """

    def __init__(
        self,
        buffer_size_bytes: Optional[int],
        enable_asyncio: bool = False,
        async_max_queue_size: Optional[int] = None,
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
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = MAX_BUFFER_SIZE
        if not isinstance(self._buffer_size_bytes, int) or self._buffer_size_bytes <= 0:
            raise ValueError(
                "`buffer_size_bytes` must be a positive integer, found "
                f"{self._buffer_size_bytes}"
            )

        self._enable_asyncio: bool = enable_asyncio
        self._fut_queue = asyncio.Queue()
        self._async_max_queue_size: Optional[int] = async_max_queue_size
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
        self.dag_input_channel: Optional[Channel] = None
        self.dag_output_channels: Optional[List[Channel]] = None
        self._dag_submitter: Optional[WriterInterface] = None
        self._dag_output_fetcher: Optional[ReaderInterface] = None

        # ObjectRef for each worker's task. The task is an infinite loop that
        # repeatedly executes the method specified in the DAG.
        self.worker_task_refs: List["ray.ObjectRef"] = []
        # Set of actors present in the DAG.
        self.actor_refs = set()

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

        # For each task node, set its upstream and downstream task nodes.
        for idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            if not (
                isinstance(dag_node, InputNode)
                or isinstance(dag_node, MultiOutputNode)
                or isinstance(dag_node, ClassMethodNode)
            ):
                if isinstance(dag_node, InputAttributeNode):
                    # TODO(swang): Support multi args.
                    raise NotImplementedError(
                        "Compiled DAGs currently do not support kwargs or "
                        "multiple args for InputNode"
                    )
                elif isinstance(dag_node, FunctionNode):
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

            for arg in task.args:
                if isinstance(arg, DAGNode):
                    arg_idx = self.dag_node_to_idx[arg]
                    self.idx_to_task[arg_idx].downstream_node_idxs.add(idx)

        for actor_id, task_count in self.actor_task_count.items():
            if task_count > 1:
                raise NotImplementedError(
                    "Compiled DAGs can contain at most one task per actor handle. "
                    f"Actor with ID {actor_id} appears {task_count}x."
                )

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

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
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
        from ray.dag import DAGNode, InputNode, MultiOutputNode, ClassMethodNode

        if self.input_task_idx is None:
            self._preprocess()

        if self._dag_submitter is not None:
            assert self._dag_output_fetcher is not None
            return

        frontier = [self.input_task_idx]
        visited = set()
        # Create output buffers
        while frontier:
            cur_idx = frontier.pop(0)
            if cur_idx in visited:
                continue
            visited.add(cur_idx)

            task = self.idx_to_task[cur_idx]
            # Create an output buffer on the actor.
            assert task.output_channel is None
            if isinstance(task.dag_node, ClassMethodNode):
                fn = task.dag_node._get_remote_method("__ray_call__")
                task.output_channel = ray.get(
                    fn.remote(
                        do_allocate_channel,
                        buffer_size_bytes=self._buffer_size_bytes,
                        num_readers=task.num_readers,
                    )
                )
                self.actor_refs.add(task.dag_node._get_actor_handle())
            elif isinstance(task.dag_node, InputNode):
                task.output_channel = Channel(
                    buffer_size_bytes=self._buffer_size_bytes,
                    num_readers=task.num_readers,
                )
            else:
                assert isinstance(task.dag_node, MultiOutputNode)

            for idx in task.downstream_node_idxs:
                frontier.append(idx)

        for node_idx, task in self.idx_to_task.items():
            if node_idx == self.input_task_idx:
                # We don't need to assign an actual task for the input node.
                continue

            if node_idx == self.output_task_idx:
                # We don't need to assign an actual task for the input node.
                continue

            resolved_args = []
            has_at_least_one_channel_input = False
            for arg in task.args:
                if isinstance(arg, DAGNode):
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

            # Assign the task with the correct input and output buffers.
            worker_fn = task.dag_node._get_remote_method("__ray_call__")
            self.worker_task_refs.append(
                worker_fn.options(concurrency_group="_ray_system").remote(
                    do_exec_compiled_task,
                    resolved_args,
                    task.dag_node.get_method_name(),
                )
            )

        self.dag_input_channel = self.idx_to_task[self.input_task_idx].output_channel

        self.dag_output_channels = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            self.dag_output_channels.append(self.idx_to_task[output_idx].output_channel)

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
        return

    def _monitor_failures(self):
        outer = self

        class Monitor(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True)
                self.in_teardown = False

            def teardown(self):
                if self.in_teardown:
                    return
                logger.info("Tearing down compiled DAG")

                outer._dag_submitter.close()
                outer._dag_output_fetcher.close()

                self.in_teardown = True
                for actor in outer.actor_refs:
                    logger.info(f"Cancelling compiled worker on actor: {actor}")
                    # TODO(swang): Suppress exceptions from actors trying to
                    # read closed channels when DAG is being torn down.
                    try:
                        ray.get(actor.__ray_call__.remote(do_cancel_compiled_task))
                    except Exception:
                        logger.exception("Error cancelling worker task")
                        pass
                logger.info("Waiting for worker tasks to exit")
                for ref in outer.worker_task_refs:
                    try:
                        ray.get(ref)
                    except Exception:
                        pass
                logger.info("Teardown complete")

            def run(self):
                try:
                    ray.get(outer.worker_task_refs)
                except Exception as e:
                    logger.debug(f"Handling exception from worker tasks: {e}")
                    if self.in_teardown:
                        return
                    for output_channel in outer.dag_output_channels:
                        output_channel.close()
                    self.teardown()

        monitor = Monitor()
        monitor.start()
        return monitor

    def execute(
        self,
        *args,
        **kwargs,
    ) -> Union[Channel, List[Channel]]:
        """Execute this DAG using the compiled execution path.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode. Not supported yet.

        Returns:
            A list of Channels that can be used to read the DAG result.
        """
        # These errors should already be caught during compilation, but just in
        # case.
        if len(args) != 1:
            raise NotImplementedError("Compiled DAGs support exactly one InputNode arg")
        if len(kwargs) != 0:
            raise NotImplementedError("Compiled DAGs do not support kwargs")

        if self._enable_asyncio:
            raise ValueError("Use execute_async if enable_asyncio=True")

        self._get_or_compile()
        self._dag_submitter.write(args[0])

        return self._dag_output_fetcher

    async def execute_async(
        self,
        *args,
        **kwargs,
    ) -> AwaitableDAGOutput:
        """Execute this DAG using the compiled execution path.

        NOTE: Not threadsafe.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode. Not supported yet.

        Returns:
            A list of Channels that can be used to read the DAG result.
        """
        # These errors should already be caught during compilation, but just in
        # case.
        if len(args) != 1:
            raise NotImplementedError("Compiled DAGs support exactly one InputNode arg")
        if len(kwargs) != 0:
            raise NotImplementedError("Compiled DAGs do not support kwargs")

        if not self._enable_asyncio:
            raise ValueError("Use execute if enable_asyncio=False")

        self._get_or_compile()
        async with self._dag_submission_lock:
            await self._dag_submitter.write(args[0])
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        return AwaitableDAGOutput(fut, self._dag_output_fetcher)

    def teardown(self):
        """Teardown and cancel all worker tasks for this DAG."""
        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            monitor.teardown()

    def __del__(self):
        self.teardown()


@DeveloperAPI
def build_compiled_dag_from_ray_dag(
    dag: "ray.dag.DAGNode",
    buffer_size_bytes: Optional[int],
    enable_asyncio: bool = False,
    async_max_queue_size: Optional[int] = None,
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        buffer_size_bytes,
        enable_asyncio,
        async_max_queue_size,
    )

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag._get_or_compile()
    return compiled_dag
