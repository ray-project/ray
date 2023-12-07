from typing import Any, List, Tuple, Union

import ray
import ray.experimental.channel as ray_channel


MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB

ChannelType = "ray.experimental.channel.Channel"


def allocate_channel(buffer_size_bytes: int = MAX_BUFFER_SIZE, num_readers: int = 1):
    if not isinstance(buffer_size_bytes, int):
        raise ValueError("buffer_size_bytes must be an integer")
    if not isinstance(num_readers, int):
        raise ValueError("num_readers must be an integer")

    return ray_channel.Channel(buffer_size_bytes, num_readers)


def do_allocate_channel(
    self, buffer_size_bytes: int = MAX_BUFFER_SIZE, num_readers: int = 1
):
    self._output_channel = allocate_channel(buffer_size_bytes)
    return self._output_channel


def do_exec_compiled_task(
    self,
    inputs: List[Union[Any, "ray_channel.Channel"]],
    actor_method_name: str,
):
    try:
        method = getattr(self, actor_method_name)

        resolved_inputs = []
        input_channel_idxs = []
        # Add placeholders for input channels.
        for inp in inputs:
            if isinstance(inp, ray_channel.Channel):
                input_channel_idxs.append((len(resolved_inputs), inp))
                resolved_inputs.append(None)
            else:
                resolved_inputs.append(inp)

        while True:
            for idx, chan in input_channel_idxs:
                resolved_inputs[idx] = chan.begin_read()

            output_val = method(*resolved_inputs)

            self._output_channel.write(output_val)
            for _, chan in input_channel_idxs:
                chan.end_read()

    except Exception as e:
        print("Task aborted", e)
        raise


class CompiledTask:
    """Wraps the normal Ray DAGNode with some metadata."""

    def __init__(self, idx, dag_node: "ray.dag.DAGNode"):
        self.idx = idx
        self.dag_node = dag_node

        self.args = []
        self.dependent_node_idxs = set()
        self.output_channel = None

    @property
    def num_readers(self):
        return len(self.dependent_node_idxs)

    def __str__(self):
        return f"""
Node: {self.dag_node}
Arguments: {self.args}
Output: {self.output_channel}
"""


class CompiledDAG:
    def __init__(self):
        # idx -> CompiledTask.
        self.idx_to_task = {}
        # DAGNode -> idx.
        self.dag_node_to_idx = {}
        # idx counter.
        self.counter = 0

        # Attributes that are set during preprocessing.
        # Preprocessing identifies the input node and output node.
        self.input_task_idx = None
        self.output_task_idx = None
        self.has_single_output = False

        # Cached attributes that are set during compilation.
        self.dag_input_channel = None
        self.dag_output_channels = None
        self.node_idx_to_output_channels = {}
        self.worker_task_refs = []

    def _add_node(self, node):
        idx = self.counter
        self.idx_to_task[idx] = CompiledTask(idx, node)
        self.dag_node_to_idx[node] = idx
        self.counter += 1

    def _preprocess(self):
        """Before compiling, preprocess the DAG to build an index from task to
        upstream and downstream tasks, and to set the input and output node(s)
        of the DAG.
        """
        from ray.dag import (
            DAGNode,
            ClassMethodNode,
            FunctionNode,
            InputAttributeNode,
            InputNode,
            OutputNode,
        )

        # For each task node, set its upstream and downstream task nodes.
        for idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            if not (
                isinstance(dag_node, InputNode)
                or isinstance(dag_node, OutputNode)
                or isinstance(dag_node, ClassMethodNode)
            ):
                if isinstance(dag_node, InputAttributeNode):
                    # TODO(swang): Support multi args.
                    raise ValueError(
                        "Compiled DAGs currently do not support kwargs or "
                        "multiple args for InputNode"
                    )
                elif isinstance(dag_node, FunctionNode):
                    # TODO(swang): Support non-actor tasks.
                    raise ValueError(
                        "Compiled DAGs currently only support actor method nodes"
                    )
                else:
                    raise ValueError(
                        f"Found unsupported node of type {type(task.dag_node)}"
                    )

            task.args = task.dag_node.get_args()
            for arg in task.args:
                if isinstance(arg, DAGNode):
                    arg_idx = self.dag_node_to_idx[arg]
                    self.idx_to_task[arg_idx].dependent_node_idxs.add(idx)

        # Find the input node to the DAG.
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "more than one InputNode found"
                self.input_task_idx = idx
        # TODO: Support no-input DAGs (use an empty object to signal).
        if self.input_task_idx is None:
            raise ValueError("Compiled DAGs currently require exactly one InputNode")

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
            if len(task.dependent_node_idxs) == 0:
                assert self.output_task_idx is None, "More than one output node found"
                self.output_task_idx = idx

        assert self.output_task_idx is not None
        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # Add an OutputNode to the end of the DAG if it's not already there.
        if not isinstance(output_node, OutputNode):
            self.has_single_output = True
            output_node = OutputNode([output_node])
            self._add_node(output_node)
            self.output_task_idx = self.dag_node_to_idx[output_node]
            # Preprocess one more time so that we have the right output node
            # now.
            self.input_task_idx, self.output_task_idx = None, None
            self._preprocess()

    def _compile(self) -> Tuple[ChannelType, Union[ChannelType, List[ChannelType]]]:
        """ """
        from ray.dag import DAGNode, InputNode, OutputNode, ClassMethodNode

        if self.input_task_idx is None:
            self._preprocess()

        if self.dag_input_channel is not None and self.dag_output_channels is not None:
            # Driver should ray.put on input, ray.get/release on output
            return (
                self.dag_input_channel,
                self.dag_output_channels,
            )

        queue = [self.input_task_idx]
        visited = set()
        # Create output buffers
        while queue:
            cur_idx = queue.pop(0)
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
                        num_readers=task.num_readers,
                    )
                )
            elif isinstance(task.dag_node, InputNode):
                task.output_channel = allocate_channel(num_readers=task.num_readers)
            else:
                assert isinstance(task.dag_node, OutputNode)

            for idx in task.dependent_node_idxs:
                queue.append(idx)

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
                worker_fn.remote(
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
        # If no OutputNode was specified during the DAG creation, there is only
        # one output. Return a single output channel instead of a list of
        # channels.
        if self.has_single_output:
            assert len(self.dag_output_channels) == 1
            self.dag_output_channels = self.dag_output_channels[0]

        # Driver should ray.put on input, ray.get/release on output
        return (self.dag_input_channel, self.dag_output_channels)

    def execute(
        self,
        *args,
        **kwargs,
    ) -> Union[ChannelType, List[ChannelType]]:
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
            raise ValueError("Compiled DAGs support exactly one InputNode arg")
        if len(kwargs) != 0:
            raise ValueError("Compiled DAGs do not support kwargs")

        input_channel, output_channels = self._compile()
        input_channel.write(args[0])
        return output_channels


def build_compiled_dag_from_ray_dag(dag: "ray.dag.DAGNode"):
    compiled_dag = CompiledDAG()

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag._compile()
    return compiled_dag
