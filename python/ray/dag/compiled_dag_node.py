from typing import List
from collections import defaultdict

import ray


class CompiledTask:
    """ Wraps the normal Ray DAGNode with some metadata."""
    def __init__(self, idx, dag_node: "DAGNode"):
        self.idx = idx
        self.dag_node = dag_node

        self.args = []
        self.dependent_node_idxs = []
        self.output_ref = None

    @property
    def max_readers(self):
        return len(self.dependent_node_idxs)


class CompiledDAG:
    def __init__(self):
        # idx -> CompiledTask.
        self.idx_to_task = {}
        # DAGNode -> idx.
        self.dag_node_to_idx = {}
        # idx counter.
        self.counter = 0

        self.input_task_idx = None
        self.output_task_idx = None
        self.node_idx_to_output_refs = {}

        # Cached.
        self.dag_input_ref = None
        self.dag_input_max_readers = None
        self.dag_output_refs = None

    def add_node(self, node):
        idx = self.counter
        self.idx_to_task[idx] = CompiledTask(idx, node)
        self.dag_node_to_idx[node] = idx
        self.counter += 1

    def preprocess(self):
        from ray.dag import DAGNode, InputNode, OutputNode

        for idx, task in self.idx_to_task.items():
            task.args = task.dag_node.get_args()
            for arg in task.args:
                if isinstance(arg, DAGNode):
                    arg_idx = self.dag_node_to_idx[arg]
                    self.idx_to_task[arg_idx].dependent_node_idxs.append(idx)
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "more than one InputNode found"
                self.input_task_idx = idx
        # TODO: Support no-input DAGs (use an empty object to signal).
        assert self.input_task_idx is not None, "no InputNode found, require exactly one"

        for idx, task in self.idx_to_task.items():
            if len(task.dependent_node_idxs) == 0:
                assert self.output_task_idx is None, "More than one output node found, make sure only one node has 0 dependent tasks"
                self.output_task_idx = idx

    def compile(self):
        from ray.dag import DAGNode, InputNode, OutputNode

        if self.dag_input_ref is not None and self.dag_output_refs is not None:
            # Driver should ray.put on input, ray.get/release on output
            return (
                    self.dag_input_ref,
                    self.dag_input_max_readers,
                    self.dag_output_refs)

        queue = [self.input_task_idx]
        visited = set()
        # Create output buffers
        while queue:
            cur_idx = queue.pop(0)
            if cur_idx in visited:
                continue
            visited.add(cur_idx)

            task = self.idx_to_task[cur_idx]
            dependent_node_idxs = task.dependent_node_idxs

            # TODO: Create an output buffer on cur_idx actor with max_readers =
            # len(dependent_node_idxs).
            assert task.output_ref is None
            task.output_ref = ray.put(1)

            for idx in dependent_node_idxs:
                queue.append(idx)

        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # TODO: Add an OutputNode to the end of the DAG if
        # it's not already there.
        assert isinstance(output_node, OutputNode)

        for node_idx, task in self.idx_to_task.items():
            if node_idx == self.input_task_idx:
                # We don't need to assign an actual task for the input node.
                continue

            resolved_args = []
            for arg in task.args:
                if isinstance(arg, DAGNode):
                    arg_idx = self.dag_node_to_idx[arg]
                    arg_buffer = self.idx_to_task[arg_idx].output_ref
                    assert arg_buffer is not None
                    resolved_args.append(arg_buffer)
                else:
                    resolved_args.append(arg)
                print("exec", node_idx, "args:", resolved_args)

            # TODO: Assign the task with the correct input and output buffers.

        self.dag_input_ref = self.idx_to_task[self.input_task_idx].output_ref
        self.dag_input_max_readers = self.idx_to_task[self.input_task_idx].max_readers

        self.dag_output_refs = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            self.dag_output_refs.append(self.idx_to_task[output_idx].output_ref)

        assert self.dag_input_ref
        assert self.dag_output_refs
        # Driver should ray.put on input, ray.get/release on output
        return (
                self.dag_input_ref,
                self.dag_input_max_readers,
                self.dag_output_refs)


def build_compiled_dag(dag: "DAGNode"):
    compiled_dag = CompiledDAG()

    def build_compiled_dag(node):
        compiled_dag.add_node(node)
        return node

    dag.apply_recursive(build_compiled_dag)
    compiled_dag.preprocess()
    return compiled_dag

class RayCompiledExecutor:
    def __init__(self):
        pass

    def _exec_compiled_task(self, task: "CompiledTask"):
        input_refs = [inp.get_object_ref() for inp in task.inputs]
        outputs = [(out.get_object_ref(), out.get_max_readers()) for out in task.outputs]
        method = getattr(self, task.actor_method_name)
        while True:
            inputs = ray.get(input_refs)
            output_vals = method(*inputs)
            for output_val, output in zip(output_vals, outputs):
                output_ref, max_readers = output
                ray.worker.global_worker.put_object(
                    output_val, object_ref=output_ref, max_readers=max_readers
                )
            for input_ref in input_refs:
                ray.release(input_ref)
