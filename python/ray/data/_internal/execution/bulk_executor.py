from typing import Dict, List, Iterator

import ray
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data._internal.execution.interfaces import (
    Executor,
    RefBundle,
    PhysicalOperator,
    OneToOneOperator,
    AllToAllOperator,
    BufferOperator,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats


@ray.remote(num_returns=2)
def _transform_one(op: OneToOneOperator, block: Block) -> (Block, BlockMetadata):
    print("Processing", block)
    import time

    time.sleep(1)
    [out] = list(op.execute_one([block], {}))
    return out, BlockAccessor.for_block(out).get_metadata([], None)


def _naive_task_execute(
    inputs: List[RefBundle], op: OneToOneOperator
) -> List[RefBundle]:
    """Naively execute a 1:1 operation using Ray tasks.

    TODO: This should be reconciled with ComputeStrategy.
    """

    input_blocks = []
    for bundle in inputs:
        for block, _ in bundle.blocks:
            input_blocks.append(block)

    out_blocks, out_meta = [], []
    for in_b in input_blocks:
        out_b, out_m = _transform_one.remote(op, in_b)
        out_blocks.append(out_b)
        out_meta.append(out_m)

    bar = ProgressBar("OneToOne", total=len(out_meta))
    out_meta = bar.fetch_until_complete(out_meta)

    return [RefBundle([(b, m)]) for b, m in zip(out_blocks, out_meta)]


class BulkExecutor(Executor):
    def execute(self, dag: PhysicalOperator) -> Iterator[RefBundle]:
        """Synchronously executes the DAG via bottom-up recursive traversal.

        TODO: optimize memory usage by deleting intermediate results and marking
        the `owned` field in the ref bundles correctly.
        """

        saved_outputs: Dict[PhysicalOperator, List[RefBundle]] = {}

        def execute_recursive(node: PhysicalOperator) -> List[RefBundle]:
            # Avoid duplicate executions.
            if node in saved_outputs:
                return saved_outputs[node]

            # Compute dependencies.
            inputs = [execute_recursive(dep) for dep in node.input_dependencies]

            # Execute this operator (3 cases).
            output = []
            if isinstance(node, OneToOneOperator):
                assert len(inputs) == 1, "OneToOne takes exactly 1 input stream"
                output = _naive_task_execute(inputs[0], node)
            elif isinstance(node, AllToAllOperator):
                assert len(inputs) == 1, "AllToAll takes exactly 1 input stream"
                output = node.execute_all(inputs[0])
            elif isinstance(node, BufferOperator):
                for i, ref_bundles in enumerate(inputs):
                    for r in ref_bundles:
                        node.add_input(r, input_index=i)
                    node.inputs_done(i)
                while node.has_next():
                    output.append(node.get_next())
            else:
                assert False, "Unknown operator type: {}".format(node)

            # Cache and return output.
            saved_outputs[node] = output
            return output

        return execute_recursive(dag)

    def get_stats() -> DatasetStats:
        raise NotImplementedError
