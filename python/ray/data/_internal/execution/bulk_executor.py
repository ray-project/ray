from typing import Dict, List, Iterator

import ray
from ray.data._internal.execution.interfaces import (
    Executor,
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats


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

            # Fully execute this operator.
            for i, ref_bundles in enumerate(inputs):
                for r in ref_bundles:
                    node.add_input(r, input_index=i)
                node.inputs_done(i)
            output = _naive_run_until_complete(node)
            node.release_unused_resources()

            # Cache and return output.
            saved_outputs[node] = output
            return output

        return execute_recursive(dag)

    def get_stats() -> DatasetStats:
        raise NotImplementedError


def _naive_run_until_complete(node: PhysicalOperator) -> List[RefBundle]:
    """Run this operator until completion, assuming all inputs have been submitted.

    Args:
        node: The operator to run.

    Returns:
        The list of output ref bundles for the operator.
    """
    output = []
    tasks = node.get_tasks()
    if tasks:
        bar = ProgressBar(node.name, total=node.num_outputs_total())
        while tasks:
            [ready], remaining = ray.wait(tasks, num_returns=1, fetch_local=True)
            node.notify_task_completed(ready)
            tasks = node.get_tasks()
            while node.has_next():
                bar.update(1)
                output.append(node.get_next())
        bar.close()
    while node.has_next():
        output.append(node.get_next())
    return output
