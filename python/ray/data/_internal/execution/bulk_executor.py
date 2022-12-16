import logging
from typing import Dict, List, Iterator, Optional

import ray
from ray.data._internal.execution.interfaces import (
    Executor,
    ExecutionOptions,
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats

logger = logging.getLogger(__name__)


class BulkExecutor(Executor):
    def __init__(self, options: ExecutionOptions):
        super().__init__(options)
        self._stats = DatasetStats(stages={}, parent=None)
        self._executed = False

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterator[RefBundle]:
        """Synchronously executes the DAG via bottom-up recursive traversal."""

        assert not self._executed, "Can only call execute once."
        self._executed = True
        if not isinstance(dag, InputDataBuffer):
            logger.info("Executing DAG %s", dag)

        if initial_stats:
            self._stats = initial_stats

        saved_outputs: Dict[PhysicalOperator, List[RefBundle]] = {}

        def execute_recursive(node: PhysicalOperator) -> List[RefBundle]:
            # Avoid duplicate executions.
            if node in saved_outputs:
                return saved_outputs[node]

            # Compute dependencies.
            inputs = [execute_recursive(dep) for dep in node.input_dependencies]

            # Fully execute this operator.
            logger.debug("Executing node %s", node.name)
            builder = self._stats.child_builder(node.name)
            for i, ref_bundles in enumerate(inputs):
                for r in ref_bundles:
                    node.add_input(r, input_index=i)
                node.inputs_done(i)
            output = _naive_run_until_complete(node)
            node.release_unused_resources()

            # Cache and return output.
            saved_outputs[node] = output
            node_stats = node.get_stats()
            node_metrics = node.get_metrics()
            if node_stats:
                self._stats = builder.build_multistage(node_stats)
                self._stats.extra_metrics = node_metrics
            return output

        return execute_recursive(dag)

    def get_stats(self) -> DatasetStats:
        assert self._stats is not None, self._stats
        return self._stats


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
            done, _ = ray.wait(tasks, fetch_local=True, timeout=0.1)
            for ready in done:
                node.notify_task_completed(ready)
            tasks = node.get_tasks()
            while node.has_next():
                bar.update(1)
                output.append(node.get_next())
        bar.close()
    while node.has_next():
        output.append(node.get_next())
    return output
