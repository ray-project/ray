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

        def execute_recursive(op: PhysicalOperator) -> List[RefBundle]:
            # Avoid duplicate executions.
            if op in saved_outputs:
                return saved_outputs[op]

            # Compute dependencies.
            inputs = [execute_recursive(dep) for dep in op.input_dependencies]

            # Fully execute this operator.
            logger.debug("Executing op %s", op.name)
            builder = self._stats.child_builder(op.name)
            try:
                for i, ref_bundles in enumerate(inputs):
                    for r in ref_bundles:
                        op.add_input(r, input_index=i)
                    op.inputs_done(i)
                output = _naive_run_until_complete(op)
            finally:
                op.shutdown()

            # Cache and return output.
            saved_outputs[op] = output
            op_stats = op.get_stats()
            op_metrics = op.get_metrics()
            if op_stats:
                self._stats = builder.build_multistage(op_stats)
                self._stats.extra_metrics = op_metrics
            return output

        return execute_recursive(dag)

    def get_stats(self) -> DatasetStats:
        assert self._stats is not None, self._stats
        return self._stats


def _naive_run_until_complete(op: PhysicalOperator) -> List[RefBundle]:
    """Run this operator until completion, assuming all inputs have been submitted.

    Args:
        op: The operator to run.

    Returns:
        The list of output ref bundles for the operator.
    """
    output = []
    tasks = op.get_work_refs()
    if tasks:
        bar = ProgressBar(op.name, total=op.num_outputs_total())
        while tasks:
            done, _ = ray.wait(tasks, fetch_local=True, timeout=0.1)
            for ready in done:
                op.notify_work_completed(ready)
            tasks = op.get_work_refs()
            while op.has_next():
                bar.update(1)
                output.append(op.get_next())
        bar.close()
    # An operator is finished only after it has no remaining work as well as no
    # remaining outputs.
    while op.has_next():
        output.append(op.get_next())
    return output
