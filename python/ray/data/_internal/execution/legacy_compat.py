"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""
import logging
from typing import Iterator, Optional

from ray.data._internal.execution.execution_callback import get_execution_callbacks
from ray.data._internal.execution.interfaces import (
    Executor,
    RefBundle,
)
from ray.data._internal.execution.interfaces.executor import OutputIterator
from ray.data._internal.execution.streaming_executor_state import Topology
from ray.data._internal.logical.util import record_operators_usage
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def execute_to_legacy_bundle_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    data_context: DataContext,
) -> Iterator[RefBundle]:
    """Execute a plan with the new executor and return a bundle iterator.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        data_context: The DataContext to use for retrieving execution callbacks.

    Returns:
        The output as a bundle iterator.
    """
    bundle_iter = _execute_dag(
        executor=executor,
        plan=plan,
        preserve_order=False,
        data_context=data_context,
    )

    class CacheMetadataIterator(OutputIterator):
        """Wrapper for `bundle_iterator` above.

        For a given iterator which yields output RefBundles,
        collect the metadata from each output bundle, and yield the
        original RefBundle. Only after the entire iterator is exhausted,
        we cache the resulting metadata to the execution plan."""

        def __init__(
            self,
            base_iterator: OutputIterator,
            topology: "Topology",
            plan: ExecutionPlan,
        ):
            # Note: the base_iterator should be of type StreamIterator,
            # defined within `StreamingExecutor.execute()`. It must
            # support the `get_next()` method.
            self._base_iterator = base_iterator
            self._num_rows = 0
            self._size_bytes = 0

            self._topology = topology
            self._plan = plan

        def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
            try:
                bundle = self._base_iterator.get_next(output_split_idx)
                self._collect_metadata(bundle)
                return bundle
            except StopIteration:
                # Once the iterator is completely exhausted, we are done
                # collecting metadata. We can add this cached metadata to the plan.

                # Traverse the topology backwards and find the first available schema
                schema = next(reversed(self._topology.values()))._schema

                dag = self._plan._logical_plan.dag
                self._plan._cache.set_num_rows(dag, self._num_rows)
                self._plan._cache.set_size_bytes(dag, self._size_bytes)
                self._plan._cache.set_schema(dag, schema)
                raise

        def _collect_metadata(self, bundle: RefBundle) -> RefBundle:
            """Collect the metadata from each output bundle and accumulate
            results, so we can access important information, such as
            row count, schema, etc., after iteration completes."""
            self._num_rows += bundle.num_rows()
            self._size_bytes += bundle.size_bytes()
            return bundle

    return CacheMetadataIterator(bundle_iter, executor._topology, plan)


def execute_to_ref_bundle(
    executor: Executor,
    plan: ExecutionPlan,
    dataset_uuid: str,
    preserve_order: bool,
    data_context: DataContext,
) -> RefBundle:
    """Execute a plan with the new executor and return the output as a RefBundle.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        dataset_uuid: UUID of the dataset for this execution.
        preserve_order: Whether to preserve order in execution.
        data_context: The DataContext to use for retrieving execution callbacks.

    Returns:
        The output as a RefBundle.
    """
    bundles = _execute_dag(
        executor=executor,
        plan=plan,
        preserve_order=preserve_order,
        data_context=data_context,
    )
    ref_bundle = RefBundle.merge_ref_bundles(bundles)
    # Set the stats UUID after execution finishes.
    _set_stats_uuid_recursive(executor.get_stats(), dataset_uuid)
    return ref_bundle


def _execute_dag(
    executor: Executor,
    plan: ExecutionPlan,
    preserve_order: bool,
    data_context: DataContext,
) -> OutputIterator:
    """Execute the optimized physical operators DAG from the plan."""
    from ray.data._internal.logical.optimizers import get_execution_plan

    # Record usage of logical operators.
    record_operators_usage(plan._logical_plan.dag)

    # Get DAG of physical operators and input statistics.
    dag = get_execution_plan(plan._logical_plan).dag
    stats = plan.initial_stats()

    # Enforce to preserve ordering if the plan has operators
    # required to do so, such as Zip and Sort.
    if preserve_order or plan.require_preserve_order():
        executor._options.preserve_order = True

    callbacks = get_execution_callbacks(data_context)
    return executor.execute(dag, initial_stats=stats, callbacks=callbacks)


def _set_stats_uuid_recursive(stats: DatasetStats, dataset_uuid: str) -> None:
    if not stats.dataset_uuid:
        stats.dataset_uuid = dataset_uuid
    for parent in stats.parents or []:
        _set_stats_uuid_recursive(parent, dataset_uuid)
