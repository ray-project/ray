"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""

from typing import Iterator, Optional, Tuple

from ray.data._internal.block_list import BlockList
from ray.data._internal.execution.interfaces import (
    Executor,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.executor import OutputIterator
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.util import record_operators_usage
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef

# Warn about tasks larger than this.
TASK_SIZE_WARN_THRESHOLD_BYTES = 100000


def execute_to_legacy_block_iterator(
    executor: Executor,
    plan: ExecutionPlan,
) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
    """Same as execute_to_legacy_bundle_iterator but returning blocks and metadata."""
    bundle_iter = execute_to_legacy_bundle_iterator(executor, plan)
    for bundle in bundle_iter:
        for block, metadata in bundle.blocks:
            yield block, metadata


def execute_to_legacy_bundle_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    dag_rewrite=None,
) -> Iterator[RefBundle]:
    """Execute a plan with the new executor and return a bundle iterator.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        dag_rewrite: Callback that can be used to mutate the DAG prior to execution.
            This is currently used as a legacy hack to inject the OutputSplit operator
            for `Dataset.streaming_split()`.

    Returns:
        The output as a bundle iterator.
    """
    dag, stats = _get_execution_dag(
        executor,
        plan,
        preserve_order=False,
    )
    if dag_rewrite:
        dag = dag_rewrite(dag)

    bundle_iter = executor.execute(dag, initial_stats=stats)

    class CacheMetadataIterator(OutputIterator):
        """Wrapper for `bundle_iterator` above.

        For a given iterator which yields output RefBundles,
        cache the metadata from each output bundle, and yield
        the original RefBundle."""

        def __init__(self, base_iterator: OutputIterator):
            # Note: the base_iterator should be of type StreamIterator,
            # defined within `StreamingExecutor.execute()`. It must
            # support the `get_next()` method.
            self._base_iterator = base_iterator

        def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
            bundle = self._base_iterator.get_next(output_split_idx)
            self._cache_metadata(bundle)
            return bundle

        def _cache_metadata(self, bundle: RefBundle) -> RefBundle:
            """Cache the metadata from each output bundle, so we can
            access important information, such as row count, schema, etc."""
            if not plan._snapshot_metadata:
                # Initialize the snapshot BlockMetadata.
                plan._snapshot_metadata = BlockMetadata(
                    num_rows=bundle.num_rows(),
                    size_bytes=bundle.size_bytes(),
                    schema=unify_block_metadata_schema(bundle.metadata),
                    input_files=None,
                    exec_stats=None,
                )
            else:
                # Update the snapshot BlockMetadata.
                snap_md = plan._snapshot_metadata
                snap_md.num_rows += bundle.num_rows()
                snap_md.size_bytes += bundle.size_bytes()
                snap_md.schema = unify_block_metadata_schema(
                    [snap_md, *bundle.metadata]
                )
            return bundle

    bundle_iter = CacheMetadataIterator(bundle_iter)
    return bundle_iter


def execute_to_legacy_block_list(
    executor: Executor,
    plan: ExecutionPlan,
    dataset_uuid: str,
    preserve_order: bool,
) -> BlockList:
    """Execute a plan with the new executor and translate it into a legacy block list.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        dataset_uuid: UUID of the dataset for this execution.
        preserve_order: Whether to preserve order in execution.

    Returns:
        The output as a legacy block list.
    """
    dag, stats = _get_execution_dag(
        executor,
        plan,
        preserve_order,
    )
    bundles = executor.execute(dag, initial_stats=stats)
    block_list = _bundles_to_block_list(bundles)
    # Set the stats UUID after execution finishes.
    _set_stats_uuid_recursive(executor.get_stats(), dataset_uuid)
    return block_list


def _get_execution_dag(
    executor: Executor,
    plan: ExecutionPlan,
    preserve_order: bool,
) -> Tuple[PhysicalOperator, DatasetStats]:
    """Get the physical operators DAG from a plan."""
    # Record usage of logical operators if available.
    if hasattr(plan, "_logical_plan") and plan._logical_plan is not None:
        record_operators_usage(plan._logical_plan.dag)

    # Get DAG of physical operators and input statistics.
    dag = get_execution_plan(plan._logical_plan).dag
    stats = _get_initial_stats_from_plan(plan)

    # Enforce to preserve ordering if the plan has operators
    # required to do so, such as Zip and Sort.
    if preserve_order or plan.require_preserve_order():
        executor._options.preserve_order = True

    return dag, stats


def _get_initial_stats_from_plan(plan: ExecutionPlan) -> DatasetStats:
    if plan._snapshot_bundle is not None:
        return plan._snapshot_stats
    # For Datasets created from "read_xxx", `plan._in_stats` contains useless data.
    # For Datasets created from "from_xxx", we need to use `plan._in_stats` as
    # the initial stats. Because the `FromXxx` logical operators will be translated to
    # "InputDataBuffer" physical operators, which will be ignored when generating
    # stats, see `StreamingExecutor._generate_stats`.
    # TODO(hchen): Unify the logic by saving the initial stats in `InputDataBuffer
    if plan.has_lazy_input():
        return DatasetStats(metadata={}, parent=None)
    else:
        return plan._in_stats


def _bundles_to_block_list(bundles: Iterator[RefBundle]) -> BlockList:
    blocks, metadata = [], []
    owns_blocks = True
    for ref_bundle in bundles:
        if not ref_bundle.owns_blocks:
            owns_blocks = False
        blocks.extend(ref_bundle.block_refs)
        metadata.extend(ref_bundle.metadata)
    return BlockList(blocks, metadata, owned_by_consumer=owns_blocks)


def _set_stats_uuid_recursive(stats: DatasetStats, dataset_uuid: str) -> None:
    if not stats.dataset_uuid:
        stats.dataset_uuid = dataset_uuid
    for parent in stats.parents or []:
        _set_stats_uuid_recursive(parent, dataset_uuid)
