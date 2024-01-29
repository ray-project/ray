"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""

from typing import Iterator, Tuple

from ray.data._internal.block_list import BlockList
from ray.data._internal.execution.interfaces import (
    Executor,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.rules.set_read_parallelism import (
    compute_additional_split_factor,
)
from ray.data._internal.logical.util import record_operators_usage
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.planner.plan_read_op import (
    apply_output_blocks_handling_to_read_task,
)
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockMetadata, List
from ray.data.context import DataContext
from ray.types import ObjectRef

# Warn about tasks larger than this.
TASK_SIZE_WARN_THRESHOLD_BYTES = 100000


def execute_to_legacy_block_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
    """Same as execute_to_legacy_bundle_iterator but returning blocks and metadata."""
    bundle_iter = execute_to_legacy_bundle_iterator(
        executor, plan, allow_clear_input_blocks, dataset_uuid
    )
    for bundle in bundle_iter:
        for block, metadata in bundle.blocks:
            yield block, metadata


def execute_to_legacy_bundle_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
    dag_rewrite=None,
) -> Iterator[RefBundle]:
    """Execute a plan with the new executor and return a bundle iterator.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
        dataset_uuid: UUID of the dataset for this execution.
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
    return bundle_iter


def execute_to_legacy_block_list(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
    preserve_order: bool,
) -> BlockList:
    """Execute a plan with the new executor and translate it into a legacy block list.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
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


def get_legacy_lazy_block_list_read_only(
    plan: ExecutionPlan,
) -> LazyBlockList:
    """For a read-only plan, construct a LazyBlockList with ReadTasks from the
    input Datasource or Reader. Note that the plan and the underlying ReadTasks
    are not executed, only their known metadata is fetched.

    Args:
        plan: The legacy plan to execute.

    Returns:
        The output as a legacy LazyBlockList.
    """
    assert plan.is_read_only(), "This function only supports read-only plans."
    assert isinstance(plan._logical_plan, LogicalPlan)
    read_logical_op = plan._logical_plan.dag
    assert isinstance(read_logical_op, Read)

    # In the full dataset execution, the logic in ApplyAdditionalSplitToOutputBlocks
    # is normally executed as part of the MapOperator created in the
    # LogicalPlan -> PhysicalPlan plan translation. In this case, since we
    # get the ReadTasks directly from the Datasource or Reader,
    # we need to manually apply this logic in order to update the ReadTasks.
    ctx = DataContext.get_current()
    (parallelism, _, estimated_num_blocks, k,) = compute_additional_split_factor(
        read_logical_op._datasource_or_legacy_reader,
        read_logical_op._parallelism,
        read_logical_op._mem_size,
        ctx.target_max_block_size,
        cur_additional_split_factor=None,
    )
    read_tasks = read_logical_op._datasource_or_legacy_reader.get_read_tasks(
        parallelism
    )
    for read_task in read_tasks:
        apply_output_blocks_handling_to_read_task(read_task, k)

    block_list = LazyBlockList(
        read_tasks,
        read_logical_op.name,
        ray_remote_args=read_logical_op._ray_remote_args,
        owned_by_consumer=False,
    )
    # Update the estimated number of blocks after applying optimizations
    # and fetching metadata (e.g. SetReadParallelismRule).
    block_list._estimated_num_blocks = estimated_num_blocks
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
    if plan._snapshot_blocks is not None and not plan._snapshot_blocks.is_cleared():
        return plan._snapshot_stats
    # For Datasets created from "read_xxx", `plan._in_blocks` is a LazyBlockList,
    # and `plan._in_stats` contains useless data.
    # For Datasets created from "from_xxx", we need to use `plan._in_stats` as
    # the initial stats. Because the `FromXxx` logical operators will be translated to
    # "InputDataBuffer" physical operators, which will be ignored when generating
    # stats, see `StreamingExecutor._generate_stats`.
    # TODO(hchen): Unify the logic by saving the initial stats in `InputDataBuffer
    if isinstance(plan._in_blocks, LazyBlockList):
        return DatasetStats(metadata={}, parent=None)
    else:
        return plan._in_stats


def _bundles_to_block_list(bundles: Iterator[RefBundle]) -> BlockList:
    blocks, metadata = [], []
    owns_blocks = True
    for ref_bundle in bundles:
        if not ref_bundle.owns_blocks:
            owns_blocks = False
        for block, meta in ref_bundle.blocks:
            blocks.append(block)
            metadata.append(meta)
    return BlockList(blocks, metadata, owned_by_consumer=owns_blocks)


def _block_list_to_bundles(blocks: BlockList, owns_blocks: bool) -> List[RefBundle]:
    output = []
    for block, meta in blocks.iter_blocks_with_metadata():
        output.append(
            RefBundle(
                [
                    (
                        block,
                        meta,
                    )
                ],
                owns_blocks=owns_blocks,
            )
        )
    return output


def _set_stats_uuid_recursive(stats: DatasetStats, dataset_uuid: str) -> None:
    if not stats.dataset_uuid:
        stats.dataset_uuid = dataset_uuid
    for parent in stats.parents or []:
        _set_stats_uuid_recursive(parent, dataset_uuid)
