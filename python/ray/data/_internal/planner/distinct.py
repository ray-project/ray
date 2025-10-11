from typing import List, Optional

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.util import unify_ref_bundles_schema
from ray.data.context import DataContext, ShuffleStrategy


def generate_distinct_fn(
    key: Optional[List[str]],
    data_context: DataContext,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to remove duplicate rows by the specified key columns.

    Args:
        key: List of column names to consider for identifying duplicates.
            If None, all columns are used.
        data_context: The data context configuration.
        _debug_limit_shuffle_execution_to_num_blocks: Debug parameter to limit shuffle.

    Returns:
        A transform function that performs deduplication.
    """

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> AllToAllTransformFnResult:
        blocks = []
        metadata = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
            metadata.extend(ref_bundle.metadata)
        if len(blocks) == 0:
            return (blocks, {})

        unified_schema = unify_ref_bundles_schema(refs)

        # If key is None, use all columns
        if key is None:
            key_columns = list(unified_schema.names)
        else:
            key_columns = key

        num_mappers = len(blocks)

        sort_key = SortKey(key_columns)

        # Use same number of output partitions
        num_outputs = num_mappers
        sample_bar = ctx.sub_progress_bar_dict[
            SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME
        ]
        # Sample boundaries for distinct key
        boundaries = SortTaskSpec.sample_boundaries(
            blocks, sort_key, num_outputs, sample_bar
        )

        # Import here to avoid circular dependency
        from ray.data._internal.planner.exchange.distinct_task_spec import (
            DistinctTaskSpec,
        )

        distinct_spec = DistinctTaskSpec(
            boundaries=boundaries,
            key=sort_key,
        )

        if data_context.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED:
            scheduler = PushBasedShuffleTaskScheduler(distinct_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(distinct_spec)

        return scheduler.execute(
            refs,
            num_outputs,
            ctx,
            _debug_limit_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    return fn

