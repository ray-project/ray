from functools import partial
from typing import List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import unify_ref_bundles_schema
from ray.data.context import DataContext, ShuffleStrategy


def generate_sort_fn(
    sort_key: SortKey,
    batch_format: str,
    data_context: DataContext,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to sort blocks by the specified key column or key function."""

    def fn(
        sort_key: SortKey,
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
        if len(blocks) == 0:
            return (blocks, {})

        sort_key.validate_schema(unify_ref_bundles_schema(refs))

        num_mappers = len(blocks)
        # Use same number of output partitions.
        num_outputs = num_mappers

        # Sample boundaries for sort key.
        if not sort_key.boundaries:
            sample_bar = ctx.sub_progress_bar_dict[
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME
            ]
            boundaries = SortTaskSpec.sample_boundaries(
                blocks, sort_key, num_outputs, sample_bar
            )
        else:
            # For user-specified boundaries (which only partition by the primary
            # sort key), reverse `boundaries` so that the partitions are produced
            # in descending order, as desired.
            boundaries = [(b,) for b in sort_key.boundaries]
            if sort_key.get_descending()[0]:
                boundaries = boundaries[::-1]
            num_outputs = len(boundaries) + 1
        sort_spec = SortTaskSpec(
            boundaries=boundaries, sort_key=sort_key, batch_format=batch_format
        )

        if data_context.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED:
            scheduler = PushBasedShuffleTaskScheduler(sort_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(sort_spec)

        return scheduler.execute(
            refs,
            num_outputs,
            ctx,
            _debug_limit_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    # NOTE: use partial function to pass parameters to avoid error like
    # "UnboundLocalError: local variable ... referenced before assignment",
    # because `key` and `descending` variables are reassigned in `fn()`.
    return partial(fn, sort_key)
