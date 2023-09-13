from functools import partial
from typing import List, Tuple

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
from ray.data._internal.planner.exchange.sort_task_spec import SortTaskSpec
from ray.data._internal.sort import SortKey
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.context import DataContext


def generate_sort_fn(
    sort_key: SortKey,
) -> AllToAllTransformFn:
    """Generate function to sort blocks by the specified key column or key function."""

    def fn(
        sort_key: SortKey,
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        metadata = []
        for ref_bundle in refs:
            for block, block_metadata in ref_bundle.blocks:
                blocks.append(block)
                metadata.append(block_metadata)
        if len(blocks) == 0:
            return (blocks, {})
        sort_key.validate_schema(unify_block_metadata_schema(metadata))

        num_mappers = len(blocks)
        # Use same number of output partitions.
        num_outputs = num_mappers

        # Sample boundaries for sort key.
        boundaries = SortTaskSpec.sample_boundaries(blocks, sort_key, num_outputs)
        _, ascending = sort_key.to_pandas_sort_args()
        if not ascending:
            boundaries.reverse()
        sort_spec = SortTaskSpec(boundaries=boundaries, sort_key=sort_key)

        if DataContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(sort_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(sort_spec)

        return scheduler.execute(refs, num_outputs, ctx)

    # NOTE: use partial function to pass parameters to avoid error like
    # "UnboundLocalError: local variable ... referenced before assignment",
    # because `key` and `descending` variables are reassigned in `fn()`.
    return partial(fn, sort_key)
