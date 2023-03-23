from functools import partial
from typing import List, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKeyT, SortTaskSpec
from ray.data._internal.stats import StatsDict
from ray.data.context import DatasetContext


def generate_sort_fn(
    key: SortKeyT,
    descending: bool,
) -> AllToAllTransformFn:
    """Generate function to sort blocks by the specified key column or key function."""
    # TODO: validate key with block._validate_key_fn.

    def fn(
        key: SortKeyT,
        descending: bool,
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        for ref_bundle in refs:
            for block, _ in ref_bundle.blocks:
                blocks.append(block)
        if len(blocks) == 0:
            return (blocks, {})

        if isinstance(key, str):
            key = [(key, "descending" if descending else "ascending")]
        if isinstance(key, list):
            descending = key[0][1] == "descending"

        num_mappers = len(blocks)
        # Use same number of output partitions.
        num_outputs = num_mappers

        # Sample boundaries for sort key.
        boundaries = SortTaskSpec.sample_boundaries(blocks, key, num_outputs)
        if descending:
            boundaries.reverse()
        sort_spec = SortTaskSpec(boundaries=boundaries, key=key, descending=descending)

        if DatasetContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(sort_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(sort_spec)

        return scheduler.execute(refs, num_outputs)

    # NOTE: use partial function to pass parameters to avoid error like
    # "UnboundLocalError: local variable ... referenced before assignment",
    # because `key` and `descending` variables are reassigned in `fn()`.
    return partial(fn, key, descending)
