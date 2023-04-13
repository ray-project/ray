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
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.block import _validate_key_fn
from ray.data.context import DataContext


def generate_sort_fn(
    key: SortKeyT,
    descending: bool,
) -> AllToAllTransformFn:
    """Generate function to sort blocks by the specified key column or key function."""

    def fn(
        key: SortKeyT,
        descending: bool,
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
        unified_schema = unify_block_metadata_schema(metadata)
        _validate_key_fn(unified_schema, key)

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

        if DataContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(sort_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(sort_spec)

        return scheduler.execute(refs, num_outputs)

    # NOTE: use partial function to pass parameters to avoid error like
    # "UnboundLocalError: local variable ... referenced before assignment",
    # because `key` and `descending` variables are reassigned in `fn()`.
    return partial(fn, key, descending)
