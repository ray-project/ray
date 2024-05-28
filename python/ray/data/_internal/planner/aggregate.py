from typing import List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.aggregate_task_spec import (
    SortAggregateTaskSpec,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.aggregate import AggregateFn
from ray.data.context import DataContext


def generate_aggregate_fn(
    key: Optional[str],
    aggs: List[AggregateFn],
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to aggregate blocks by the specified key column or key
    function.
    """
    if len(aggs) == 0:
        raise ValueError("Aggregate requires at least one aggregation")

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        metadata = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
            metadata.extend(ref_bundle.metadata)
        if len(blocks) == 0:
            return (blocks, {})
        unified_schema = unify_block_metadata_schema(metadata)
        for agg_fn in aggs:
            agg_fn._validate(unified_schema)

        num_mappers = len(blocks)

        if key is None:
            num_outputs = 1
            boundaries = []
        else:
            # Use same number of output partitions.
            num_outputs = num_mappers
            # Sample boundaries for aggregate key.
            boundaries = SortTaskSpec.sample_boundaries(
                blocks,
                SortKey(key),
                num_outputs,
            )

        agg_spec = SortAggregateTaskSpec(
            boundaries=boundaries,
            key=key,
            aggs=aggs,
        )
        if DataContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(agg_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(agg_spec)

        return scheduler.execute(
            refs,
            num_outputs,
            ctx,
            _debug_limit_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    return fn
