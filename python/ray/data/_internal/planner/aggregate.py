from typing import List, Optional, Tuple, Union

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.aggregate_task_spec import (
    SortAggregateTaskSpec,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortTaskSpec
from ray.data._internal.stats import StatsDict
from ray.data.aggregate import AggregateFn
from ray.data.block import KeyFn
from ray.data.context import DataContext
from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas


def generate_aggregate_fn(
    key: Optional[KeyFn],
    aggs: List[AggregateFn],
) -> AllToAllTransformFn:
    """Generate function to aggregate blocks by the specified key column or key
    function.
    """
    if len(aggs) == 0:
        raise ValueError("Aggregate requires at least one aggregation")

    import pyarrow as pa

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        block_schemas: List[Optional[Union[type, pa.lib.Schema]]] = []
        valid_block_schema: Optional[Union[type, pa.lib.Schema]] = None
        for ref_bundle in refs:
            for block, block_metadata in ref_bundle.blocks:
                blocks.append(block)
                if block_metadata.schema is not None and valid_block_schema is None:
                    valid_block_schema = block_metadata.schema
                block_schemas.append(block_metadata.schema)
        if len(blocks) == 0:
            return (blocks, {})
        if isinstance(valid_block_schema, pa.lib.Schema):
            unified_schema = unify_schemas(block_schemas)
        else:  # Covers both simple-type and None cases.
            if isinstance(valid_block_schema, type):
                assert all([b == valid_block_schema for b in block_schemas])
            unified_schema = valid_block_schema
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
                [(key, "ascending")] if isinstance(key, str) else key,
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

        return scheduler.execute(refs, num_outputs)

    return fn
