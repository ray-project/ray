import logging
from typing import List, Optional, Union

import ray

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
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
from ray.data._internal.util import unify_ref_bundles_schema
from ray.data.aggregate import AggregateFn
from ray.data.context import DataContext, ShuffleStrategy

logger = logging.getLogger(__name__)


def _local_aggregate(
    refs: List[RefBundle],
    sort_key: SortKey,
    aggs: List[AggregateFn],
    batch_format: str,
) -> AllToAllTransformFnResult:
    """Execute aggregation locally on the driver for small datasets.

    This avoids the overhead of spawning a distributed actor pool when the
    total input data size is below the configured threshold.
    """
    all_block_refs = [ref for bundle in refs for ref in bundle.block_refs]
    blocks = ray.get(all_block_refs)

    # Partially aggregate each block (map step): sort by key, then aggregate
    # within a single partition (boundaries=[]).
    partial_agg_blocks = []
    for idx, block in enumerate(blocks):
        map_result = SortAggregateTaskSpec.map(
            idx=idx,
            block=block,
            output_num_blocks=1,
            boundaries=[],
            sort_key=sort_key,
            aggs=aggs,
        )
        # map() returns [partial_agg_block_0, ..., BlockMetadataWithSchema]
        partial_agg_blocks.extend(map_result[:-1])

    # Filter out any empty partial blocks before combining.
    from ray.data.block import BlockAccessor

    partial_agg_blocks = [
        b for b in partial_agg_blocks if BlockAccessor.for_block(b).num_rows() > 0
    ]

    if not partial_agg_blocks:
        return ([], {})

    # Combine and finalize all partial results (reduce step).
    result_block, meta_with_schema = SortAggregateTaskSpec.reduce(
        sort_key, aggs, batch_format, *partial_agg_blocks
    )

    result_ref = ray.put(result_block)
    result_bundle = RefBundle(
        [(result_ref, meta_with_schema.metadata)],
        owns_blocks=True,
        schema=meta_with_schema.schema,
    )
    return ([result_bundle], {})


def generate_aggregate_fn(
    key: Optional[Union[str, List[str]]],
    aggs: List[AggregateFn],
    batch_format: str,
    data_context: DataContext,
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
    ) -> AllToAllTransformFnResult:
        blocks = []
        metadata = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
            metadata.extend(ref_bundle.metadata)
        if len(blocks) == 0:
            return (blocks, {})

        unified_schema = unify_ref_bundles_schema(refs)
        for agg_fn in aggs:
            agg_fn._validate(unified_schema)

        sort_key = SortKey(key)

        # Fast-path: for small datasets, run aggregation locally on the driver
        # to avoid actor pool startup and distributed coordination overhead.
        threshold = data_context.small_dataset_agg_threshold_bytes
        if threshold > 0:
            total_bytes = sum(bundle.size_bytes() for bundle in refs)
            if total_bytes <= threshold:
                logger.debug(
                    f"Using local aggregation fast-path for small dataset "
                    f"({total_bytes} bytes <= threshold {threshold} bytes)."
                )
                return _local_aggregate(refs, sort_key, aggs, batch_format)

        num_mappers = len(blocks)

        if key is None:
            num_outputs = 1
            boundaries = []
        else:
            # Use same number of output partitions.
            num_outputs = num_mappers
            sample_bar = ctx.sub_progress_bar_dict[
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME
            ]
            # Sample boundaries for aggregate key.
            boundaries = SortTaskSpec.sample_boundaries(
                blocks, sort_key, num_outputs, sample_bar
            )

        agg_spec = SortAggregateTaskSpec(
            boundaries=boundaries,
            key=sort_key,
            aggs=aggs,
            batch_format=batch_format,
        )

        if data_context.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED:
            scheduler = PushBasedShuffleTaskScheduler(agg_spec)
        else:
            # Covers SORT_SHUFFLE_PULL_BASED and HASH_SHUFFLE (when used as the
            # large-data fallback from the local fast-path AllToAllOperator path).
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
