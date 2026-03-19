from functools import partial
from typing import List, Optional, Tuple

import ray
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
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import unify_ref_bundles_schema
from ray.data.block import Block, BlockAccessor, BlockMetadataWithSchema
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


def generate_topk_fn(
    sort_key: SortKey,
    batch_format: str,
    k: int,
    _data_context: DataContext,
) -> AllToAllTransformFn:
    def fn(
        sort_key: SortKey,
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks: List[ray.ObjectRef[Block]] = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
        if len(blocks) == 0 or k <= 0:
            return ([], {})

        sort_key.validate_schema(unify_ref_bundles_schema(refs))

        topk_block = cached_remote_fn(_topk_sort_block)
        topk_block_refs = [topk_block.remote(block, sort_key, k) for block in blocks]
        topk_blocks = ray.get(topk_block_refs)

        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
        from ray.data._internal.table_block import TableBlockAccessor

        target_block_type = ExchangeTaskSpec._derive_target_block_type(batch_format)
        normalized_blocks = TableBlockAccessor.normalize_block_types(
            topk_blocks,
            target_block_type=target_block_type,
        )
        merged_block, _ = BlockAccessor.for_block(normalized_blocks[0]).merge_sorted_blocks(
            normalized_blocks, sort_key
        )
        merged_accessor = BlockAccessor.for_block(merged_block)
        result_block = merged_accessor.slice(0, min(k, merged_accessor.num_rows()), copy=False)

        meta_with_schema = BlockMetadataWithSchema.from_block(result_block)
        output = RefBundle(
            [
                (
                    ray.put(result_block),
                    meta_with_schema.metadata,
                )
            ],
            owns_blocks=True,
            schema=meta_with_schema.schema,
        )
        return ([output], {})

    return partial(fn, sort_key)


def _topk_sort_block(block: Block, sort_key: SortKey, k: int) -> Block:
    accessor = BlockAccessor.for_block(block)
    sorted_block = accessor.sort(sort_key)
    sorted_accessor = BlockAccessor.for_block(sorted_block)
    return sorted_accessor.slice(0, min(k, sorted_accessor.num_rows()), copy=False)
