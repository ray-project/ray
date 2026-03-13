from typing import List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    Aggregate,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.planner.aggregate import generate_aggregate_fn
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.planner.random_shuffle import generate_random_shuffle_fn
from ray.data._internal.planner.randomize_blocks import generate_randomize_blocks_fn
from ray.data._internal.planner.repartition import generate_repartition_fn
from ray.data._internal.planner.sort import generate_sort_fn
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.aggregate import AggregateFn
from ray.data.block import Block, BlockAccessor, BlockMetadataWithSchema
from ray.data.context import DataContext, ShuffleStrategy


def _estimate_input_size_bytes(refs: List[RefBundle]) -> int:
    """Estimate the total size in bytes of the input ref bundles."""
    return sum(bundle.size_bytes() for bundle in refs)


def _generate_local_aggregate_fn(
    key: Optional[str],
    aggs: Tuple[AggregateFn, ...],
    batch_format: str,
):
    """Generate a local aggregation function for small datasets."""
    from ray.data._internal.planner.exchange.aggregate_task_spec import (
        SortAggregateTaskSpec,
    )
    from ray.data._internal.util import unify_ref_bundles_schema

    if len(aggs) == 0:
        raise ValueError("Aggregate requires at least one aggregation")

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> AllToAllTransformFnResult:
        blocks = []
        for ref_bundle in refs:
            blocks.extend(ref_bundle.block_refs)
        if len(blocks) == 0:
            return ([], {})

        unified_schema = unify_ref_bundles_schema(refs)
        for agg_fn in aggs:
            agg_fn._validate(unified_schema)

        sort_key = SortKey(key)

        all_blocks: List[Block] = ray.get(blocks)

        pruned_blocks = [
            SortAggregateTaskSpec._prune_unused_columns(block, sort_key, aggs)
            for block in all_blocks
        ]

        if sort_key.get_columns():
            pruned_blocks = [
                BlockAccessor.for_block(block).sort(sort_key) for block in pruned_blocks
            ]

        aggregated_blocks = [
            BlockAccessor.for_block(block)._aggregate(sort_key, aggs)
            for block in pruned_blocks
        ]

        target_block_type = ExchangeTaskSpec._derive_target_block_type(batch_format)
        normalized_blocks = TableBlockAccessor.normalize_block_types(
            aggregated_blocks, target_block_type=target_block_type
        )

        if not normalized_blocks:
            return ([], {})

        final_block, _ = BlockAccessor.for_block(
            normalized_blocks[0]
        )._combine_aggregated_blocks(
            list(normalized_blocks), sort_key, aggs, finalize=True
        )

        final_metadata = BlockMetadataWithSchema.from_block(final_block)

        return (
            [
                RefBundle(
                    [(ray.put(final_block), final_metadata.metadata)],
                    schema=final_metadata.schema,
                    owns_blocks=True,
                )
            ],
            {},
        )

    return fn


def _plan_hash_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_shuffle import (
        HashShuffleOperator,
    )

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    return HashShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),  # type: ignore[arg-type]
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
        # TODO wire in aggregator args overrides
    )


def _plan_hash_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_aggregate import (
        HashAggregateOperator,
    )

    normalized_key_columns = SortKey(logical_op.key).get_columns()

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(normalized_key_columns),  # type: ignore[arg-type]
        aggregation_fns=tuple(logical_op.aggs),  # type: ignore[arg-type]
        num_partitions=logical_op.num_partitions,
    )


def plan_all_to_all_op(
    op: AbstractAllToAll,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op)

    elif isinstance(op, RandomShuffle):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_random_shuffle_fn(
            data_context,
            op.seed,
            op.num_outputs,
            op.ray_remote_args,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Repartition):
        if op.keys:
            if data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
                return _plan_hash_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            else:
                raise ValueError(
                    "Key-based repartitioning only supported for "
                    f"`DataContext.shuffle_strategy=HASH_SHUFFLE` "
                    f"(got {data_context.shuffle_strategy})"
                )

        elif op.shuffle:
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        else:
            debug_limit_shuffle_execution_to_num_blocks = None

        fn = generate_repartition_fn(
            op.num_outputs,
            op.shuffle,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Sort):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_sort_fn(
            op.sort_key,
            op.batch_format,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
            return _plan_hash_shuffle_aggregate(data_context, op, input_physical_dag)

        if data_context.small_data_threshold_for_local_aggregation > 0:
            local_fn = _generate_local_aggregate_fn(
                op.key,
                tuple(op.aggs),
                op.batch_format or "default",
            )

            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
            distributed_fn = generate_aggregate_fn(
                op.key,
                op.aggs,
                op.batch_format or "default",
                data_context,
                debug_limit_shuffle_execution_to_num_blocks,
            )

            def aggregate_fn_with_local_fallback(
                refs: List[RefBundle],
                ctx: TaskContext,
            ) -> AllToAllTransformFnResult:
                input_size = _estimate_input_size_bytes(refs)
                if (
                    input_size
                    <= data_context.small_data_threshold_for_local_aggregation
                ):
                    return local_fn(refs, ctx)
                return distributed_fn(refs, ctx)

            return AllToAllOperator(
                aggregate_fn_with_local_fallback,
                input_physical_dag,
                data_context,
                num_outputs=op.num_partitions,
                sub_progress_bar_names=op.sub_progress_bar_names,
                name=op.name,
            )

        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_aggregate_fn(
            op.key,
            op.aggs,
            op.batch_format,  # type: ignore[arg-type]
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        num_outputs=op.num_outputs,
        sub_progress_bar_names=op.sub_progress_bar_names,
        name=op.name,
    )
