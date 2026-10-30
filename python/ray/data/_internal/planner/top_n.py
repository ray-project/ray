from typing import Iterable

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.bundle_queue import EstimateBytes, RebundleQueue
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.top_n_operator import TopNMergeOperator
from ray.data._internal.logical.operators import TopN
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

_LOCAL_TOP_N_MIN_BYTES = 1024 * 1024


def plan_top_n_op(
    logical_op: TopN,
    data_context: DataContext,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Plan Top-N as local candidate reduction followed by a global merge."""

    def transform(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        # Bound each block to at most ``limit`` candidates before the final merge.
        for block in blocks:
            yield BlockAccessor.for_block(block).top_n(
                logical_op.sort_key, logical_op.limit
            )

    # Respect small streaming blocks and explicit memory limits when bundling.
    min_bundle_bytes = min(
        _LOCAL_TOP_N_MIN_BYTES,
        data_context.target_max_block_size or _LOCAL_TOP_N_MIN_BYTES,
        input_physical_op.target_max_block_size_override or _LOCAL_TOP_N_MIN_BYTES,
    )
    memory_limit = data_context.execution_options.resource_limits.object_store_memory
    if memory_limit < min_bundle_bytes:
        min_bundle_bytes = max(1, int(memory_limit))

    local_map = MapOperator.create(
        MapTransformer([BlockMapTransformFn(transform, disable_block_shaping=True)]),
        input_physical_op,
        data_context,
        name=f"LocalTopN(limit={logical_op.limit})",
        compute_strategy=TaskPoolStrategy(),
        ref_bundler=RebundleQueue(EstimateBytes(min_bundle_bytes)),
        supports_fusion=False,
        on_start=logical_op.sort_key.validate_schema,
    )

    return TopNMergeOperator(
        local_map,
        data_context,
        sort_key=logical_op.sort_key,
        limit=logical_op.limit,
        ray_remote_args=logical_op.ray_remote_args,
        output_schema=logical_op.infer_schema(),
        name=f"TopNMerge(limit={logical_op.limit})",
    )
