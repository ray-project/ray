from typing import Iterable

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.bundle_queue import EstimateSize, RebundleQueue
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


def _local_top_n_transform(
    blocks: Iterable[Block],
    _: TaskContext,
    *,
    sort_key,
    limit: int,
) -> Iterable[Block]:
    """Sort each input block and retain only its first ``limit`` rows."""
    for block in blocks:
        # TODO(vicwen): Use an exact BlockAccessor Top-N primitive instead of
        # sorting the entire block when the limit is small.
        sorted_block = BlockAccessor.for_block(block).sort(sort_key)
        sorted_accessor = BlockAccessor.for_block(sorted_block)
        yield sorted_accessor.slice(0, min(limit, sorted_accessor.num_rows()))


def plan_top_n_op(
    logical_op: TopN,
    data_context: DataContext,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Plan Top-N as local candidate reduction followed by a global merge."""

    def transform(blocks: Iterable[Block], context: TaskContext) -> Iterable[Block]:
        return _local_top_n_transform(
            blocks,
            context,
            sort_key=logical_op.sort_key,
            limit=logical_op.limit,
        )

    # Each block contributes only its best candidates before the final global merge.
    local_map = MapOperator.create(
        MapTransformer([BlockMapTransformFn(transform, disable_block_shaping=True)]),
        input_physical_op,
        data_context,
        name=f"LocalTopN(limit={logical_op.limit})",
        compute_strategy=TaskPoolStrategy(),
        ref_bundler=RebundleQueue(EstimateSize(None)),
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
