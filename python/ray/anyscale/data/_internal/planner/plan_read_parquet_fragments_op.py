import logging
from typing import Iterable, List

from ray.anyscale.data._internal.logical.operators.read_parquet_fragments_operator import (  # noqa: E501
    ReadParquetFragments,
)
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)


def plan_read_parquet_fragments_op(
    op: ReadParquetFragments, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_op = physical_children[0]

    def read_fragments(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        from ray.data._internal.datasource.parquet_datasource import read_fragments

        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            serialized_fragments = [
                row["fragment"]
                for row in block_accessor.iter_rows(public_row_format=False)
            ]
            yield from read_fragments(
                serialized_fragments=serialized_fragments,
                block_udf=op.block_udf,
                to_batches_kwargs=op.to_batches_kwargs,
                default_read_batch_size_rows=op.default_read_batch_size_rows,
                columns=op.columns,
                schema=op.parquet_schema,
                include_paths=op.include_paths,
            )

    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(read_fragments),
        BuildOutputBlocksMapTransformFn.for_blocks(),
    ]
    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        input_op,
        name="ReadParquetFragments",
        target_max_block_size=None,
        ray_remote_args=op.ray_remote_args,
        compute_strategy=TaskPoolStrategy(op.concurrency),
        supports_fusion=False,
    )
