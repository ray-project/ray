import logging
from typing import Iterable, List

import pyarrow as pa

from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlocksToBatchesMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data.block import Block, DataBatch

logger = logging.getLogger(__name__)


def plan_read_files_op(
    op: ReadFiles, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_op = physical_children[0]

    def read_paths(blocks: Iterable[Block], _: TaskContext) -> Iterable[DataBatch]:
        for block in blocks:
            assert isinstance(block, pa.Table), type(block)
            paths = block[PATH_COLUMN_NAME].to_pylist()
            yield from op.reader.read_paths(paths, filesystem=op.filesystem)

    transform_fns: List[MapTransformFn] = [
        BlocksToBatchesMapTransformFn(batch_format=None),
        BatchMapTransformFn(read_paths),
        BuildOutputBlocksMapTransformFn.for_batches(),
    ]

    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_op,
        name="ReadFiles",
        target_max_block_size=None,
        ray_remote_args=op.ray_remote_args,
        compute_strategy=TaskPoolStrategy(op.concurrency),
        supports_fusion=False,
    )
