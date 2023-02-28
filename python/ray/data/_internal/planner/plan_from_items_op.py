from typing import Iterator, List

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_items_operator import FromItems
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.types import ObjectRef


def _plan_from_items_op(op: FromItems) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromItems.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        block_size = max(
            1,
            len(op._items) // op._parallelism,
        )
        blocks: List[ObjectRef[Block]] = []
        metadatas: List[BlockMetadata] = []
        ref_bundles: List[RefBundle] = []
        i = 0
        while i < len(op._items):
            stats = BlockExecStats.builder()
            builder = DelegatingBlockBuilder()
            for item in op._items[i : i + block_size]:
                builder.add(item)
        block = builder.build()
        blocks.append(ray.put(block))
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        )
        metadatas.append(metadata)
        # ref_bundles.append(RefBundle([(block, metadata)], True))
        i += block_size

        # return ref_bundles
        return [
            RefBundle(
                [(blocks, metadatas)],
                owns_blocks=True,  # TODO(Scott): what does this signify?
            )
        ]

    return InputDataBuffer(input_data_factory=get_input_data)
