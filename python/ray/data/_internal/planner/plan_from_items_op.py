import collections
from typing import List

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_items_operator import FromItems
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata


def _plan_from_items_op(op: FromItems) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromItems.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        ctx = ray.data.DataContext.get_current()
        if op._parallelism > 0:
            block_size, remainder = divmod(len(op._items), op._parallelism)
        else:
            block_size, remainder = 0, 0

        ref_bundles: List[RefBundle] = []
        for i in range(op._parallelism):
            stats = BlockExecStats.builder()
            builder = DelegatingBlockBuilder()

            # Evenly distribute remainder across block slices while
            # preserving record order.
            block_start = i * block_size + min(i, remainder)
            block_end = (i + 1) * block_size + min(i + 1, remainder)
            for j in range(block_start, block_end):
                item = op._items[j]
                if ctx.strict_mode:
                    if not isinstance(item, collections.abc.Mapping):
                        item = {"item": item}
                builder.add(item)

            block: Block = builder.build()
            block_metadata: BlockMetadata = BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
            block_ref_bundle = RefBundle(
                [(ray.put(block), block_metadata)],
                owns_blocks=True,
            )
            ref_bundles.append(block_ref_bundle)
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)
