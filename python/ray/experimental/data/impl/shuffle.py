from typing import TypeVar, List

import ray
from ray.experimental.data.impl.block import Block, ObjectRef
from ray.experimental.data.impl.progress_bar import ProgressBar
from ray.experimental.data.impl.arrow_block import DelegatingArrowBlockBuilder

T = TypeVar("T")


def simple_shuffle(input_blocks: List[ObjectRef[Block[T]]],
                   output_num_blocks: int) -> List[ObjectRef[Block[T]]]:
    input_num_blocks = len(input_blocks)

    @ray.remote(num_returns=output_num_blocks)
    def shuffle_map(block: Block[T]) -> List[Block[T]]:
        slice_sz = max(1, block.num_rows() // output_num_blocks)
        slices = []
        for i in range(output_num_blocks):
            slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz))
        return slices

    @ray.remote
    def shuffle_reduce(*mapper_outputs: List[Block[T]]) -> Block[T]:
        builder = DelegatingArrowBlockBuilder()
        assert len(mapper_outputs) == input_num_blocks
        for block in mapper_outputs:
            builder.add_block(block)
        return builder.build()

    map_bar = ProgressBar("Shuffle Map", position=0, total=input_num_blocks)
    reduce_bar = ProgressBar(
        "Shuffle Reduce", position=1, total=output_num_blocks)

    shuffle_map_out = [shuffle_map.remote(block) for block in input_blocks]
    map_bar.block_until_complete([x[0] for x in shuffle_map_out])

    shuffle_reduce_out = [
        shuffle_reduce.remote(
            *[shuffle_map_out[i][j] for i in range(input_num_blocks)])
        for j in range(output_num_blocks)
    ]
    reduce_bar.block_until_complete(shuffle_reduce_out)

    map_bar.close()
    reduce_bar.close()
    return shuffle_reduce_out
