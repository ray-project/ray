import math
from typing import TypeVar, List

import ray
from ray.experimental.data.impl.block import Block
from ray.experimental.data.impl.progress_bar import ProgressBar
from ray.experimental.data.impl.block_list import BlockMetadata, BlockList
from ray.experimental.data.impl.arrow_block import DelegatingArrowBlockBuilder

T = TypeVar("T")


def simple_shuffle(input_blocks: BlockList[T],
                   output_num_blocks: int) -> BlockList[T]:
    input_num_blocks = len(input_blocks)

    @ray.remote(num_returns=output_num_blocks)
    def shuffle_map(block: Block[T]) -> List[Block[T]]:
        slice_sz = max(1, math.ceil(block.num_rows() / output_num_blocks))
        slices = []
        for i in range(output_num_blocks):
            slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz))
        num_rows = sum(s.num_rows() for s in slices)
        assert num_rows == block.num_rows(), (num_rows, block.num_rows())
        return slices

    @ray.remote(num_returns=2)
    def shuffle_reduce(
            *mapper_outputs: List[Block[T]]) -> (Block[T], BlockMetadata):
        builder = DelegatingArrowBlockBuilder()
        assert len(mapper_outputs) == input_num_blocks
        for block in mapper_outputs:
            builder.add_block(block)
        new_block = builder.build()
        new_metadata = BlockMetadata(
            num_rows=new_block.num_rows(),
            size_bytes=new_block.size_bytes(),
            schema=new_block.schema(),
            input_files=None)
        return new_block, new_metadata

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
    new_blocks = [s[0] for s in shuffle_reduce_out]
    new_metadata = [s[1] for s in shuffle_reduce_out]
    reduce_bar.block_until_complete(new_blocks)
    new_metadata = ray.get(new_metadata)

    map_bar.close()
    reduce_bar.close()
    return BlockList(new_blocks, new_metadata)
