import math
from typing import TypeVar, List, Optional, Dict, Any, Tuple, Union, Callable, Iterable

import numpy as np

import ray
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.block_list import BlockList
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder
from ray.data.impl.remote_fn import cached_remote_fn

T = TypeVar("T")


def simple_shuffle(
    input_blocks: BlockList,
    block_udf: Optional[Callable[[Block], Iterable[Block]]],
    output_num_blocks: int,
    *,
    random_shuffle: bool = False,
    random_seed: Optional[int] = None,
    map_ray_remote_args: Optional[Dict[str, Any]] = None,
    reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple[BlockList, Dict[str, List[BlockMetadata]]]:
    input_blocks = input_blocks.get_blocks()
    if map_ray_remote_args is None:
        map_ray_remote_args = {}
    if reduce_ray_remote_args is None:
        reduce_ray_remote_args = {}
    if "scheduling_strategy" not in reduce_ray_remote_args:
        reduce_ray_remote_args = reduce_ray_remote_args.copy()
        reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"
    input_num_blocks = len(input_blocks)

    shuffle_map = cached_remote_fn(_shuffle_map)
    shuffle_reduce = cached_remote_fn(_shuffle_reduce)

    map_bar = ProgressBar("Shuffle Map", position=0, total=input_num_blocks)

    shuffle_map_out = [
        shuffle_map.options(
            **map_ray_remote_args,
            num_returns=1 + output_num_blocks,
        ).remote(block, block_udf, i, output_num_blocks, random_shuffle, random_seed)
        for i, block in enumerate(input_blocks)
    ]

    # The first item returned is the BlockMetadata.
    shuffle_map_metadata = []
    for i, refs in enumerate(shuffle_map_out):
        shuffle_map_metadata.append(refs[0])
        shuffle_map_out[i] = refs[1:]

    # Eagerly delete the input block references in order to eagerly release
    # the blocks' memory.
    del input_blocks
    shuffle_map_metadata = map_bar.fetch_until_complete(shuffle_map_metadata)
    map_bar.close()

    # Randomize the reduce order of the blocks.
    if random_shuffle:
        random = np.random.RandomState(random_seed)
        random.shuffle(shuffle_map_out)

    reduce_bar = ProgressBar("Shuffle Reduce", position=0, total=output_num_blocks)
    shuffle_reduce_out = [
        shuffle_reduce.options(
            **reduce_ray_remote_args,
            num_returns=2,
        ).remote(*[shuffle_map_out[i][j] for i in range(input_num_blocks)])
        for j in range(output_num_blocks)
    ]
    # Eagerly delete the map block references in order to eagerly release
    # the blocks' memory.
    del shuffle_map_out
    new_blocks, new_metadata = zip(*shuffle_reduce_out)
    reduce_bar.block_until_complete(list(new_blocks))
    new_metadata = ray.get(list(new_metadata))
    reduce_bar.close()

    stats = {
        "map": shuffle_map_metadata,
        "reduce": new_metadata,
    }

    return BlockList(list(new_blocks), list(new_metadata)), stats


def _shuffle_map(
    block: Block,
    block_udf: Optional[Callable[[Block], Iterable[Block]]],
    idx: int,
    output_num_blocks: int,
    random_shuffle: bool,
    random_seed: Optional[int],
) -> List[Union[BlockMetadata, Block]]:
    """Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks]."""
    stats = BlockExecStats.builder()
    if block_udf:
        # TODO(ekl) note that this effectively disables block splitting.
        blocks = list(block_udf(block))
        if len(blocks) > 1:
            builder = BlockAccessor.for_block(blocks[0]).builder()
            for b in blocks:
                builder.add_block(b)
            block = builder.build()
        else:
            block = blocks[0]
    block = BlockAccessor.for_block(block)

    # Randomize the distribution of records to blocks.
    if random_shuffle:
        seed_i = random_seed + idx if random_seed is not None else None
        block = block.random_shuffle(seed_i)
        block = BlockAccessor.for_block(block)

    slice_sz = max(1, math.ceil(block.num_rows() / output_num_blocks))
    slices = []
    for i in range(output_num_blocks):
        slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz, copy=True))

    # Randomize the distribution order of the blocks (this matters when
    # some blocks are larger than others).
    if random_shuffle:
        random = np.random.RandomState(seed_i)
        random.shuffle(slices)

    num_rows = sum(BlockAccessor.for_block(s).num_rows() for s in slices)
    assert num_rows == block.num_rows(), (num_rows, block.num_rows())
    metadata = block.get_metadata(input_files=None, exec_stats=stats.build())
    return [metadata] + slices


def _shuffle_reduce(*mapper_outputs: List[Block]) -> (Block, BlockMetadata):
    stats = BlockExecStats.builder()
    builder = DelegatingBlockBuilder()
    for block in mapper_outputs:
        builder.add_block(block)
    new_block = builder.build()
    accessor = BlockAccessor.for_block(new_block)
    new_metadata = BlockMetadata(
        num_rows=accessor.num_rows(),
        size_bytes=accessor.size_bytes(),
        schema=accessor.schema(),
        input_files=None,
        exec_stats=stats.build(),
    )
    return new_block, new_metadata
