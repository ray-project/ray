import logging
from typing import Iterable, Tuple, List

import ray
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
)
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


def _calculate_blocks_size(
    blocks_with_metadata: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
) -> List[int]:
    get_num_rows = cached_remote_fn(_get_num_rows)
    block_sizes = []
    for block, metadata in blocks_with_metadata:
        if metadata.num_rows is None:
            # Need to fetch number of rows.
            num_rows = ray.get(get_num_rows.remote(block))
        else:
            num_rows = metadata.num_rows
        block_sizes.append(num_rows)
    return block_sizes


def _generate_invalid_indices(
    num_rows_per_block: List[int],
    split_indices: List[int],
) -> List[int]:
    total_rows = sum(num_rows_per_block)
    return [min(index, total_rows) for index in split_indices]


def _generate_split_plan(
    num_rows_per_block: List[int],
    split_indices: List[int],
) -> List[List[int]]:
    split_plan = []
    current_input_block_id = 0
    current_block_split_index = []
    offset = 0
    current_index_id = 0

    while current_index_id < len(split_indices):
        split_index = split_indices[current_index_id]
        current_block_size = num_rows_per_block[current_input_block_id]
        if split_index - offset <= current_block_size:
            current_block_split_index.append(split_index - offset)
            current_index_id += 1
            continue
        split_plan.append(current_block_split_index)
        current_block_split_index = []
        offset += num_rows_per_block[current_input_block_id]
        current_input_block_id += 1

    while len(split_plan) < len(num_rows_per_block):
        split_plan.append(current_block_split_index)
        current_block_split_index = []
    return split_plan


def _split_single_block(
    block: Block,
    meta: BlockMetadata,
    block_size: int,
    split_indices: List[int],
) -> List[Tuple[ObjectRef[Block], BlockMetadata, int]]:
    """Split the provided block at the given row index."""
    split_indices.append(block_size)
    split_result = []
    stats = BlockExecStats.builder()
    block = BlockAccessor.for_block(block)
    prev_index = 0
    for index in split_indices:
        logger.debug(f"slicing block {prev_index}:{index}")
        split_block = block.slice(prev_index, index, copy=True)
        accessor = BlockAccessor.for_block(split_block)
        split_meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
        split_result.append((ray.put(split_block), split_meta, (index - prev_index)))
        prev_index = index
    return split_result


def _split_at_index(
    blocks_with_metadata: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
    index: int,
    return_right_half: bool,
) -> Tuple[
    List[ObjectRef[Block]],
    List[BlockMetadata],
    List[ObjectRef[Block]],
    List[BlockMetadata],
]:
    """Split blocks at the provided index.
    Args:
        blocks_with_metadata: Block futures to split, including the associated metadata.
        index: The (global) index at which to split the blocks.
        return_right_half: Whether we want to return or drop the data to the right of
            the index.
    Returns:
        The block split futures and their metadata for left and right of the index.
    """
    blocks_splits, metadata_splits = _split_at_indices(blocks_with_metadata, [index])
    return blocks_splits[0], metadata_splits[0], blocks_splits[1], metadata_splits[1]


def _get_all_blocks_split_results(
    blocks_with_metadata: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
    block_sizes: List[int],
    split_plan: List[List[int]],
) -> List[Tuple[ObjectRef[Block], BlockMetadata, int]]:
    split_single_block = cached_remote_fn(_split_single_block)
    all_blocks_split_results: List[
        List[Tuple[ObjectRef[Block], BlockMetadata, int]]
    ] = ray.get(
        [
            split_single_block.remote(
                block_with_metadata[0],
                block_with_metadata[1],
                block_sizes[i],
                split_plan[i],
            )
            for i, block_with_metadata in enumerate(blocks_with_metadata)
        ]
    )
    return all_blocks_split_results


def _split_at_indices(
    blocks_with_metadata: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
    indices: List[int],
) -> Tuple[List[List[ObjectRef[Block]]], List[List[BlockMetadata]]]:
    """Split blocks at the provided indices.

    Args:
        blocks_with_metadata: Block futures to split, including the associated metadata.
        indices: The (global) indices at which to split the blocks.
    Returns:
        The block split futures and their metadata. If an index split is empty, the
        corresponding block split future will be None.
    """
    block_sizes: List[int] = _calculate_blocks_size(blocks_with_metadata)
    valid_indices = _generate_invalid_indices(block_sizes, indices)
    split_plan: List[List[int]] = _generate_split_plan(block_sizes, valid_indices)
    all_blocks_split_results: List[
        List[Tuple[ObjectRef[Block], BlockMetadata, int]]
    ] = _get_all_blocks_split_results(blocks_with_metadata, block_sizes, split_plan)

    result_blocks = []
    result_metas = []
    current_blocks = []
    current_meta = []
    for single_block_split_result in all_blocks_split_results:
        for i, (block, meta, _) in enumerate(single_block_split_result):
            if i != 0:
                result_blocks.append(current_blocks)
                result_metas.append(current_meta)
                current_blocks = []
                current_meta = []
            current_blocks.append(block)
            current_meta.append(meta)
    result_blocks.append(current_blocks)
    result_metas.append(current_meta)

    return result_blocks, result_metas


def _get_num_rows(block: Block) -> int:
    """Get the number of rows contained in the provided block."""
    return BlockAccessor.for_block(block).num_rows()
