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
    """Calculate the number of rows for a list of blocks with metadata."""
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


def _generate_valid_indices(
    num_rows_per_block: List[int],
    split_indices: List[int],
) -> List[int]:
    """Generate valid split indices by apply min(index, total_num_rows)
    to every index."""
    total_rows = sum(num_rows_per_block)
    return [min(index, total_rows) for index in split_indices]


def _generate_per_block_split_indices(
    num_rows_per_block: List[int],
    split_indices: List[int],
) -> List[List[int]]:
    """Given num rows per block and valid split indices, generate per block split indices.

    Args:
        num_rows_per_block: num of rows per block.
        split_indices: The (global) indices at which to split the blocks.
    Returns:
        Per block split indices indicates each input block's split point(s).
    """
    per_block_split_indices = []
    current_input_block_id = 0
    current_block_split_indice = []
    current_block_global_offset = 0
    current_index_id = 0

    # for each split index, we iterate though the current block
    # to see if the index fall into this block. if the index
    # fall into this block we push it back to the current block's
    # split indices. Otherwise, we move on to next block.
    while current_index_id < len(split_indices):
        split_index = split_indices[current_index_id]
        current_block_size = num_rows_per_block[current_input_block_id]
        if split_index - current_block_global_offset <= current_block_size:
            current_block_split_indice.append(split_index - current_block_global_offset)
            current_index_id += 1
            continue
        per_block_split_indices.append(current_block_split_indice)
        current_block_split_indice = []
        current_block_global_offset += num_rows_per_block[current_input_block_id]
        current_input_block_id += 1

    while len(per_block_split_indices) < len(num_rows_per_block):
        per_block_split_indices.append(current_block_split_indice)
        current_block_split_indice = []
    return per_block_split_indices


def _split_single_block(
    block: Block,
    meta: BlockMetadata,
    block_size: int,
    split_indices: List[int],
) -> List[Tuple[ObjectRef[Block], BlockMetadata, int]]:
    """Split the provided block at the given indices."""
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


def _split_all_blocks(
    blocks_with_metadata: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
    block_sizes: List[int],
    per_block_split_indices: List[List[int]],
) -> List[Tuple[ObjectRef[Block], BlockMetadata, int]]:
    """Split all the input blocks based on the split indices"""
    split_single_block = cached_remote_fn(_split_single_block)
    all_blocks_split_results: List[
        List[Tuple[ObjectRef[Block], BlockMetadata, int]]
    ] = ray.get(
        [
            split_single_block.remote(
                block_with_metadata[0],
                block_with_metadata[1],
                block_sizes[i],
                per_block_split_indices[i],
            )
            for i, block_with_metadata in enumerate(blocks_with_metadata)
        ]
    )
    return all_blocks_split_results


def _merge_all_blocks_split_results(
    all_blocks_split_results: List[List[Tuple[ObjectRef[Block], BlockMetadata, int]]],
) -> Tuple[List[List[ObjectRef[Block]]], List[List[BlockMetadata]]]:
    """Merge per block's split result into final split result."""
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

    # We implement the split in 3 phases.
    # phase 1: calculate the per block split indices.
    block_sizes: List[int] = _calculate_blocks_size(blocks_with_metadata)
    valid_indices = _generate_valid_indices(block_sizes, indices)
    per_block_split_indices: List[List[int]] = _generate_per_block_split_indices(
        block_sizes, valid_indices
    )

    # phase 2: split each block based on the indices from previous step.
    all_blocks_split_results: List[
        List[Tuple[ObjectRef[Block], BlockMetadata, int]]
    ] = _split_all_blocks(blocks_with_metadata, block_sizes, per_block_split_indices)

    # phase 3: generate the final split.
    return _merge_all_blocks_split_results(all_blocks_split_results)


def _get_num_rows(block: Block) -> int:
    """Get the number of rows contained in the provided block."""
    return BlockAccessor.for_block(block).num_rows()
