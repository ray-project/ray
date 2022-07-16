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


def _calculate_blocks_rows(
    blocks_with_metadata: List[Tuple[ObjectRef[Block], BlockMetadata]],
) -> List[int]:
    """Calculate the number of rows for a list of blocks with metadata."""
    get_num_rows = cached_remote_fn(_get_num_rows)
    block_rows = []
    for block, metadata in blocks_with_metadata:
        if metadata.num_rows is None:
            # Need to fetch number of rows.
            num_rows = ray.get(get_num_rows.remote(block))
        else:
            num_rows = metadata.num_rows
        block_rows.append(num_rows)
    return block_rows


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
    # for each split index, we iterate though the currnet input block
    # to see if the index falls into this block. if the index
    # falls into this block, we push it back to the current block's
    # split indices. Otherwise, we move on to the next block.
    per_block_split_indices = []
    current_input_block_id = 0
    current_block_split_indices = []
    current_block_global_offset = 0
    current_index_id = 0

    while current_index_id < len(split_indices):
        split_index = split_indices[current_index_id]
        current_block_row = num_rows_per_block[current_input_block_id]
        if split_index - current_block_global_offset <= current_block_row:
            current_block_split_indices.append(
                split_index - current_block_global_offset
            )
            current_index_id += 1
            continue
        per_block_split_indices.append(current_block_split_indices)
        current_block_split_indices = []
        current_block_global_offset += num_rows_per_block[current_input_block_id]
        current_input_block_id += 1

    # we might finished all the indices but there are still blocks left, also
    # current_block_split_indices might not be added yet.
    while len(per_block_split_indices) < len(num_rows_per_block):
        per_block_split_indices.append(current_block_split_indices)
        current_block_split_indices = []
    return per_block_split_indices


def _split_single_block(
    block_id: int,
    block: Block,
    meta: BlockMetadata,
    block_row: int,
    split_indices: List[int],
) -> Tuple[int, List[Tuple[ObjectRef[Block], BlockMetadata]]]:
    """Split the provided block at the given indices."""
    split_result = []
    block_accessor = BlockAccessor.for_block(block)
    prev_index = 0
    # append one more entry at the last so we don't
    # need handle empty edge case.
    split_indices.append(block_row)
    for index in split_indices:
        logger.debug(f"slicing block {prev_index}:{index}")
        stats = BlockExecStats.builder()
        split_block = block_accessor.slice(prev_index, index, copy=True)
        accessor = BlockAccessor.for_block(split_block)
        split_meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
        split_result.append((ray.put(split_block), split_meta))
        prev_index = index
    return (block_id, split_result)


def _split_all_blocks(
    blocks_with_metadata: List[Tuple[ObjectRef[Block], BlockMetadata]],
    block_rows: List[int],
    per_block_split_indices: List[List[int]],
) -> List[Tuple[ObjectRef[Block], BlockMetadata]]:
    """Split all the input blocks based on the split indices"""
    split_single_block = cached_remote_fn(_split_single_block)

    all_blocks_split_results: List[List[Tuple[ObjectRef[Block], BlockMetadata]]] = [
        None
    ] * len(blocks_with_metadata)

    split_single_block_futures = []

    for block_id, block_split_indices in enumerate(per_block_split_indices):
        (block_ref, meta) = blocks_with_metadata[block_id]
        block_row = block_rows[block_id]
        if len(block_split_indices) == 0:
            # optimization: if no split is needed, we just need to add it to the
            # result
            all_blocks_split_results[block_id] = [(block_ref, meta)]
        else:
            # otherwise call split remote function.
            split_single_block_futures.append(
                split_single_block.remote(
                    block_id,
                    block_ref,
                    meta,
                    block_row,
                    block_split_indices,
                )
            )
    if split_single_block_futures:
        split_single_block_results = ray.get(split_single_block_futures)
        for block_id, block_split_result in split_single_block_results:
            all_blocks_split_results[block_id] = block_split_result
    return all_blocks_split_results


def _generate_global_split_results(
    all_blocks_split_results: List[List[Tuple[ObjectRef[Block], BlockMetadata]]],
) -> Tuple[List[List[ObjectRef[Block]]], List[List[BlockMetadata]]]:
    """Reassemble per block's split result into final split result."""
    result_blocks = []
    result_metas = []
    current_blocks = []
    current_meta = []

    if len(all_blocks_split_results) == 0:
        return ([], [])

    for single_block_split_result in all_blocks_split_results:
        assert len(single_block_split_result) > 0
        for i, (block, meta) in enumerate(single_block_split_result):
            # we should create a new global split whenever
            # we encountered a new local split in the per block
            # split result.
            if i != 0:
                result_blocks.append(current_blocks)
                result_metas.append(current_meta)
                current_blocks = []
                current_meta = []
            current_blocks.append(block)
            current_meta.append(meta)

    assert len(current_blocks) > 0
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
        corresponding block split will be empty .
    """

    # We implement the split in 3 phases.
    # phase 1: calculate the per block split indices.
    blocks_with_metadata = list(blocks_with_metadata)
    if len(blocks_with_metadata) == 0:
        return ([] * (len(indices) + 1), [] * (len(indices) + 1))
    block_rows: List[int] = _calculate_blocks_rows(blocks_with_metadata)
    valid_indices = _generate_valid_indices(block_rows, indices)
    per_block_split_indices: List[List[int]] = _generate_per_block_split_indices(
        block_rows, valid_indices
    )

    # phase 2: split each block based on the indices from previous step.
    all_blocks_split_results: List[
        List[Tuple[ObjectRef[Block], BlockMetadata]]
    ] = _split_all_blocks(blocks_with_metadata, block_rows, per_block_split_indices)

    # phase 3: generate the final split.
    return _generate_global_split_results(all_blocks_split_results)


def _get_num_rows(block: Block) -> int:
    """Get the number of rows contained in the provided block."""
    return BlockAccessor.for_block(block).num_rows()


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
