import itertools
import logging
from typing import Union, Iterable, Tuple, List

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import (
    Block,
    BlockPartition,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
)
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


def _calculate_blocks_rows(
    blocks_with_metadata: BlockPartition,
) -> List[int]:
    """Calculate the number of rows for a list of blocks with metadata."""
    get_num_rows = cached_remote_fn(_get_num_rows)
    block_rows = []
    for block, metadata in blocks_with_metadata:
        if metadata.num_rows is None:
            # Need to fetch number of rows.
            num_rows = ray.get(get_num_rows.remote(block))
            metadata.num_rows = num_rows
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
    split_indices: List[int],
) -> Tuple[Union[Tuple[int, List[BlockMetadata]], Block], ...]:
    """Split the provided block at the given indices.

    Args:
        block_id: the id of this block in the block list.
        block: block to be split.
        meta: metadata of the block, we expect meta.num is valid.
        split_indices: the indices where the block should be split.
    Returns:
        returns block_id, split blocks metadata, and a list of blocks
        in the following form. We return blocks in this way
        so that the owner of blocks could be the caller(driver)
        instead of worker itself.
        Tuple(block_id, split_blocks_meta), block0, block1 ...
    """
    split_meta = []
    split_blocks = []
    block_accessor = BlockAccessor.for_block(block)
    prev_index = 0
    # append one more entry at the last so we don't
    # need handle empty edge case.
    split_indices.append(meta.num_rows)
    for index in split_indices:
        logger.debug(f"slicing block {prev_index}:{index}")
        stats = BlockExecStats.builder()
        split_block = block_accessor.slice(prev_index, index)
        accessor = BlockAccessor.for_block(split_block)
        _meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
        split_meta.append(_meta)
        split_blocks.append(split_block)
        prev_index = index
    results = [(block_id, split_meta)]
    results.extend(split_blocks)
    return tuple(results)


def _drop_empty_block_split(block_split_indices: List[int], num_rows: int) -> List[int]:
    """drop split indices that creates empty block split. This could happen when there
    are duplicated indices, or index equal to 0 (start of the block) or num_block_rows
    (end of the block).
    """
    prev_index = -1
    optimized_indices = []
    for index in block_split_indices:
        if index == 0 or index == num_rows:
            continue
        if index == prev_index:
            continue
        optimized_indices.append(index)
        prev_index = index
    return optimized_indices


def _split_all_blocks(
    block_list: BlockList,
    per_block_split_indices: List[List[int]],
) -> Iterable[Tuple[ObjectRef[Block], BlockMetadata]]:
    """Split all the input blocks based on the split indices"""
    split_single_block = cached_remote_fn(_split_single_block)

    blocks_with_metadata = block_list.get_blocks_with_metadata()
    all_blocks_split_results: List[BlockPartition] = [None] * len(blocks_with_metadata)

    per_block_split_metadata_futures = []
    per_block_split_block_refs = []

    # tracking splitted blocks for gc.
    blocks_splitted = []
    for block_id, block_split_indices in enumerate(per_block_split_indices):
        (block_ref, meta) = blocks_with_metadata[block_id]
        block_row = meta.num_rows
        block_split_indices = _drop_empty_block_split(block_split_indices, block_row)
        if len(block_split_indices) == 0:
            # optimization: if no split is needed, we just need to add it to the
            # result
            all_blocks_split_results[block_id] = [(block_ref, meta)]
        else:
            # otherwise call split remote function.
            object_refs = split_single_block.options(
                scheduling_strategy="SPREAD", num_returns=2 + len(block_split_indices)
            ).remote(
                block_id,
                block_ref,
                meta,
                block_split_indices,
            )
            per_block_split_metadata_futures.append(object_refs[0])
            per_block_split_block_refs.append(object_refs[1:])

            blocks_splitted.append(block_ref)

    if per_block_split_metadata_futures:
        # only get metadata.
        per_block_split_metadata = ray.get(per_block_split_metadata_futures)
        for (block_id, meta), block_refs in zip(
            per_block_split_metadata, per_block_split_block_refs
        ):
            assert len(meta) == len(block_refs)
            all_blocks_split_results[block_id] = zip(block_refs, meta)

    # We make a copy for the blocks that have been splitted, so the input blocks
    # can be cleared if they are owned by consumer (consumer-owned blocks will
    # only be consumed by the owner).
    if block_list._owned_by_consumer:
        ray._private.internal_api.free(blocks_splitted, local_only=False)

    return itertools.chain.from_iterable(all_blocks_split_results)


def _generate_global_split_results(
    all_blocks_split_results: Iterable[Tuple[ObjectRef[Block], BlockMetadata]],
    global_split_sizes: List[int],
) -> Tuple[List[List[ObjectRef[Block]]], List[List[BlockMetadata]]]:
    """Reassemble per block's split result into final split result."""
    result_blocks = []
    result_metas = []

    current_blocks = []
    current_meta = []
    current_split_size = 0
    current_split_id = 0

    while current_split_id < len(global_split_sizes):
        if current_split_size >= global_split_sizes[current_split_id]:
            assert current_split_size == global_split_sizes[current_split_id]
            result_blocks.append(current_blocks)
            result_metas.append(current_meta)

            current_blocks = []
            current_meta = []
            current_split_size = 0
            current_split_id += 1
        else:
            (block_ref, meta) = next(all_blocks_split_results)
            current_blocks.append(block_ref)
            current_meta.append(meta)
            current_split_size += meta.num_rows

    return result_blocks, result_metas


def _split_at_indices(
    block_list: BlockList,
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

    blocks_with_metadata = block_list.get_blocks_with_metadata()
    # We implement the split in 3 phases.
    # phase 1: calculate the per block split indices.
    blocks_with_metadata = list(blocks_with_metadata)
    if len(blocks_with_metadata) == 0:
        return ([[]] * (len(indices) + 1), [[]] * (len(indices) + 1))
    block_rows: List[int] = _calculate_blocks_rows(blocks_with_metadata)
    valid_indices = _generate_valid_indices(block_rows, indices)
    per_block_split_indices: List[List[int]] = _generate_per_block_split_indices(
        block_rows, valid_indices
    )

    # phase 2: split each block based on the indices from previous step.
    all_blocks_split_results: Iterable[
        Tuple[ObjectRef[Block], BlockMetadata]
    ] = _split_all_blocks(block_list, per_block_split_indices)

    # phase 3: generate the final split.

    # first calculate the size for each split.
    helper = [0] + valid_indices + [sum(block_rows)]
    split_sizes = [helper[i] - helper[i - 1] for i in range(1, len(helper))]

    return _generate_global_split_results(all_blocks_split_results, split_sizes)


def _get_num_rows(block: Block) -> int:
    """Get the number of rows contained in the provided block."""
    return BlockAccessor.for_block(block).num_rows()


def _split_at_index(
    block_list: BlockList,
    index: int,
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
    Returns:
        The block split futures and their metadata for left and right of the index.
    """
    blocks_splits, metadata_splits = _split_at_indices(block_list, [index])
    return blocks_splits[0], metadata_splits[0], blocks_splits[1], metadata_splits[1]
