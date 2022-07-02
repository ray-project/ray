import collections
import logging
from typing import Iterable, Tuple, List, Optional

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
)
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


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
    get_num_rows = cached_remote_fn(_get_num_rows)
    split_block_at_index = cached_remote_fn(_split_block_at_index, num_returns=4)

    count = 0
    left_blocks = []
    left_metadata = []
    right_blocks = []
    right_metadata = []
    for b, m in blocks_with_metadata:
        if m.num_rows is None:
            num_rows = ray.get(get_num_rows.remote(b))
        else:
            num_rows = m.num_rows
        if count >= index:
            if not return_right_half:
                break
            right_blocks.append(b)
            right_metadata.append(m)
        elif count + num_rows <= index:
            left_blocks.append(b)
            left_metadata.append(m)
        else:
            b0, m0, b1, m1 = split_block_at_index.remote(
                b, m, index - count, return_right_half
            )
            left_blocks.append(b0)
            left_metadata.append(ray.get(m0))
            right_blocks.append(b1)
            right_metadata.append(ray.get(m1))
        count += num_rows
    return left_blocks, left_metadata, right_blocks, right_metadata


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
    # Implementation note: This splitting algorithm attempts to minimize the number of
    # split tasks submitted and the amount of data moved/copied, such that:
    #  - blocks that fit entirely within a split will be untouched,
    #  - a block that constitutes multiple splits will be split via a single task with
    #  multiple split returns,
    #  - repeated splitting of the same block (via recursive splitting of leftovers)
    #  should be minimal (a few cases remain but should only arise with very skewed
    #  splits; see TODOs below),
    #  - each split task will involve at most 1 block, for the sake of memory stability.

    # The end-result is the number of split tasks should be
    # O(min(num_splits, num_blocks)); explicitly,
    #  - For split sizes >> block sizes (common case), split tasks will only be launched
    #  and data will only be moved/copied for blocks crossing split boundaries, yielding
    #  minimal number of tasks and amount of data movement.
    #  - For split sizes << block sizes, 1-2 split tasks per block can be expected.
    get_num_rows = cached_remote_fn(_get_num_rows)
    split_block_at_index = cached_remote_fn(_split_block_at_index, num_returns=4)
    split_block_at_indices = cached_remote_fn(_split_block_at_indices)

    # Add infinite boundary index, which simplifies the splitting algorithm below.
    indices = indices + [float("inf")]

    # Queue of blocks that still need to be mapped to a split.
    blocks = collections.deque(blocks_with_metadata)
    # Block and metadata buffers for the current split.
    block_buffer = []
    metadata_buffer = []
    block_buffer_size = 0
    # Buffer of indices that will split a single block into multiple splits.
    index_buffer = []
    index_buffer_size = 0
    # Output blocks and metadata.
    out_blocks = []
    out_metadata = []
    # Running total of rows added to the output.
    total_num_rows = 0
    # indices_ = collections.deque(indices)
    prev_idx = 0
    i = 0

    # Loop invariant:
    #  - Index queue decreases in size OR block queue decreases in size.
    #    - If the index head and block head are the same size, both are popped.
    #    - If the block head fits completely into index head, block is popped and added
    #    to block buffer.
    #    - If the index head fits completely into block head, index is popped and added
    #    to index buffer.
    #  - If index buffer nonempty, block at head of queue must be getting split on
    #  multiple indices. If current index fits, add to index buffer; otherwise, finalize
    #  index buffer, removing block from head of queue and adding any leftover slice to
    #  head of queue.
    #  - If block buffer nonempty, index at head of queue must consist of multiple
    #  to-be-merged blocks. If current block fits, add to block buffer; otherwise,
    #  finalize block buffer, adding slice of current block to complete index slice and
    #  adding any leftover slice to head of queue.
    #  - Only one of the index buffer and block buffer will be nonempty on a given loop.

    # Case coverage:
    # 1. If current block is empty, ignore it.
    # 2. If current split is empty, add empty split to output.
    # 3. If current block + block buffer fits current split exactly, add block + buffer
    # to output. Advance block and index pointers.
    # 4. If current block + block buffer is greater than current split, slice off
    # remainder and add to front of block queue, and add block + buffer to output.
    # Advance block and index pointers.
    # 5. If current block + block buffer is less than current split, add current block
    # to the block buffer. Advance block pointer.
    # 6. If current index + index buffer fits current block exactly, split block on
    # index + index buffer and add splits to output. Advance block and index pointers.
    # 7. If current index + index buffer is greater than current block, split block on
    # index buffer, add remainder to front of block queue, and add splits to output.
    # Advance block pointer.
    # 8. If current index + index buffer is less than current block, add current index
    # to index buffer. Advance index pointer.

    # (2) can only be done eagerly if the index buffer is currently empty, otherwise the
    # split ordering will be incorrect; if the index buffer is nonempty, the empty split
    # will have to be done as part of (6) or (7).
    while i < len(indices):
        idx = indices[i]
        curr_split_len = idx - prev_idx
        if not blocks:
            # If block queue is empty, add empty splits until all split indices have
            # been accounted for.
            out_blocks.append([])
            out_metadata.append([])
            prev_idx = idx
            i += 1
            continue
        b, m = blocks.popleft()
        if m.num_rows is None:
            # Need to fetch number of rows.
            num_rows = ray.get(get_num_rows.remote(b))
        else:
            num_rows = m.num_rows
        if num_rows == 0:
            # Ignore empty blocks.
            continue
        if curr_split_len == float("inf") and not blocks:
            # At final split and final block, so final split length is now known.
            idx = total_num_rows + block_buffer_size + num_rows
            curr_split_len = idx - prev_idx
            indices[i] = idx
        # Whether we should move on to the next split index.
        should_advance_split = True
        # Whether we should move on to the next block in the queue.
        should_advance_block = True
        # Whether we have a full split that we can add to the output.
        has_output = False
        if index_buffer_size > 0 or index_buffer:
            # Building an index buffer for slicing a block into multiple splits.
            assert block_buffer_size == 0
            if curr_split_len + index_buffer_size == num_rows:
                # Add current split to index buffer.
                index_buffer.append(i)
                index_buffer_size += curr_split_len
                # Split block on indices in index buffer and add splits to output.
                has_output = True
            elif curr_split_len + index_buffer_size < num_rows:
                # Add current split to index buffer.
                index_buffer.append(i)
                index_buffer_size += curr_split_len
                # Move to next split, which may or may ont fit into this block as well.
                should_advance_block = False
            else:
                # Split block on indices in index buffer, adding remainder to front of
                # block queue, and add splits to output. Current split doesn't fit in
                # the block, so it will be processed on the next loop.
                should_advance_split = False
                has_output = True
        elif block_buffer_size > 0:
            # Building a block buffer for creating a split out of multiple blocks.
            if block_buffer_size + num_rows == curr_split_len:
                # Block fits in split so add block to buffer.
                block_buffer.append(b)
                metadata_buffer.append(m)
                block_buffer_size += num_rows
                # We have a complete split, so add block buffer as split to output.
                has_output = True
            elif block_buffer_size + num_rows < curr_split_len:
                # Block fits in split so add block to buffer.
                block_buffer.append(b)
                metadata_buffer.append(m)
                block_buffer_size += num_rows
                # We don't have a complete split. If we have remaining blocks, advance
                # to next block and attempt to add it to this split; otherwise, consider
                # this split complete.
                should_advance_split = not blocks
                has_output = not blocks
            else:
                # Need to slice block for it to fit for this split. Slice the needed bit
                # from the block and add it to the block buffer, putting the rest back
                # into the block queue.
                # TODO(Clark): Delay this split, since we will be able to absorb it into
                # a downstream split_at_indices if the next index would split the
                # remainder.
                b0, m0, b1, m1 = split_block_at_index.remote(
                    b,
                    m,
                    curr_split_len - block_buffer_size,
                    True,
                )
                block_buffer.append(b0)
                m0 = ray.get(m0)
                metadata_buffer.append(m0)
                block_buffer_size += m0.num_rows
                # Add remainder to the front of the block queue.
                blocks.appendleft((b1, ray.get(m1)))
                # We have a complete split after adding the slice, so add the block
                # buffer as split to output.
                has_output = True
        else:
            # No existing block or index buffer, so we can consider this block in
            # isolation and either...
            # (1) delay its processing until the next split if the current split is
            # empty...
            if curr_split_len == 0:
                should_advance_block = False
                has_output = True
            # (2) add it as a split if it's equal to the split length...
            elif num_rows == curr_split_len:
                # Block fits perfectly into the split, add block to buffer.
                block_buffer.append(b)
                metadata_buffer.append(m)
                block_buffer_size += num_rows
                # We have a complete split, so we add this single block as a split to
                # the output.
                has_output = True
            # (3) add it to the block buffer if it's smaller than the split length...
            elif num_rows < curr_split_len:
                # Block fits into split, so add block to buffer.
                block_buffer.append(b)
                metadata_buffer.append(m)
                block_buffer_size += num_rows
                # We don't have a complete split. If we have remaining blocks, advance
                # to next block and attempt to add it to this split; otherwise, consider
                # this split complete.
                should_advance_split = not blocks
                has_output = not blocks
            # (4) add the index to the index buffer if the block is larger than the
            # split length.
            else:
                # Block is larger than the current split, so we add the current split
                # index to the index buffer.
                index_buffer.append(i)
                index_buffer_size += curr_split_len
                # We still have slack in the block, so we proceed to the next split.
                should_advance_block = False
        # If any of the above cases indicate that we are ready to add one or more splits
        # to the output, do so.
        if has_output:
            if index_buffer_size > 0:
                # We're splitting a block into multiple splits whose indices are given
                # in the index buffer.
                # Convert indices in index buffer to in-block indices, accounting for
                # the base offset.
                block_split_indices = [0]
                base_offset = indices[index_buffer[0] - 1] if index_buffer[0] > 0 else 0
                for j in index_buffer:
                    block_split_indices.append(indices[j] - base_offset)
                if block_split_indices[-1] < num_rows:
                    # Always add an end-of-block boundary index; if this split doesn't
                    # correspond to an index in the index buffer, it will be added back
                    # to the block queue.
                    block_split_indices.append(num_rows)
                    has_leftover = True
                else:
                    # No leftover split.
                    has_leftover = False
                num_returns = 2 * (len(block_split_indices) - 1)
                block_splits = split_block_at_indices.options(
                    num_returns=num_returns,
                ).remote(b, m.input_files, block_split_indices)
                if has_leftover:
                    # Add any leftovers to the queue.
                    # TODO(Clark): This leftover could get repeatedly split if splits
                    # are much smaller than blocks.
                    leftover_meta = ray.get(block_splits.pop(-1))
                    leftover_block = block_splits.pop(-1)
                    blocks.appendleft((leftover_block, leftover_meta))
                split_blocks, split_metadata = [], []
                for block, meta in zip(block_splits[::2], block_splits[1::2]):
                    split_blocks.append(block)
                    split_metadata.append(meta)
                # Fetch metadata for split blocks.
                split_metadata = ray.get(split_metadata)
                # Add splits to output.
                for block, meta in zip(split_blocks, split_metadata):
                    total_num_rows += meta.num_rows
                    if meta.num_rows > 0:
                        out_blocks.append([block])
                        out_metadata.append([meta])
                    else:
                        out_blocks.append([])
                        out_metadata.append([])
                # Reset index buffer.
                index_buffer = []
                index_buffer_size = 0
            elif block_buffer_size > 0:
                # We're using the blocks in the block buffer to create a split.
                # Add block buffer to output.
                out_blocks.append(block_buffer)
                out_metadata.append(metadata_buffer)
                total_num_rows += block_buffer_size
                # Reset block buffer.
                block_buffer = []
                metadata_buffer = []
                block_buffer_size = 0
            else:
                # No index buffer or block buffer, so split must be empty.
                out_blocks.append([])
                out_metadata.append([])
        if should_advance_split:
            # Move to the next split index.
            prev_idx = idx
            i += 1
        if not should_advance_block:
            # Add block back to the queue.
            blocks.appendleft((b, m))
    return out_blocks, out_metadata


def _split_block_at_index(
    block: Block, meta: BlockMetadata, index: int, return_right_half: bool
) -> Tuple[Block, BlockMetadata, Optional[Block], Optional[BlockMetadata]]:
    """Split the provided block at the given row index."""
    stats = BlockExecStats.builder()
    block = BlockAccessor.for_block(block)
    logger.debug("Truncating last block to size: {}".format(index))
    b0 = block.slice(0, index, copy=True)
    a0 = BlockAccessor.for_block(b0)
    m0 = BlockMetadata(
        num_rows=a0.num_rows(),
        size_bytes=a0.size_bytes(),
        schema=meta.schema,
        input_files=meta.input_files,
        exec_stats=stats.build(),
    )
    if return_right_half:
        b1 = block.slice(index, block.num_rows(), copy=True)
        a1 = BlockAccessor.for_block(b1)
        m1 = BlockMetadata(
            num_rows=a1.num_rows(),
            size_bytes=a1.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
    else:
        b1 = None
        m1 = None
    return b0, m0, b1, m1


def _split_block_at_indices(block: Block, input_files: List[str], indices: List[int]):
    """Split the block at the provided indices, producing len(indices) - 1
    blocks. If given indices [a, b, c, d], this will return splits
    [a, b), [b, c), and [c, d).
    """
    block = BlockAccessor.for_block(block)
    assert len(indices) >= 2
    out = []
    for low, high in zip(indices[:-1], indices[1:]):
        stats = BlockExecStats.builder()
        b = block.slice(low, high, copy=True)
        a = BlockAccessor.for_block(b)
        m = BlockMetadata(
            num_rows=a.num_rows(),
            size_bytes=a.size_bytes(),
            schema=a.schema(),
            input_files=input_files,
            exec_stats=stats.build(),
        )
        out.append(b)
        out.append(m)
    return out


def _merge_blocks(input_files: List[str], *blocks: List[Block]):
    """Merge the provided blocks into a single block."""
    stats = BlockExecStats.builder()
    builder = DelegatingBlockBuilder()
    for block in blocks:
        builder.add_block(block)
    out_block = builder.build()
    return out_block, BlockAccessor.for_block(out_block).get_metadata(
        input_files, stats.build()
    )


def _get_num_rows(block: Block) -> int:
    """Get the number of rows contained in the provided block."""
    return BlockAccessor.for_block(block).num_rows()
