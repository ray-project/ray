from typing import List, Tuple

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.split import _calculate_blocks_rows, _split_at_indices
from ray.data.block import (
    Block,
    BlockMetadata,
    BlockPartition,
    _take_first_non_empty_schema,
)
from ray.types import ObjectRef


def _equalize(
    per_split_bundles: List[RefBundle],
    owned_by_consumer: bool,
) -> List[RefBundle]:
    """Equalize split ref bundles into equal number of rows.

    Args:
        per_split_bundles: ref bundles to equalize.
    Returns:
        the equalized ref bundles.
    """
    if len(per_split_bundles) == 0:
        return per_split_bundles
    per_split_blocks_with_metadata = [bundle.blocks for bundle in per_split_bundles]
    per_split_num_rows: List[List[int]] = [
        _calculate_blocks_rows(split) for split in per_split_blocks_with_metadata
    ]
    total_rows = sum([sum(blocks_rows) for blocks_rows in per_split_num_rows])
    target_split_size = total_rows // len(per_split_blocks_with_metadata)

    # phase 1: shave the current splits by dropping blocks (into leftovers)
    # and calculate num rows needed to the meet target.
    shaved_splits, per_split_needed_rows, leftovers = _shave_all_splits(
        per_split_blocks_with_metadata, per_split_num_rows, target_split_size
    )

    # validate invariants
    for shaved_split, split_needed_row in zip(shaved_splits, per_split_needed_rows):
        num_shaved_rows = sum([meta.num_rows for _, meta in shaved_split])
        assert num_shaved_rows <= target_split_size
        assert num_shaved_rows + split_needed_row == target_split_size

    # phase 2: based on the num rows needed for each shaved split, split the leftovers
    # in the shape that exactly matches the rows needed.
    schema = _take_first_non_empty_schema(bundle.schema for bundle in per_split_bundles)
    leftover_bundle = RefBundle(leftovers, owns_blocks=owned_by_consumer, schema=schema)
    leftover_splits = _split_leftovers(leftover_bundle, per_split_needed_rows)

    # phase 3: merge the shaved_splits and leftoever splits and return.
    for i, leftover_split in enumerate(leftover_splits):
        shaved_splits[i].extend(leftover_split)

        # validate invariants.
        num_shaved_rows = sum([meta.num_rows for _, meta in shaved_splits[i]])
        assert num_shaved_rows == target_split_size

    # Compose the result back to RefBundle
    equalized_ref_bundles: List[RefBundle] = []
    for split in shaved_splits:
        equalized_ref_bundles.append(
            RefBundle(split, owns_blocks=owned_by_consumer, schema=schema)
        )
    return equalized_ref_bundles


def _shave_one_split(
    split: BlockPartition, num_rows_per_block: List[int], target_size: int
) -> Tuple[BlockPartition, int, BlockPartition]:
    """Shave a block list to the target size.

    Args:
        split: the block list to shave.
        num_rows_per_block: num rows for each block in the list.
        target_size: the upper bound target size of the shaved list.
    Returns:
        A tuple of:
            - shaved block list.
            - num of rows needed for the block list to meet the target size.
            - leftover blocks.

    """
    # iterates through the blocks from the input list and
    shaved = []
    leftovers = []
    shaved_rows = 0
    for block_with_meta, block_rows in zip(split, num_rows_per_block):
        if block_rows + shaved_rows <= target_size:
            shaved.append(block_with_meta)
            shaved_rows += block_rows
        else:
            leftovers.append(block_with_meta)
    num_rows_needed = target_size - shaved_rows
    return shaved, num_rows_needed, leftovers


def _shave_all_splits(
    input_splits: List[BlockPartition],
    per_split_num_rows: List[List[int]],
    target_size: int,
) -> Tuple[List[BlockPartition], List[int], BlockPartition]:
    """Shave all block list to the target size.

    Args:
        input_splits: all block list to shave.
        input_splits: num rows (per block) for each block list.
        target_size: the upper bound target size of the shaved lists.
    Returns:
        A tuple of:
            - all shaved block list.
            - num of rows needed for the block list to meet the target size.
            - leftover blocks.
    """
    shaved_splits = []
    per_split_needed_rows = []
    leftovers = []

    for split, num_rows_per_block in zip(input_splits, per_split_num_rows):
        shaved, num_rows_needed, _leftovers = _shave_one_split(
            split, num_rows_per_block, target_size
        )
        shaved_splits.append(shaved)
        per_split_needed_rows.append(num_rows_needed)
        leftovers.extend(_leftovers)

    return shaved_splits, per_split_needed_rows, leftovers


def _split_leftovers(
    leftovers: RefBundle, per_split_needed_rows: List[int]
) -> List[BlockPartition]:
    """Split leftover blocks by the num of rows needed."""
    num_splits = len(per_split_needed_rows)
    split_indices = []
    prev = 0
    for i, num_rows_needed in enumerate(per_split_needed_rows):
        split_indices.append(prev + num_rows_needed)
        prev = split_indices[i]
    split_result: Tuple[
        List[List[ObjectRef[Block]]], List[List[BlockMetadata]]
    ] = _split_at_indices(
        leftovers.blocks,
        split_indices,
        leftovers.owns_blocks,
    )
    return [list(zip(block_refs, meta)) for block_refs, meta in zip(*split_result)][
        :num_splits
    ]
