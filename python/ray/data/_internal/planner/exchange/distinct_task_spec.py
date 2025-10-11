from typing import List, Tuple, Union

from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    KeyType,
)


class DistinctTaskSpec(ExchangeTaskSpec):
    """The implementation for sort-based distinct (deduplication) tasks.

    Distinct is done in 2 steps: partial deduplication of individual blocks, and
    final deduplication of sorted blocks.

    Partial deduplication (`map`): each block is sorted locally, then partitioned into
    smaller blocks according to the boundaries. Each partitioned block is deduplicated
    separately (keeping only one row per unique key combination), then passed to a
    final deduplication task.

    Final deduplication (`reduce`): each task receives a block from every worker that
    consists of items in a certain range. It merges the sorted blocks and deduplicates
    on-the-fly, keeping only the first occurrence of each unique key combination.
    """

    def __init__(
        self,
        boundaries: List[KeyType],
        key: SortKey,
    ):
        super().__init__(
            map_args=[boundaries, key],
            reduce_args=[key],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[KeyType],
        sort_key: SortKey,
    ) -> List[Union[Block, "BlockMetadataWithSchema"]]:
        """Map phase: sort, partition, and deduplicate each block locally.
        
        Args:
            idx: Block index.
            block: The input block to process.
            output_num_blocks: Number of output blocks.
            boundaries: Partition boundaries for the sort key.
            sort_key: The key to use for sorting and deduplication.
        
        Returns:
            List of deduplicated partitioned blocks plus metadata.
        """
        stats = BlockExecStats.builder()

        # Sort and partition the block
        if sort_key.get_columns():
            partitions = BlockAccessor.for_block(block).sort_and_partition(
                boundaries,
                sort_key,
            )
        else:
            partitions = [block]
        
        # Deduplicate each partition by keeping only one row per unique key
        parts = [
            DistinctTaskSpec._deduplicate_sorted_block(p, sort_key)
            for p in partitions
        ]
        
        from ray.data.block import BlockMetadataWithSchema

        meta_with_schema = BlockMetadataWithSchema.from_block(
            block, stats=stats.build()
        )
        return parts + [meta_with_schema]

    @staticmethod
    def reduce(
        key: SortKey,
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, "BlockMetadataWithSchema"]:
        """Reduce phase: merge and deduplicate sorted blocks.
        
        Args:
            key: The key to use for deduplication.
            *mapper_outputs: Blocks from map tasks.
            partial_reduce: Whether this is a partial reduce.
        
        Returns:
            Tuple of deduplicated block and its metadata.
        """
        # Normalize block types to PyArrow tables for consistent processing
        normalized_blocks = TableBlockAccessor.normalize_block_types(
            mapper_outputs,
            target_block_type=ExchangeTaskSpec._derive_target_block_type("default"),
        )
        
        # Merge and deduplicate the blocks
        block = BlockAccessor.for_block(
            normalized_blocks[0]
        )._merge_sorted_blocks_and_keep_first(
            list(normalized_blocks), key
        )
        
        meta_with_schema = BlockMetadataWithSchema.from_block(block)
        return block, meta_with_schema

    @staticmethod
    def _deduplicate_sorted_block(block: Block, sort_key: SortKey) -> Block:
        """Deduplicate a sorted block by keeping only the first row for each unique key.
        
        Args:
            block: A block that is already sorted by the sort_key.
            sort_key: The key columns to use for identifying duplicates.
        
        Returns:
            A deduplicated block.
        """
        block_accessor = BlockAccessor.for_block(block)
        
        if block_accessor.num_rows() == 0:
            return block
        
        # Get the key columns
        keys = sort_key.get_columns()
        if not keys:
            # If no keys specified, all columns are used - just return first row
            return block_accessor.slice(0, 1, copy=False)
        
        # Get group boundaries and keep only first row of each group
        boundaries = block_accessor._get_group_boundaries_sorted(keys)
        
        # Build a new block with only the first row from each group
        builder = block_accessor.builder()
        for i in range(len(boundaries) - 1):
            # Get first row of each group
            first_row_idx = boundaries[i]
            row = block_accessor.slice(first_row_idx, first_row_idx + 1, copy=False)
            for r in BlockAccessor.for_block(row).iter_rows(public_row_format=False):
                builder.add(r)
        
        return builder.build()

