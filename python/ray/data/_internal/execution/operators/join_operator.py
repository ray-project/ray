import itertools
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata, to_stats
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

class JoinOperator(PhysicalOperator):
    """An operator that joins two inputs based on a key."""

    def __init__(
        self,
        left_input_op: PhysicalOperator,
        right_input_op: PhysicalOperator,
        left_on: Union[str, List[str]],
        right_on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
        data_context: DataContext = None,
    ):
        """Create a JoinOperator.

        Args:
            left_input_op: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
            left_on: The column(s) to join on from the left dataset
            right_on: The column(s) to join on from the right dataset. If None,
                use left_on for both datasets.
            how: The type of join to perform. Options are:
                - "inner": return records that have matching keys in both datasets
                - "left_outer": return all records from left and matched records from right
                - "right_outer": return matched records from left and all records from right
                - "full_outer": return all records from both datasets
        """
        self._left_on = [left_on] if isinstance(left_on, str) else left_on
        self._right_on = right_on
        if self._right_on is None:
            self._right_on = self._left_on
        else:
            self._right_on = [self._right_on] if isinstance(self._right_on, str) else self._right_on
        
        if len(self._left_on) != len(self._right_on):
            raise ValueError(
                f"left_on and right_on must have the same length. "
                f"Got {self._left_on} and {self._right_on}"
            )
        
        self._how = how
        if how not in ["inner", "left_outer", "right_outer", "full_outer"]:
            raise ValueError(
                f"Invalid join type: {how}. Must be one of: "
                f"inner, left_outer, right_outer, full_outer"
            )
        
        self._left_buffer: List[RefBundle] = []
        self._right_buffer: List[RefBundle] = []
        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {}
        
        super().__init__(
            "Join",
            [left_input_op, right_input_op],
            data_context,
            target_max_block_size=None,
        )

    def num_outputs_total(self) -> Optional[int]:
        # Hard to predict the exact output size for a join
        return None

    def num_output_rows_total(self) -> Optional[int]:
        # Hard to predict the exact number of output rows for a join
        return None

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0 or input_index == 1, input_index
        if input_index == 0:
            self._left_buffer.append(refs)
            self._metrics.on_input_queued(refs)
        else:
            self._right_buffer.append(refs)
            self._metrics.on_input_queued(refs)

    def all_inputs_done(self) -> None:
        self._output_buffer, self._stats = self._join(
            self._left_buffer, self._right_buffer, self._left_on, self._right_on, self._how
        )

        while self._left_buffer:
            refs = self._left_buffer.pop()
            self._metrics.on_input_dequeued(refs)
        while self._right_buffer:
            refs = self._right_buffer.pop()
            self._metrics.on_input_dequeued(refs)
        for ref in self._output_buffer:
            self._metrics.on_output_queued(ref)

        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.pop(0)
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        return self._stats

    def implements_accurate_memory_accounting(self):
        return True

    def _join(
        self, 
        left_input: List[RefBundle], 
        right_input: List[RefBundle],
        left_on: List[str],
        right_on: List[str],
        how: str
    ) -> Tuple[List[RefBundle], StatsDict]:
        """Join the RefBundles based on key columns."""
        # Collect all blocks and metadata into flat lists
        left_blocks_with_metadata = []
        for bundle in left_input:
            for block, meta in bundle.blocks:
                left_blocks_with_metadata.append((block, meta))
        
        right_blocks_with_metadata = []
        for bundle in right_input:
            for block, meta in bundle.blocks:
                right_blocks_with_metadata.append((block, meta))

        # First, we'll create a hash table of right blocks by join key
        hash_join_remote = cached_remote_fn(_hash_join_blocks)
        
        # Build a multi-block hash join
        output_blocks = []
        output_metadata = []
        
        # Process hash join as a many to many operation
        for left_block, left_meta in left_blocks_with_metadata:
            for right_block, right_meta in right_blocks_with_metadata:
                res, meta = hash_join_remote.remote(
                    left_block, right_block, left_on, right_on, how
                )
                output_blocks.append(res)
                output_metadata.append(meta)

        # Retrieve metadata
        output_metadata = ray.get(output_metadata)
        
        # Create output ref bundles 
        output_refs = []
        input_owned = all(b.owns_blocks for b in left_input)
        for block, meta in zip(output_blocks, output_metadata):
            output_refs.append(
                RefBundle(
                    [
                        (
                            block,
                            meta,
                        )
                    ],
                    owns_blocks=input_owned,
                )
            )
        
        stats = {self._name: to_stats(output_metadata)}

        # Clean up inputs
        for ref in left_input:
            ref.destroy_if_owned()
        for ref in right_input:
            ref.destroy_if_owned()

        return output_refs, stats


def _hash_join_blocks(
    left_block: Block,
    right_block: Block,
    left_on: List[str],
    right_on: List[str],
    how: str
) -> Tuple[Block, BlockMetadata]:
    """Perform hash join on two blocks."""
    stats = BlockExecStats.builder()
    
    left_accessor = BlockAccessor.for_block(left_block)
    right_accessor = BlockAccessor.for_block(right_block)
    
    # Convert blocks to pandas for the join operation
    left_df = left_accessor.to_pandas()
    right_df = right_accessor.to_pandas()
    
    # If the blocks are empty, return an empty result
    if len(left_df) == 0 and how in ["inner", "right_outer"]:
        # For inner or right join with empty left df, return empty block
        empty_block = left_accessor._empty_table()
        empty_accessor = BlockAccessor.for_block(empty_block)
        return empty_block, empty_accessor.get_metadata(exec_stats=stats.build())
    
    if len(right_df) == 0 and how in ["inner", "left_outer"]:
        # For inner or left join with empty right df, return empty block
        empty_block = left_accessor._empty_table()
        empty_accessor = BlockAccessor.for_block(empty_block)
        return empty_block, empty_accessor.get_metadata(exec_stats=stats.build())
    
    # Add suffixes if there are duplicate columns
    suffixes = ("", "_right")
    
    # Handle the default case by implementing a hash join manually
    # Build hash table on right side
    if how == "inner":
        # For inner joins, include only matches
        result_df = left_df.merge(
            right_df, 
            left_on=left_on, 
            right_on=right_on,
            how="inner",
            suffixes=suffixes
        )
    elif how == "left_outer":
        # For left outer joins, include all left rows
        result_df = left_df.merge(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how="left",
            suffixes=suffixes
        )
    elif how == "right_outer":
        # For right outer joins, include all right rows
        result_df = left_df.merge(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how="right",
            suffixes=suffixes
        )
    elif how == "full_outer":
        # For full outer joins, include all rows from both
        result_df = left_df.merge(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how="outer",
            suffixes=suffixes
        )
    
    # Convert the joined result back to a block
    if len(result_df) == 0:
        # Handle empty result
        empty_block = left_accessor._empty_table()
        empty_accessor = BlockAccessor.for_block(empty_block)
        return empty_block, empty_accessor.get_metadata(exec_stats=stats.build())
    
    # Convert the pandas DataFrame to the appropriate block type
    if isinstance(left_block, type(right_block)):
        # If blocks are the same type, convert to that type
        if hasattr(BlockAccessor.for_block(left_block), "_pandas_to_block"):
            result_block = BlockAccessor.for_block(left_block)._pandas_to_block(result_df)
        else:
            # Default to arrow table
            result_block = BlockAccessor.batch_to_block(result_df)
    else:
        # Default to arrow table for mixed types
        result_block = BlockAccessor.batch_to_block(result_df)
    
    # Get metadata for the result block
    result_acc = BlockAccessor.for_block(result_block)
    return result_block, result_acc.get_metadata(exec_stats=stats.build())