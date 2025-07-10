from typing import Dict, Generic

from ray.data.block import Block, BlockAccessor, BlockType, T
from ray.data.expressions import Expr


class BlockBuilder(Generic[T]):
    """A builder class for blocks."""

    @staticmethod
    def for_block(block: Block) -> "BlockBuilder":
        return BlockAccessor.for_block(block).builder()

    def add(self, item: T) -> None:
        """Append a single row to the block being built."""
        raise NotImplementedError

    def add_block(self, block: Block) -> None:
        """Append an entire block to the block being built."""
        raise NotImplementedError

    def append_columns(self, block: Block, exprs: Dict[str, Expr]) -> Block:
        """Add columns from evaluated expressions to a new builder.

        Args:
            block: The source block to copy existing columns from
            exprs: A dictionary mapping new column names to expressions that
                define the column values.

        Returns:
            A new block with existing columns from block and new columns from expressions.
        """
        from ray.data._expression_evaluator import eval_expr

        # Extract existing columns directly from the block
        block_accessor = BlockAccessor.for_block(block)
        new_columns = {}
        for col_name in block_accessor.column_names():
            # For Arrow blocks, block[col_name] gives us a ChunkedArray
            # For Pandas blocks, block[col_name] gives us a Series
            new_columns[col_name] = block[col_name]

        # Add/update with expression results
        for name, expr in exprs.items():
            result = eval_expr(expr, block)
            new_columns[name] = result

        # Create a new block from the combined columns and add it
        new_block = BlockAccessor.batch_to_block(new_columns)

        return new_block

    def will_build_yield_copy(self) -> bool:
        """Whether building this block will yield a new block copy."""
        raise NotImplementedError

    def build(self) -> Block:
        """Build the block."""
        raise NotImplementedError

    def num_rows(self) -> int:
        """Return the number of rows added in the block."""
        raise NotImplementedError

    def get_estimated_memory_usage(self) -> int:
        """Return the estimated memory usage so far in bytes."""
        raise NotImplementedError

    def block_type(self) -> BlockType:
        """Return the block type."""
        raise NotImplementedError
