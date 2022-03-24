from typing import Generic

from ray.data.block import Block, BlockAccessor, T


class BlockBuilder(Generic[T]):
    """A builder class for blocks."""

    @staticmethod
    def for_block(block: Block) -> "BlockBuilder[T]":
        return BlockAccessor.for_block(block).builder()

    @staticmethod
    def for_format(dataset_format: str) -> "BlockBuilder[T]":
        if dataset_format == "arrow":
            from ray.data.impl.arrow_block import ArrowBlockBuilder

            return ArrowBlockBuilder()
        elif dataset_format == "pandas":
            from ray.data.impl.pandas_block import PandasBlockBuilder

            return PandasBlockBuilder()
        elif dataset_format == "simple":
            from ray.data.impl.simple_block import SimpleBlockBuilder

            return SimpleBlockBuilder()
        else:
            raise ValueError(
                'Valid dataset formats are "arrow", "pandas", and "simple", got '
                f'"{dataset_format}"'
            )

    def add(self, item: T) -> None:
        """Append a single row to the block being built."""
        raise NotImplementedError

    def add_block(self, block: Block) -> None:
        """Append an entire block to the block being built."""
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
