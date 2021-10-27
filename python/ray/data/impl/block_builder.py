from typing import Generic

from ray.data.block import Block, T


class BlockBuilder(Generic[T]):
    """A builder class for blocks."""

    def add(self, item: T) -> None:
        """Append a single row to the block being built."""
        raise NotImplementedError

    def add_block(self, block: Block) -> None:
        """Append an entire block to the block being built."""
        raise NotImplementedError

    def build(self) -> Block:
        """Build the block."""
        raise NotImplementedError

    def get_estimated_memory_usage(self) -> int:
        """Return the estimated memory usage so far in bytes."""
        raise NotImplementedError
