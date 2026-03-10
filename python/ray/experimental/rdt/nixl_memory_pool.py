"""Memory pool management for NIXL RDT optimization."""

from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    import torch


class MemoryBlock:
    """Represents a memory block in the pool."""

    def __init__(self, offset: int, size: int):
        self.offset = offset
        self.size = size

    def __repr__(self):
        return f"MemoryBlock(offset={self.offset}, size={self.size})"


class MemoryPoolManager:
    """Manages a pre-allocated memory pool for NIXL RDT transfers.

    This class provides a memory allocator interface over a pre-allocated memory pool,
    allowing reuse of registered memory descriptors across multiple transfers.
    """

    def __init__(self, pool_size: int, device: "torch.device"):
        """Initialize the memory pool manager.

        Args:
            pool_size: Size of the memory pool in bytes.
            device: Device to allocate the pool on.
        """
        import torch

        self.pool_size = pool_size
        self.device = device

        # Allocate the memory pool as a single tensor
        # We use a 1D tensor of uint8 to represent raw memory
        self._pool_tensor = torch.zeros(
            pool_size, dtype=torch.uint8, device=self.device
        )

        # Track free blocks using a first-fit allocator
        # List of (offset, size) tuples for free blocks, sorted by offset
        self._free_blocks: List[MemoryBlock] = [MemoryBlock(offset=0, size=pool_size)]

    def get_pool_tensor(self) -> "torch.Tensor":
        """Get the underlying pool tensor.

        Returns:
            The pre-allocated tensor representing the memory pool.
        """
        return self._pool_tensor

    def allocate_multiple(self, sizes: List[int]) -> Optional[List[Tuple[int, int]]]:
        """Allocate multiple memory blocks from the pool atomically.

        Either all allocations succeed, or none of them do.

        Args:
            sizes: List of sizes to allocate in bytes.

        Returns:
            List of (offset, size) tuples if all allocations succeed, None otherwise.
        """
        if not sizes or any(s <= 0 for s in sizes):
            return None

        # Allocate largest first to reduce fragmentation; then return in original order.
        order = sorted(range(len(sizes)), key=lambda i: -sizes[i])
        sorted_sizes = [sizes[i] for i in order]

        # Try to allocate all blocks atomically.
        allocations: List[Tuple[int, int]] = []
        temp_free_blocks = [MemoryBlock(b.offset, b.size) for b in self._free_blocks]

        for size in sorted_sizes:
            allocated = False
            for i, block in enumerate(temp_free_blocks):
                if block.size >= size:
                    # Allocate at the start of the current free block
                    offset = block.offset
                    remaining_after = block.size - size

                    if remaining_after == 0:
                        temp_free_blocks.pop(i)
                    else:
                        block.offset = offset + size
                        block.size = remaining_after

                    allocations.append((offset, size))
                    allocated = True
                    break

            if not allocated:
                # If any size cannot be allocated, the entire batch fails, do not modify the real state
                return None

        # Reorder allocations back to original request order
        result: List[Tuple[int, int]] = [(0, 0)] * len(sizes)
        for k, (offset, size) in enumerate(allocations):
            result[order[k]] = (offset, size)

        # All successful, submit modifications
        temp_free_blocks.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free_blocks

        return result

    def free_multiple(self, offsets: List[int], sizes: List[int]) -> None:
        """Free multiple memory blocks back to the pool.

        Args:
            offsets: Offsets of the memory blocks to free.
            sizes: Sizes of the memory blocks to free (same length as offsets).

        Returns:
            None.
        """
        for offset, size in zip(offsets, sizes):
            self._free_blocks.append(MemoryBlock(offset=offset, size=size))

        if not offsets:
            return
        # Single pass: merge all adjacent free blocks
        self._free_blocks.sort(key=lambda b: b.offset)
        i = 0
        while i < len(self._free_blocks) - 1:
            curr = self._free_blocks[i]
            next_block = self._free_blocks[i + 1]
            if curr.offset + curr.size == next_block.offset:
                curr.size += next_block.size
                self._free_blocks.pop(i + 1)
            else:
                i += 1

    def copy_to_pool(self, tensor: "torch.Tensor", offset: int) -> None:
        """Copy tensor data to the memory pool at the specified offset.

        Args:
            tensor: Source tensor to copy from.
            offset: Destination offset in the memory pool (bytes).

        Returns:
            None.
        """
        bytes_to_copy = tensor.numel() * tensor.element_size()

        flat_tensor = tensor.contiguous().view(-1)
        pool_bytes = self._pool_tensor[offset : offset + bytes_to_copy]
        pool_view = pool_bytes.view(tensor.dtype)
        pool_view.copy_(flat_tensor.to(dtype=tensor.dtype))
