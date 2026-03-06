"""Memory pool management for NIXL RDT optimization."""

import os
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

if TYPE_CHECKING:
    import torch
else:
    try:
        import torch
    except ImportError:
        torch = None


class MemoryBlock:
    """Represents a memory block in the pool."""

    def __init__(self, offset: int, size: int, is_free: bool = True):
        self.offset = offset
        self.size = size
        self.is_free = is_free

    def __repr__(self):
        return (
            f"MemoryBlock(offset={self.offset}, size={self.size}, free={self.is_free})"
        )


class MemoryPoolManager:
    """Manages a pre-allocated memory pool for NIXL RDT transfers.

    This class provides a memory allocator interface over a pre-allocated GPU memory pool,
    allowing reuse of registered NIXL memory descriptors across multiple transfers.
    """

    def __init__(
        self, pool_size: int, device: Optional["torch.device"] = torch.device("cpu")
    ):
        """Initialize the memory pool manager.

        Args:
            pool_size: Size of the memory pool in bytes.
            device: Device to allocate the pool on. If None, uses CPU.
        """
        self.pool_size = pool_size
        self.device = device

        # Allocate the memory pool as a single tensor
        # We use a 1D tensor of uint8 to represent raw memory
        self._pool_tensor = torch.zeros(
            pool_size, dtype=torch.uint8, device=self.device
        )

        # Track free blocks using a simple first-fit allocator
        # List of (offset, size) tuples for free blocks, sorted by offset
        self._free_blocks: List[MemoryBlock] = [
            MemoryBlock(offset=0, size=pool_size, is_free=True)
        ]

        # Track allocated blocks for debugging and validation
        self._allocated_blocks: List[MemoryBlock] = []

    def get_pool_tensor(self) -> "torch.Tensor":
        """Get the underlying pool tensor.

        Returns:
            The pre-allocated tensor representing the memory pool.
        """
        return self._pool_tensor

    def allocate_multiple(self, sizes: List[int]) -> Optional[List[Tuple[int, int]]]:
        """Allocate multiple memory blocks from the pool atomically.

        Either all allocations succeed, or none of them do. This ensures consistency
        - if we can't allocate space for all tensors, we fall back to traditional mode
        for all of them.

        Args:
            sizes: List of sizes to allocate in bytes.

        Returns:
            List of (offset, size) tuples if all allocations succeed, None otherwise.
        """
        if not sizes or any(s <= 0 for s in sizes):
            return None

        # Try to allocate all blocks atomically.
        allocations: List[Tuple[int, int]] = []
        temp_free_blocks = [
            MemoryBlock(b.offset, b.size, b.is_free) for b in self._free_blocks
        ]
        temp_allocated_blocks = list(self._allocated_blocks)

        for size in sizes:
            allocated = False
            for i, block in enumerate(temp_free_blocks):
                if not block.is_free:
                    continue

                if block.size >= size:
                    # Allocate at the start of the current free block
                    offset = block.offset

                    # Update / delete the current free block
                    remaining_after = block.size - size
                    temp_free_blocks.pop(i)
                    if remaining_after > 0:
                        new_free = MemoryBlock(
                            offset=offset + size,
                            size=remaining_after,
                            is_free=True,
                        )
                        # Keep sorted insertion by offset
                        inserted = False
                        for j, free_block in enumerate(temp_free_blocks):
                            if free_block.offset > new_free.offset:
                                temp_free_blocks.insert(j, new_free)
                                inserted = True
                                break
                        if not inserted:
                            temp_free_blocks.append(new_free)

                        # Try to merge with subsequent adjacent free blocks to reduce fragmentation.
                        # Since temp_free_blocks is kept sorted by offset, we only need to check
                        # the block immediately after the newly inserted one.
                        idx = temp_free_blocks.index(new_free)
                        if idx + 1 < len(temp_free_blocks):
                            next_block = temp_free_blocks[idx + 1]
                            if (
                                next_block.is_free
                                and new_free.offset + new_free.size == next_block.offset
                            ):
                                # Merge new_free and next_block
                                merged = MemoryBlock(
                                    offset=new_free.offset,
                                    size=new_free.size + next_block.size,
                                    is_free=True,
                                )
                                # Replace new_free with merged, remove next_block
                                temp_free_blocks[idx] = merged
                                temp_free_blocks.pop(idx + 1)

                    # Record the allocated block
                    allocated_block = MemoryBlock(
                        offset=offset, size=size, is_free=False
                    )
                    temp_allocated_blocks.append(allocated_block)
                    allocations.append((offset, size))
                    allocated = True
                    break

            if not allocated:
                # If any size cannot be allocated, the entire batch fails, do not modify the real state
                return None

        # All successful, submit modifications
        temp_free_blocks.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free_blocks
        self._allocated_blocks = temp_allocated_blocks

        return allocations

    def free(self, offset: int, size: int):
        """Free a memory block back to the pool.

        Args:
            offset: Offset of the memory block to free.
            size: Size of the memory block to free.
        """
        # Find and remove the allocated block
        for i, block in enumerate(self._allocated_blocks):
            if block.offset == offset and block.size == size and not block.is_free:
                self._allocated_blocks.pop(i)
                break
        else:
            # Block not found, might be a double-free or invalid free
            return

        # Add as free block and merge with adjacent free blocks
        new_free = MemoryBlock(offset=offset, size=size, is_free=True)

        # Merge with adjacent free blocks
        merged = False
        for free_block in self._free_blocks:
            # Check if adjacent to existing free block
            if free_block.offset + free_block.size == new_free.offset:
                # Merge with block before
                free_block.size += new_free.size
                merged = True
                break
            elif new_free.offset + new_free.size == free_block.offset:
                # Merge with block after
                free_block.offset = new_free.offset
                free_block.size += new_free.size
                merged = True
                break

        if not merged:
            # Insert in sorted order
            inserted = False
            for i, free_block in enumerate(self._free_blocks):
                if free_block.offset > new_free.offset:
                    self._free_blocks.insert(i, new_free)
                    inserted = True
                    break
            if not inserted:
                self._free_blocks.append(new_free)

        # Merge adjacent free blocks
        self._free_blocks.sort(key=lambda b: b.offset)
        i = 0
        while i < len(self._free_blocks) - 1:
            if (
                self._free_blocks[i].offset + self._free_blocks[i].size
                == self._free_blocks[i + 1].offset
            ):
                # Merge blocks
                self._free_blocks[i].size += self._free_blocks[i + 1].size
                self._free_blocks.pop(i + 1)
            else:
                i += 1

    def copy_to_pool(
        self, tensor: "torch.Tensor", offset: int, tensor_offset: int = 0
    ) -> int:
        """Copy tensor data to the memory pool at the specified offset.

        Args:
            tensor: Source tensor to copy from.
            offset: Destination offset in the memory pool (bytes).
            tensor_offset: Offset in the source tensor (elements, default: 0).

        Returns:
            Number of bytes copied.
        """
        if tensor.device != self.device:
            raise ValueError(
                f"Tensor device {tensor.device} does not match pool device {self.device}"
            )

        # Calculate number of elements to copy
        num_elements = tensor.numel() - tensor_offset
        if num_elements <= 0:
            return 0

        bytes_to_copy = num_elements * tensor.element_size()

        # Ensure we don't overflow the pool
        if offset + bytes_to_copy > self.pool_size:
            bytes_to_copy = self.pool_size - offset
            if bytes_to_copy <= 0:
                return 0
            # Recalculate elements based on available space
            num_elements = bytes_to_copy // tensor.element_size()
            if num_elements == 0:
                return 0
            bytes_to_copy = num_elements * tensor.element_size()

        # Flatten the tensor
        flat_tensor = tensor.flatten()
        if tensor_offset > 0:
            flat_tensor = flat_tensor[tensor_offset : tensor_offset + num_elements]

        # View the pool memory as the tensor's dtype and copy
        # Calculate how many elements we can fit
        pool_bytes = self._pool_tensor[offset : offset + bytes_to_copy]
        pool_elements = len(pool_bytes) // tensor.element_size()
        if pool_elements == 0:
            return 0

        # Create a view of the pool as the tensor dtype
        pool_view = pool_bytes.view(tensor.dtype)
        # Copy the data
        pool_view[:pool_elements].copy_(
            flat_tensor[:pool_elements].to(dtype=tensor.dtype)
        )

        return pool_elements * tensor.element_size()

    def copy_from_pool(
        self, tensor: "torch.Tensor", offset: int, tensor_offset: int = 0
    ) -> int:
        """Copy data from the memory pool to a tensor at the specified offset.

        Args:
            tensor: Destination tensor to copy to.
            offset: Source offset in the memory pool (bytes).
            tensor_offset: Offset in the destination tensor (elements, default: 0).

        Returns:
            Number of bytes copied.
        """
        if tensor.device != self.device:
            raise ValueError(
                f"Tensor device {tensor.device} does not match pool device {self.device}"
            )

        # Calculate number of elements to copy
        num_elements = tensor.numel() - tensor_offset
        if num_elements <= 0:
            return 0

        bytes_to_copy = num_elements * tensor.element_size()

        # Ensure we don't overflow the pool
        if offset + bytes_to_copy > self.pool_size:
            bytes_to_copy = self.pool_size - offset
            if bytes_to_copy <= 0:
                return 0
            # Recalculate elements based on available space
            num_elements = bytes_to_copy // tensor.element_size()
            if num_elements == 0:
                return 0
            bytes_to_copy = num_elements * tensor.element_size()

        # Flatten the tensor
        flat_tensor = tensor.flatten()
        if tensor_offset > 0:
            flat_tensor = flat_tensor[tensor_offset : tensor_offset + num_elements]

        # View the pool memory as the tensor's dtype and copy
        pool_bytes = self._pool_tensor[offset : offset + bytes_to_copy]
        pool_elements = len(pool_bytes) // tensor.element_size()
        if pool_elements == 0:
            return 0

        # Create a view of the pool as the tensor dtype
        pool_view = pool_bytes.view(tensor.dtype)
        # Copy the data
        flat_tensor[:pool_elements].copy_(
            pool_view[:pool_elements].to(dtype=tensor.dtype)
        )

        return pool_elements * tensor.element_size()

    def get_stats(self) -> dict:
        """Get statistics about the memory pool.

        Returns:
            Dictionary with pool statistics.
        """
        total_free = sum(block.size for block in self._free_blocks if block.is_free)
        total_allocated = sum(
            block.size for block in self._allocated_blocks if not block.is_free
        )

        return {
            "pool_size": self.pool_size,
            "total_free": total_free,
            "total_allocated": total_allocated,
            "fragmentation": len(self._free_blocks),
            "num_allocated_blocks": len(self._allocated_blocks),
        }

 