"""Memory pool management for NIXL RDT optimization."""

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

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

    It also tracks which storage data pointers have allocated blocks, enabling
    cross-call reuse (the same storage can reuse its pool slot across multiple
    ray.put calls) and pool-level block management.
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

        # Track allocated blocks by storage data pointer.
        # Maps storage_data_ptr -> (offset, size) in the pool.
        self._allocated_blocks: Dict[int, Tuple[int, int]] = {}

    def get_pool_tensor(self) -> "torch.Tensor":
        """Get the underlying pool tensor.

        Returns:
            The pre-allocated tensor representing the memory pool.
        """
        return self._pool_tensor

    def has_block(self, storage_ptr: int) -> bool:
        """Check if a storage pointer has an allocated block in the pool.

        Args:
            storage_ptr: The storage data pointer to check.

        Returns:
            True if the storage pointer has an allocated block.
        """
        return storage_ptr in self._allocated_blocks

    def track_allocation(self, storage_ptr: int, offset: int, size: int) -> None:
        """Record a new allocation in the block tracking.

        Args:
            storage_ptr: The storage data pointer to associate with this block.
            offset: Offset of the allocated block in the pool.
            size: Size of the allocated block in bytes.
        """
        self._allocated_blocks[storage_ptr] = (offset, size)

    def untrack_allocation(self, storage_ptr: int) -> None:
        """Remove a storage pointer from the block tracking without freeing memory.

        This is used for error rollback — the corresponding pool memory should
        be freed separately via ``free_multiple``.

        Args:
            storage_ptr: The storage data pointer to remove from tracking.
        """
        self._allocated_blocks.pop(storage_ptr, None)

    def return_blocks(self, storage_ptrs: List[int]) -> None:
        """Return multiple allocated blocks to the pool.

        Args:
            storage_ptrs: List of storage data pointers whose blocks to return.
        """
        offsets = []
        sizes = []
        for ptr in storage_ptrs:
            if ptr in self._allocated_blocks:
                offset, size = self._allocated_blocks.pop(ptr)
                offsets.append(offset)
                sizes.append(size)
        if offsets:
            self.free_multiple(offsets, sizes)

    def copy_storage_to_pool_block(
        self, storage_ptr: int, src_tensor: "torch.Tensor"
    ) -> None:
        """Copy ``src_tensor``'s full underlying storage into the pool block.

        The block must have been previously allocated and tracked for
        ``storage_ptr`` (via ``allocate_multiple`` + ``track_allocation``).

        Args:
            storage_ptr: The storage data pointer identifying the pool block.
            src_tensor: Tensor whose underlying storage will be copied in.

        Raises:
            KeyError: If no block is allocated for this storage pointer.
        """
        import torch

        offset, _ = self._allocated_blocks[storage_ptr]
        storage_size = src_tensor.untyped_storage().nbytes()
        storage_bytes = torch.tensor(
            [], dtype=torch.uint8, device=src_tensor.device
        ).set_(src_tensor.untyped_storage())
        self._pool_tensor[offset : offset + storage_size].copy_(storage_bytes)

    def get_tensor_view_in_block(
        self,
        storage_ptr: int,
        storage_offset_bytes: int,
        byte_size: int,
        dtype: "torch.dtype",
        shape: "torch.Size",
    ) -> "torch.Tensor":
        """Return a tensor view into a tensor's region of its pool block.

        Args:
            storage_ptr: The storage data pointer identifying the pool block.
            storage_offset_bytes: Byte offset of the tensor within its storage.
            byte_size: Byte length of the tensor view.
            dtype: Torch dtype of the resulting view.
            shape: Shape of the resulting view.

        Returns:
            A tensor view of ``shape`` and ``dtype`` backed by the pool block.

        Raises:
            KeyError: If no block is allocated for this storage pointer.
        """
        block_offset, _ = self._allocated_blocks[storage_ptr]
        pool_offset = block_offset + storage_offset_bytes
        pool_bytes = self._pool_tensor[pool_offset : pool_offset + byte_size]
        return pool_bytes.view(dtype).reshape(shape)

    def sync_device(self) -> None:
        """Synchronize the pool's CUDA stream if the pool is on CUDA."""
        import torch

        if self.device.type == "cuda":
            torch.cuda.synchronize(self.device)

    def allocate_multiple(self, sizes: List[int]) -> Optional[List[Tuple[int, int]]]:
        """Allocate multiple memory blocks from the pool atomically.

        Either all allocations succeed, or none of them do.

        Args:
            sizes: List of sizes to allocate in bytes.

        Returns:
            List of (offset, size) tuples if all allocations succeed, None otherwise.
        """
        if not sizes or any(s <= 0 for s in sizes):
            raise ValueError("Invalid allocation request")

        # If total free space is less than total requested, fail fast.
        total_requested = sum(sizes)
        total_free = sum(b.size for b in self._free_blocks)
        if total_free < total_requested:
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
        if not offsets:
            raise ValueError("Invalid free request")
        for offset, size in zip(offsets, sizes):
            self._free_blocks.append(MemoryBlock(offset=offset, size=size))

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
