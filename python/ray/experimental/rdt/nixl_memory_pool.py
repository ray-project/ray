"""Memory pool management for NIXL RDT optimization."""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)


class NixlOutOfMemoryError(RuntimeError):
    """Raised when the NIXL memory pool runs out of space.

    The pre-allocated memory pool does not have enough free space for the
    requested allocation. Increase the pool size passed to
    ``register_nixl_memory_pool`` to avoid this error.
    """


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

        # Track free blocks using a largest-request-first, first-fit allocator.
        # List of MemoryBlock for free blocks, sorted by offset.
        self._free_blocks: List[MemoryBlock] = [MemoryBlock(offset=0, size=pool_size)]

        # Track allocated blocks by storage data pointer.
        # Maps storage_data_ptr -> MemoryBlock in the pool.
        self._allocated_blocks: Dict[int, MemoryBlock] = {}

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

    def free_tensors(self, tensors: List["torch.Tensor"]) -> None:
        """Return pool blocks for the given tensors back to the pool.

        Args:
            tensors: Tensors whose pool blocks should be freed.
        """
        blocks = []
        for tensor in tensors:
            ptr = tensor.untyped_storage().data_ptr()
            if ptr in self._allocated_blocks:
                blocks.append(self._allocated_blocks.pop(ptr))
        if blocks:
            self._free_multiple(blocks)

    def sync_device(self) -> None:
        """Synchronize the pool's CUDA stream if the pool is on CUDA."""
        import torch

        if self.device.type == "cuda":
            torch.cuda.synchronize(self.device)

    def allocate_for_tensors(
        self, tensors: List["torch.Tensor"]
    ) -> List["torch.Tensor"]:
        """Allocate pool blocks for unique storages, copy data in,
        and return pool-backed tensor views for each input tensor.

        Handles storage-level deduplication: views of the same storage share
        one pool block within a single call, and the same storage reuses its
        existing pool slot across calls.

        Args:
            tensors: Source tensors to allocate pool memory for.

        Returns:
            List of pool-backed tensor views, one per input tensor,
            in the same order.

        Raises:
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
        new_allocations = None
        newly_tracked_ptrs: List[int] = []
        try:
            import torch

            # Deduplicate storages: group tensors by storage data_ptr so
            # views of the same storage share one pool allocation.
            # Maps storage data_ptr -> index in alloc_sizes/new_allocations,
            # or -1 for storages that already have a pool block (cache hit).
            storage_idx: Dict[int, int] = {}
            # Maps storage data_ptr -> a representative tensor (for copy).
            ptr_to_tensor: Dict[int, "torch.Tensor"] = {}
            alloc_sizes: List[int] = []

            for tensor in tensors:
                ptr = tensor.untyped_storage().data_ptr()
                if ptr in storage_idx:
                    continue
                ptr_to_tensor[ptr] = tensor
                if self.has_block(ptr):
                    storage_idx[ptr] = -1
                else:
                    storage_idx[ptr] = len(alloc_sizes)
                    alloc_sizes.append(tensor.untyped_storage().nbytes())

            # Allocate new (non-cached) storages atomically.
            if alloc_sizes:
                new_allocations = self._allocate_multiple(alloc_sizes)
                if new_allocations is None:
                    raise NixlOutOfMemoryError(
                        f"NIXL memory pool out of memory: cannot allocate "
                        f"{len(alloc_sizes)} block(s) totaling "
                        f"{sum(alloc_sizes)} bytes. Consider increasing "
                        f"the pool size when calling "
                        f"register_nixl_memory_pool."
                    )

            # Track and copy newly allocated blocks. Cache hits keep the
            # originally copied data -- any mutations to the source storage
            # since the first ray.put are not reflected in outstanding refs.
            for ptr, idx in storage_idx.items():
                if idx < 0:
                    continue
                blk = new_allocations[idx]
                self._allocated_blocks[ptr] = blk
                newly_tracked_ptrs.append(ptr)
                # Copy the tensor's full underlying storage into the pool block.
                src = ptr_to_tensor[ptr]
                storage_size = src.untyped_storage().nbytes()
                storage_bytes = torch.tensor(
                    [], dtype=torch.uint8, device=src.device
                ).set_(src.untyped_storage())
                self._pool_tensor[blk.offset : blk.offset + storage_size].copy_(
                    storage_bytes
                )

            self.sync_device()

            # Build pool-backed tensor views for each input tensor.
            pool_views: List["torch.Tensor"] = []
            for tensor in tensors:
                ptr = tensor.untyped_storage().data_ptr()
                blk = self._allocated_blocks[ptr]
                pool_offset = blk.offset + (
                    tensor.storage_offset() * tensor.element_size()
                )
                view_byte_size = tensor.numel() * tensor.element_size()
                pool_bytes = self._pool_tensor[
                    pool_offset : pool_offset + view_byte_size
                ]
                pool_views.append(pool_bytes.view(tensor.dtype).reshape(tensor.shape))

            return pool_views

        except Exception:
            # Roll back any pool mutations made in this call, then re-raise.
            try:
                if new_allocations is not None:
                    self._free_multiple(new_allocations)
                for ptr in newly_tracked_ptrs:
                    self._allocated_blocks.pop(ptr, None)
            except Exception as cleanup_err:
                logger.error(f"Memory pool cleanup failed: {cleanup_err}.")
            raise

    def _allocate_multiple(self, sizes: List[int]) -> Optional[List[MemoryBlock]]:
        """Allocate multiple memory blocks from the pool atomically.

        Either all allocations succeed, or none of them do.

        Args:
            sizes: List of sizes to allocate in bytes.

        Returns:
            List of MemoryBlock if all allocations succeed, None otherwise.
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
        allocations: List[MemoryBlock] = []
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

                    allocations.append(MemoryBlock(offset, size))
                    allocated = True
                    break

            if not allocated:
                # If any size cannot be allocated, the entire batch fails,
                # do not modify the real state.
                return None

        # Reorder allocations back to original request order
        result: List[MemoryBlock] = [MemoryBlock(0, 0)] * len(sizes)
        for k, alloc in enumerate(allocations):
            result[order[k]] = alloc

        # All successful, submit modifications
        temp_free_blocks.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free_blocks

        return result

    def _free_multiple(self, blocks: List[MemoryBlock]) -> None:
        """Free multiple memory blocks back to the pool.

        Args:
            blocks: Memory blocks to free.
        """
        if not blocks:
            raise ValueError("Invalid free request")
        self._free_blocks.extend(blocks)

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
