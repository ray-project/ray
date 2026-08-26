"""Memory pool management for NIXL RDT optimization."""

import logging
import threading
from typing import TYPE_CHECKING, Dict, List, Tuple, Union

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# Pool offsets are kept at a multiple of this many bytes. ``Tensor.view(dtype)``
# requires the byte offset into the storage to be divisible by the target dtype's
# element size, so an unpadded 1-byte allocation would otherwise leave the next
# block at an offset that no wider dtype can view.
BLOCK_ALIGNMENT = 8


def _align_up(size: int) -> int:
    return ((size + BLOCK_ALIGNMENT - 1) // BLOCK_ALIGNMENT) * BLOCK_ALIGNMENT


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
        self._allocator_lock = threading.RLock()

    def get_pool_tensor(self) -> "torch.Tensor":
        """Get the underlying pool tensor.

        Returns:
            The pre-allocated tensor representing the memory pool.
        """
        return self._pool_tensor

    def has_block(self, tensor: "torch.Tensor") -> bool:
        """Check if a tensor has an allocated block in the pool.

        Args:
            tensor: The tensor to check.

        Returns:
            True if the tensor's storage has an allocated block.
        """
        with self._allocator_lock:
            return tensor.untyped_storage().data_ptr() in self._allocated_blocks

    def _allocate_memory_blocks(self, sizes: List[int]) -> List[MemoryBlock]:
        """Allocate multiple memory blocks from the pool atomically.

        Either all allocations succeed, or none of them do.

        Blocks are padded up to ``BLOCK_ALIGNMENT`` so that every block offset
        stays aligned, which means a returned block may be larger than the
        requested size.

        Args:
            sizes: List of sizes to allocate in bytes.

        Returns:
            List of MemoryBlock objects, one per requested size.

        Raises:
            ValueError: If ``sizes`` is empty or contains non-positive values.
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
        with self._allocator_lock:
            if not sizes or any(s <= 0 for s in sizes):
                raise ValueError("Invalid allocation request")

            # If total free space is less than total requested, fail fast.
            total_requested = sum(sizes)
            total_free = sum(b.size for b in self._free_blocks)
            if total_free < total_requested:
                raise NixlOutOfMemoryError(
                    f"NIXL memory pool out of memory: cannot allocate "
                    f"{len(sizes)} block(s) totaling "
                    f"{total_requested} bytes. Consider increasing "
                    f"the pool size when calling "
                    f"register_nixl_memory_pool."
                )

            # Allocate largest first to reduce fragmentation; then return in original order.
            order = sorted(range(len(sizes)), key=lambda i: -sizes[i])
            sorted_sizes = [sizes[i] for i in order]

            # Try to allocate all blocks atomically.
            allocations: List[MemoryBlock] = []
            temp_free_blocks = [
                MemoryBlock(b.offset, b.size) for b in self._free_blocks
            ]

            for size in sorted_sizes:
                allocated = False
                for i, block in enumerate(temp_free_blocks):
                    if block.size >= size:
                        # Allocate at the start of the current free block. Take
                        # the padding along with the requested bytes so the next
                        # block starts aligned; a trailing free block that is
                        # too small to hold the padding is consumed whole
                        # instead of leaving an unaligned remainder.
                        offset = block.offset
                        consumed = min(_align_up(size), block.size)
                        remaining_after = block.size - consumed

                        if remaining_after == 0:
                            temp_free_blocks.pop(i)
                        else:
                            block.offset = offset + consumed
                            block.size = remaining_after

                        allocations.append(MemoryBlock(offset, consumed))
                        allocated = True
                        break

                if not allocated:
                    raise NixlOutOfMemoryError(
                        f"NIXL memory pool out of memory: cannot allocate "
                        f"{len(sizes)} block(s) totaling "
                        f"{total_requested} bytes. Consider increasing "
                        f"the pool size when calling "
                        f"register_nixl_memory_pool."
                    )

            # Reorder allocations back to original request order
            result: List[MemoryBlock] = [MemoryBlock(0, 0)] * len(sizes)
            for k, alloc in enumerate(allocations):
                result[order[k]] = alloc

            # All successful, submit modifications
            temp_free_blocks.sort(key=lambda b: b.offset)
            self._free_blocks = temp_free_blocks

            return result

    def allocate_for_tensor_meta(
        self,
        tensor_meta: List[Tuple[Union["torch.Size", Tuple[int, ...]], "torch.dtype"]],
    ) -> Tuple[List["torch.Tensor"], List[MemoryBlock]]:
        """Allocate pool-backed receive buffers for the given tensor metadata.

        For each ``(shape, dtype)`` entry, allocates a pool block sized to the
        tensor byte count and returns a typed view backed by the pool.

        The views are only valid until their blocks are freed, so the caller
        owns the returned blocks and must pass them to ``copy_out_and_free``
        once the transfer completes, or to ``free_blocks`` if it fails. The
        views must not be handed to user code, since a pool block returned to
        the pool can be overwritten by the next transfer.

        Args:
            tensor_meta: List of ``(shape, dtype)`` tuples describing tensors
                to receive.

        Returns:
            A tuple of (pool-backed tensor views, blocks backing those views),
            one entry each per metadata entry.

        Raises:
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
        import torch

        sizes = []
        for shape, dtype in tensor_meta:
            numel = 1
            for dim in shape:
                numel *= dim
            sizes.append(numel * torch.tensor([], dtype=dtype).element_size())

        blocks = self._allocate_memory_blocks(sizes)
        try:
            views = [
                self._view_for_block(block, shape, dtype)
                for block, (shape, dtype) in zip(blocks, tensor_meta)
            ]
        except Exception:
            self._free_multiple_blocks(blocks)
            raise
        return views, blocks

    def copy_out_and_free(
        self, views: List["torch.Tensor"], blocks: List[MemoryBlock]
    ) -> List["torch.Tensor"]:
        """Copy pool-backed views into independent tensors and free their blocks.

        This decouples the returned tensors from the pool, so their lifetime is
        no longer tied to the pool's free list. Callers can hand the copies to
        user code and the blocks are immediately reusable by the next transfer.

        Args:
            views: Pool-backed views returned by ``allocate_for_tensor_meta``.
            blocks: The blocks backing those views.

        Returns:
            One independently allocated tensor per view, in the same order.
        """
        import torch

        try:
            copies = [view.clone() for view in views]
            if self.device.type == "cuda":
                # The clones are queued on the current stream, while the pool
                # block is reused by NIXL outside of any stream ordering, so
                # wait for the copies before the block becomes reusable.
                torch.cuda.synchronize(self.device)
        finally:
            self.free_blocks(blocks)
        return copies

    def free_blocks(self, blocks: List[MemoryBlock]) -> None:
        """Return the given blocks to the pool.

        Args:
            blocks: Memory blocks to free. An empty list is a no-op.
        """
        if not blocks:
            return
        self._free_multiple_blocks(blocks)

    def _view_for_block(
        self,
        block: MemoryBlock,
        shape: Union["torch.Size", Tuple[int, ...]],
        dtype: "torch.dtype",
    ) -> "torch.Tensor":
        """Build a typed tensor view over a pool block.

        Args:
            block: The pool block to view.
            shape: Shape of the resulting tensor.
            dtype: Data type of the resulting tensor.

        Returns:
            A pool-backed tensor view with the requested shape and dtype.
        """
        import torch

        numel = 1
        for dim in shape:
            numel *= dim
        view_byte_size = numel * torch.tensor([], dtype=dtype).element_size()
        pool_bytes = self._pool_tensor[block.offset : block.offset + view_byte_size]
        return pool_bytes.view(dtype).reshape(shape)

    def free_tensors(self, tensors: List["torch.Tensor"]) -> None:
        """Return pool blocks for the given tensors back to the pool.

        The caller is responsible for calling this method on the same tensors that were previously allocated in the pool before those tensors go out of scope.

        Args:
            tensors: Tensors whose pool blocks should be freed.
        """
        with self._allocator_lock:
            blocks = []
            for tensor in tensors:
                ptr = tensor.untyped_storage().data_ptr()
                if ptr in self._allocated_blocks:
                    blocks.append(self._allocated_blocks.pop(ptr))
            if blocks:
                self._free_multiple_blocks(blocks)

    def allocate_for_tensors(
        self, tensors: List["torch.Tensor"]
    ) -> List["torch.Tensor"]:
        """Allocate pool blocks for unique storages, copy data in,
        and return pool-backed tensor views for each input tensor. The caller is responsible for calling free on the original tensors to return the allocated tensor views back to the pool before the original tensors go out of scope.

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
        with self._allocator_lock:
            try:
                import torch

                pool_ptr = self._pool_tensor.untyped_storage().data_ptr()

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
                    if ptr == pool_ptr:
                        # Already pool-backed; nothing to allocate or copy.
                        continue
                    if ptr in storage_idx:
                        continue
                    ptr_to_tensor[ptr] = tensor
                    if ptr in self._allocated_blocks:
                        storage_idx[ptr] = -1
                    else:
                        storage_idx[ptr] = len(alloc_sizes)
                        alloc_sizes.append(tensor.untyped_storage().nbytes())

                # Allocate new (non-cached) storages atomically.
                if alloc_sizes:
                    new_allocations = self._allocate_memory_blocks(alloc_sizes)

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

                # Build pool-backed tensor views for each input tensor.
                pool_views: List["torch.Tensor"] = []
                for tensor in tensors:
                    ptr = tensor.untyped_storage().data_ptr()
                    if ptr == pool_ptr:
                        # Already pool-backed; return the tensor as-is.
                        pool_views.append(tensor)
                        continue
                    blk = self._allocated_blocks[ptr]
                    byte_offset = tensor.storage_offset() * tensor.element_size()
                    view_block = MemoryBlock(
                        blk.offset + byte_offset,
                        tensor.numel() * tensor.element_size(),
                    )
                    pool_views.append(
                        self._view_for_block(view_block, tensor.shape, tensor.dtype)
                    )

                return pool_views

            except Exception:
                # Roll back any pool mutations made in this call, then re-raise.
                try:
                    if new_allocations is not None:
                        self._free_multiple_blocks(new_allocations)
                    for ptr in newly_tracked_ptrs:
                        self._allocated_blocks.pop(ptr, None)
                except Exception as cleanup_err:
                    logger.error(f"Memory pool cleanup failed: {cleanup_err}.")
                raise

    def _free_multiple_blocks(self, blocks: List[MemoryBlock]) -> None:
        """Free multiple memory blocks back to the pool.

        Args:
            blocks: Memory blocks to free.
        """
        with self._allocator_lock:
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
