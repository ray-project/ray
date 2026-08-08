"""Memory pool management for NIXL RDT optimization."""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# Pool blocks are carved on this boundary so every block starts aligned for any
# torch dtype (complex128 has the largest element size).
_MAX_ALIGNMENT = 16


def _align_up(value: int, alignment: int) -> int:
    return (value + alignment - 1) // alignment * alignment


def packed_run_offsets(
    sizes: Sequence[int], alignments: Sequence[int]
) -> Tuple[List[int], int]:
    """Compute offsets for one consecutive run of tensors.

    Each tensor starts on a multiple of its own element size, which is what
    ``Tensor.view(dtype)`` needs to reinterpret the packed bytes. A run of
    tensors sharing a dtype therefore packs with no padding, so a receiver
    holding one contiguous buffer can match this layout exactly.

    Both sender and receiver use this to place tensors within a descriptor.
    The returned extent ends at the last tensor with no trailing pad.

    Args:
        sizes: Byte sizes of the tensors in the run, in tensor order.
        alignments: Element size of each tensor, in tensor order.

    Returns:
        (offsets, extent) for the run.
    """
    offsets: List[int] = []
    cursor = 0
    for size, alignment in zip(sizes, alignments):
        cursor = _align_up(cursor, alignment)
        offsets.append(cursor)
        cursor += size
    return offsets, cursor


def split_run_by_desc_lens(
    sizes: Sequence[int], alignments: Sequence[int], desc_lens: Sequence[int]
) -> List[List[int]]:
    """Recover consecutive runs from packed sizes and descriptor lengths.

    Walks sizes in order and closes a run when its extent equals the current
    descriptor length. Raises if sizes and lengths are not consumed exactly.

    Args:
        sizes: Byte sizes of the tensors, in tensor order.
        alignments: Element size of each tensor, in tensor order.
        desc_lens: Length of each NIXL transfer descriptor, in order.

    Returns:
        A list of runs, each a list of indices into sizes.
    """
    runs: List[List[int]] = []
    size_idx = 0
    for desc_len in desc_lens:
        if size_idx >= len(sizes):
            raise ValueError(
                f"Extra descriptor length {desc_len} after consuming all "
                f"{len(sizes)} tensor sizes"
            )
        run: List[int] = []
        while size_idx < len(sizes):
            candidate = run + [size_idx]
            _, extent = packed_run_offsets(
                [sizes[i] for i in candidate], [alignments[i] for i in candidate]
            )
            if extent > desc_len:
                raise ValueError(
                    f"Tensor sizes {[sizes[i] for i in candidate]} do not pack "
                    f"into descriptor length {desc_len} under the wire contract "
                    f"(extent={extent})"
                )
            run = candidate
            size_idx += 1
            if extent == desc_len:
                break
        else:
            _, extent = packed_run_offsets(
                [sizes[i] for i in run], [alignments[i] for i in run]
            )
            raise ValueError(
                f"Descriptor length {desc_len} does not match packed extent "
                f"{extent} for tensor indices {run}"
            )
        runs.append(run)

    if size_idx != len(sizes):
        raise ValueError(
            f"Descriptor lengths {list(desc_lens)} did not consume all "
            f"{len(sizes)} tensor sizes (stopped at index {size_idx})"
        )
    return runs


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

        # List of MemoryBlock for free blocks, sorted by offset.
        self._free_blocks: List[MemoryBlock] = [MemoryBlock(offset=0, size=pool_size)]
        # Blocks allocated per object ID.
        self._allocated_by_obj: Dict[str, List[MemoryBlock]] = {}

    def get_pool_tensor(self) -> "torch.Tensor":
        """Get the underlying pool tensor.

        Returns:
            The pre-allocated tensor representing the memory pool.
        """
        return self._pool_tensor

    def allocate_group(
        self, obj_id: str, tensors: List["torch.Tensor"]
    ) -> Tuple[List["torch.Tensor"], List[Tuple[int, int]], List["torch.Tensor"],]:
        """Pack tensors into as few contiguous pool blocks as possible.

        Copies only each tensor's own bytes (numel * element_size), not the
        full underlying storage. Packs in tensor order so each block covers a
        consecutive run. Replaces any prior allocation for ``obj_id``.

        Args:
            obj_id: Object ID that owns the allocation.
            tensors: Source tensors to allocate pool memory for.

        Returns:
            (regions, placements, views) where placements[i] is
            (block_index, byte_offset) and views are pool-backed tensors in
            input order.

        Raises:
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
        import torch

        sizes = [t.numel() * t.element_size() for t in tensors]
        alignments = [t.element_size() for t in tensors]

        # Snapshot the free list so the whole group is atomic. If this obj_id
        # already owns blocks (re-extract), treat them as free for packing so
        # the new allocation can reuse that space; on failure the real state
        # is left untouched.
        temp_free = [MemoryBlock(b.offset, b.size) for b in self._free_blocks]
        prior = self._allocated_by_obj.get(obj_id)
        if prior:
            temp_free.extend(MemoryBlock(b.offset, b.size) for b in prior)
            temp_free.sort(key=lambda b: b.offset)
            i = 0
            while i < len(temp_free) - 1:
                curr = temp_free[i]
                nxt = temp_free[i + 1]
                if curr.offset + curr.size == nxt.offset:
                    curr.size += nxt.size
                    temp_free.pop(i + 1)
                else:
                    i += 1

        total_free = sum(b.size for b in temp_free)

        if total_free < sum(sizes):
            raise NixlOutOfMemoryError(
                f"NIXL memory pool out of memory: cannot allocate "
                f"{len(sizes)} tensor(s) totaling {sum(sizes)} bytes. "
                f"Consider increasing the pool size when calling "
                f"register_nixl_memory_pool."
            )

        blocks: List[MemoryBlock] = []
        placements: List[Optional[Tuple[int, int]]] = [None] * len(tensors)
        remaining = list(range(len(tensors)))

        while remaining:
            rem_sizes = [sizes[i] for i in remaining]
            rem_aligns = [alignments[i] for i in remaining]
            _, full_extent = packed_run_offsets(rem_sizes, rem_aligns)

            # Prefer the smallest free block that fits everything remaining.
            best_fit_idx = None
            for i, block in enumerate(temp_free):
                if block.size >= full_extent:
                    if (
                        best_fit_idx is None
                        or block.size < temp_free[best_fit_idx].size
                    ):
                        best_fit_idx = i

            if best_fit_idx is not None:
                free_idx = best_fit_idx
                take_count = len(remaining)
                used_extent = full_extent
            else:
                # Take the largest free block and pack as many as fit in order.
                if not temp_free:
                    raise NixlOutOfMemoryError(
                        f"NIXL memory pool out of memory: cannot allocate "
                        f"{len(sizes)} tensor(s) totaling {sum(sizes)} bytes. "
                        f"Consider increasing the pool size when calling "
                        f"register_nixl_memory_pool."
                    )
                free_idx = max(range(len(temp_free)), key=lambda i: temp_free[i].size)
                hole = temp_free[free_idx]
                take_count = 0
                used_extent = 0
                for n in range(1, len(remaining) + 1):
                    _, extent = packed_run_offsets(rem_sizes[:n], rem_aligns[:n])
                    if extent <= hole.size:
                        take_count = n
                        used_extent = extent
                    else:
                        break
                if take_count == 0:
                    raise NixlOutOfMemoryError(
                        f"NIXL memory pool out of memory: cannot allocate "
                        f"next tensor of {rem_sizes[0]} bytes "
                        f"(largest free block is {hole.size} bytes). "
                        f"Consider increasing the pool size when calling "
                        f"register_nixl_memory_pool."
                    )

            # Round the carved free-list extent up so subsequent offsets stay aligned.
            carved = _align_up(used_extent, _MAX_ALIGNMENT)
            free_block = temp_free[free_idx]
            block_offset = free_block.offset
            carved = min(carved, free_block.size)
            if carved < used_extent:
                raise NixlOutOfMemoryError(
                    f"NIXL memory pool out of memory: free block of "
                    f"{free_block.size} bytes cannot hold packed extent "
                    f"{used_extent}."
                )

            remaining_after = free_block.size - carved
            if remaining_after == 0:
                temp_free.pop(free_idx)
            else:
                free_block.offset = block_offset + carved
                free_block.size = remaining_after

            block_index = len(blocks)
            blocks.append(MemoryBlock(block_offset, carved))

            run_indices = remaining[:take_count]
            run_offsets, run_extent = packed_run_offsets(
                [sizes[i] for i in run_indices],
                [alignments[i] for i in run_indices],
            )
            assert run_extent == used_extent
            for local_i, tensor_i in enumerate(run_indices):
                placements[tensor_i] = (block_index, run_offsets[local_i])
            remaining = remaining[take_count:]

        # Commit only after the full group packs successfully.
        temp_free.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free
        self._allocated_by_obj[obj_id] = blocks

        assert all(p is not None for p in placements)
        typed_placements: List[Tuple[int, int]] = placements  # type: ignore[assignment]

        regions: List["torch.Tensor"] = []
        block_used: List[int] = [0] * len(blocks)
        for tensor_i, (block_index, byte_offset) in enumerate(typed_placements):
            end = byte_offset + sizes[tensor_i]
            if end > block_used[block_index]:
                block_used[block_index] = end

        for b, blk in enumerate(blocks):
            regions.append(self._pool_tensor[blk.offset : blk.offset + block_used[b]])

        views: List["torch.Tensor"] = []
        for tensor_i, tensor in enumerate(tensors):
            block_index, byte_offset = typed_placements[tensor_i]
            nbytes = sizes[tensor_i]
            src_bytes = torch.empty(0, dtype=torch.uint8, device=tensor.device).set_(
                tensor.untyped_storage(),
                tensor.storage_offset() * tensor.element_size(),
                (nbytes,),
            )
            pool_start = blocks[block_index].offset + byte_offset
            self._pool_tensor[pool_start : pool_start + nbytes].copy_(src_bytes)
            pool_bytes = self._pool_tensor[pool_start : pool_start + nbytes]
            views.append(pool_bytes.view(tensor.dtype).reshape(tensor.shape))

        return regions, typed_placements, views

    def free_object(self, obj_id: str) -> bool:
        """Return pool blocks for ``obj_id`` if any.

        Args:
            obj_id: Object ID whose allocation should be released.

        Returns:
            True if blocks were freed, False if ``obj_id`` had no allocation.
        """
        blocks = self._allocated_by_obj.pop(obj_id, None)
        if blocks is None:
            return False
        self._return_blocks(blocks)
        return True

    def _return_blocks(self, blocks: List[MemoryBlock]) -> None:
        if not blocks:
            return
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
