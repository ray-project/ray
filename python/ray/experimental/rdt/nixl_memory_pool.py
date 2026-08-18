"""Memory pool management for NIXL RDT optimization."""

from typing import TYPE_CHECKING, Dict, List, Sequence, Tuple

if TYPE_CHECKING:
    import torch

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

    The extent is carried forward one tensor at a time rather than repacking the
    run from scratch on each step, which keeps this linear in the tensor count.

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
        extent = 0
        closed = False
        while size_idx < len(sizes):
            candidate_extent = _align_up(extent, alignments[size_idx]) + sizes[size_idx]
            if candidate_extent > desc_len:
                candidate = run + [size_idx]
                raise ValueError(
                    f"Tensor sizes {[sizes[i] for i in candidate]} do not pack "
                    f"into descriptor length {desc_len} under the wire contract "
                    f"(extent={candidate_extent})"
                )
            run.append(size_idx)
            extent = candidate_extent
            size_idx += 1
            if extent == desc_len:
                closed = True
                break
        if not closed:
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


def _out_of_memory(detail: str) -> NixlOutOfMemoryError:
    return NixlOutOfMemoryError(
        f"NIXL memory pool out of memory: {detail}. Consider increasing the "
        f"pool size when calling register_nixl_memory_pool."
    )


class MemoryBlock:
    """Represents a memory block in the pool."""

    def __init__(self, offset: int, size: int):
        self.offset = offset
        self.size = size

    def __repr__(self):
        return f"MemoryBlock(offset={self.offset}, size={self.size})"


def _merge_free_blocks(blocks: List[MemoryBlock]) -> None:
    """Sort a free list by offset and merge adjacent blocks, in place."""
    blocks.sort(key=lambda b: b.offset)
    i = 0
    while i < len(blocks) - 1:
        curr = blocks[i]
        nxt = blocks[i + 1]
        if curr.offset + curr.size == nxt.offset:
            curr.size += nxt.size
            blocks.pop(i + 1)
        else:
            i += 1


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
        self,
        obj_id: str,
        tensors: List["torch.Tensor"],
    ) -> List["torch.Tensor"]:
        """Pack tensors into as few contiguous pool blocks as possible.

        Copies only each tensor's own bytes (numel * element_size), not the
        full underlying storage. Packs in tensor order so each block covers a
        consecutive run. Replaces any prior allocation for ``obj_id``.

        Args:
            obj_id: Object ID that owns the allocation.
            tensors: Source tensors to allocate pool memory for.

        Returns:
            One pool-backed region per block, in order, to transfer as is. The
            receiver recovers the individual tensors from the region lengths
            with ``split_run_by_desc_lens`` and ``packed_run_offsets``.

        Raises:
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
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
            _merge_free_blocks(temp_free)

        if sum(b.size for b in temp_free) < sum(sizes):
            raise _out_of_memory(
                f"cannot allocate {len(sizes)} tensor(s) totaling {sum(sizes)} bytes"
            )

        blocks: List[MemoryBlock] = []
        # Bytes actually packed into each block, and the absolute pool offset of
        # each tensor. Tensors are taken in order, so pool_starts ends up in
        # tensor order.
        extents: List[int] = []
        pool_starts: List[int] = []
        remaining = list(range(len(tensors)))

        while remaining:
            rem_sizes = [sizes[i] for i in remaining]
            rem_aligns = [alignments[i] for i in remaining]
            offsets, full_extent = packed_run_offsets(rem_sizes, rem_aligns)

            # Prefer the smallest free block that fits everything remaining.
            free_idx = min(
                (i for i, b in enumerate(temp_free) if b.size >= full_extent),
                key=lambda i: temp_free[i].size,
                default=None,
            )
            if free_idx is not None:
                take_count = len(remaining)
                used_extent = full_extent
            else:
                # Take the largest free block and pack as many as fit in order.
                # Each offset already carries the packing forward, so a prefix's
                # extent is a lookup rather than a repack.
                free_idx = max(
                    range(len(temp_free)),
                    key=lambda i: temp_free[i].size,
                    default=None,
                )
                hole_size = 0 if free_idx is None else temp_free[free_idx].size
                take_count = 0
                used_extent = 0
                for n, size in enumerate(rem_sizes):
                    if offsets[n] + size > hole_size:
                        break
                    take_count = n + 1
                    used_extent = offsets[n] + size
                if take_count == 0:
                    raise _out_of_memory(
                        f"cannot allocate next tensor of {rem_sizes[0]} bytes "
                        f"(largest free block is {hole_size} bytes)"
                    )

            # Round the carved free-list extent up so subsequent offsets stay aligned.
            free_block = temp_free[free_idx]
            block_offset = free_block.offset
            carved = min(_align_up(used_extent, _MAX_ALIGNMENT), free_block.size)
            if carved == free_block.size:
                temp_free.pop(free_idx)
            else:
                free_block.offset += carved
                free_block.size -= carved

            blocks.append(MemoryBlock(block_offset, carved))
            extents.append(used_extent)
            pool_starts.extend(block_offset + off for off in offsets[:take_count])
            remaining = remaining[take_count:]

        # Commit only after the full group packs successfully.
        temp_free.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free
        self._allocated_by_obj[obj_id] = blocks

        regions = [
            self._pool_tensor[b.offset : b.offset + extent]
            for b, extent in zip(blocks, extents)
        ]
        self._copy_into_pool(tensors, sizes, pool_starts)
        return regions

    def _copy_into_pool(
        self,
        tensors: List["torch.Tensor"],
        sizes: List[int],
        pool_starts: List[int],
    ) -> None:
        """Copy each tensor's own bytes into its packed slot in the pool.

        Consecutive tensors that are adjacent in both the source storage and the
        pool are copied together as one device copy. Weight-sync layouts, where
        the tensors are ordered views of one weight, collapse to a single copy
        per block. Anything else, such as interleaved order or separately
        allocated tensors, simply forms chains of one and copies per tensor.

        This is about launch overhead, not bandwidth: a per-tensor copy costs
        roughly the same regardless of size, so at tens of thousands of tensors
        the launches cost far more than the bytes.

        Args:
            tensors: Source tensors, in input order.
            sizes: Byte size of each tensor.
            pool_starts: Absolute pool offset each tensor was placed at.
        """
        import torch

        # Gather the addresses once. Probing them inside the scan below would
        # cost a few tensor attribute lookups per comparison, which at tens of
        # thousands of tensors outweighs the scan itself.
        storage_ptrs = [t.untyped_storage().data_ptr() for t in tensors]
        data_ptrs = [t.data_ptr() for t in tensors]

        num_tensors = len(tensors)
        chain_start = 0
        while chain_start < num_tensors:
            chain_end = chain_start + 1
            while (
                chain_end < num_tensors
                # A chain must stay inside one storage: the source view below is
                # built from the head's storage and cannot run past its end.
                and storage_ptrs[chain_end] == storage_ptrs[chain_end - 1]
                and data_ptrs[chain_end]
                == data_ptrs[chain_end - 1] + sizes[chain_end - 1]
                and pool_starts[chain_end]
                == pool_starts[chain_end - 1] + sizes[chain_end - 1]
            ):
                chain_end += 1

            head = tensors[chain_start]
            pool_start = pool_starts[chain_start]
            nbytes = pool_starts[chain_end - 1] + sizes[chain_end - 1] - pool_start
            src_bytes = torch.empty(0, dtype=torch.uint8, device=head.device).set_(
                head.untyped_storage(),
                head.storage_offset() * head.element_size(),
                (nbytes,),
            )
            self._pool_tensor[pool_start : pool_start + nbytes].copy_(src_bytes)
            chain_start = chain_end

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
        self._free_blocks.extend(blocks)
        _merge_free_blocks(self._free_blocks)
        return True
