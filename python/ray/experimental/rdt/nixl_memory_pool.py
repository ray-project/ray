"""Memory pool management for NIXL RDT optimization."""

from typing import TYPE_CHECKING, Dict, List, NamedTuple, Sequence, Tuple

if TYPE_CHECKING:
    import torch

# Pool blocks are carved on this boundary so every block starts aligned for any
# torch dtype (complex128 has the largest element size).
_MAX_ALIGNMENT = 16


def _align_up(value: int, alignment: int) -> int:
    """Round ``value`` up to the next multiple of ``alignment``.

    Adding ``alignment - 1`` pushes any value past the boundary it belongs to and
    the floor division truncates back down to it, so a value already on a
    boundary comes back unchanged. At most ``alignment - 1`` is added, which is
    what bounds the padding a dtype boundary can cost.

    Args:
        value: Byte count or offset to round up.
        alignment: Boundary to round to, always a power of two here: either a
            dtype's element size or ``_MAX_ALIGNMENT``.

    Returns:
        The smallest multiple of ``alignment`` that is at least ``value``, for
        example 8 for both ``(1, 8)`` and ``(8, 8)``, and 16 for ``(9, 8)``.
    """
    return (value + alignment - 1) // alignment * alignment


class TensorLayout(NamedTuple):
    """A tensor's size and alignment.

    Attributes:
        nbytes: The tensor's own byte size without padding, ``numel * element_size``.
        alignment: The boundary the tensor must start on, its element size.
    """

    nbytes: int
    alignment: int


def packed_offsets(
    tensor_layouts: Sequence[TensorLayout],
) -> Tuple[List[int], int]:
    """Compute offsets for one consecutive group of tensors.

    Each tensor starts on a multiple of its own element size, which is what
    ``Tensor.view(dtype)`` needs to reinterpret the packed bytes. A group of
    tensors sharing a dtype therefore packs with no padding, so a receiver
    holding one contiguous buffer can match this layout exactly.

    Both sender and receiver use this to place tensors within a descriptor.
    The returned byte count ends at the last tensor with no trailing pad.

    Args:
        tensor_layouts: Size and alignment of each tensor in the group, in
            tensor order.

    Returns:
        (offsets, packed_nbytes) for the group.
    """
    offsets: List[int] = []
    byte_index = 0
    for nbytes, alignment in tensor_layouts:
        byte_index = _align_up(byte_index, alignment)
        offsets.append(byte_index)
        byte_index += nbytes
    return offsets, byte_index


def group_tensors_by_desc(
    tensor_layouts: Sequence[TensorLayout],
    packed_group_nbytes: Sequence[int],
) -> List[List[int]]:
    """Recover which tensors each descriptor covers from the packed sizes.

    Walks the tensors in order and closes a group when its packed byte count
    equals the current descriptor's. Raises if the tensors and byte counts are
    not consumed exactly.

    Args:
        tensor_layouts: Size and alignment of each tensor, in tensor order.
        packed_group_nbytes: Total byte count of each NIXL transfer descriptor,
            in order. Each descriptor covers a consecutive group of tensors, so
            there are no more of these than there are tensors.

    Returns:
        One list per packed descriptor. Each list contains indices into
        tensor_layouts corresponding to the contained tensors.
    """
    num_tensors = len(tensor_layouts)
    desc_groups: List[List[int]] = []
    tensor_idx = 0
    for group_nbytes in packed_group_nbytes:
        if tensor_idx >= num_tensors:
            raise ValueError(
                f"Extra descriptor byte count {group_nbytes} after consuming "
                f"all {num_tensors} tensors"
            )
        desc_group: List[int] = []
        packed_nbytes = 0
        closed = False
        while tensor_idx < num_tensors:
            nbytes, alignment = tensor_layouts[tensor_idx]
            candidate_nbytes = _align_up(packed_nbytes, alignment) + nbytes
            if candidate_nbytes > group_nbytes:
                raise ValueError(
                    f"Tensor sizes "
                    f"{[tensor_layouts[i].nbytes for i in desc_group + [tensor_idx]]}"
                    f" do not pack into descriptor byte count {group_nbytes} under "
                    f"the wire contract (packed_nbytes={candidate_nbytes})"
                )
            desc_group.append(tensor_idx)
            packed_nbytes = candidate_nbytes
            tensor_idx += 1
            if packed_nbytes == group_nbytes:
                closed = True
                break
        if not closed:
            raise ValueError(
                f"Descriptor byte count {group_nbytes} does not match packed byte "
                f"count {packed_nbytes} for tensor indices {desc_group}"
            )
        desc_groups.append(desc_group)

    if tensor_idx != num_tensors:
        raise ValueError(
            f"Descriptor byte counts {list(packed_group_nbytes)} did not consume "
            f"all {num_tensors} tensors (stopped at index {tensor_idx})"
        )
    return desc_groups


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
        consecutive group. Replaces any prior allocation for ``obj_id``.

        Args:
            obj_id: Object ID that owns the allocation.
            tensors: Source tensors to allocate pool memory for.

        Returns:
            One pool-backed region per block, in order, to transfer as is. The
            receiver recovers the individual tensors from the region lengths
            with ``group_tensors_by_desc`` and ``packed_offsets``.

        Raises:
            NixlOutOfMemoryError: If the pool has insufficient space.
        """
        tensor_layouts = [
            TensorLayout(t.numel() * t.element_size(), t.element_size())
            for t in tensors
        ]
        sizes = [layout.nbytes for layout in tensor_layouts]

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
            raise NixlOutOfMemoryError(
                f"NIXL memory pool out of memory: cannot allocate {len(sizes)} "
                f"tensor(s) totaling {sum(sizes)} bytes. Consider increasing the "
                f"pool size when calling register_nixl_memory_pool."
            )

        blocks: List[MemoryBlock] = []
        # Bytes actually packed into each block, and the absolute pool offset of
        # each tensor. Tensors are taken in order, so pool_starts ends up in
        # tensor order.
        block_nbytes: List[int] = []
        pool_starts: List[int] = []
        remaining = list(range(len(tensors)))

        while remaining:
            rem_layouts = [tensor_layouts[i] for i in remaining]
            offsets, total_nbytes = packed_offsets(rem_layouts)

            # Prefer the smallest free block that fits everything remaining.
            free_idx = min(
                (i for i, b in enumerate(temp_free) if b.size >= total_nbytes),
                key=lambda i: temp_free[i].size,
                default=None,
            )
            if free_idx is not None:
                take_count = len(remaining)
                placed_nbytes = total_nbytes
            else:
                # Take the largest free block and pack as many as fit in order.
                free_idx = max(
                    range(len(temp_free)),
                    key=lambda i: temp_free[i].size,
                    default=None,
                )
                hole_size = 0 if free_idx is None else temp_free[free_idx].size
                take_count = 0
                placed_nbytes = 0
                for n, layout in enumerate(rem_layouts):
                    if offsets[n] + layout.nbytes > hole_size:
                        break
                    take_count = n + 1
                    placed_nbytes = offsets[n] + layout.nbytes
                if take_count == 0:
                    raise NixlOutOfMemoryError(
                        f"NIXL memory pool out of memory: cannot allocate next "
                        f"tensor of {rem_layouts[0].nbytes} bytes (largest free "
                        f"block is {hole_size} bytes). Consider increasing the "
                        f"pool size when calling register_nixl_memory_pool."
                    )

            # Round the carved block up so subsequent offsets stay aligned.
            free_block = temp_free[free_idx]
            block_offset = free_block.offset
            carved = min(_align_up(placed_nbytes, _MAX_ALIGNMENT), free_block.size)
            if carved == free_block.size:
                temp_free.pop(free_idx)
            else:
                free_block.offset += carved
                free_block.size -= carved

            blocks.append(MemoryBlock(block_offset, carved))
            block_nbytes.append(placed_nbytes)
            pool_starts.extend(block_offset + off for off in offsets[:take_count])
            remaining = remaining[take_count:]

        # Commit only after the full group packs successfully.
        temp_free.sort(key=lambda b: b.offset)
        self._free_blocks = temp_free
        self._allocated_by_obj[obj_id] = blocks

        regions = [
            self._pool_tensor[b.offset : b.offset + nbytes]
            for b, nbytes in zip(blocks, block_nbytes)
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
        Only the tensor's own bytes are copied, never its whole storage, so a
        view into a larger weight costs only the bytes it occupies.

        Args:
            tensors: Source tensors, in input order.
            sizes: Byte size of each tensor.
            pool_starts: Absolute pool offset each tensor was placed at.
        """
        import torch

        for tensor, nbytes, pool_start in zip(tensors, sizes, pool_starts):
            src_bytes = tensor.flatten().view(torch.uint8)
            self._pool_tensor[pool_start : pool_start + nbytes].copy_(src_bytes)

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
