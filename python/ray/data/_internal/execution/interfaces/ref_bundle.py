import itertools
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import ray
from .common import NodeIdStr
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data.block import Block, BlockAccessor, BlockMetadata, Schema
from ray.data.context import DataContext
from ray.types import ObjectRef


@dataclass
class BlockSlice:
    """A slice of a block."""

    # Starting row offset (inclusive) within the block.
    start_offset: int
    # Ending row offset (exclusive) within the block.
    end_offset: int

    @property
    def num_rows(self) -> int:
        return self.end_offset - self.start_offset


@dataclass
class RefBundle:
    """A group of data block references and their metadata.

    Operators take in and produce streams of RefBundles.

    Most commonly a RefBundle consists of a single block object reference.
    In some cases, e.g., due to block splitting, or for a reduce task, there may
    be more than one block.

    Block bundles have ownership semantics, i.e., shared ownership (similar to C++
    shared_ptr, multiple operators share the same block bundle), or unique ownership
    (similar to C++ unique_ptr, only one operator owns the block bundle). This
    allows operators to know whether they can destroy blocks when they don't need
    them. Destroying blocks eagerly is more efficient than waiting for Python GC /
    Ray reference counting to kick in.
    """

    # The size_bytes must be known in the metadata, num_rows is optional.
    blocks: Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]

    # The schema of the blocks in this bundle. This is optional, and may be None
    # if blocks are empty.
    schema: Optional["Schema"]

    # Whether we own the blocks (can safely destroy them).
    owns_blocks: bool

    # The slices of the blocks in this bundle. After __post_init__, this is always
    # a list with length equal to len(blocks). Individual entries can be None to
    # represent a full block (equivalent to BlockSlice(0, num_rows)).
    # Pass None during construction to initialize all slices as None (full blocks).
    slices: Optional[List[Optional[BlockSlice]]] = None

    # This attribute is used by the split() operator to assign bundles to logical
    # output splits. It is otherwise None.
    output_split_idx: Optional[int] = None

    # Object metadata (size, locations, spilling status)
    _cached_object_meta: Optional[Dict[ObjectRef, "_ObjectMetadata"]] = None

    # Preferred locations for this bundle determined based on the locations
    # of individual objects and their corresponding size, ie location with the
    # largest total number of bytes present there has the highest preference.
    _cached_preferred_locations: Optional[Dict[NodeIdStr, int]] = None

    def __post_init__(self):
        if not isinstance(self.blocks, tuple):
            object.__setattr__(self, "blocks", tuple(self.blocks))

        if self.slices is None:
            self.slices = [None] * len(self.blocks)
        else:
            assert len(self.blocks) == len(
                self.slices
            ), "Number of blocks and slices must match"
            # Validate slice ranges
            for (_, metadata), block_slice in zip(self.blocks, self.slices):
                if block_slice is not None:
                    assert (
                        block_slice.start_offset >= 0
                    ), f"Slice start_offset must be non-negative: {block_slice.start_offset}"
                    assert (
                        block_slice.end_offset >= block_slice.start_offset
                    ), f"Slice end_offset must be >= start_offset: [{block_slice.start_offset}, {block_slice.end_offset})"
                    if metadata.num_rows is not None:
                        assert (
                            block_slice.end_offset <= metadata.num_rows
                        ), f"Slice range [{block_slice.start_offset}, {block_slice.end_offset}) exceeds block num_rows: {metadata.num_rows}"

        for b in self.blocks:
            assert isinstance(b, tuple), b
            assert len(b) == 2, b
            assert isinstance(b[0], ray.ObjectRef), b[0]
            assert isinstance(b[1], BlockMetadata), b[1]
            if b[1].size_bytes is None:
                raise ValueError(
                    "The size in bytes of the block must be known: {}".format(b)
                )

    def __setattr__(self, key, value):
        if hasattr(self, key) and key in ["blocks", "owns_blocks"]:
            raise ValueError(f"The `{key}` field of RefBundle cannot be updated.")
        object.__setattr__(self, key, value)

    @property
    def block_refs(self) -> List[ObjectRef[Block]]:
        """List of block references in this bundle."""
        return [block_ref for block_ref, _ in self.blocks]

    @property
    def metadata(self) -> List[BlockMetadata]:
        """List of block metadata in this bundle."""
        return [metadata for _, metadata in self.blocks]

    def num_rows(self) -> Optional[int]:
        """Number of rows present in this bundle, if known.

        Iterates through blocks and their corresponding slices to calculate the total.
        Note: Block metadata always refers to the full block, not the slice.

        - If block_slice is None, uses the full block's metadata.num_rows
        - If block_slice is present, uses the slice's num_rows (partial block portion)
        - Returns None if any full block has unknown row count (metadata.num_rows is None)
        """
        total = 0
        for metadata, block_slice in zip(self.metadata, self.slices):
            if block_slice is None:
                if metadata.num_rows is None:
                    return None
                total += metadata.num_rows
            else:
                total += block_slice.num_rows
        return total

    def size_bytes(self) -> int:
        """Size of the blocks of this bundle in bytes.

        Iterates through blocks and their corresponding slices to calculate the total size.
        Note: Block metadata always refers to the full block, not the slice.

        - If block_slice is None, uses the full block's metadata.size_bytes
        - If block_slice is present but num_rows is unknown or zero, uses full metadata.size_bytes
        - If block_slice represents a partial block, estimates size proportionally based on
          (metadata.size_bytes / metadata.num_rows) * block_slice.num_rows
        - Otherwise, uses the full metadata.size_bytes
        """
        total = 0
        for (_, metadata), block_slice in zip(self.blocks, self.slices):
            if block_slice is None:
                # Full block
                total += metadata.size_bytes
            elif metadata.num_rows is None or metadata.num_rows == 0:
                # Unknown num_rows or empty block - use full metadata size
                total += metadata.size_bytes
            elif metadata.num_rows != block_slice.num_rows:
                # Partial block - estimate size based on rows
                per_row = metadata.size_bytes / metadata.num_rows
                total += max(1, int(math.ceil(per_row * block_slice.num_rows)))
            else:
                total += metadata.size_bytes
        return total

    def destroy_if_owned(self) -> int:
        """Clears the object store memory for these blocks if owned.

        Returns:
            The number of bytes freed.
        """
        should_free = self.owns_blocks and DataContext.get_current().eager_free
        for block_ref in self.block_refs:
            trace_deallocation(
                block_ref, "RefBundle.destroy_if_owned", free=should_free
            )
        return self.size_bytes() if should_free else 0

    def get_preferred_object_locations(self) -> Dict[NodeIdStr, int]:
        """Returns a mapping of node IDs to total bytes stored on each node.

        Returns:
            Dict mapping node ID to total bytes stored on that node
        """
        meta = self._get_cached_metadata()

        if self._cached_preferred_locations is None:
            preferred_locs: Dict[NodeIdStr, int] = defaultdict(int)

            for ref, obj_meta in meta.items():
                for loc in obj_meta.locs:
                    preferred_locs[loc] += obj_meta.size

            self._cached_preferred_locations = preferred_locs

        return self._cached_preferred_locations

    def _get_cached_metadata(self) -> Dict[ObjectRef, "_ObjectMetadata"]:
        if self._cached_object_meta is None:
            # This call is pretty fast for owned objects (~5k/s), so we don't need to
            # batch it for now.
            meta = ray.experimental.get_local_object_locations(self.block_refs)
            # Extract locations
            object_metas: Dict[ObjectRef, _ObjectMetadata] = {
                ref: _ObjectMetadata(
                    size=meta[ref]["object_size"],
                    spilled=meta[ref]["did_spill"],
                    locs=meta[ref]["node_ids"],
                )
                for ref in self.block_refs
            }

            self._cached_object_meta = object_metas

        return self._cached_object_meta

    def slice(self, needed_rows: int) -> Tuple["RefBundle", "RefBundle"]:
        """Slice a Ref Bundle into the first bundle containing the first `needed_rows` rows and the remaining bundle containing the remaining rows.

        Args:
            needed_rows: Number of rows to take from the head of the bundle.

        Returns:
            A tuple of (sliced_bundle, remaining_bundle). The needed rows must be less than the number of rows in the bundle.
        """
        assert needed_rows > 0, "needed_rows must be positive."
        assert (
            self.num_rows() is not None
        ), "Cannot slice a RefBundle with unknown number of rows."
        assert (
            needed_rows < self.num_rows()
        ), f"To slice a RefBundle, the number of requested rows must be less than the number of rows in the bundle. Requested {needed_rows} rows but bundle only has {self.num_rows()} rows."

        block_slices = []
        for metadata, block_slice in zip(self.metadata, self.slices):
            if block_slice is None:
                # None represents a full block, convert to explicit BlockSlice
                assert (
                    metadata.num_rows is not None
                ), "Cannot derive block slice for a RefBundle with unknown block row counts."
                block_slices.append(
                    BlockSlice(start_offset=0, end_offset=metadata.num_rows)
                )
            else:
                block_slices.append(block_slice)

        consumed_blocks: List[Tuple[ObjectRef[Block], BlockMetadata]] = []
        consumed_slices: List[BlockSlice] = []
        remaining_blocks: List[Tuple[ObjectRef[Block], BlockMetadata]] = []
        remaining_slices: List[BlockSlice] = []

        rows_to_take = needed_rows

        for (block_ref, metadata), block_slice in zip(self.blocks, block_slices):
            block_rows = block_slice.num_rows
            if rows_to_take >= block_rows:
                consumed_blocks.append((block_ref, metadata))
                consumed_slices.append(block_slice)
                rows_to_take -= block_rows
            else:
                if rows_to_take == 0:
                    remaining_blocks.append((block_ref, metadata))
                    remaining_slices.append(block_slice)
                    continue
                consume_slice = BlockSlice(
                    start_offset=block_slice.start_offset,
                    end_offset=block_slice.start_offset + rows_to_take,
                )
                consumed_blocks.append((block_ref, metadata))
                consumed_slices.append(consume_slice)

                leftover_rows = block_rows - rows_to_take
                if leftover_rows > 0:
                    remainder_slice = BlockSlice(
                        start_offset=consume_slice.end_offset,
                        end_offset=block_slice.end_offset,
                    )
                    remaining_blocks.append((block_ref, metadata))
                    remaining_slices.append(remainder_slice)

                rows_to_take = 0

        sliced_bundle = RefBundle(
            blocks=tuple(consumed_blocks),
            schema=self.schema,
            owns_blocks=False,
            slices=consumed_slices if consumed_slices else None,
        )

        remaining_bundle = RefBundle(
            blocks=tuple(remaining_blocks),
            schema=self.schema,
            owns_blocks=False,
            slices=remaining_slices if remaining_slices else None,
        )

        return sliced_bundle, remaining_bundle

    @classmethod
    def merge_ref_bundles(cls, bundles: List["RefBundle"]) -> "RefBundle":
        assert bundles, "Cannot merge an empty list of RefBundles."
        merged_blocks = list(itertools.chain(*[bundle.blocks for bundle in bundles]))
        merged_slices = list(itertools.chain(*[bundle.slices for bundle in bundles]))
        return cls(
            blocks=tuple(merged_blocks),
            schema=bundles[0].schema,  # Assume all bundles have the same schema
            owns_blocks=bundles[
                0
            ].owns_blocks,  # Assume all bundles have the same ownership
            slices=merged_slices,
        )

    def __eq__(self, other) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)

    def __len__(self) -> int:
        return len(self.blocks)

    def __str__(self) -> str:
        lines = [
            f"RefBundle({len(self.blocks)} blocks,",
            f"  {self.num_rows()} rows,",
            f"  schema={self.schema},",
            f"  owns_blocks={self.owns_blocks},",
            "  blocks=(",
        ]

        # Loop through each block and show details
        for i, ((block_ref, metadata), block_slice) in enumerate(
            zip(self.blocks, self.slices)
        ):
            row_str = (
                f"{metadata.num_rows} rows"
                if metadata.num_rows is not None
                else "unknown rows"
            )
            bytes_str = f"{metadata.size_bytes} bytes"
            slice_str = (
                f"slice={block_slice}"
                if block_slice is not None
                else "slice=None (full block)"
            )

            lines.append(f"    {i}: {row_str}, {bytes_str}, {slice_str}")

        lines.append("  )")
        lines.append(")")

        return "\n".join(lines)


@dataclass
class _ObjectMetadata:
    # Object size in bytes
    size: int
    # Flag whether object has been spilled
    spilled: bool
    # List of nodes object exists on
    locs: List[NodeIdStr] = None


def _ref_bundles_iterator_to_block_refs_list(
    ref_bundles: Iterator[RefBundle],
) -> List[ObjectRef[Block]]:
    """Convert an iterator of RefBundles to a list of Block object references."""
    return [
        block_ref for ref_bundle in ref_bundles for block_ref in ref_bundle.block_refs
    ]


def _iter_sliced_blocks(
    blocks: Iterable[Block],
    slices: List[Optional[BlockSlice]],
) -> Iterator[Block]:
    blocks_list = list(blocks)
    for block, block_slice in zip(blocks_list, slices):
        if block_slice is None:
            # None represents a full block - yield it as is
            yield block
        else:
            accessor = BlockAccessor.for_block(block)
            start = block_slice.start_offset
            end = block_slice.end_offset
            assert start <= end, "start must be less than end"
            assert start >= 0, "start must be non-negative"
            assert (
                end <= accessor.num_rows()
            ), "end must be less than or equal to the number of rows in the block"

            yield accessor.slice(start, end, copy=False)
