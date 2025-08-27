from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

import ray
from .common import NodeIdStr
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.context import DataContext
from ray.types import ObjectRef


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
        for b in self.blocks:
            assert isinstance(b, tuple), b
            assert len(b) == 2, b
            assert isinstance(b[0], ray.ObjectRef), b[0]
            assert isinstance(b[1], BlockMetadata), b[1]
            if b[1].size_bytes is None:
                raise ValueError(
                    "The size in bytes of the block must be known: {}".format(b)
                )

        import pyarrow as pa

        # The schema metadata might be unhashable.
        # We need schemas to be hashable for unification
        if isinstance(self.schema, pa.lib.Schema):
            self.schema = self.schema.remove_metadata()

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
        """Number of rows present in this bundle, if known."""
        total = 0
        for m in self.metadata:
            if m.num_rows is None:
                return None
            else:
                total += m.num_rows
        return total

    def size_bytes(self) -> int:
        """Size of the blocks of this bundle in bytes."""
        return sum(m.size_bytes for m in self.metadata)

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

    def __eq__(self, other) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)

    def __len__(self) -> int:
        return len(self.blocks)


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
