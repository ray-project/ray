import collections
import itertools
import math
from typing import List, Any, Optional, Dict, Union, Callable, Tuple

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder

DEFAULT_ROWS_PER_BLOCK = 1000000

_SPLIT_BLOCK_AT_INDICES = None


def _cached_split_block_at_indices():
    global _SPLIT_BLOCK_AT_INDICES

    if _SPLIT_BLOCK_AT_INDICES is None:
        _SPLIT_BLOCK_AT_INDICES = cached_remote_fn(_split_block_at_indices)
    return _SPLIT_BLOCK_AT_INDICES


_MERGE_BLOCKS = None


def _cached_merge_blocks():
    global _MERGE_BLOCKS

    if _MERGE_BLOCKS is None:
        _MERGE_BLOCKS = cached_remote_fn(_merge_blocks)
    return _MERGE_BLOCKS


# TODO(Clark): Expose rows_per_block instead of blocks_per_split? The
# former is probably more intuitive.
def split(blocks: BlockList,
          n: int,
          *,
          equal: bool,
          locality_hints: Optional[List[Any]] = None) -> List[BlockList]:
    """Split the dataset into ``n`` disjoint pieces.

    This returns a list of sub-datasets that can be passed to Ray tasks
    and actors and used to read the dataset records in parallel.

    Examples:
        >>> # Split up a dataset to process over `n` worker actors.
        >>> shards = ds.split(len(workers), locality_hints=workers)
        >>> for shard, worker in zip(shards, workers):
        ...     worker.consume.remote(shard)

    Time complexity: O(1)

    Args:
        n: Number of child datasets to return.
        equal: Whether to guarantee each split has an equal
            number of records. This will drop at most ds.count() % n rows.
        locality_hints: A list of Ray actor handles of size ``n``. The
            system will try to co-locate the blocks of the ith dataset
            with the ith actor to maximize data locality.

    Returns:
        A list of ``n`` disjoint dataset splits.
    """
    if n <= 0:
        raise ValueError(f"The number of splits {n} is not positive.")

    if locality_hints is None:
        return _split_simple(n)

    if locality_hints and len(locality_hints) != n:
        raise ValueError(f"The length of locality_hints {len(locality_hints)} "
                         "doesn't equal the number of splits {n}.")

    return locality_aware_fair_split(blocks, equal, locality_hints)


def locality_aware_fair_split(
        blocks: BlockList,
        equal: bool,
        locality_hints: List[ray.actor.ActorHandle],
        blocks_per_split: Optional[int] = None) -> List[BlockList]:
    """Locality-aware fair splitting of the provided blocks.

    Args:
        blocks: The BlockList to split.
        equal: Whether to guarantee equal-sized blocks.
        locality_hints: The actors on which we wish to colocate split blocks.
        blocks_per_split: The number of desired blocks per split.

    Returns:
        One BlockList split per provided locality hint, each containing
        blocks_per_split blocks.
    """
    # If locality hints are given, we use a five-round greedy algorithm
    # for creating a fair redistribution of blocks across the provided set
    # of actors such that:
    #
    #  1. All actors are allocated the same number of blocks each with the
    #     same number of rows (guarantees no dataset or block skew).
    #  2. Block locality is maximized (minimizes network transfer).
    #  3. Block splitting is minimized (minimizes in-memory copying).
    #  4. Blocks within splits are balanced (minimizes downstream data skew).
    #
    # This algorithm has the following rounds:
    #
    # Round 1 - primary location allocation:
    #  - Allocate each block to the actors on the block's primary
    #    location, up to each actor's row allocation limit.
    #
    # Round 2 - secondary location allocation:
    #  - Allocate each block to the actors on the block's secondary
    #    locations, up to each actor's row allocation limit.
    #
    # Round 3 - locality-aware splitting:
    #  - Split the remaining blocks, attempting to allocate the splits to
    #    actors that are located on nodes that already have the block.
    #
    # Round 4 - locality-agnostic residual splitting:
    #  - Split the remaining blocks, allocating the splits to any actors that
    #    have capacity.
    #
    # Round 5 - rebalance blocks:
    #  - Split blocks larger than the target block size into target-sized
    #    blocks; merge blocks smaller than the target block size into
    #    target-sized blocks.

    # Wait for all dataset blocks to materialize.
    block_refs = list(blocks)
    ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)

    block_set: BlockSet = BlockSet.build(blocks)
    total_rows = sum(block_datum.num_rows for block_datum in iter(block_set))
    capacity = total_rows // len(locality_hints)
    actor_bin_set: BinSet = BinSet.build(locality_hints, capacity)

    actors_by_node_id = _get_actors_by_node_id(locality_hints)
    addresses_by_node_id = _get_addresses_by_node_id()

    # Round 1: Allocate blocks to the actor with the most allocation
    # availability that's at the block's primary location.
    allocate_blocks_to_primary_actors(
        block_set, actor_bin_set, actors_by_node_id, overallocate=not equal)

    # Round 2: Allocate blocks to the actor with the most allocation
    # capacity that's at one of the block's secondary locations.
    allocate_blocks_to_secondary_actors(
        block_set, actor_bin_set, actors_by_node_id, overallocate=not equal)

    if equal:
        # TODO(Clark): Some Ray clusters won't have access to node
        # ID resources (e.g. if their "node" primitive is a container); should
        # we expose a custom resource hook here? This will probably require
        # support from ray.experimental.get_object_locations().
        # We could also fall back to relying on locality-aware
        # scheduling in that case, but that could result in a lot of
        # unnecessary data transfer if the cluster is busy.

        # Round 3: Split the blocks, attempting to allocate the splits to
        # actors that are located on nodes that already have the block.
        split_blocks_over_actors(
            block_set, actor_bin_set,
            lambda node_id: actors_by_node_id[node_id],
            lambda node_id: f"node:{addresses_by_node_id[node_id]}")

        # Round 4: Split the blocks, allocating the splits to arbitrary
        # nodes.
        split_blocks_over_arbitrary_actors(block_set, actor_bin_set)

        block_set.resolve_metadata_futures()

        if blocks_per_split is None:
            # TODO(Clark): Tweak this default.
            blocks_per_split = max(
                100, min(2, math.ceil(capacity / DEFAULT_ROWS_PER_BLOCK)))
        # TODO(Clark): This will further truncate capacity % blocks_per_split
        # rows from each block; since it's not important for the blocks to
        # be exactly balanced within splits, we should allow for off-by-one
        # blocks to minimize truncation. We truncate for now in order to
        # make the rebalancing logic much simpler.
        block_size = capacity // blocks_per_split

        # Round 5: Split blocks larger than the target block size into
        # target-sized blocks; merge blocks smaller than the target block
        # size into target-sized blocks.
        rebalance_blocks(block_set, actor_bin_set, block_size)

        block_set.resolve_metadata_futures()
    else:
        # We don't want to split blocks, so we allocate the remaining blocks
        # to arbitrary actors.
        allocate_blocks_to_arbitrary_actors(block_set, actor_bin_set)

    # Build output block lists.
    block_lists = []
    for actor in locality_hints:
        allocated_blocks = actor_bin_set.get_bin(actor).allocated_blocks
        block_lists.append(
            BlockList([block.block for block in allocated_blocks],
                      [block.meta for block in allocated_blocks]))
    return block_lists


class BlockData:
    """A block that we wish to pack into a bin. This is a helper data structure
    for tracking the nodes that a block prefers (e.g. locality), the number of
    rows in the block (calculated separately from the block metadata for split
    blocks), and whether the block has been allocated to an actor bin yet.
    """
    # The underlying block future.
    block: ObjectRef[Block]
    # Either concrete block metadata, or a block metadata future if this block
    # was the result of a split and we haven't resolved the metadata yet.
    # TODO(Clark): Make meta field private, expose get_meta() method,
    # and throw an error if method called while meta is still unresolved.
    # (Or transparently resolve the metadata future when called.)
    meta: Union[BlockMetadata, ObjectRef[BlockMetadata]]
    # Number of rows in the block.
    num_rows: int
    # Nodes that the block prefers, e.g. for locality-aware allocation and
    # splitting, this would be the set of nodes on which the block already
    # resides.
    preferred_nodes: Optional[List[str]]
    # Whether the block has been allocated to an actor bin yet.
    allocated: bool

    def __init__(self,
                 block: ObjectRef[Block],
                 meta: Union[BlockMetadata, ObjectRef[BlockMetadata]],
                 num_rows: int,
                 preferred_nodes: Optional[List[str]] = None):
        self.block = block
        self.meta = meta
        self.num_rows = num_rows
        self.preferred_nodes = preferred_nodes
        self.allocated = False

    def set_allocated(self):
        """Mark the block as allocated to an actor bin.
        """
        self.allocated = True

    def __str__(self):
        return (f"{type(self).__name__}: block={self.block}, "
                f"num_rows={self.num_rows}, "
                f"preferred_nodes={self.preferred_nodes}, "
                f"meta={self.meta}, allocated={self.allocated}")

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.block)


class BlockSet:
    """A set of blocks that we wish to allocate to actor bins.
    """
    # A map of blocks in the set.
    blocks: Dict[ObjectRef[Block], BlockData]
    # Blocks that need metadata future resolution.
    blocks_missing_meta: List[ObjectRef[Block]]

    def __init__(self, blocks: List[BlockData] = None):
        if blocks is None:
            blocks = []
        self.blocks = {b.block: b for b in blocks}
        self.blocks_missing_meta = []

    @classmethod
    def build(cls, blocks: List[ObjectRef[Block]]):
        """Build a BlockSet from a list of block futures.
        """
        metadata = _get_metadata_for_blocks(blocks)
        node_ids_by_block = _get_node_ids_by_block(blocks)
        return cls([
            BlockData(b, metadata[b], metadata[b].num_rows,
                      node_ids_by_block[b]) for b in blocks
        ])

    def get_block(self, block: ObjectRef[Block]) -> BlockData:
        """Retrieve a single block.
        """
        return self.blocks[block]

    def get_unallocated_blocks(self) -> List[BlockData]:
        """Get all currently unallocated blocks.
        """
        return [block for block in self.blocks.values() if not block.allocated]

    def get_allocated_blocks(self) -> List[BlockData]:
        """Get all allocated blocks.
        """
        return [block for block in self.blocks.values() if block.allocated]

    def get_unallocated_blocks_ordered_by_size(
            self, descending=True) -> List[BlockData]:
        """Get all unallocated blocks, sorted by their size.

        Args:
            descending: Whether to sort in descending order. Defaults to True.

        Returns:
            A list of unallocated blocks sorted by their size.
        """
        # TODO(Clark): Use a size-based priority queue for unallocated block
        # set so we don't have to continuously re-sort.
        return sorted(
            self.get_unallocated_blocks(),
            key=lambda block: block.num_rows,
            reverse=descending)

    def mark_allocated(self, block: BlockData):
        """Mark the block as allocated.
        """
        self.get_block(block.block).set_allocated()

    def add_block(self, block_data: BlockData):
        """Add a new block to the set, e.g. after a split.
        """
        self.blocks[block_data.block] = block_data
        if isinstance(block_data.meta, ray.ObjectRef):
            self.blocks_missing_meta.append(block_data.block)

    def discard_block(self, block_data: BlockData):
        """Discard the block from the set.
        """
        del self.blocks[block_data.block]

    def resolve_metadata_futures(self):
        """Resolve all metadata futures for all blocks that don't have concrete
        block metadata.
        """
        if self.blocks_missing_meta:
            meta_futures = [
                self.get_block(block).meta
                for block in self.blocks_missing_meta
            ]
            for block, meta in zip(self.blocks_missing_meta,
                                   ray.get(meta_futures)):
                # Overwrite the block metadata future with the concrete
                # resolved block metadata.
                self.get_block(block).meta = meta
            # All metadata futures have been resolved, clear the futures.
            self.blocks_missing_meta = []

    def num_blocks(self) -> int:
        """Get the number of blocks.
        """
        return len(self.blocks)

    def __str__(self):
        return (f"{type(self).__name__}: blocks="
                f"{list(map(repr, self.blocks.values()))}")

    def __repr__(self):
        return str(self)


# Typically an actor handle.
BinID = Any


class Bin:
    """A bin that we wish to pack with blocks. This is a helper data structure
    for tracking the blocks that have been allocated to the bin, along with
    helper methods for e.g. calculating the remaining slack in the bin.
    """
    # An identifier for the bin.
    id: BinID
    # The capacity of the bin.
    capacity: int
    # All blocks that have been allocated to the bin.
    # TODO(Clark): Should this itself be a BlockSet, since we perform similar
    # operations during intra-split block rebalancing?
    allocated_blocks: List[BlockData]
    # The cumulative size of all blocks that have been allocated to the bin.
    allocated_size: int

    def __init__(self, id: BinID, capacity: int):
        self.id = id
        self.capacity = capacity
        self.allocated_blocks = []
        self.allocated_size = 0

    @property
    def slack(self):
        return self.capacity - self.allocated_size

    def can_allocate(self, size: int, overallocate=False) -> bool:
        """Whether we can allocate a block of the given size to this bin.

        Args:
            size: The size of the block that we wish to allocate.
            overallocate: Whether to overallocate the bin by one block, i.e.
                letting a single block straddle the capacity boundary.

        Returns:
            Whether we can allocate a block of the given size to this bin.
        """
        if overallocate:
            return self.slack > 0
        else:
            return size <= self.slack

    def try_allocate(self, block: BlockData, overallocate=False) -> bool:
        """Try to allocate the given block to the bin. Returns whether the
        allocation was successful.

        Args:
            block: The block that we wish to allocate.
            overallocate: Whether to overallocate the bin by one block, i.e.
                letting a single block straddle the capacity boundary.

        Returns:
            Whether the allocation was successful.
        """
        if self.can_allocate(block.num_rows, overallocate=overallocate):
            self.allocate(block)
            return True
        return False

    def allocate(self, block: BlockData):
        """Allocate the given block to the bin. This won't check the bin's
        capacity.

        Args:
            block: The block that we will allocate.
        """
        self.allocated_blocks.append(block)
        self.allocated_size += block.num_rows

    def get_allocation(self):
        """Returns the blocks that have been allocated to this bin.
        """
        return self.allocated_blocks

    def get_allocated_size(self):
        """Returns the cumulative size of all allocated blocks.
        """
        return self.allocated_size

    def clear_allocation(self):
        """Clears the current block allocation for this bin.
        """
        self.allocated_blocks = []
        self.allocated_size = 0

    def __eq__(self, other):
        if not isinstance(other, Bin):
            return False
        return self.id == other.id

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{type(self).__name__}: id={self.id}, cap={self.capacity}"

    def __hash__(self):
        return hash(self.id)


class BinSet:
    """A set of bins that we wish to allocate blocks to.
    """
    # A map of bins in the set.
    bins: Dict[BinID, Bin]

    def __init__(self, bins: List[Bin] = None):
        if bins is None:
            bins = []
        self.bins = {bin_.id: bin_ for bin_ in bins}

    @classmethod
    def build(cls, bin_ids: List[BinID], capacity: int):
        """Create a BinSet with uniform capacity.
        """
        return cls([Bin(bin_id, capacity) for bin_id in bin_ids])

    def get_bin(self, key: Union[BinID, int]) -> Bin:
        """Get a bin. The provided key can either be a bin identifier or an
        integer index; in the latter case, the integer index key will give the
        corresponding bin according to insertion order.
        """
        try:
            return self.bins[key]
        except KeyError:
            return list(iter(self))[key]

    def get_bin_subset(self, bin_ids: List[BinID]) -> "BinSet":
        """Get a subset of bins.

        Args:
            bin_ids: The ids of bins that we want in the subset.

        Returns:
            A BinSet containing only the bins that correspond to the provided
            bin IDs.
        """
        return BinSet([self.get_bin(bin_id) for bin_id in bin_ids])

    def get_bin_with_largest_slack(self) -> Bin:
        """Get the bin with the most slack.
        """
        return max(iter(self), key=lambda bin_: bin_.slack)

    def get_bin_with_best_fit(self, block: BlockData) -> Bin:
        """Get bin with the best (tightest) fit.
        """
        return min(iter(self), key=lambda bin_: bin_.slack - block.num_rows)

    def get_bins_ordered_by_slack(self, descending: bool = True) -> List[Bin]:
        """Get all available (not full) bins ordered by slack.

        Args:
            descending: Whether to order the list in descending order.

        Returns:
            A list of bins in sorted slack order, with full bins removed.
        """
        # TODO(Clark): Use a slack-based priority queue for the available bin
        # set so we don't have to continuously re-sort and recalculate the
        # max slack.
        bins = filter(lambda bin_: bin_.slack > 0, iter(self))
        return sorted(bins, key=lambda bin_: bin_.slack, reverse=descending)

    def is_empty(self):
        """Whether this bin set is empty.
        """
        return len(self.bins) == 0

    def __iter__(self):
        return iter(self.bins.values())

    def __getitem__(self, key):
        return self.get_bin(key)


def allocate_blocks_to_primary_actors(
        block_set: BlockSet,
        actor_bin_set: BinSet,
        actors_by_node_id: Dict[str, ray.actor.ActorHandle],
        overallocate: bool = False):
    """Attempt to allocate full blocks to actors on primary nodes.
    """
    primary_actors_by_block = {
        b: actors_by_node_id[b.preferred_nodes[0]]
        for b in block_set.get_unallocated_blocks()
    }
    _allocate_blocks_to_actors(
        block_set,
        actor_bin_set,
        lambda b: primary_actors_by_block[b],
        overallocate=overallocate)


def allocate_blocks_to_secondary_actors(
        block_set: BlockSet,
        actor_bin_set: BinSet,
        actors_by_node_id: Dict[str, ray.actor.ActorHandle],
        overallocate: bool = False):
    """Attempt to allocate full blocks to actors on secondary nodes.
    """
    secondary_actors_by_block = {
        b: itertools.chain.from_iterable(
            actors_by_node_id[node_id] for node_id in b.preferred_nodes[1:])
        for b in block_set.get_unallocated_blocks()
    }
    _allocate_blocks_to_actors(
        block_set,
        actor_bin_set,
        lambda b: secondary_actors_by_block[b],
        overallocate=overallocate)


def allocate_blocks_to_arbitrary_actors(block_set: BlockSet,
                                        actor_bin_set: BinSet):
    """Attempt to allocate blocks to arbitrary actors, with no preference
    for data locality.
    """
    _allocate_blocks_to_actors(block_set, actor_bin_set)


def _allocate_blocks_to_actors(block_set: BlockSet,
                               actor_bin_set: BinSet,
                               actor_subsetter: Optional[Callable[[str], List[
                                   ray.actor.ActorHandle]]] = None,
                               overallocate: bool = False):
    """Attempt to allocate full blocks to actors in the given (possibly
    improper) subset.
    """
    # Sort blocks into decreasing order.
    blocks = block_set.get_unallocated_blocks_ordered_by_size()
    # Allocate blocks to the actor with the most allocation
    # availability that resides within the given location slice.
    for block in blocks:
        if actor_subsetter is not None:
            actors = actor_subsetter(block)
            actor_bin_subset = actor_bin_set.get_bin_subset(actors)
        # Get the actor with the most allocation availability.
        # TODO(Clark): Try out best-fit rather than largest-slack-fit.
        actor_bin = actor_bin_subset.get_bin_with_largest_slack()
        # Try to allocate the block to this bin.
        did_allocate = actor_bin.try_allocate(block, overallocate=overallocate)
        if did_allocate:
            block_set.mark_allocated(block)


def split_blocks_over_actors(
        block_set: BlockSet, actor_bin_set: BinSet,
        actor_subsetter: Callable[[str], List[ray.actor.ActorHandle]],
        get_resource_label: Callable[[str], str]):
    """Split the given block set over the given actor bin set, adhering to the
    provided subsetting (locality) constraints.
    """
    # Assumed invariant: No full block will fit in a local bin.
    # Split the blocks, attempting to allocate the splits to
    # actors that are located on nodes that already have the block.
    for block in block_set.get_unallocated_blocks():
        _split_block_over_actor_bins_by_node(block, block_set, actor_bin_set,
                                             actor_subsetter,
                                             get_resource_label)


def _split_block_over_actor_bins_by_node(
        block: BlockData, block_set: BlockSet, actor_bin_set: BinSet,
        actor_subsetter: Callable[[str], List[ray.actor.ActorHandle]],
        get_resource_label: Callable[[str], str]):
    """Split the given block over the given actor bin set, adhering to the
    provided subsetting (locality) constraints.
    """
    offset = 0
    did_split = False
    for node_id in block.preferred_nodes:
        if offset >= block.num_rows:
            break
        # Get actor subset for this node.
        actors = actor_subsetter(node_id)
        actor_bin_subset = actor_bin_set.get_bin_subset(actors)
        # We give a custom node resource in order to ensure that this
        # splitting task is scheduled on the same node as the block.
        resource_label = get_resource_label(node_id)
        did_split_for_node, offset = _split_block_over_actor_bins(
            block,
            block_set,
            actor_bin_subset,
            split_resources={resource_label: 0.001},
            offset=offset)
        did_split |= did_split_for_node
    if did_split:
        block_set.discard_block(block)
        if offset < block.num_rows:
            # There's a leftover slice of the block that wasn't allocated
            # in this round, save it for allocation in the next round.
            (leftover_block,
             leftover_metadata) = _cached_split_block_at_indices().options(
                 num_returns=2, resources={
                     resource_label: 0.001
                 }).remote(block.block, block.meta, [offset, block.num_rows])
            leftover_size = block.num_rows - offset
            leftover_block_data = BlockData(leftover_block, leftover_metadata,
                                            leftover_size)
            block_set.add_block(leftover_block_data)


def _split_block_over_actor_bins(
        block: BlockData,
        block_set: BlockSet,
        actor_bin_set: BinSet,
        split_resources: Optional[Dict[str, float]] = None,
        offset: Optional[int] = 0):
    """Split the given block over the provided actor bin set, returning
    the end-boundary offset if the entire block's contents aren't allocated.
    """
    split_indices, actor_bins, offset = _get_block_split_indices(
        actor_bin_set, block.num_rows, offset)
    if split_indices is None:
        return False, offset
    split_sizes = _get_split_sizes(split_indices)
    # Submit splitting task.
    block_splits = _create_splits(
        block.block,
        block.meta,
        split_indices,
        split_resources=split_resources)
    for (split_block, split_meta), actor_bin, num_rows in zip(
            block_splits, actor_bins, split_sizes):
        block_data = BlockData(split_block, split_meta, num_rows)
        block_set.add_block(block_data)
        actor_bin.allocate(block_data)
        block_set.mark_allocated(block_data)
    return True, offset


def split_blocks_over_arbitrary_actors(block_set: BlockSet,
                                       actor_bin_set: BinSet):
    """Split the given block set over the given actor set, with not subsetting
    (locality) constraints.
    """
    # Get blocks sorted by size (increasing).
    blocks = block_set.get_unallocated_blocks_ordered_by_size()
    for block in blocks:
        did_split, _ = _split_block_over_actor_bins(block, block_set,
                                                    actor_bin_set)
        assert did_split


def rebalance_blocks(block_set: BlockSet, actor_bin_set: BinSet,
                     target_block_size: int):
    """Rebalance each actor bin's set of allocated blocks such that each block
    has the given target block size. This only rebalances within a given bin,
    i.e. it does not transfer data between bins.
    This assumes that the total number of rows in each bin is evenly divisible
    by the provided target block size.
    """
    for actor_bin in iter(actor_bin_set):
        blocks = actor_bin.get_allocation()
        blocks = sorted(blocks, key=lambda b: b.num_rows, reverse=True)
        idx = next(
            i for i, b in enumerate(blocks) if b.num_rows < target_block_size)
        larger_blocks, smaller_blocks = blocks[:idx], blocks[idx:]
        # Split large blocks.
        new_allocation_large, small_leftovers = _rebalance_larger_blocks(
            larger_blocks, block_set, target_block_size)
        # Move on to blocks smaller than target.
        smaller_blocks += small_leftovers
        smaller_blocks = sorted(
            smaller_blocks, lambda b: b.num_rows, reverse=True)
        # Merge small blocks.
        new_allocation_small = _rebalance_smaller_blocks(
            smaller_blocks, block_set, target_block_size)
        new_allocation = new_allocation_large + new_allocation_small
        assert all(b.num_rows == target_block_size for b in new_allocation)
        actor_bin.clear_allocation()
        for block in new_allocation:
            actor_bin.allocate(block)
            block_set.mark_allocated(block)


def _rebalance_larger_blocks(
        blocks: List[BlockData], block_set: BlockSet,
        target_block_size: int) -> Tuple[List[BlockData], List[BlockData]]:
    """Rebalance blocks that are larger than the target block size;
    this will involve splitting each large block into one or more blocks of
    the target block size, plus at most one remainder block that's smaller and
    will be returned as a leftover.
    """
    new_allocation = []
    leftovers = []
    # First, process blocks larger than or equal to target.
    for block in enumerate(blocks):
        size = block.num_rows
        assert size >= target_block_size
        if size == target_block_size:
            new_allocation.append(block)
            continue
        # Case: size > target_block_size
        split_indices = list(range(0, size, target_block_size))
        if split_indices[-1] < size:
            split_indices.append(size)
        split_sizes = _get_split_sizes(split_indices)
        block_splits = _create_splits(block.block, block.meta, split_indices)
        last_split_size = split_sizes[-1]
        if last_split_size < target_block_size:
            # Handle leftover block.
            leftover_block, leftover_meta = block_splits.pop()
            split_sizes.pop()
            leftovers.append(
                BlockData(leftover_block, leftover_meta, last_split_size))
        for (block, meta), split_size in zip(block_splits, split_sizes):
            new_block_data = BlockData(block, meta, split_size)
            new_allocation.append(new_block_data)
            block_set.add_block(new_block_data)
        block_set.discard_block(block)
    return new_allocation, leftovers


def _rebalance_smaller_blocks(blocks: List[BlockData], block_set: BlockSet,
                              target_block_size: int) -> List[BlockData]:
    """Rebalance blocks that are smaller than the target block size; this will
    involve merging small blocks until we reach the target block size,
    splitting the last small block if needed.
    """
    new_allocation = []
    # This merge buffer is added to until we've reached the
    # target_block_size target, at which point we merge all of the
    # blocks and clear the buffer.
    # TODO(Clark): Add an abstraction (class) for the merge buffer?
    merge_buffer = []
    merge_buffer_size = 0
    # We use two-pointer loop to allocate blocks to the buffer, where
    # we first try to allocate the larger (left) block, then try to
    # allocate the smaller (right) block, and if neither fit, then we
    # split the smaller (right) block to add a sub-block that does fit.
    # This naive method approximates an optimal packing of blocks into
    # target_block_size bins that minimizes splitting.
    left = 0
    right = len(blocks) - 1
    while left < right:
        left_block = blocks[left]
        left_size = left_block.num_rows
        right_block = blocks[right]
        right_size = right_block.num_rows
        if merge_buffer_size + left_size <= target_block_size:
            # The left (larger) block can fit into the merge buffer,
            # so add it to the buffer and increment the left pointer.
            merge_buffer.append(left_block)
            merge_buffer_size += left_size
            left += 1
        elif merge_buffer_size + right_size <= target_block_size:
            # The right (smaller) block can fit into the merge buffer,
            # so add it to the buffer and increment the right pointer.
            merge_buffer.append(right_block)
            merge_buffer_size += right_size
            right -= 1
        else:
            # Neither the left nor the right block fit in the merge
            # buffer, so we split right (smaller) block into a block
            # that will fit into the buffer and a leftover block that
            # we add back into the candidate block list.

            # Submit splitting task.
            diff = target_block_size - merge_buffer_size
            (block, meta, leftover_block, leftover_meta
             ) = _cached_split_block_at_indices().options(num_return=4).remote(
                 right_block.block, right_block.meta, [0, diff, right_size])
            block_set.discard_block(right_block)
            leftover_block_data = BlockData(leftover_block, leftover_meta,
                                            right_size - diff)
            block_set.add_block(leftover_block_data)
            block_data = BlockData(block, meta, diff)
            block_set.add_block(block_data)
            merge_buffer.append(block)
            merge_buffer_size += diff
            assert merge_buffer_size == target_block_size
            # We overwrite the old rightmost block and don't advance
            # the right pointer since (1) the old block can be
            # discarded, (2) the leftover block is guaranteed to be
            # smaller than the old block, and (3) the rightmost block
            # should be the smallest block in the to-be-allocated set.
            blocks[right] = leftover_block
        if merge_buffer_size == target_block_size:
            # Resolve all outstanding metadata futures in the merge buffer,
            # e.g. from the above splitting of blocks larger than the target
            # size.
            # TODO(Clark): Find way to avoid this by separately tracking input
            # files and propagating them to splits, as we do with num_rows.
            block_set.resolve_metadata_futures()
            input_files = set(
                itertools.chain(b.meta.input_files for b in merge_buffer))
            merged_block, merged_metadata = _cached_merge_blocks().remote(
                *[b.block for b in merge_buffer], input_files=input_files)
            for b in merge_buffer:
                block_set.discard_block(b)
            block_set.add_block(
                BlockData(merged_block, merged_metadata, target_block_size))
            new_allocation.append(merged_block)
            merge_buffer = []
            merge_buffer_size = 0
    return new_allocation


def _create_splits(
        block: BlockData,
        block_metadata: BlockMetadata,
        split_indices: List[int],
        split_resources: Optional[Dict[str, float]] = None
) -> List[Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]]]:
    """Split the provided block according to the given split indices.
    """
    block_splits = _cached_split_block_at_indices().options(
        num_returns=2 * (len(split_indices) - 1),
        resources=split_resources).remote(block, block_metadata, split_indices)
    # Pack [b0, m0, b1, m1] into [(b0, m0), (b1, m1)].
    return list(zip(block_splits[::2], block_splits[1::2]))


def _get_block_split_indices(actor_bin_set: BinSet,
                             block_size: int,
                             offset: int = 0):
    """Get the indices at which a block of the given size should be split in
    order to allocate the splits across the provided actor bins.
    """
    # Get block splits that would fill some or all of the remaining
    # slack in some or all of the given actors.
    assert actor_bin_set.is_empty() or block_size > actor_bin_set[0].slack
    sorted_bins = actor_bin_set.get_bins_ordered_by_slack()
    splits = []
    cum_num_rows = offset
    idx = 0
    while cum_num_rows < block_size and idx < len(sorted_bins):
        actor_bin = sorted_bins[idx]
        cum_num_rows += actor_bin.slack
        splits.append((cum_num_rows, actor_bin))
        idx += 1
    if not splits:
        # No splits were made, so we move on to the next location.
        return None, None, None
    if len(splits) == 1 and splits[0][0] >= block_size and offset == 0:
        # Entire block fits in a single split; this function should only be
        # called when the block doesn't completely fit in an actor bin.
        raise ValueError(
            "Entire block fits in a single actor bin; "
            "_get_block_split_indices() should only be called on a block that "
            "doesn't completely fit in an actor bin.")
    split_indices, actor_bins = zip(*splits)
    split_indices = [offset] + list(split_indices)
    # Bound final block by block size. This will also be the returned
    # offset.
    split_indices[-1] = min(split_indices[-1], block_size)
    return split_indices, list(actor_bins), split_indices[-1]


def _get_split_sizes(split_indices: List[int]):
    # Get the size (number of rows) of each block split.
    return [
        high - low for low, high in zip(split_indices[:-1], split_indices[1:])
    ]


def _get_metadata_for_blocks(blocks: BlockList):
    """Get a block --> metadata mapping for all provided blocks. If the number
    of rows is missing on a block's metadata, we manually resolve the number of
    rows.
    """
    metadata = {b: m for b, m in zip(blocks, blocks.get_metadata())}
    missing = [b for b, m in metadata.items() if m.num_rows is None]
    if missing:
        get_num_rows = cached_remote_fn(
            lambda block: BlockAccessor.for_block(block).num_rows())
        # Get missing row data.
        missing_num_rows = ray.get([get_num_rows.remote(b) for b in missing])
        for block, num_rows in zip(missing, missing_num_rows):
            metadata[block].num_rows = num_rows
    assert all(m.num_rows is not None for m in metadata.values())
    return metadata


def _get_node_ids_by_block(
        blocks: List[ObjectRef[Block]]) -> Dict[str, List[ObjectRef[Block]]]:
    """Build the reverse index from node_id to block_refs.
    """
    block_ref_locations = ray.experimental.get_object_locations(blocks)
    node_ids_by_block = collections.defaultdict(list)
    for block_ref in blocks:
        node_ids = block_ref_locations.get(block_ref, {}).get("node_ids", [])
        node_ids_by_block[block_ref] = node_ids
    return node_ids_by_block


def _get_actors_by_node_id(actors: List[Any]) -> Dict[Any, str]:
    """Build a map from a actor to its node_id.
    """
    actors_state = ray.state.actors()
    actors_by_node_id = collections.defaultdict(list)
    for actor in actors:
        node_id = actors_state.get(actor._actor_id.hex(), {}).get(
            "Address", {}).get("NodeID")
        actors_by_node_id[node_id].append(actor)
    return actors_by_node_id


def _get_addresses_by_node_id():
    """Build a map from node ID to IP address.
    """
    return {node["NodeID"]: node["NodeManagerAddress"] for node in ray.nodes()}


def _split_simple(blocks: BlockList,
                  n: int,
                  *,
                  blocks_per_split: Optional[int] = None):
    # TODO(Clark): Complete this implementation, merge with data structures
    # developed for locality-aware split.
    metadata_mapping = {b: m for b, m in zip(blocks, blocks.get_metadata())}
    num_rows_by_block = {b: m.num_rows for b, m in metadata_mapping.items()}
    missing = [
        b for b, num_rows in num_rows_by_block.items() if num_rows is None
    ]
    if missing:
        get_num_rows = cached_remote_fn(
            lambda block: BlockAccessor.for_block(block).num_rows())
        missing_num_rows = ray.get([get_num_rows.remote(b) for b in missing])
        num_rows_by_block.update(zip(missing, missing_num_rows))
    assert all(num_rows is not None for num_rows in num_rows_by_block.values())
    total_rows = sum(num_rows_by_block.values())

    split_block_at_indices = _cached_split_block_at_indices()
    # merge_blocks = _cached_merge_blocks()

    target_num_rows_per_slot = total_rows // n
    blocks = list(blocks)
    blocks = sorted(blocks, lambda b: num_rows_by_block[b], reverse=True)
    block_allocation_per_slot = [[] for _ in range(n)]
    num_rows_allocated_per_slot = [0] * n
    metadata_futures = []
    # Allocate blocks to dataset slots, considering largest blocks
    # first.
    for block in blocks:
        size = num_rows_by_block[block]
        # Get actors with any remaining allocation slack.
        slots_with_slack = [
            ((target_num_rows_per_slot - num_rows_allocated_per_slot[slot]),
             slot) for slot in range(n)
        ]
        slots_with_slack = list(filter(lambda x: x[0] > 0, slots_with_slack))
        # Order the slots from most slack to least.
        slots_with_slack = sorted(slots_with_slack, reverse=True)
        # Get block splits that would fill some or all of the remaining
        # slack in some or all of the slots.
        splits = []
        cum_num_rows = 0
        idx = 0
        while cum_num_rows < size:
            assert idx < len(slots_with_slack)
            slack, slot = slots_with_slack[idx]
            cum_num_rows += slack
            splits.append((cum_num_rows, slot))
            idx += 1

        assert len(splits) > 0
        split_indices, slots = zip(*splits)
        # Bound final block by block size.
        split_indices[-1] = min(split_indices[-1], size)
        # Get the size (number of rows) of each block split.
        split_sizes = [
            high - low
            for low, high in zip(split_indices[:-1], split_indices[1:])
        ]
        split_indices = [0] + split_indices
        # Submit splitting task.
        block_splits = split_block_at_indices.options(
            num_return=2 * (len(split_indices) - 1)).remote(
                block, metadata_mapping[block], split_indices)
        del num_rows_by_block[block]
        del metadata_mapping[block]
        # Pack [b0, m0, b1, m1] into [(b0, m0), (b1, m1)], and pair
        # with the corresponding block split sizes.
        block_splits = list(
            zip(split_sizes, zip(block_splits[::2], block_splits[1::2])))
        # Allocate the splits to the corresponding slots.
        for slot, (num_rows, (block_split, meta)) in zip(slots, block_splits):
            block_allocation_per_slot[slot].append(block_split)
            num_rows_allocated_per_slot[slot] += num_rows
            num_rows_by_block[block_split] = num_rows
            # Save these block metadata futures; we'll fetch them in
            # bulk later.
            metadata_futures.append((block_split, meta))
    # All blocks should be allocated at this point.
    assert all(allocated == target_num_rows_per_slot
               for allocated in num_rows_allocated_per_slot.values())

    # TODO(Clark): Add block balancing round, see locality-aware
    # splitting, round 5.

    # Fetch new metadata for block splits.
    blocks, metas = zip(*metadata_futures)
    if metas:
        metas = ray.get(metas)
        metadata_mapping.update(dict(zip(blocks, metas)))

    return [
        BlockList(
            block_allocation_per_slot[slot],
            [metadata_mapping[b] for b in block_allocation_per_slot[slot]])
        for slot in range(n)
    ]


def _split_block_at_indices(block: Block, meta: BlockMetadata,
                            indices: List[int]):
    """Split the block at the provided indices, producing len(indices) - 1
    blocks. If given indices [a, b, c, d], this will return splits
    [a, b), [b, c), and [c, d).
    """
    block = BlockAccessor.for_block(block)
    assert len(indices) >= 2
    out = []
    for low, high in zip(indices[:-1], indices[1:]):
        b = block.slice(low, high, copy=True)
        a = BlockAccessor.for_block(b)
        m = BlockMetadata(
            num_rows=a.num_rows(),
            size_bytes=a.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files)
        out.append(b)
        out.append(m)
    return out


def _merge_blocks(*blocks: List[Block], input_files: List[str]):
    """Merge the provided blocks into a single block.
    """
    builder = DelegatingArrowBlockBuilder()
    for block in blocks:
        builder.add_block(block)
    out_block = builder.build()
    return out_block, BlockAccessor.for_block(out_block).get_metadata(
        input_files)
