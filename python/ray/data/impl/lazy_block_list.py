import math
from typing import List, Iterator, Tuple, Optional, Dict, Any
import uuid

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockPartitionMetadata,
    MaybeBlockPartition,
)
from ray.data.context import DatasetContext
from ray.data.datasource import ReadTask
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.stats import DatasetStats, _get_or_create_stats_actor


def execute_read_task(
    i: int,
    task: ReadTask,
    context: DatasetContext,
    stats_uuid: str,
    stats_actor: ray.actor.ActorHandle,
) -> Tuple[MaybeBlockPartition, BlockPartitionMetadata]:
    DatasetContext._set_current(context)
    stats = BlockExecStats.builder()

    # Execute the task.
    block = task()

    metadata = task.get_metadata()
    if context.block_splitting_enabled:
        metadata.exec_stats = stats.build()
    else:
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=metadata.input_files, exec_stats=stats.build()
        )
    stats_actor.record_task.remote(stats_uuid, i, metadata)
    return block, metadata


class LazyBlockList(BlockList):
    """A BlockList that submits tasks lazily on-demand.

    This BlockList is used for implementing read operations (e.g., to avoid
    needing to read all files of a Dataset when the user is just wanting to
    .take() the first few rows or view the schema).
    """

    def __init__(
        self,
        tasks: List[ReadTask],
        block_partitions: Optional[List[ObjectRef[MaybeBlockPartition]]] = None,
        block_partitions_meta: Optional[List[ObjectRef[BlockPartitionMetadata]]] = None,
        fetched_metadata: Optional[List[BlockPartitionMetadata]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        stats_uuid: str = None,
    ):
        """Create a LazyBlockList on the provided read tasks.

        Args:
            tasks: The read tasks that will produce the blocks of this lazy block list.
            block_partitions: An optional list of already submitted read task futures
                (i.e. block partition refs). This should be the same length as the tasks
                argument.
            block_partitions_meta: An optional list of block partition metadata refs.
                This should be the same length as the tasks argument.
            fetched_metadata: An optional list of already computed AND fetched metadata.
                This serves as a cache of fetched block metadata.
            ray_remote_args: Ray remote arguments for the read tasks.
            stats_uuid: UUID for the dataset stats, used to group and fetch read task
                stats. If not provided, a new UUID will be created.
        """
        self._tasks = tasks
        self._num_blocks = len(self._tasks)
        if stats_uuid is None:
            stats_uuid = uuid.uuid4()
        self._stats_uuid = stats_uuid
        self._execution_started = False
        self._remote_args = ray_remote_args or {}
        # Block partition metadata that have already been computed and fetched.
        if fetched_metadata is not None:
            self._fetched_metadata = fetched_metadata
        else:
            self._fetched_metadata = [None] * len(tasks)
        # Block partition metadata that have already been computed.
        if block_partitions_meta is not None:
            self._block_partitions_meta = block_partitions_meta
        else:
            self._block_partitions_meta = [None] * len(tasks)
        # Block partitions that have already been computed.
        if block_partitions is not None:
            self._block_partitions = block_partitions
        else:
            self._block_partitions = [None] * len(tasks)
        assert len(tasks) == len(self._block_partitions), (
            tasks,
            self._block_partitions,
        )
        assert len(tasks) == len(self._block_partitions_meta), (
            tasks,
            self._block_partitions_meta,
        )
        assert len(tasks) == len(self._fetched_metadata), (
            tasks,
            self._fetched_metadata,
        )

    def get_metadata(self, fetch_if_missing: bool = False) -> List[BlockMetadata]:
        """Get the metadata for all blocks."""
        if all(meta is not None for meta in self._fetched_metadata):
            # Always return fetched metadata if we already have it.
            metadata = self._fetched_metadata
        elif not fetch_if_missing:
            metadata = [
                m if m is not None else t.get_metadata()
                for m, t in zip(self._fetched_metadata, self._tasks)
            ]
        else:
            _, metadata = self._get_blocks_with_metadata()
        return metadata

    def stats(self) -> DatasetStats:
        """Create DatasetStats for this LazyBlockList."""
        return DatasetStats(
            stages={"read": self.get_metadata(fetch_if_missing=False)},
            parent=None,
            needs_stats_actor=True,
            stats_uuid=self._stats_uuid,
        )

    def _submit_task(self, task_idx: int) -> ObjectRef[MaybeBlockPartition]:
        """Submit the task with index task_idx."""
        stats_actor = _get_or_create_stats_actor()
        if not self._execution_started:
            stats_actor.record_start.remote(self._stats_uuid)
            self._execution_started = True
        task = self._tasks[task_idx]
        return (
            cached_remote_fn(execute_read_task)
            .options(num_returns=2, **self._remote_args)
            .remote(
                i=task_idx,
                task=task,
                context=DatasetContext.get_current(),
                stats_uuid=self._stats_uuid,
                stats_actor=stats_actor,
            )
        )

    def copy(self) -> "LazyBlockList":
        return LazyBlockList(
            self._tasks.copy(),
            block_partitions=self._block_partitions.copy(),
            block_partitions_meta=self._block_partitions_meta.copy(),
            fetched_metadata=self._fetched_metadata,
            ray_remote_args=self._remote_args.copy(),
            stats_uuid=self._stats_uuid,
        )

    def clear(self):
        """Clears all object references (block partitions and base block partitions)
        from this lazy block list.
        """
        self._block_partitions = [None for _ in self._block_partitions]
        self._block_partitions_meta = [None for _ in self._block_partitions_meta]
        self._fetched_metadata = [None for _ in self._fetched_metadata]

    def _check_if_cleared(self):
        pass  # LazyBlockList can always be re-computed.

    # Note: does not force execution prior to splitting.
    def split(self, split_size: int) -> List["LazyBlockList"]:
        num_splits = math.ceil(len(self._tasks) / split_size)
        tasks = np.array_split(self._tasks, num_splits)
        block_partitions = np.array_split(self._block_partitions, num_splits)
        block_partitions_meta = np.array_split(self._block_partitions_meta, num_splits)
        output = []
        for t, b, m in zip(tasks, block_partitions, block_partitions_meta):
            output.append(
                LazyBlockList(
                    t.tolist(),
                    b.tolist(),
                    m.tolist(),
                )
            )
        return output

    # Note: does not force execution prior to splitting.
    def split_by_bytes(self, bytes_per_split: int) -> List["BlockList"]:
        self._check_if_cleared()
        output = []
        cur_tasks, cur_blocks, cur_blocks_meta = [], [], []
        cur_size = 0
        for t, b, bm in zip(
            self._tasks,
            self._block_partitions,
            self._block_partitions_meta,
        ):
            m = t.get_metadata()
            if m.size_bytes is None:
                raise RuntimeError(
                    "Block has unknown size, cannot use split_by_bytes()"
                )
            size = m.size_bytes
            if cur_blocks and cur_size + size > bytes_per_split:
                output.append(
                    LazyBlockList(cur_tasks, cur_blocks, cur_blocks_meta),
                )
                cur_tasks, cur_blocks, cur_blocks_meta = [], [], []
                cur_size = 0
            cur_tasks.append(t)
            cur_blocks.append(b)
            cur_blocks_meta.append(b)
            cur_size += size
        if cur_blocks:
            output.append(LazyBlockList(cur_tasks, cur_blocks, cur_blocks_meta))
        return output

    # Note: does not force execution prior to division.
    def divide(self, part_idx: int) -> ("LazyBlockList", "LazyBlockList"):
        left = LazyBlockList(
            self._tasks[:part_idx],
            self._block_partitions[:part_idx],
            self._block_partitions_meta[:part_idx],
        )
        right = LazyBlockList(
            self._tasks[part_idx:],
            self._block_partitions[part_idx:],
            self._block_partitions_meta[part_idx:],
        )
        return left, right

    def get_blocks(self) -> List[ObjectRef[Block]]:
        """Bulk version of iter_blocks().

        Prefer calling this instead of the iter form for performance if you
        don't need lazy evaluation.
        """
        blocks, _ = self._get_blocks_with_metadata()
        return blocks

    def get_blocks_with_metadata(self) -> List[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Bulk version of iter_blocks_with_metadata().

        Prefer calling this instead of the iter form for performance if you
        don't need lazy evaluation.
        """
        blocks, metadata = self._get_blocks_with_metadata()
        return list(zip(blocks, metadata))

    def _get_blocks_with_metadata(
        self,
    ) -> Tuple[List[ObjectRef[Block]], List[BlockMetadata]]:
        """Get all underlying block futures and concrete metadata.

        This will block on the completion of the underlying read tasks and will fetch
        all block metadata outputted by those tasks.
        """
        context = DatasetContext.get_current()
        blocks, meta_refs = [], []
        for block, meta_ref in self._iter_block_partition_refs():
            blocks.append(block)
            meta_refs.append(meta_ref)
        if context.block_splitting_enabled:
            # If block splitting is enabled, fetch the partitions.
            parts = ray.get(blocks)
            blocks, metadata = [], []
            for part in parts:
                for block, meta in part:
                    blocks.append(block)
                    metadata.append(meta)
            self._fetched_metadata = metadata
            return blocks, metadata
        if all(meta is not None for meta in self._fetched_metadata):
            # Short-circuit on cached metadata.
            return blocks, self._fetched_metadata
        if not meta_refs:
            # Short-circuit on empty set of block partitions.
            assert not blocks, blocks
            return [], []
        read_progress_bar = ProgressBar("Read progress", total=len(meta_refs))
        # Fetch the metadata in bulk.
        # Handle duplicates (e.g. due to unioning the same dataset).
        unique_meta_refs = set(meta_refs)
        metadata = read_progress_bar.fetch_until_complete(list(unique_meta_refs))
        ref_to_data = {
            meta_ref: data for meta_ref, data in zip(unique_meta_refs, metadata)
        }
        metadata = [ref_to_data[meta_ref] for meta_ref in meta_refs]
        self._fetched_metadata = metadata
        return blocks, metadata

    def compute_to_blocklist(self) -> BlockList:
        """Launch all tasks and return a concrete BlockList."""
        blocks, metadata = self._get_blocks_with_metadata()
        return BlockList(blocks, metadata)

    def compute_first_block(self):
        """Kick off computation for the first block in the list.

        This is useful if looking to support rapid lightweight interaction with a small
        amount of the dataset.
        """
        if self._tasks:
            self._get_or_compute(0)

    def ensure_metadata_for_first_block(self) -> BlockMetadata:
        """Ensure that the metadata is fetched and set for the first block.

        Returns None if the block list is empty.
        """
        try:
            _, metadata = next(self.iter_blocks_with_metadata())
        except (StopIteration, ValueError):
            # Dataset is empty (no blocks) or was manually cleared.
            return None

        # Set the metadata.
        self._fetched_metadata[0] = metadata
        return metadata

    def iter_blocks_with_metadata(
        self,
    ) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Iterate over the blocks along with their runtime metadata.

        This blocks on the execution of each submitted task.
        The length of this iterator is not known until execution.
        """
        context = DatasetContext.get_current()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer._iter_block_partition_refs()
                self._pos = -1
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                while not self._buffer:
                    self._pos += 1
                    if context.block_splitting_enabled:
                        part_ref, _ = next(self._base_iter)
                        partition = ray.get(part_ref)
                    else:
                        block, metadata_ref = next(self._base_iter)
                        # This blocks until the underlying read task is finished.
                        metadata = ray.get(metadata_ref)
                        partition = [(block, metadata)]
                    for ref, metadata in partition:
                        self._buffer.append((ref, metadata))
                return self._buffer.pop(0)

        return Iter()

    def _iter_block_partition_refs(
        self,
    ) -> Iterator[
        Tuple[ObjectRef[MaybeBlockPartition], ObjectRef[BlockPartitionMetadata]]
    ]:
        """Iterate over the block futures and their corresponding metadata futures.

        This does NOT block on the execution of each submitted task.
        """
        outer = self

        class Iter:
            def __init__(self):
                self._pos = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._pos += 1
                if self._pos < len(outer._tasks):
                    return outer._get_or_compute(self._pos)
                raise StopIteration

        return Iter()

    def _get_or_compute(
        self,
        i: int,
    ) -> Tuple[ObjectRef[MaybeBlockPartition], ObjectRef[BlockPartitionMetadata]]:
        assert i < len(self._tasks), i
        # Check if we need to compute more block_partitions.
        if not self._block_partitions[i]:
            # Exponentially increase the number computed per batch.
            for j in range(max(i + 1, i * 2)):
                if j >= len(self._block_partitions):
                    break
                if not self._block_partitions[j]:
                    (
                        self._block_partitions[j],
                        self._block_partitions_meta[j],
                    ) = self._submit_task(j)
            assert self._block_partitions[i], self._block_partitions
            assert self._block_partitions_meta[i], self._block_partitions_meta
        return self._block_partitions[i], self._block_partitions_meta[i]

    def _num_computed(self) -> int:
        i = 0
        for b in self._block_partitions:
            if b is not None:
                i += 1
        return i
