import logging
import os
import time
from typing import (
    List,
    Any,
    Callable,
    Iterator,
    Iterable,
    Generic,
    Dict,
    Optional,
    Union,
    TYPE_CHECKING,
    Tuple,
)
from uuid import uuid4

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import mars
    import modin
    import dask
    import pyspark
    import ray.util.sgd
    import torch
    import tensorflow as tf
    from ray.data.dataset_pipeline import DatasetPipeline
    from ray.data.grouped_dataset import GroupedDataset

import collections
import itertools
import numpy as np

import ray
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    T,
    U,
    BlockPartition,
    BlockPartitionMetadata,
    BlockExecStats,
    KeyFn,
    _validate_key_fn,
)
from ray.data.context import DatasetContext
from ray.data.datasource import (
    Datasource,
    CSVDatasource,
    JSONDatasource,
    NumpyDatasource,
    ParquetDatasource,
    BlockWritePathProvider,
    DefaultBlockWritePathProvider,
    WriteResult,
)
from ray.data.datasource.file_based_datasource import (
    _wrap_arrow_serialization_workaround,
    _unwrap_arrow_serialization_workaround,
)
from ray.data.row import TableRow
from ray.data.aggregate import AggregateFn, Sum, Max, Min, Mean, Std
from ray.data.random_access_dataset import RandomAccessDataset
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.block_batching import batch_blocks, BatchType
from ray.data.impl.plan import ExecutionPlan, OneToOneStage, AllToAllStage
from ray.data.impl.stats import DatasetStats
from ray.data.impl.compute import cache_wrapper, CallableClass, ComputeStrategy
from ray.data.impl.output_buffer import BlockOutputBuffer
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.shuffle import simple_shuffle
from ray.data.impl.fast_repartition import fast_repartition
from ray.data.impl.sort import sort_impl
from ray.data.impl.block_list import BlockList
from ray.data.impl.lazy_block_list import LazyBlockList
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder

logger = logging.getLogger(__name__)

# Whether we have warned of Datasets containing multiple epochs of data.
_epoch_warned = False


@PublicAPI
class Dataset(Generic[T]):
    """Implements a distributed Arrow dataset.

    Datasets are implemented as a list of ``ObjectRef[Block]``. The block
    also determines the unit of parallelism. The default block type is the
    ``pyarrow.Table``. Arrow-incompatible objects are held in ``list`` blocks.

    Since Datasets are just lists of Ray object refs, they can be passed
    between Ray tasks and actors just like any other object. Datasets support
    conversion to/from several more featureful dataframe libraries
    (e.g., Spark, Dask, Modin, MARS), and are also compatible with distributed
    TensorFlow / PyTorch.

    Dataset supports parallel transformations such as .map(), .map_batches(),
    and simple repartition, but currently not aggregations and joins.
    """

    def __init__(
        self,
        plan: ExecutionPlan,
        epoch: int,
        lazy: bool,
    ):
        """Construct a Dataset (internal API).

        The constructor is not part of the Dataset API. Use the ``ray.data.*``
        read methods to construct a dataset.
        """
        assert isinstance(plan, ExecutionPlan)
        self._plan = plan
        self._uuid = uuid4().hex
        self._epoch = epoch
        self._lazy = lazy

        if not lazy:
            # TODO(ekl) we should clear inputs once we have full lineage recorded.
            self._plan.execute(clear_input_blocks=False)

    @staticmethod
    def copy(dataset: "Dataset[T]") -> "Dataset[T]":
        return Dataset(dataset._plan, dataset._epoch, dataset._lazy)

    def map(
        self,
        fn: Union[CallableClass, Callable[[T], U]],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset[U]":
        """Apply the given function to each record of this dataset.

        This is a blocking operation. Note that mapping individual records
        can be quite slow. Consider using `.map_batches()` for performance.

        Examples:
            >>> # Transform python objects.
            >>> ds.map(lambda x: x * 2)

            >>> # Transform Arrow records.
            >>> ds.map(lambda record: {"v2": record["value"] * 2})

            >>> # Define a callable class that persists state across
            >>> # function invocations for efficiency.
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_model()
            ...    def __call__(self, batch):
            ...        return self.model(batch)

            >>> # Apply the transform in parallel on GPUs. Since
            >>> # compute=ActorPoolStrategy(2, 8) the transform will be applied on an
            >>> # autoscaling pool of 2-8 Ray actors, each allocated 1 GPU by Ray.
            >>> ds.map(CachedModel, compute=ActorPoolStrategy(2, 8), num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn, compute)
        context = DatasetContext.get_current()

        def transform(block: Block) -> Iterable[Block]:
            DatasetContext._set_current(context)
            block = BlockAccessor.for_block(block)
            output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
            for row in block.iter_rows():
                output_buffer.add(fn(row))
                if output_buffer.has_next():
                    yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        plan = self._plan.with_stage(
            OneToOneStage("map", transform, compute, ray_remote_args)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def map_batches(
        self,
        fn: Union[CallableClass, Callable[[BatchType], BatchType]],
        *,
        batch_size: Optional[int] = 4096,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: str = "native",
        **ray_remote_args,
    ) -> "Dataset[Any]":
        """Apply the given function to batches of records of this dataset.

        This is a blocking operation.

        Examples:
            >>> # Transform batches in parallel.
            >>> ds.map_batches(lambda batch: [v * 2 for v in batch])

            >>> # Define a callable class that persists state across
            >>> # function invocations for efficiency.
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_model()
            ...    def __call__(self, item):
            ...        return self.model(item)

            >>> # Apply the transform in parallel on GPUs. Since
            >>> # compute=ActorPoolStrategy(2, 8) the transform will be applied on an
            >>> # autoscaling pool of 2-8 Ray actors, each allocated 1 GPU by Ray.
            >>> ds.map_batches(
            ...    CachedModel,
            ...    batch_size=256, compute=ActorPoolStrategy(2, 8), num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record batch, or a class type
                that can be instantiated to create such a callable.
            batch_size: Request a specific batch size, or None to use entire
                blocks as batches. Defaults to a system-chosen batch size.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            batch_format: Specify "native" to use the native block format
                (promotes Arrow to pandas), "pandas" to select
                ``pandas.DataFrame`` as the batch format,
                or "pyarrow" to select ``pyarrow.Table``.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if batch_size is not None and batch_size < 1:
            raise ValueError("Batch size cannot be negative or 0")
        import pyarrow as pa
        import pandas as pd

        fn = cache_wrapper(fn, compute)
        context = DatasetContext.get_current()

        def transform(block: Block) -> Iterable[Block]:
            DatasetContext._set_current(context)
            output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
            block = BlockAccessor.for_block(block)
            total_rows = block.num_rows()
            max_batch_size = batch_size
            if max_batch_size is None:
                max_batch_size = max(total_rows, 1)

            for start in range(0, total_rows, max_batch_size):
                # Build a block for each batch.
                end = min(total_rows, start + max_batch_size)
                # Make sure to copy if slicing to avoid the Arrow serialization
                # bug where we include the entire base view on serialization.
                view = block.slice(start, end, copy=batch_size is not None)
                if batch_format == "native":
                    # Always promote Arrow blocks to pandas for consistency.
                    if isinstance(view, pa.Table) or isinstance(view, bytes):
                        view = BlockAccessor.for_block(view).to_pandas()
                elif batch_format == "pandas":
                    view = BlockAccessor.for_block(view).to_pandas()
                elif batch_format == "pyarrow":
                    view = BlockAccessor.for_block(view).to_arrow()
                else:
                    raise ValueError(
                        "The batch format must be one of 'native', 'pandas', "
                        "or 'pyarrow', got: {}".format(batch_format)
                    )

                applied = fn(view)
                if not (
                    isinstance(applied, list)
                    or isinstance(applied, pa.Table)
                    or isinstance(applied, pd.core.frame.DataFrame)
                ):
                    raise ValueError(
                        "The map batches UDF returned the value "
                        f"{applied}, which is not allowed. "
                        "The return type must be either list, "
                        "pandas.DataFrame, or pyarrow.Table"
                    )
                output_buffer.add_block(applied)
                if output_buffer.has_next():
                    yield output_buffer.next()

            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        plan = self._plan.with_stage(
            OneToOneStage("map_batches", transform, compute, ray_remote_args)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def add_column(
        self,
        col: str,
        fn: Callable[["pandas.DataFrame"], "pandas.Series"],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset[T]":
        """Add the given column to the dataset.

        This is only supported for datasets convertible to pandas format.
        A function generating the new column values given the batch in pandas
        format must be specified.

        This is a convenience wrapper over ``.map_batches()``.

        Examples:
            >>> ds = ray.data.range_arrow(100)
            >>> # Add a new column equal to value * 2.
            >>> ds = ds.add_column("new_col", lambda df: df["value"] * 2)
            >>> # Overwrite the existing "value" with zeros.
            >>> ds = ds.add_column("value", lambda df: 0)

        Time complexity: O(dataset size / parallelism)

        Args:
            col: Name of the column to add. If the name already exists, the
                column will be overwritten.
            fn: Map function generating the column values given a batch of
                records in pandas format.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def process_batch(batch):
            batch[col] = fn(batch)
            return batch

        if not callable(fn):
            raise ValueError("`fn` must be callable, got {}".format(fn))

        return self.map_batches(
            process_batch, batch_format="pandas", compute=compute, **ray_remote_args
        )

    def flat_map(
        self,
        fn: Union[CallableClass, Callable[[T], Iterable[U]]],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset[U]":
        """Apply the given function to each record and then flatten results.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (the batch size can be altered in map_batches).

        Examples:
            >>> ds.flat_map(lambda x: [x, x ** 2, x ** 3])

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn, compute)
        context = DatasetContext.get_current()

        def transform(block: Block) -> Iterable[Block]:
            DatasetContext._set_current(context)
            output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows():
                for r2 in fn(row):
                    output_buffer.add(r2)
                    if output_buffer.has_next():
                        yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        plan = self._plan.with_stage(
            OneToOneStage("flat_map", transform, compute, ray_remote_args)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def filter(
        self,
        fn: Union[CallableClass, Callable[[T], bool]],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset[T]":
        """Filter out records that do not satisfy the given predicate.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (you can implement filter by dropping records).

        Examples:
            >>> ds.filter(lambda x: x % 2 == 0)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The predicate to apply to each record, or a class type
                that can be instantiated to create such a callable.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn, compute)
        context = DatasetContext.get_current()

        def transform(block: Block) -> Iterable[Block]:
            DatasetContext._set_current(context)
            block = BlockAccessor.for_block(block)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return [builder.build()]

        plan = self._plan.with_stage(
            OneToOneStage("filter", transform, compute, ray_remote_args)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def repartition(self, num_blocks: int, *, shuffle: bool = False) -> "Dataset[T]":
        """Repartition the dataset into exactly this number of blocks.

        This is a blocking operation. After repartitioning, all blocks in the
        returned dataset will have approximately the same number of rows.

        Examples:
            >>> # Set the number of output partitions to write to disk.
            >>> ds.repartition(10).write_parquet(...)

        Time complexity: O(dataset size / parallelism)

        Args:
            num_blocks: The number of blocks.
            shuffle: Whether to perform a distributed shuffle during the
                repartition. When shuffle is enabled, each output block
                contains a subset of data rows from each input block, which
                requires all-to-all data movement. When shuffle is disabled,
                output blocks are created from adjacent input blocks,
                minimizing data movement.

        Returns:
            The repartitioned dataset.
        """

        if shuffle:

            def do_shuffle(
                block_list, clear_input_blocks: bool, block_udf, remote_args
            ):
                if clear_input_blocks:
                    blocks = block_list.copy()
                    block_list.clear()
                else:
                    blocks = block_list
                return simple_shuffle(
                    blocks,
                    block_udf,
                    num_blocks,
                    map_ray_remote_args=remote_args,
                    reduce_ray_remote_args=remote_args,
                )

            plan = self._plan.with_stage(
                AllToAllStage(
                    "repartition", num_blocks, do_shuffle, supports_block_udf=True
                )
            )

        else:

            def do_fast_repartition(block_list, clear_input_blocks: bool, *_):
                if clear_input_blocks:
                    blocks = block_list.copy()
                    block_list.clear()
                else:
                    blocks = block_list
                return fast_repartition(blocks, num_blocks)

            plan = self._plan.with_stage(
                AllToAllStage("repartition", num_blocks, do_fast_repartition)
            )

        return Dataset(plan, self._epoch, self._lazy)

    def random_shuffle(
        self,
        *,
        seed: Optional[int] = None,
        num_blocks: Optional[int] = None,
    ) -> "Dataset[T]":
        """Randomly shuffle the elements of this dataset.

        This is a blocking operation similar to repartition().

        Examples:
            >>> # Shuffle this dataset randomly.
            >>> ds.random_shuffle()

            >>> # Shuffle this dataset with a fixed random seed.
            >>> ds.random_shuffle(seed=12345)

        Time complexity: O(dataset size / parallelism)

        Args:
            seed: Fix the random seed to use, otherwise one will be chosen
                based on system randomness.
            num_blocks: The number of output blocks after the shuffle, or None
                to retain the number of blocks.

        Returns:
            The shuffled dataset.
        """

        def do_shuffle(block_list, clear_input_blocks: bool, block_udf, remote_args):
            num_blocks = block_list.executed_num_blocks()  # Blocking.
            if num_blocks == 0:
                return block_list, {}
            if clear_input_blocks:
                blocks = block_list.copy()
                block_list.clear()
            else:
                blocks = block_list
            new_blocks, stage_info = simple_shuffle(
                blocks,
                block_udf,
                num_blocks,
                random_shuffle=True,
                random_seed=seed,
                map_ray_remote_args=remote_args,
                reduce_ray_remote_args=remote_args,
            )
            return new_blocks, stage_info

        plan = self._plan.with_stage(
            AllToAllStage(
                "random_shuffle", num_blocks, do_shuffle, supports_block_udf=True
            )
        )
        return Dataset(plan, self._epoch, self._lazy)

    def split(
        self, n: int, *, equal: bool = False, locality_hints: Optional[List[Any]] = None
    ) -> List["Dataset[T]"]:
        """Split the dataset into ``n`` disjoint pieces.

        This returns a list of sub-datasets that can be passed to Ray tasks
        and actors and used to read the dataset records in parallel.

        Examples:
            >>> # Split up a dataset to process over `n` worker actors.
            >>> shards = ds.split(len(workers), locality_hints=workers)
            >>> for shard, worker in zip(shards, workers):
            ...     worker.consume.remote(shard)

        Time complexity: O(1)

        See also: ``Dataset.split_at_indices``

        Args:
            n: Number of child datasets to return.
            equal: Whether to guarantee each split has an equal
                number of records. This may drop records if they cannot be
                divided equally among the splits.
            locality_hints: A list of Ray actor handles of size ``n``. The
                system will try to co-locate the blocks of the ith dataset
                with the ith actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint dataset splits.
        """
        if n <= 0:
            raise ValueError(f"The number of splits {n} is not positive.")

        if locality_hints and len(locality_hints) != n:
            raise ValueError(
                f"The length of locality_hints {len(locality_hints)} "
                "doesn't equal the number of splits {n}."
            )
            if len(set(locality_hints)) != len(locality_hints):
                raise ValueError(
                    "locality_hints must not contain duplicate actor handles"
                )

        blocks = self._plan.execute()
        stats = self._plan.stats()

        def _partition_splits(
            splits: List[Dataset[T]], part_size: int, counts_cache: Dict[str, int]
        ):
            """Partition splits into two sets: splits that are smaller than the
            target size and splits that are larger than the target size.
            """
            splits = sorted(splits, key=lambda s: counts_cache[s._get_uuid()])
            idx = next(
                i
                for i, split in enumerate(splits)
                if counts_cache[split._get_uuid()] >= part_size
            )
            return splits[:idx], splits[idx:]

        def _equalize_larger_splits(
            splits: List[Dataset[T]],
            target_size: int,
            counts_cache: Dict[str, int],
            num_splits_required: int,
        ):
            """Split each split into one or more subsplits that are each the
            target size, with at most one leftover split that's smaller
            than the target size.

            This assume that the given splits are sorted in ascending order.
            """
            if target_size == 0:
                return splits, []
            new_splits = []
            leftovers = []
            for split in splits:
                size = counts_cache[split._get_uuid()]
                if size == target_size:
                    new_splits.append(split)
                    continue
                split_indices = list(range(target_size, size, target_size))
                split_splits = split.split_at_indices(split_indices)
                last_split_size = split_splits[-1].count()
                if last_split_size < target_size:
                    # Last split is smaller than the target size, save it for
                    # our unioning of small splits.
                    leftover = split_splits.pop()
                    leftovers.append(leftover)
                    counts_cache[leftover._get_uuid()] = leftover.count()
                if len(new_splits) + len(split_splits) >= num_splits_required:
                    # Short-circuit if the new splits will make us reach the
                    # desired number of splits.
                    new_splits.extend(
                        split_splits[: num_splits_required - len(new_splits)]
                    )
                    break
                new_splits.extend(split_splits)
            return new_splits, leftovers

        def _equalize_smaller_splits(
            splits: List[Dataset[T]],
            target_size: int,
            counts_cache: Dict[str, int],
            num_splits_required: int,
        ):
            """Union small splits up to the target split size.

            This assume that the given splits are sorted in ascending order.
            """
            new_splits = []
            union_buffer = []
            union_buffer_size = 0
            low = 0
            high = len(splits) - 1
            while low <= high:
                # Union small splits up to the target split size.
                low_split = splits[low]
                low_count = counts_cache[low_split._get_uuid()]
                high_split = splits[high]
                high_count = counts_cache[high_split._get_uuid()]
                if union_buffer_size + high_count <= target_size:
                    # Try to add the larger split to the union buffer first.
                    union_buffer.append(high_split)
                    union_buffer_size += high_count
                    high -= 1
                elif union_buffer_size + low_count <= target_size:
                    union_buffer.append(low_split)
                    union_buffer_size += low_count
                    low += 1
                else:
                    # Neither the larger nor smaller split fit in the union
                    # buffer, so we split the smaller split into a subsplit
                    # that will fit into the union buffer and a leftover
                    # subsplit that we add back into the candidate split list.
                    diff = target_size - union_buffer_size
                    diff_split, new_low_split = low_split.split_at_indices([diff])
                    union_buffer.append(diff_split)
                    union_buffer_size += diff
                    # We overwrite the old low split and don't advance the low
                    # pointer since (1) the old low split can be discarded,
                    # (2) the leftover subsplit is guaranteed to be smaller
                    # than the old low split, and (3) the low split should be
                    # the smallest split in the candidate split list, which is
                    # this subsplit.
                    splits[low] = new_low_split
                    counts_cache[new_low_split._get_uuid()] = low_count - diff
                if union_buffer_size == target_size:
                    # Once the union buffer is full, we union together the
                    # splits.
                    assert len(union_buffer) > 1, union_buffer
                    first_ds = union_buffer[0]
                    new_split = first_ds.union(*union_buffer[1:])
                    new_splits.append(new_split)
                    # Clear the union buffer.
                    union_buffer = []
                    union_buffer_size = 0
                    if len(new_splits) == num_splits_required:
                        # Short-circuit if we've reached the desired number of
                        # splits.
                        break
            return new_splits

        def equalize(splits: List[Dataset[T]], num_splits: int) -> List[Dataset[T]]:
            if not equal:
                return splits
            counts = {s._get_uuid(): s.count() for s in splits}
            total_rows = sum(counts.values())
            # Number of rows for each split.
            target_size = total_rows // num_splits

            # Partition splits.
            smaller_splits, larger_splits = _partition_splits(
                splits, target_size, counts
            )
            if len(smaller_splits) == 0 and num_splits < len(splits):
                # All splits are already equal.
                return splits

            # Split larger splits.
            new_splits, leftovers = _equalize_larger_splits(
                larger_splits, target_size, counts, num_splits
            )
            # Short-circuit if we've already reached the desired number of
            # splits.
            if len(new_splits) == num_splits:
                return new_splits
            # Add leftovers to small splits and re-sort.
            smaller_splits += leftovers
            smaller_splits = sorted(smaller_splits, key=lambda s: counts[s._get_uuid()])

            # Union smaller splits.
            new_splits_small = _equalize_smaller_splits(
                smaller_splits, target_size, counts, num_splits - len(new_splits)
            )
            new_splits.extend(new_splits_small)
            return new_splits

        block_refs, metadata = zip(*blocks.get_blocks_with_metadata())
        metadata_mapping = {b: m for b, m in zip(block_refs, metadata)}

        if locality_hints is None:
            ds = equalize(
                [
                    Dataset(
                        ExecutionPlan(
                            BlockList(
                                list(blocks), [metadata_mapping[b] for b in blocks]
                            ),
                            stats,
                        ),
                        self._epoch,
                        self._lazy,
                    )
                    for blocks in np.array_split(block_refs, n)
                ],
                n,
            )
            assert len(ds) == n, (ds, n)
            return ds

        # If the locality_hints is set, we use a two-round greedy algorithm
        # to co-locate the blocks with the actors based on block
        # and actor's location (node_id).
        #
        # The split algorithm tries to allocate equally-sized blocks regardless
        # of locality. Thus we first calculate the expected number of blocks
        # for each split.
        #
        # In the first round, for each actor, we look for all blocks that
        # match the actor's node_id, then allocate those matched blocks to
        # this actor until we reach the limit(expected number).
        #
        # In the second round: fill each actor's allocation with
        # remaining unallocated blocks until we reach the limit.

        def build_allocation_size_map(
            num_blocks: int, actors: List[Any]
        ) -> Dict[Any, int]:
            """Given the total number of blocks and a list of actors, calcuate
            the expected number of blocks to allocate for each actor.
            """
            num_actors = len(actors)
            num_blocks_per_actor = num_blocks // num_actors
            num_blocks_left = num_blocks - num_blocks_per_actor * n
            num_blocks_by_actor = {}
            for i, actor in enumerate(actors):
                num_blocks_by_actor[actor] = num_blocks_per_actor
                if i < num_blocks_left:
                    num_blocks_by_actor[actor] += 1
            return num_blocks_by_actor

        def build_block_refs_by_node_id(
            blocks: List[ObjectRef[Block]],
        ) -> Dict[str, List[ObjectRef[Block]]]:
            """Build the reverse index from node_id to block_refs. For
            simplicity, if the block is stored on multiple nodes we
            only pick the first one.
            """
            block_ref_locations = ray.experimental.get_object_locations(blocks)
            block_refs_by_node_id = collections.defaultdict(list)
            for block_ref in blocks:
                node_ids = block_ref_locations.get(block_ref, {}).get("node_ids", [])
                node_id = node_ids[0] if node_ids else None
                block_refs_by_node_id[node_id].append(block_ref)
            return block_refs_by_node_id

        def build_node_id_by_actor(actors: List[Any]) -> Dict[Any, str]:
            """Build a map from a actor to its node_id."""
            actors_state = ray.state.actors()
            return {
                actor: actors_state.get(actor._actor_id.hex(), {})
                .get("Address", {})
                .get("NodeID")
                for actor in actors
            }

        # expected number of blocks to be allocated for each actor
        expected_block_count_by_actor = build_allocation_size_map(
            len(block_refs), locality_hints
        )
        # the reverse index from node_id to block_refs
        block_refs_by_node_id = build_block_refs_by_node_id(block_refs)
        # the map from actor to its node_id
        node_id_by_actor = build_node_id_by_actor(locality_hints)

        allocation_per_actor = collections.defaultdict(list)

        # In the first round, for each actor, we look for all blocks that
        # match the actor's node_id, then allocate those matched blocks to
        # this actor until we reach the limit(expected number)
        for actor in locality_hints:
            node_id = node_id_by_actor[actor]
            matching_blocks = block_refs_by_node_id[node_id]
            expected_block_count = expected_block_count_by_actor[actor]
            allocation = []
            while matching_blocks and len(allocation) < expected_block_count:
                allocation.append(matching_blocks.pop())
            allocation_per_actor[actor] = allocation

        # In the second round: fill each actor's allocation with
        # remaining unallocated blocks until we reach the limit
        remaining_block_refs = list(
            itertools.chain.from_iterable(block_refs_by_node_id.values())
        )
        for actor in locality_hints:
            while (
                len(allocation_per_actor[actor]) < expected_block_count_by_actor[actor]
            ):
                allocation_per_actor[actor].append(remaining_block_refs.pop())

        assert len(remaining_block_refs) == 0, len(remaining_block_refs)

        return equalize(
            [
                Dataset(
                    ExecutionPlan(
                        BlockList(
                            allocation_per_actor[actor],
                            [metadata_mapping[b] for b in allocation_per_actor[actor]],
                        ),
                        stats,
                    ),
                    self._epoch,
                    self._lazy,
                )
                for actor in locality_hints
            ],
            n,
        )

    def split_at_indices(self, indices: List[int]) -> List["Dataset[T]"]:
        """Split the dataset at the given indices (like np.split).

        Examples:
            >>> d1, d2, d3 = ray.data.range(10).split_at_indices([2, 5])
            >>> d1.take()
            [0, 1]
            >>> d2.take()
            [2, 3, 4]
            >>> d3.take()
            [5, 6, 7, 8, 9]

        Time complexity: O(num splits)

        See also: ``Dataset.split``

        Args:
            indices: List of sorted integers which indicate where the dataset
                will be split. If an index exceeds the length of the dataset,
                an empty dataset will be returned.

        Returns:
            The dataset splits.
        """

        if len(indices) < 1:
            raise ValueError("indices must be at least of length 1")
        if sorted(indices) != indices:
            raise ValueError("indices must be sorted")
        if indices[0] < 0:
            raise ValueError("indices must be positive")

        rest = self
        splits = []
        prev = 0
        for i in indices:
            first, rest = rest._split(i - prev, return_right_half=True)
            prev = i
            splits.append(first)
        splits.append(rest)

        return splits

    def union(self, *other: List["Dataset[T]"]) -> "Dataset[T]":
        """Combine this dataset with others of the same type.

        The order of the blocks in the datasets is preserved, as is the
        relative ordering between the datasets passed in the argument list.

        Args:
            other: List of datasets to combine with this one. The datasets
                must have the same schema as this dataset, otherwise the
                behavior is undefined.

        Returns:
            A new dataset holding the union of their data.
        """

        start_time = time.perf_counter()
        context = DatasetContext.get_current()
        calls: List[Callable[[], ObjectRef[BlockPartition]]] = []
        metadata: List[BlockPartitionMetadata] = []
        block_partitions: List[ObjectRef[BlockPartition]] = []

        datasets = [self] + list(other)
        for ds in datasets:
            bl = ds._plan.execute()
            if isinstance(bl, LazyBlockList):
                calls.extend(bl._calls)
                metadata.extend(bl._metadata)
                block_partitions.extend(bl._block_partitions)
            else:
                calls.extend([None] * bl.initial_num_blocks())
                metadata.extend(bl._metadata)
                if context.block_splitting_enabled:
                    block_partitions.extend(
                        [ray.put([(b, m)]) for b, m in bl.get_blocks_with_metadata()]
                    )
                else:
                    block_partitions.extend(bl.get_blocks())

        epochs = [ds._get_epoch() for ds in datasets]
        max_epoch = max(*epochs)
        if len(set(epochs)) > 1:
            global _epoch_warned
            if not _epoch_warned:
                logger.warning(
                    "Dataset contains data from multiple epochs: {}, "
                    "likely due to a `rewindow()` call. The higher epoch "
                    "number {} will be used. This warning will not "
                    "be shown again.".format(set(epochs), max_epoch)
                )
                _epoch_warned = True
        dataset_stats = DatasetStats(
            stages={"union": []},
            parent=[d._plan.stats() for d in datasets],
        )
        dataset_stats.time_total_s = time.perf_counter() - start_time
        return Dataset(
            ExecutionPlan(
                LazyBlockList(calls, metadata, block_partitions), dataset_stats
            ),
            max_epoch,
            self._lazy,
        )

    def groupby(self, key: Optional[KeyFn]) -> "GroupedDataset[T]":
        """Group the dataset by the key function or column name.

        This is a lazy operation.

        Examples:
            >>> # Group by a key function and aggregate.
            >>> ray.data.range(100).groupby(lambda x: x % 3).count()
            >>> # Group by an Arrow table column and aggregate.
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").count()

        Time complexity: O(dataset size * log(dataset size / parallelism))

        Args:
            key: A key function or Arrow column name. If this is None, the
                grouping is global.

        Returns:
            A lazy GroupedDataset that can be aggregated later.
        """
        from ray.data.grouped_dataset import GroupedDataset

        # Always allow None since groupby interprets that as grouping all
        # records into a single global group.
        if key is not None:
            _validate_key_fn(self, key)

        return GroupedDataset(self, key)

    def aggregate(self, *aggs: AggregateFn) -> U:
        """Aggregate the entire dataset as one group.

        This is a blocking operation.

        Examples:
            >>> from ray.data.aggregate import Max, Mean
            >>> ray.data.range(100).aggregate(Max())
            >>> ray.data.range_arrow(100).aggregate(
                Max("value"), Mean("value"))

        Time complexity: O(dataset size / parallelism)

        Args:
            aggs: Aggregations to do.

        Returns:
            If the input dataset is a simple dataset then the output is
            a tuple of ``(agg1, agg2, ...)`` where each tuple element is
            the corresponding aggregation result.
            If the input dataset is an Arrow dataset then the output is
            an ``ArrowRow`` where each column is the corresponding
            aggregation result.
            If the dataset is empty, return ``None``.
        """
        ret = self.groupby(None).aggregate(*aggs).take(1)
        return ret[0] if len(ret) > 0 else None

    def sum(
        self, on: Optional[Union[KeyFn, List[KeyFn]]] = None, ignore_nulls: bool = True
    ) -> U:
        """Compute sum over entire dataset.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).sum()
            >>> ray.data.from_items([
            ...     (i, i**2)
            ...     for i in range(100)]).sum(lambda x: x[1])
            >>> ray.data.range_arrow(100).sum("value")
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).sum(["A", "B"])

        Args:
            on: The data subset on which to compute the sum.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to return a scalar sum of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to return an ``ArrowRow``
                  containing the column-wise sum of all columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the sum; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The sum result.

            For a simple dataset, the output is:

            - ``on=None``: a scalar representing the sum of all rows,
            - ``on=callable``: a scalar representing the sum of the outputs of
              the callable called on each row,
            - ``on=[callable_1, ..., calalble_n]``: a tuple of
              ``(sum_1, ..., sum_n)`` representing the sum of the outputs of
              the corresponding callables called on each row.

            For an Arrow dataset, the output is:

            - ``on=None``: an ArrowRow containing the column-wise sum of all
              columns,
            - ``on="col"``: a scalar representing the sum of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``ArrowRow``
              containing the column-wise sum of the provided columns.

            If the dataset is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Sum, on, ignore_nulls)
        return self._aggregate_result(ret)

    def min(
        self, on: Optional[Union[KeyFn, List[KeyFn]]] = None, ignore_nulls: bool = True
    ) -> U:
        """Compute minimum over entire dataset.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).min()
            >>> ray.data.from_items([
            ...     (i, i**2)
            ...     for i in range(100)]).min(lambda x: x[1])
            >>> ray.data.range_arrow(100).min("value")
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).min(["A", "B"])

        Args:
            on: The data subset on which to compute the min.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to return a scalar min of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to return an ``ArrowRow``
                  containing the column-wise min of all columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the min; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The min result.

            For a simple dataset, the output is:

            - ``on=None``: a scalar representing the min of all rows,
            - ``on=callable``: a scalar representing the min of the outputs
              of the callable called on each row,
            - ``on=[callable_1, ..., calalble_n]``: a tuple of
              ``(min_1, ..., min_n)`` representing the min of the outputs
              of the corresponding callables called on each row.

            For an Arrow dataset, the output is:

            - ``on=None``: an ``ArrowRow`` containing the column-wise min of
              all columns,
            - ``on="col"``: a scalar representing the min of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``ArrowRow``
              containing the column-wise min of the provided columns.

            If the dataset is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Min, on, ignore_nulls)
        return self._aggregate_result(ret)

    def max(
        self, on: Optional[Union[KeyFn, List[KeyFn]]] = None, ignore_nulls: bool = True
    ) -> U:
        """Compute maximum over entire dataset.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).max()
            >>> ray.data.from_items([
            ...     (i, i**2)
            ...     for i in range(100)]).max(lambda x: x[1])
            >>> ray.data.range_arrow(100).max("value")
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).max(["A", "B"])

        Args:
            on: The data subset on which to compute the max.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to return a scalar max of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to return an ``ArrowRow``
                  containing the column-wise max of all columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the max; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The max result.

            For a simple dataset, the output is:

            - ``on=None``: a scalar representing the max of all rows,
            - ``on=callable``: a scalar representing the max of the outputs of
              the callable called on each row,
            - ``on=[callable_1, ..., calalble_n]``: a tuple of
              ``(max_1, ..., max_n)`` representing the max of the outputs of
              the corresponding callables called on each row.

            For an Arrow dataset, the output is:

            - ``on=None``: an ``ArrowRow`` containing the column-wise max of
              all columns,
            - ``on="col"``: a scalar representing the max of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``ArrowRow``
              containing the column-wise max of the provided columns.

            If the dataset is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Max, on, ignore_nulls)
        return self._aggregate_result(ret)

    def mean(
        self, on: Optional[Union[KeyFn, List[KeyFn]]] = None, ignore_nulls: bool = True
    ) -> U:
        """Compute mean over entire dataset.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).mean()
            >>> ray.data.from_items([
            ...     (i, i**2)
            ...     for i in range(100)]).mean(lambda x: x[1])
            >>> ray.data.range_arrow(100).mean("value")
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).mean(["A", "B"])

        Args:
            on: The data subset on which to compute the mean.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to return a scalar mean of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to return an ``ArrowRow``
                  containing the column-wise mean of all columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the mean; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The mean result.

            For a simple dataset, the output is:

            - ``on=None``: a scalar representing the mean of all rows,
            - ``on=callable``: a scalar representing the mean of the outputs
              of the callable called on each row,
            - ``on=[callable_1, ..., calalble_n]``: a tuple of
              ``(mean_1, ..., mean_n)`` representing the mean of the outputs
              of the corresponding callables called on each row.

            For an Arrow dataset, the output is:

            - ``on=None``: an ``ArrowRow`` containing the column-wise mean of
              all columns,
            - ``on="col"``: a scalar representing the mean of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``ArrowRow``
              containing the column-wise mean of the provided columns.

            If the dataset is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Mean, on, ignore_nulls)
        return self._aggregate_result(ret)

    def std(
        self,
        on: Optional[Union[KeyFn, List[KeyFn]]] = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> U:
        """Compute standard deviation over entire dataset.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).std()
            >>> ray.data.from_items([
            ...     (i, i**2)
            ...     for i in range(100)]).std(lambda x: x[1])
            >>> ray.data.range_arrow(100).std("value", ddof=0)
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).std(["A", "B"])

        NOTE: This uses Welford's online method for an accumulator-style
        computation of the standard deviation. This method was chosen due to
        it's numerical stability, and it being computable in a single pass.
        This may give different (but more accurate) results than NumPy, Pandas,
        and sklearn, which use a less numerically stable two-pass algorithm.
        See
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        Args:
            on: The data subset on which to compute the std.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to return a scalar std of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to return an ``ArrowRow``
                  containing the column-wise std of all columns.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is ``N - ddof``, where ``N`` represents the number of elements.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the std; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The standard deviation result.

            For a simple dataset, the output is:

            - ``on=None``: a scalar representing the std of all rows,
            - ``on=callable``: a scalar representing the std of the outputs of
              the callable called on each row,
            - ``on=[callable_1, ..., calalble_n]``: a tuple of
              ``(std_1, ..., std_n)`` representing the std of the outputs of
              the corresponding callables called on each row.

            For an Arrow dataset, the output is:

            - ``on=None``: an ``ArrowRow`` containing the column-wise std of
              all columns,
            - ``on="col"``: a scalar representing the std of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``ArrowRow``
              containing the column-wise std of the provided columns.

            If the dataset is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)
        return self._aggregate_result(ret)

    def sort(
        self, key: Optional[KeyFn] = None, descending: bool = False
    ) -> "Dataset[T]":
        """Sort the dataset by the specified key column or key function.

        This is a blocking operation.

        Examples:
            >>> # Sort using the entire record as the key.
            >>> ds.sort()

            >>> # Sort by a single column in descending order.
            >>> ds.sort("field1", descending=True)

            >>> # Sort by a key function.
            >>> ds.sort(lambda record: record["field1"] % 100)

        Time complexity: O(dataset size * log(dataset size / parallelism))

        Args:
            key:
                - For Arrow tables, key must be a single column name.
                - For datasets of Python objects, key can be either a lambda
                  function that returns a comparison key to sort by, or None
                  to sort by the original value.
            descending: Whether to sort in descending order.

        Returns:
            A new, sorted dataset.
        """

        def do_sort(block_list, clear_input_blocks: bool, *_):
            # Handle empty dataset.
            if block_list.initial_num_blocks() == 0:
                return block_list, {}
            if clear_input_blocks:
                blocks = block_list.copy()
                block_list.clear()
            else:
                blocks = block_list
            if isinstance(key, list):
                if not key:
                    raise ValueError("`key` must be a list of non-zero length")
                for subkey in key:
                    _validate_key_fn(self, subkey)
            else:
                _validate_key_fn(self, key)
            return sort_impl(blocks, key, descending)

        plan = self._plan.with_stage(AllToAllStage("sort", None, do_sort))
        return Dataset(plan, self._epoch, self._lazy)

    def zip(self, other: "Dataset[U]") -> "Dataset[(T, U)]":
        """Zip this dataset with the elements of another.

        The datasets must have identical num rows, block types, and block sizes
        (e.g., one was produced from a ``.map()`` of another). For Arrow
        blocks, the schema will be concatenated, and any duplicate column
        names disambiguated with _1, _2, etc. suffixes.

        Time complexity: O(dataset size / parallelism)

        Args:
            other: The dataset to zip with on the right hand side.

        Examples:
            >>> ds = ray.data.range(5)
            >>> ds.zip(ds).take()
            [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]

        Returns:
            A Dataset with (k, v) pairs (or concatenated Arrow schema) where k
            comes from the first dataset and v comes from the second.
        """

        def do_zip_all(block_list, clear_input_blocks: bool, *_):
            blocks1 = block_list.get_blocks()
            blocks2 = other.get_internal_block_refs()

            if clear_input_blocks:
                block_list.clear()

            if len(blocks1) != len(blocks2):
                # TODO(ekl) consider supporting if num_rows are equal.
                raise ValueError(
                    "Cannot zip dataset of different num blocks: {} vs {}".format(
                        len(blocks1), len(blocks2)
                    )
                )

            def do_zip(block1: Block, block2: Block) -> (Block, BlockMetadata):
                stats = BlockExecStats.builder()
                b1 = BlockAccessor.for_block(block1)
                result = b1.zip(block2)
                br = BlockAccessor.for_block(result)
                return result, br.get_metadata(input_files=[], exec_stats=stats.build())

            do_zip_fn = cached_remote_fn(do_zip, num_returns=2)

            blocks = []
            metadata = []
            for b1, b2 in zip(blocks1, blocks2):
                res, meta = do_zip_fn.remote(b1, b2)
                blocks.append(res)
                metadata.append(meta)

            # Early release memory.
            del blocks1, blocks2

            # TODO(ekl) it might be nice to have a progress bar here.
            metadata = ray.get(metadata)
            blocks = BlockList(blocks, metadata)
            return blocks, {}

        plan = self._plan.with_stage(AllToAllStage("zip", None, do_zip_all))
        return Dataset(plan, self._epoch, self._lazy)

    def limit(self, limit: int) -> "Dataset[T]":
        """Limit the dataset to the first number of records specified.

        Examples:
            >>> ds.limit(100).map(lambda x: x * 2).take()

        Time complexity: O(limit specified)

        Args:
            limit: The size of the dataset to truncate to.

        Returns:
            The truncated dataset.
        """

        left, _ = self._split(limit, return_right_half=False)
        return left

    def take(self, limit: int = 20) -> List[T]:
        """Take up to the given number of records from the dataset.

        Time complexity: O(limit specified)

        Args:
            limit: The max number of records to return.

        Returns:
            A list of up to ``limit`` records from the dataset.
        """
        output = []
        for row in self.iter_rows():
            output.append(row)
            if len(output) >= limit:
                break
        return output

    def take_all(self, limit: int = 100000) -> List[T]:
        """Take all the records in the dataset.

        Time complexity: O(dataset size)

        Args:
            limit: Raise an error if the size exceeds the specified limit.

        Returns:
            A list of all the records in the dataset.
        """
        output = []
        for row in self.iter_rows():
            output.append(row)
            if len(output) > limit:
                raise ValueError(
                    "The dataset has more than the given limit of {} records.".format(
                        limit
                    )
                )
        return output

    def show(self, limit: int = 20) -> None:
        """Print up to the given number of records from the dataset.

        Time complexity: O(limit specified)

        Args:
            limit: The max number of records to print.
        """
        for row in self.take(limit):
            print(row)

    def count(self) -> int:
        """Count the number of records in the dataset.

        Time complexity: O(dataset size / parallelism), O(1) for parquet

        Returns:
            The number of records in the dataset.
        """
        # Handle empty dataset.
        if self.num_blocks() == 0:
            return 0

        # For parquet, we can return the count directly from metadata.
        meta_count = self._meta_count()
        if meta_count is not None:
            return meta_count

        get_num_rows = cached_remote_fn(_get_num_rows)

        return sum(
            ray.get(
                [get_num_rows.remote(block) for block in self.get_internal_block_refs()]
            )
        )

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the schema of the dataset.

        For datasets of Arrow records, this will return the Arrow schema.
        For datasets of Python objects, this returns their Python type.

        Time complexity: O(1)

        Args:
            fetch_if_missing: If True, synchronously fetch the schema if it's
                not known. Default is False, where None is returned if the
                schema is not known.

        Returns:
            The Python type or Arrow schema of the records, or None if the
            schema is not known and fetch_if_missing is False.
        """
        return self._plan.schema(fetch_if_missing=fetch_if_missing)

    def num_blocks(self) -> int:
        """Return the number of blocks of this dataset.

        Note that during read and transform operations, the number of blocks
        may be dynamically adjusted to respect memory limits, increasing the
        number of blocks at runtime.

        Time complexity: O(1)

        Returns:
            The number of blocks of this dataset.
        """
        return self._plan.initial_num_blocks()

    def size_bytes(self) -> int:
        """Return the in-memory size of the dataset.

        Time complexity: O(1)

        Returns:
            The in-memory size of the dataset in bytes, or None if the
            in-memory size is not known.
        """
        metadata = self._plan.execute().get_metadata()
        if not metadata or metadata[0].size_bytes is None:
            return None
        return sum(m.size_bytes for m in metadata)

    def input_files(self) -> List[str]:
        """Return the list of input files for the dataset.

        Time complexity: O(num input files)

        Returns:
            The list of input files used to create the dataset, or an empty
            list if the input files is not known.
        """
        metadata = self._plan.execute().get_metadata()
        files = set()
        for m in metadata:
            for f in m.input_files:
                files.add(f)
        return list(files)

    def write_parquet(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **arrow_parquet_args,
    ) -> None:
        """Write the dataset to parquet.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.parquet, where ``uuid`` is an unique
        id for the dataset.

        Examples:
            >>> ds.write_parquet("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where Parquet
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each dataset block to a custom output path.
            arrow_parquet_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from arrow_parquet_args. This should be used
                instead of arrow_parquet_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            arrow_parquet_args: Options to pass to
                pyarrow.parquet.write_table(), which is used to write out each
                block to a file.
        """
        self.write_datasource(
            ParquetDatasource(),
            path=path,
            dataset_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=arrow_parquet_args_fn,
            **arrow_parquet_args,
        )

    def write_json(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        pandas_json_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **pandas_json_args,
    ) -> None:
        """Write the dataset to json.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.json, where ``uuid`` is an
        unique id for the dataset.

        Examples:
            >>> ds.write_json("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where json
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each dataset block to a custom output path.
            pandas_json_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from pandas_json_args. This should be used
                instead of pandas_json_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            pandas_json_args: These args will be passed to
                pandas.DataFrame.to_json(), which we use under the hood to
                write out each Datasets block. These
                are dict(orient="records", lines=True) by default.
        """
        self.write_datasource(
            JSONDatasource(),
            path=path,
            dataset_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=pandas_json_args_fn,
            **pandas_json_args,
        )

    def write_csv(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        arrow_csv_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **arrow_csv_args,
    ) -> None:
        """Write the dataset to csv.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.csv, where ``uuid`` is an unique id
        for the dataset.

        Examples:
            >>> ds.write_csv("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where csv
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each dataset block to a custom output path.
            arrow_csv_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from arrow_csv_args. This should be used
                instead of arrow_csv_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            arrow_csv_args: Other CSV write options to pass to pyarrow.
        """
        self.write_datasource(
            CSVDatasource(),
            path=path,
            dataset_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=arrow_csv_args_fn,
            **arrow_csv_args,
        )

    def write_numpy(
        self,
        path: str,
        *,
        column: str = "value",
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
    ) -> None:
        """Write a tensor column of the dataset to npy files.

        This is only supported for datasets convertible to Arrow records that
        contain a TensorArray column. To control the number of files, use
        ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.npy, where ``uuid`` is an unique
        id for the dataset.

        Examples:
            >>> ds.write_numpy("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where npy
                files will be written to.
            column: The name of the table column that contains the tensor to
                be written. This defaults to "value".
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each dataset block to a custom output path.
        """
        self.write_datasource(
            NumpyDatasource(),
            path=path,
            dataset_uuid=self._uuid,
            column=column,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
        )

    def write_datasource(self, datasource: Datasource[T], **write_args) -> None:
        """Write the dataset to a custom datasource.

        Examples:
            >>> ds.write_datasource(CustomDatasourceImpl(...))

        Time complexity: O(dataset size / parallelism)

        Args:
            datasource: The datasource to write to.
            write_args: Additional write args to pass to the datasource.
        """

        ctx = DatasetContext.get_current()
        blocks, metadata = zip(*self._plan.execute().get_blocks_with_metadata())

        # TODO(ekl) remove this feature flag.
        if "RAY_DATASET_FORCE_LOCAL_METADATA" in os.environ:
            write_results: List[ObjectRef[WriteResult]] = datasource.do_write(
                blocks, metadata, **write_args
            )
        else:
            # Prepare write in a remote task so that in Ray client mode, we
            # don't do metadata resolution from the client machine.
            do_write = cached_remote_fn(_do_write, retry_exceptions=False, num_cpus=0)
            write_results: List[ObjectRef[WriteResult]] = ray.get(
                do_write.remote(
                    datasource,
                    ctx,
                    blocks,
                    metadata,
                    _wrap_arrow_serialization_workaround(write_args),
                )
            )

        progress = ProgressBar("Write Progress", len(write_results))
        try:
            progress.block_until_complete(write_results)
            datasource.on_write_complete(ray.get(write_results))
        except Exception as e:
            datasource.on_write_failed(write_results, e)
            raise
        finally:
            progress.close()

    def iter_rows(self, *, prefetch_blocks: int = 0) -> Iterator[Union[T, TableRow]]:
        """Return a local row iterator over the dataset.

        If the dataset is a tabular dataset (Arrow/Pandas blocks), dict-like mappings
        :py:class:`~ray.data.row.TableRow` are yielded for each row by the iterator.
        If the dataset is not tabular, the raw row is yielded.

        Examples:
            >>> for i in ray.data.range(1000000).iter_rows():
            ...     print(i)

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.

        Returns:
            A local iterator over the entire dataset.
        """
        # During row-based ops, we also choose a batch format that lines up with the
        # current dataset format in order to eliminate unnecessary copies and type
        # conversions.
        try:
            dataset_format = self._dataset_format()
        except ValueError:
            # Dataset is empty or cleared, so fall back to "native".
            batch_format = "native"
        else:
            batch_format = (
                "pyarrow"
                if dataset_format == "arrow"
                else "pandas"
                if dataset_format == "pandas"
                else "native"
            )
        for batch in self.iter_batches(
            prefetch_blocks=prefetch_blocks, batch_format=batch_format
        ):
            batch = BlockAccessor.for_block(batch)
            for row in batch.iter_rows():
                yield row

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = None,
        batch_format: str = "native",
        drop_last: bool = False,
    ) -> Iterator[BatchType]:
        """Return a local batched iterator over the dataset.

        Examples:
            >>> for batch in ray.data.range(1000000).iter_batches():
            ...     print(batch)

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: Record batch size, or None to let the system pick.
            batch_format: The format in which to return each batch.
                Specify "native" to use the current block format (promoting
                Arrow to pandas automatically), "pandas" to
                select ``pandas.DataFrame`` or "pyarrow" to select
                ``pyarrow.Table``. Default is "native".
            drop_last: Whether to drop the last batch if it's incomplete.

        Returns:
            An iterator over record batches.
        """
        blocks = self._plan.execute()
        stats = self._plan.stats()

        time_start = time.perf_counter()

        yield from batch_blocks(
            blocks.iter_blocks(),
            stats,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
        )

        stats.iter_total_s.add(time.perf_counter() - time_start)

    def to_torch(
        self,
        *,
        label_column: Optional[str] = None,
        feature_columns: Optional[
            Union[List[str], List[List[str]], Dict[str, List[str]]]
        ] = None,
        label_column_dtype: Optional["torch.dtype"] = None,
        feature_column_dtypes: Optional[
            Union["torch.dtype", List["torch.dtype"], Dict[str, "torch.dtype"]]
        ] = None,
        batch_size: int = 1,
        prefetch_blocks: int = 0,
        drop_last: bool = False,
        unsqueeze_label_tensor: bool = True,
    ) -> "torch.utils.data.IterableDataset":
        """Return a Torch IterableDataset over this dataset.

        This is only supported for datasets convertible to Arrow records.

        It is recommended to use the returned ``IterableDataset`` directly
        instead of passing it into a torch ``DataLoader``.

        Each element in IterableDataset will be a tuple consisting of 2
        elements. The first item contains the feature tensor(s), and the
        second item is the label tensor. Those can take on different
        forms, depending on the specified arguments.

        For the features tensor (N is the ``batch_size`` and n, m, k
        are the number of features per tensor):

        * If ``feature_columns`` is a ``List[str]``, the features will be
          a tensor of shape (N, n), with columns corresponding to
          ``feature_columns``

        * If ``feature_columns`` is a ``List[List[str]]``, the features will be
          a list of tensors of shape [(N, m),...,(N, k)], with columns of each
          tensor corresponding to the elements of ``feature_columns``

        * If ``feature_columns`` is a ``Dict[str, List[str]]``, the features
          will be a dict of key-tensor pairs of shape
          {key1: (N, m),..., keyN: (N, k)}, with columns of each
          tensor corresponding to the value of ``feature_columns`` under the
          key.

        If ``unsqueeze_label_tensor=True`` (default), the label tensor will be
        of shape (N, 1). Otherwise, it will be of shape (N,).
        If ``label_column`` is specified as ``None``, then no column from the
        ``Dataset`` will be treated as the label, and the output label tensor
        will be ``None``.

        Note that you probably want to call ``.split()`` on this dataset if
        there are to be multiple Torch workers consuming the data.

        Time complexity: O(1)

        Args:
            label_column (Optional[str]): The name of the column used as the
                label (second element of the output list). Can be None for
                prediction, in which case the second element of returned
                tuple will also be None.
            feature_columns (Union[None, List[str], List[List[str]], \
Dict[str, List[str]]]): The names of the columns
                to use as the features. Can be a list of lists or
                a dict of string-list pairs for multi-tensor output.
                If None, then use all columns except the label columns as
                the features.
            label_column_dtype (Optional[torch.dtype]): The torch dtype to
                use for the label column. If None, then automatically infer
                the dtype.
            feature_column_dtypes (Union[None, torch.dtype, List[torch.dtype],\
 Dict[str, torch.dtype]]): The dtypes to use for the feature
                tensors. This should match the format of ``feature_columns``,
                or be a single dtype, in which case it will be applied to
                all tensors. If None, then automatically infer the dtype.
            batch_size (int): How many samples per batch to yield at a time.
                Defaults to 1.
            prefetch_blocks (int): The number of blocks to prefetch ahead of
                the current block during the scan.
            drop_last (bool): Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of dataset is not divisible by the batch
                size, then the last batch will be smaller. Defaults to False.
            unsqueeze_label_tensor (bool): If set to True, the label tensor
                will be unsqueezed (reshaped to (N, 1)). Otherwise, it will
                be left as is, that is (N, ). In general, regression loss
                functions expect an unsqueezed tensor, while classification
                loss functions expect a squeezed one. Defaults to True.

        Returns:
            A torch IterableDataset.
        """
        import torch

        from ray.data.impl.torch_iterable_dataset import TorchIterableDataset
        from ray.ml.utils.torch_utils import convert_pandas_to_torch_tensor

        # If an empty collection is passed in, treat it the same as None
        if not feature_columns:
            feature_columns = None

        if feature_column_dtypes and not isinstance(feature_column_dtypes, torch.dtype):
            if isinstance(feature_columns, dict):
                if not isinstance(feature_column_dtypes, dict):
                    raise TypeError(
                        "If `feature_columns` is a dict, "
                        "`feature_column_dtypes` must be None, `torch.dtype`,"
                        f" or dict, got {type(feature_column_dtypes)}."
                    )
                if set(feature_columns) != set(feature_column_dtypes):
                    raise ValueError(
                        "`feature_columns` and `feature_column_dtypes` "
                        "must have the same keys."
                    )
                if any(not subcolumns for subcolumns in feature_columns.values()):
                    raise ValueError("column list may not be empty")
            elif isinstance(feature_columns[0], (list, tuple)):
                if not isinstance(feature_column_dtypes, (list, tuple)):
                    raise TypeError(
                        "If `feature_columns` is a list of lists, "
                        "`feature_column_dtypes` must be None, `torch.dtype`,"
                        f" or a sequence, got {type(feature_column_dtypes)}."
                    )
                if len(feature_columns) != len(feature_column_dtypes):
                    raise ValueError(
                        "`feature_columns` and `feature_column_dtypes` "
                        "must have the same length."
                    )
                if any(not subcolumns for subcolumns in feature_columns):
                    raise ValueError("column list may not be empty")

        def make_generator():
            for batch in self.iter_batches(
                batch_size=batch_size,
                batch_format="pandas",
                prefetch_blocks=prefetch_blocks,
                drop_last=drop_last,
            ):
                if label_column:
                    label_vals = batch.pop(label_column).values
                    label_tensor = torch.as_tensor(label_vals, dtype=label_column_dtype)
                    if unsqueeze_label_tensor:
                        label_tensor = label_tensor.view(-1, 1)
                else:
                    label_tensor = None

                if isinstance(feature_columns, dict):
                    features_tensor = {
                        key: convert_pandas_to_torch_tensor(
                            batch,
                            feature_columns[key],
                            feature_column_dtypes[key]
                            if isinstance(feature_column_dtypes, dict)
                            else feature_column_dtypes,
                        )
                        for key in feature_columns
                    }
                else:
                    features_tensor = convert_pandas_to_torch_tensor(
                        batch,
                        columns=feature_columns,
                        column_dtypes=feature_column_dtypes,
                    )

                yield (features_tensor, label_tensor)

        return TorchIterableDataset(make_generator)

    def to_tf(
        self,
        *,
        output_signature: Union["tf.TypeSpec", Tuple["tf.TypeSpec", "tf.TypeSpec"]],
        label_column: Optional[str] = None,
        feature_columns: Optional[List[str]] = None,
        prefetch_blocks: int = 0,
        batch_size: int = 1,
    ) -> "tf.data.Dataset":
        """Return a TF Dataset over this dataset.

        The TF Dataset will be created from the generator returned by the
        ``iter_batches`` method. ``prefetch_blocks`` and ``batch_size``
        arguments will be passed to that method.

        This is only supported for datasets convertible to Arrow records.

        Requires all datasets to have the same columns.

        It is recommended to call ``.split()`` on this dataset if
        there are to be multiple TensorFlow workers consuming the data.

        The elements generated must be compatible with the given
        ``output_signature`` argument (same as in
        ``tf.data.Dataset.from_generator``).

        Time complexity: O(1)

        Args:
            output_signature (Union[tf.TypeSpec, Tuple[tf.TypeSpec, tf.TypeSpec]]):
                If ``label_column`` is specified, a 2-element
                tuple of ``tf.TypeSpec`` objects corresponding to
                (features, label). Otherwise, a single ``tf.TypeSpec``
                corresponding to features tensor.
            label_column (Optional[str]): The name of the column used as the label
                (second element of the output tuple). If not specified, output
                will be just one tensor instead of a tuple.
            feature_columns (Optional[List[str]]): List of columns in datasets
                to use. If None, all columns will be used.
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: Record batch size. Defaults to 1.

        Returns:
            A tf.data.Dataset.
        """

        # argument exception checking is done in from_generator

        try:
            import tensorflow as tf
        except ImportError:
            raise ValueError("tensorflow must be installed!")

        def make_generator():
            for batch in self.iter_batches(
                prefetch_blocks=prefetch_blocks,
                batch_size=batch_size,
                batch_format="pandas",
            ):
                if label_column:
                    target_col = batch.pop(label_column)
                if feature_columns:
                    batch = batch[feature_columns]
                # TODO(Clark): Support batches containing our extension array
                # TensorArray.
                if label_column:
                    yield batch.values, target_col.values
                else:
                    yield batch.values

        dataset = tf.data.Dataset.from_generator(
            make_generator, output_signature=output_signature
        )

        return dataset

    def to_dask(self) -> "dask.DataFrame":
        """Convert this dataset into a Dask DataFrame.

        This is only supported for datasets convertible to Arrow records.

        Note that this function will set the Dask scheduler to Dask-on-Ray
        globally, via the config.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A Dask DataFrame created from this dataset.
        """
        import dask
        import dask.dataframe as dd
        from ray.util.client.common import ClientObjectRef
        from ray.util.dask import ray_dask_get

        dask.config.set(scheduler=ray_dask_get)

        @dask.delayed
        def block_to_df(block: Block):
            block = BlockAccessor.for_block(block)
            if isinstance(block, (ray.ObjectRef, ClientObjectRef)):
                raise ValueError(
                    "Dataset.to_dask() must be used with Dask-on-Ray, please "
                    "set the Dask scheduler to ray_dask_get (located in "
                    "ray.util.dask)."
                )
            return block.to_pandas()

        # TODO(Clark): Give Dask a Pandas-esque schema via the Pyarrow schema,
        # once that's implemented.
        ddf = dd.from_delayed(
            [block_to_df(block) for block in self.get_internal_block_refs()]
        )
        return ddf

    def to_mars(self) -> "mars.DataFrame":
        """Convert this dataset into a MARS dataframe.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A MARS dataframe created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_modin(self) -> "modin.DataFrame":
        """Convert this dataset into a Modin dataframe.

        This works by first converting this dataset into a distributed set of
        Pandas dataframes (using ``.to_pandas_refs()``). Please see caveats
        there. Then the individual dataframes are used to create the modin
        DataFrame using
        ``modin.distributed.dataframe.pandas.partitions.from_partitions()``.

        This is only supported for datasets convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A Modin dataframe created from this dataset.
        """

        from modin.distributed.dataframe.pandas.partitions import from_partitions

        pd_objs = self.to_pandas_refs()
        return from_partitions(pd_objs, axis=0)

    def to_spark(self, spark: "pyspark.sql.SparkSession") -> "pyspark.sql.DataFrame":
        """Convert this dataset into a Spark dataframe.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A Spark dataframe created from this dataset.
        """
        import raydp

        core_worker = ray.worker.global_worker.core_worker
        locations = [
            core_worker.get_owner_address(block)
            for block in self.get_internal_block_refs()
        ]
        return raydp.spark.ray_dataset_to_spark_dataframe(
            spark, self.schema(), self.get_internal_block_refs(), locations
        )

    def to_pandas(self, limit: int = 100000) -> "pandas.DataFrame":
        """Convert this dataset into a single Pandas DataFrame.

        This is only supported for datasets convertible to Arrow or Pandas
        records. An error is raised if the number of records exceeds the
        provided limit. Note that you can use ``.limit()`` on the dataset
        beforehand to truncate the dataset manually.

        Time complexity: O(dataset size)

        Args:
            limit: The maximum number of records to return. An error will be
                raised if the limit is exceeded.

        Returns:
            A Pandas DataFrame created from this dataset, containing a limited
            number of records.
        """

        if self.count() > limit:
            raise ValueError(
                "The dataset has more than the given limit of {} records. "
                "Use ds.limit(N).to_pandas().".format(limit)
            )
        blocks = self.get_internal_block_refs()
        output = DelegatingBlockBuilder()
        for block in blocks:
            output.add_block(ray.get(block))
        return BlockAccessor.for_block(output.build()).to_pandas()

    def to_pandas_refs(self) -> List[ObjectRef["pandas.DataFrame"]]:
        """Convert this dataset into a distributed set of Pandas dataframes.

        This is only supported for datasets convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A list of remote Pandas dataframes created from this dataset.
        """

        block_to_df = cached_remote_fn(_block_to_df)
        return [block_to_df.remote(block) for block in self.get_internal_block_refs()]

    def to_numpy_refs(
        self, *, column: Optional[str] = None
    ) -> List[ObjectRef[np.ndarray]]:
        """Convert this dataset into a distributed set of NumPy ndarrays.

        This is only supported for datasets convertible to NumPy ndarrays.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(dataset size / parallelism)

        Args:
            column: The name of the column to convert to numpy, or None to
                specify the entire row. Required for Arrow tables.

        Returns:
            A list of remote NumPy ndarrays created from this dataset.
        """
        block_to_ndarray = cached_remote_fn(_block_to_ndarray)
        return [
            block_to_ndarray.remote(block, column=column)
            for block in self.get_internal_block_refs()
        ]

    def to_arrow_refs(self) -> List[ObjectRef["pyarrow.Table"]]:
        """Convert this dataset into a distributed set of Arrow tables.

        This is only supported for datasets convertible to Arrow records.
        This function is zero-copy if the existing data is already in Arrow
        format. Otherwise, the data will be converted to Arrow format.

        Time complexity: O(1) unless conversion is required.

        Returns:
            A list of remote Arrow tables created from this dataset.
        """
        blocks: List[ObjectRef[Block]] = self.get_internal_block_refs()

        if self._dataset_format() == "arrow":
            # Zero-copy path.
            return blocks

        block_to_arrow = cached_remote_fn(_block_to_arrow)
        return [block_to_arrow.remote(block) for block in blocks]

    def to_random_access_dataset(
        self,
        key: str,
        num_workers: Optional[int] = None,
    ) -> RandomAccessDataset:
        """Convert this Dataset into a distributed RandomAccessDataset (EXPERIMENTAL).

        RandomAccessDataset partitions the dataset across the cluster by the given sort
        key, providing efficient random access to records via binary search. A number
        of worker actors are created, each of which has zero-copy access to the
        underlying sorted data blocks of the Dataset.

        Note that the key must be unique in the dataset. If there are duplicate keys,
        an arbitrary value is returned.

        This is only supported for Arrow-format datasets.

        Args:
            key: The key column over which records can be queried.
            num_workers: The number of actors to use to serve random access queries.
                By default, this is determined by multiplying the number of Ray nodes
                in the cluster by four. As a rule of thumb, you can expect each worker
                to provide ~3000 records / second via ``get_async()``, and
                ~10000 records / second via ``multiget()``.
        """
        if num_workers is None:
            num_workers = 4 * len(ray.nodes())
        return RandomAccessDataset(self, key, num_workers=num_workers)

    def repeat(self, times: Optional[int] = None) -> "DatasetPipeline[T]":
        """Convert this into a DatasetPipeline by looping over this dataset.

        Transformations prior to the call to ``repeat()`` are evaluated once.
        Transformations done on the returned pipeline are evaluated on each
        loop of the pipeline over the base dataset.

        Note that every repeat of the dataset is considered an "epoch" for
        the purposes of ``DatasetPipeline.iter_epochs()``.

        Examples:
            >>> # Infinite pipeline of numbers [0, 5)
            >>> ray.data.range(5).repeat().take()
            [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...]

            >>> # Can apply transformations to the pipeline.
            >>> ray.data.range(5).repeat().map(lambda x: -x).take()
            [0, -1, -2, -3, -4, 0, -1, -2, -3, -4, ...]

            >>> # Can shuffle each epoch (dataset) in the pipeline.
            >>> ray.data.range(5).repeat().random_shuffle().take()
            [2, 3, 0, 4, 1, 4, 0, 2, 1, 3, ...]

        Args:
            times: The number of times to loop over this dataset, or None
                to repeat indefinitely.
        """
        from ray.data.dataset_pipeline import DatasetPipeline

        # If optimizations are enabled, rewrite the read stage into a OneToOneStage
        # to enable fusion with downstream map stages.
        ctx = DatasetContext.get_current()
        if self._plan._is_read_stage() and ctx.optimize_fuse_read_stages:
            blocks, read_stage = self._plan._rewrite_read_stage()
            outer_stats = DatasetStats(stages={}, parent=None)
        else:
            blocks = self._plan.execute()
            read_stage = None
            outer_stats = self._plan.stats()

        if times is not None and times < 1:
            raise ValueError("`times` must be >= 1, got {}".format(times))
        uuid = self._get_uuid()

        class Iterator:
            def __init__(self, blocks):
                self._blocks = blocks
                self._i = 0

            def __next__(self) -> "Dataset[T]":
                if times and self._i >= times:
                    raise StopIteration
                epoch = self._i
                blocks = self._blocks
                self._i += 1

                def gen():
                    ds = Dataset(
                        ExecutionPlan(blocks, outer_stats, dataset_uuid=uuid),
                        epoch,
                        lazy=False,
                    )
                    ds._set_uuid(uuid)
                    return ds

                return gen

        class Iterable:
            def __init__(self, blocks):
                self._blocks = blocks

            def __iter__(self):
                return Iterator(self._blocks)

        pipe = DatasetPipeline(Iterable(blocks), length=times or float("inf"))
        if read_stage:
            pipe = pipe.foreach_window(
                lambda ds, read_stage=read_stage: Dataset(
                    ds._plan.with_stage(read_stage), ds._epoch, True
                )
            )
        return pipe

    def window(
        self,
        *,
        blocks_per_window: Optional[int] = None,
        bytes_per_window: Optional[int] = None,
    ) -> "DatasetPipeline[T]":
        """Convert this into a DatasetPipeline by windowing over data blocks.

        Transformations prior to the call to ``window()`` are evaluated in
        bulk on the entire dataset. Transformations done on the returned
        pipeline are evaluated incrementally per window of blocks as data is
        read from the output of the pipeline.

        Windowing execution allows for output to be read sooner without
        waiting for all transformations to fully execute, and can also improve
        efficiency if transforms use different resources (e.g., GPUs).

        Without windowing::

            [preprocessing......]
                                  [inference.......]
                                                     [write........]
            Time ----------------------------------------------------------->

        With windowing::

            [prep1] [prep2] [prep3]
                    [infer1] [infer2] [infer3]
                             [write1] [write2] [write3]
            Time ----------------------------------------------------------->

        Examples:
            >>> # Create an inference pipeline.
            >>> ds = ray.data.read_binary_files(dir)
            >>> pipe = ds.window(blocks_per_window=10).map(infer)
            DatasetPipeline(num_windows=40, num_stages=2)

            >>> # The higher the stage parallelism, the shorter the pipeline.
            >>> pipe = ds.window(blocks_per_window=20).map(infer)
            DatasetPipeline(num_windows=20, num_stages=2)

            >>> # Outputs can be incrementally read from the pipeline.
            >>> for item in pipe.iter_rows():
            ...    print(item)

        Args:
            blocks_per_window: The window size (parallelism) in blocks.
                Increasing window size increases pipeline throughput, but also
                increases the latency to initial output, since it decreases the
                length of the pipeline. Setting this to infinity effectively
                disables pipelining.
            bytes_per_window: Specify the window size in bytes instead of blocks.
                This will be treated as an upper bound for the window size, but each
                window will still include at least one block. This is mutually
                exclusive with ``blocks_per_window``.
        """
        from ray.data.dataset_pipeline import DatasetPipeline

        if blocks_per_window is not None and bytes_per_window is not None:
            raise ValueError("Only one windowing scheme can be specified.")

        if blocks_per_window is None:
            blocks_per_window = 10

        # If optimizations are enabled, rewrite the read stage into a OneToOneStage
        # to enable fusion with downstream map stages.
        ctx = DatasetContext.get_current()
        if self._plan._is_read_stage() and ctx.optimize_fuse_read_stages:
            blocks, read_stage = self._plan._rewrite_read_stage()
            outer_stats = DatasetStats(stages={}, parent=None)
        else:
            blocks = self._plan.execute()
            read_stage = None
            outer_stats = self._plan.stats()

        class Iterator:
            def __init__(self, splits, epoch):
                self._splits = splits.copy()
                self._epoch = epoch

            def __next__(self) -> "Dataset[T]":
                if not self._splits:
                    raise StopIteration

                blocks = self._splits.pop(0)

                def gen():
                    ds = Dataset(
                        ExecutionPlan(blocks, outer_stats), self._epoch, lazy=False
                    )
                    return ds

                return gen

        class Iterable:
            def __init__(self, blocks, epoch):
                if bytes_per_window:
                    self._splits = blocks.split_by_bytes(bytes_per_window)
                else:
                    self._splits = blocks.split(split_size=blocks_per_window)
                try:
                    sizes = [s.size_bytes() for s in self._splits]
                    assert [s > 0 for s in sizes], sizes

                    def fmt(size_bytes):
                        if size_bytes > 10 * 1024:
                            return "{}MiB".format(round(size_bytes / (1024 * 1024), 2))
                        else:
                            return "{}b".format(size_bytes)

                    logger.info(
                        "Created DatasetPipeline with {} windows: "
                        "{} min, {} max, {} mean".format(
                            len(self._splits),
                            fmt(min(sizes)),
                            fmt(max(sizes)),
                            fmt(int(np.mean(sizes))),
                        )
                    )
                except Exception as e:
                    logger.info(
                        "Created DatasetPipeline with {} windows; "
                        "error getting sizes: {}".format(
                            len(self._splits),
                            e,
                        )
                    )
                self._epoch = epoch

            def __iter__(self):
                return Iterator(self._splits, self._epoch)

        it = Iterable(blocks, self._epoch)
        pipe = DatasetPipeline(it, length=len(it._splits))
        if read_stage:
            pipe = pipe.foreach_window(
                lambda ds, read_stage=read_stage: Dataset(
                    ds._plan.with_stage(read_stage), ds._epoch, True
                )
            )
        return pipe

    def fully_executed(self) -> "Dataset[T]":
        """Force full evaluation of the blocks of this dataset.

        This can be used to read all blocks into memory. By default, Datasets
        doesn't read blocks from the datasource until the first transform.

        Returns:
            A Dataset with all blocks fully materialized in memory.
        """
        blocks = self.get_internal_block_refs()
        bar = ProgressBar("Force reads", len(blocks))
        bar.block_until_complete(blocks)
        ds = Dataset(
            ExecutionPlan(
                BlockList(blocks, self._plan.execute().get_metadata()),
                self._plan.stats(),
                dataset_uuid=self._get_uuid(),
            ),
            self._epoch,
            lazy=False,
        )
        ds._set_uuid(self._get_uuid())
        return ds

    def stats(self) -> str:
        """Returns a string containing execution timing information."""
        return self._plan.stats().summary_string()

    @DeveloperAPI
    def get_internal_block_refs(self) -> List[ObjectRef[Block]]:
        """Get a list of references to the underlying blocks of this dataset.

        This function can be used for zero-copy access to the data. It blocks
        until the underlying blocks are computed.

        Time complexity: O(1)

        Returns:
            A list of references to this dataset's blocks.
        """
        return self._plan.execute().get_blocks()

    def _experimental_lazy(self) -> "Dataset[T]":
        """Enable lazy evaluation (experimental)."""
        self._lazy = True
        return self

    def _split(
        self, index: int, return_right_half: bool
    ) -> ("Dataset[T]", "Dataset[T]"):
        get_num_rows = cached_remote_fn(_get_num_rows)
        split_block = cached_remote_fn(_split_block, num_returns=4)

        count = 0
        left_blocks = []
        left_metadata = []
        right_blocks = []
        right_metadata = []
        it = self._plan.execute().get_blocks_with_metadata()
        for b, m in it:
            if m.num_rows is None:
                num_rows = ray.get(get_num_rows.remote(b))
            else:
                num_rows = m.num_rows
            if count >= index:
                if not return_right_half:
                    break
                right_blocks.append(b)
                right_metadata.append(m)
            elif count + num_rows < index:
                left_blocks.append(b)
                left_metadata.append(m)
            elif count + num_rows == index:
                left_blocks.append(b)
                left_metadata.append(m)
            else:
                b0, m0, b1, m1 = split_block.remote(
                    b, m, index - count, return_right_half
                )
                left_blocks.append(b0)
                left_metadata.append(ray.get(m0))
                right_blocks.append(b1)
                right_metadata.append(ray.get(m1))
            count += num_rows

        left = Dataset(
            ExecutionPlan(
                BlockList(left_blocks, left_metadata),
                self._plan.stats().child_TODO("split"),
            ),
            self._epoch,
            self._lazy,
        )
        if return_right_half:
            right = Dataset(
                ExecutionPlan(
                    BlockList(right_blocks, right_metadata),
                    self._plan.stats().child_TODO("split"),
                ),
                self._epoch,
                self._lazy,
            )
        else:
            right = None
        return left, right

    def _divide(self, block_idx: int) -> ("Dataset[T]", "Dataset[T]"):
        left, right = self._plan.execute().divide(block_idx)
        l_ds = Dataset(ExecutionPlan(left, self._plan.stats()), self._epoch, self._lazy)
        r_ds = Dataset(
            ExecutionPlan(right, self._plan.stats()), self._epoch, self._lazy
        )
        return l_ds, r_ds

    def _dataset_format(self) -> str:
        """Determine the format of the dataset. Possible values are: "arrow",
        "pandas", "simple".

        This may block; if the schema is unknown, this will synchronously fetch
        the schema for the first block.
        """
        # We need schema to properly validate, so synchronously
        # fetch it if necessary.
        schema = self.schema(fetch_if_missing=True)
        if schema is None:
            raise ValueError(
                "Dataset is empty or cleared, can't determine the format of "
                "the dataset."
            )

        try:
            import pyarrow as pa

            if isinstance(schema, pa.Schema):
                return "arrow"
        except ModuleNotFoundError:
            pass
        from ray.data.impl.pandas_block import PandasBlockSchema

        if isinstance(schema, PandasBlockSchema):
            return "pandas"
        return "simple"

    def _aggregate_on(
        self, agg_cls: type, on: Optional[Union[KeyFn, List[KeyFn]]], *args, **kwargs
    ):
        """Helper for aggregating on a particular subset of the dataset.

        This validates the `on` argument, and converts a list of column names
        or lambdas to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Dataset, and a single
        aggregation on the entire row for a simple Dataset.
        """
        aggs = self._build_multicolumn_aggs(agg_cls, on, *args, **kwargs)
        return self.aggregate(*aggs)

    def _build_multicolumn_aggs(
        self,
        agg_cls: type,
        on: Optional[Union[KeyFn, List[KeyFn]]],
        ignore_nulls: bool,
        *args,
        skip_cols: Optional[List[str]] = None,
        **kwargs,
    ):
        """Build set of aggregations for applying a single aggregation to
        multiple columns.
        """

        # Expand None into an aggregation for each column.
        if on is None:
            try:
                dataset_format = self._dataset_format()
            except ValueError:
                dataset_format = None
            if dataset_format in ["arrow", "pandas"]:
                # This should be cached from the ._dataset_format() check, so we
                # don't fetch and we assert that the schema is not None.
                schema = self.schema(fetch_if_missing=False)
                if not skip_cols:
                    skip_cols = []
                if len(schema.names) > 0:
                    on = [col for col in schema.names if col not in skip_cols]

        if not isinstance(on, list):
            on = [on]
        return [agg_cls(on_, *args, ignore_nulls=ignore_nulls, **kwargs) for on_ in on]

    def _aggregate_result(self, result: Union[Tuple, TableRow]) -> U:
        if result is not None and len(result) == 1:
            if isinstance(result, tuple):
                return result[0]
            else:
                # NOTE (kfstorm): We cannot call `result[0]` directly on
                # `PandasRow` because indexing a column with position is not
                # supported by pandas.
                return list(result.values())[0]
        else:
            return result

    def __repr__(self) -> str:
        schema = self.schema()
        if schema is None:
            schema_str = "Unknown schema"
        elif isinstance(schema, type):
            schema_str = str(schema)
        else:
            schema_str = []
            for n, t in zip(schema.names, schema.types):
                if hasattr(t, "__name__"):
                    t = t.__name__
                schema_str.append("{}: {}".format(n, t))
            schema_str = ", ".join(schema_str)
            schema_str = "{" + schema_str + "}"
        count = self._meta_count()
        return "Dataset(num_blocks={}, num_rows={}, schema={})".format(
            self._plan.initial_num_blocks(), count, schema_str
        )

    def __str__(self) -> str:
        return repr(self)

    def _block_num_rows(self) -> List[int]:
        get_num_rows = cached_remote_fn(_get_num_rows)
        return ray.get([get_num_rows.remote(b) for b in self.get_internal_block_refs()])

    def _block_size_bytes(self) -> List[int]:
        get_size_bytes = cached_remote_fn(_get_size_bytes)
        return ray.get(
            [get_size_bytes.remote(b) for b in self.get_internal_block_refs()]
        )

    def _meta_count(self) -> Optional[int]:
        return self._plan.meta_count()

    def _get_uuid(self) -> str:
        return self._uuid

    def _set_uuid(self, uuid: str) -> None:
        self._uuid = uuid

    def _get_epoch(self) -> int:
        return self._epoch

    def _set_epoch(self, epoch: int) -> None:
        self._epoch = epoch


def _get_num_rows(block: Block) -> int:
    block = BlockAccessor.for_block(block)
    return block.num_rows()


def _get_size_bytes(block: Block) -> int:
    block = BlockAccessor.for_block(block)
    return block.size_bytes()


def _block_to_df(block: Block):
    block = BlockAccessor.for_block(block)
    return block.to_pandas()


def _block_to_ndarray(block: Block, column: Optional[str]):
    block = BlockAccessor.for_block(block)
    return block.to_numpy(column)


def _block_to_arrow(block: Block):
    block = BlockAccessor.for_block(block)
    return block.to_arrow()


def _sliding_window(iterable: Iterable, n: int):
    """Creates an iterator consisting of n-width sliding windows over
    iterable. The sliding windows are constructed lazily such that an
    element on the base iterator (iterable) isn't consumed until the
    first sliding window containing that element is reached.

    If n > len(iterable), then a single len(iterable) window is
    returned.

    Args:
        iterable: The iterable on which the sliding window will be
            created.
        n: The width of the sliding window.

    Returns:
        An iterator of n-width windows over iterable.
        If n > len(iterable), then a single len(iterable) window is
        returned.
    """
    it = iter(iterable)
    window = collections.deque(itertools.islice(it, n), maxlen=n)
    if len(window) > 0:
        yield tuple(window)
    for elem in it:
        window.append(elem)
        yield tuple(window)


def _split_block(
    block: Block, meta: BlockMetadata, count: int, return_right_half: bool
) -> (Block, BlockMetadata, Optional[Block], Optional[BlockMetadata]):
    stats = BlockExecStats.builder()
    block = BlockAccessor.for_block(block)
    logger.debug("Truncating last block to size: {}".format(count))
    b0 = block.slice(0, count, copy=True)
    a0 = BlockAccessor.for_block(b0)
    m0 = BlockMetadata(
        num_rows=a0.num_rows(),
        size_bytes=a0.size_bytes(),
        schema=meta.schema,
        input_files=meta.input_files,
        exec_stats=stats.build(),
    )
    if return_right_half:
        b1 = block.slice(count, block.num_rows(), copy=True)
        a1 = BlockAccessor.for_block(b1)
        m1 = BlockMetadata(
            num_rows=a1.num_rows(),
            size_bytes=a1.size_bytes(),
            schema=meta.schema,
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
    else:
        b1 = None
        m1 = None
    return b0, m0, b1, m1


def _do_write(
    ds: Datasource,
    ctx: DatasetContext,
    blocks: List[Block],
    meta: List[BlockMetadata],
    write_args: dict,
) -> List[ObjectRef[WriteResult]]:
    write_args = _unwrap_arrow_serialization_workaround(write_args)
    DatasetContext._set_current(ctx)
    return ds.do_write(blocks, meta, **write_args)
