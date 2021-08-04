import logging
import os
from typing import List, Any, Callable, Iterator, Iterable, Generic, TypeVar, \
    Dict, Optional, Union, TYPE_CHECKING
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

import collections
import itertools
import numpy as np

import ray
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource import Datasource, WriteTask
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.batcher import Batcher
from ray.data.impl.compute import get_compute, cache_wrapper, \
    CallableClass
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.shuffle import simple_shuffle
from ray.data.impl.sort import sort_impl
from ray.data.impl.block_list import BlockList
from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder

T = TypeVar("T")
U = TypeVar("U")

# An output type of iter_batches() determined by the batch_format parameter.
BatchType = Union["pandas.DataFrame", "pyarrow.Table", np.ndarray, list]

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class Dataset(Generic[T]):
    """Implements a distributed Arrow dataset.

    Datasets are implemented as a list of ``ObjectRef[Block]``. The block
    also determines the unit of parallelism. The default block type is the
    ``pyarrow.Table``. Tensor objects are held in ``np.ndarray`` blocks,
    and other Arrow-incompatible objects are held in ``list`` blocks.

    Since Datasets are just lists of Ray object refs, they can be passed
    between Ray tasks and actors just like any other object. Datasets support
    conversion to/from several more featureful dataframe libraries
    (e.g., Spark, Dask, Modin, MARS), and are also compatible with distributed
    TensorFlow / PyTorch.

    Dataset supports parallel transformations such as .map(), .map_batches(),
    and simple repartition, but currently not aggregations and joins.
    """

    def __init__(self, blocks: BlockList[T]):
        """Construct a Dataset (internal API).

        The constructor is not part of the Dataset API. Use the ``ray.data.*``
        read methods to construct a dataset.
        """
        self._blocks: BlockList[T] = blocks
        self._uuid = uuid4().hex
        assert isinstance(self._blocks, BlockList), self._blocks

    def map(self,
            fn: Union[CallableClass, Callable[[T], U]],
            *,
            compute: Optional[str] = None,
            **ray_remote_args) -> "Dataset[U]":
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
            >>> # compute="actors", the transform will be applied on an
            >>> # autoscaling pool of Ray actors, each allocated 1 GPU by Ray.
            >>> ds.map(CachedModel, compute="actors", num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn)

        def transform(block: Block) -> Block:
            block = BlockAccessor.for_block(block)
            builder = DelegatingArrowBlockBuilder()
            for row in block.iter_rows():
                builder.add(fn(row))
            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def map_batches(self,
                    fn: Union[CallableClass, Callable[[BatchType], BatchType]],
                    *,
                    batch_size: int = None,
                    compute: Optional[str] = None,
                    batch_format: str = "native",
                    **ray_remote_args) -> "Dataset[Any]":
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
            >>> # compute="actors", the transform will be applied on an
            >>> # autoscaling pool of Ray actors, each allocated 1 GPU by Ray.
            >>> ds.map_batches(
            ...    CachedModel,
            ...    batch_size=256, compute="actors", num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record batch, or a class type
                that can be instantiated to create such a callable.
            batch_size: Request a specific batch size, or leave unspecified
                to use entire blocks as batches.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling Ray actor pool.
            batch_format: Specify "native" to use the native block format,
                "pandas" to select ``pandas.DataFrame`` as the batch format,
                or "pyarrow" to select ``pyarrow.Table/Tensor``.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if batch_size is not None and batch_size < 1:
            raise ValueError("Batch size cannot be negative or 0")
        import pyarrow as pa
        import pandas as pd

        fn = cache_wrapper(fn)

        def transform(block: Block) -> Block:
            block = BlockAccessor.for_block(block)
            total_rows = block.num_rows()
            max_batch_size = batch_size
            if max_batch_size is None:
                max_batch_size = max(total_rows, 1)

            builder = DelegatingArrowBlockBuilder()

            for start in range(0, total_rows, max_batch_size):
                # Build a block for each batch.
                end = min(total_rows, start + max_batch_size)
                view = block.slice(start, end, copy=False)
                if batch_format == "native":
                    pass
                elif batch_format == "pandas":
                    view = BlockAccessor.for_block(view).to_pandas()
                elif batch_format == "pyarrow":
                    view = BlockAccessor.for_block(view).to_arrow()
                else:
                    raise ValueError(
                        "The batch format must be one of 'native', 'pandas', "
                        "or 'pyarrow', got: {}".format(batch_format))

                applied = fn(view)
                if (isinstance(applied, list) or isinstance(applied, pa.Table)
                        or isinstance(applied, np.ndarray)):
                    applied = applied
                elif isinstance(applied, pd.core.frame.DataFrame):
                    applied = pa.Table.from_pandas(applied)
                elif isinstance(applied, pa.Tensor):
                    applied = applied.to_numpy()
                else:
                    raise ValueError("The map batches UDF returned a type "
                                     f"{type(applied)}, which is not allowed. "
                                     "The return type must be either list, "
                                     "pandas.DataFrame, np.ndarray, "
                                     "pyarrow.Tensor, or pyarrow.Table")
                builder.add_block(applied)

            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def flat_map(self,
                 fn: Union[CallableClass, Callable[[T], Iterable[U]]],
                 *,
                 compute: Optional[str] = None,
                 **ray_remote_args) -> "Dataset[U]":
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
                tasks, or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn)

        def transform(block: Block) -> Block:
            block = BlockAccessor.for_block(block)
            builder = DelegatingArrowBlockBuilder()
            for row in block.iter_rows():
                for r2 in fn(row):
                    builder.add(r2)
            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def filter(self,
               fn: Union[CallableClass, Callable[[T], bool]],
               *,
               compute: Optional[str] = None,
               **ray_remote_args) -> "Dataset[T]":
        """Filter out records that do not satisfy the given predicate.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (you can implement filter by dropping records).

        Examples:
            >>> ds.flat_map(lambda x: x % 2 == 0)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The predicate to apply to each record, or a class type
                that can be instantiated to create such a callable.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        fn = cache_wrapper(fn)

        def transform(block: Block) -> Block:
            block = BlockAccessor.for_block(block)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def repartition(self, num_blocks: int) -> "Dataset[T]":
        """Repartition the dataset into exactly this number of blocks.

        This is a blocking operation.

        Examples:
            >>> # Set the number of output partitions to write to disk.
            >>> ds.repartition(100).write_parquet(...)

        Time complexity: O(dataset size / parallelism)

        Args:
            num_blocks: The number of blocks.

        Returns:
            The repartitioned dataset.
        """

        new_blocks = simple_shuffle(self._blocks, num_blocks)
        return Dataset(new_blocks)

    def random_shuffle(self,
                       *,
                       seed: Optional[int] = None,
                       num_blocks: Optional[int] = None) -> "Dataset[T]":
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

        new_blocks = simple_shuffle(
            self._blocks,
            num_blocks or self.num_blocks(),
            random_shuffle=True,
            random_seed=seed)
        return Dataset(new_blocks)

    def split(self,
              n: int,
              *,
              equal: bool = False,
              locality_hints: List[Any] = None) -> List["Dataset[T]"]:
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

        if n > self.num_blocks() and equal:
            raise NotImplementedError(
                f"The number of splits {n} > the number of dataset blocks "
                f"{self.num_blocks()}, yet an equal split was requested.")

        if locality_hints and len(locality_hints) != n:
            raise ValueError(
                f"The length of locality_hints {len(locality_hints)} "
                "doesn't equal the number of splits {n}.")

        # TODO(ekl) we could do better than truncation here. This could be a
        # problem if block sizes are very skewed.
        def equalize(splits: List[Dataset[T]]) -> List[Dataset[T]]:
            if not equal:
                return splits
            lower_bound = min([s.count() for s in splits])
            assert lower_bound > 0, splits
            return [s.limit(lower_bound) for s in splits]

        block_refs = list(self._blocks)
        metadata_mapping = {
            b: m
            for b, m in zip(self._blocks, self._blocks.get_metadata())
        }

        if locality_hints is None:
            return equalize([
                Dataset(
                    BlockList(
                        list(blocks), [metadata_mapping[b] for b in blocks]))
                for blocks in np.array_split(block_refs, n)
            ])

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

        ray.wait(block_refs, num_returns=len(block_refs))

        def build_allocation_size_map(num_blocks: int,
                                      actors: List[Any]) -> Dict[Any, int]:
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

        def build_block_refs_by_node_id(blocks: List[ObjectRef[Block]]
                                        ) -> Dict[str, List[ObjectRef[Block]]]:
            """Build the reverse index from node_id to block_refs. For
            simplicity, if the block is stored on multiple nodes we
            only pick the first one.
            """
            block_ref_locations = ray.experimental.get_object_locations(blocks)
            block_refs_by_node_id = collections.defaultdict(list)
            for block_ref in blocks:
                node_ids = block_ref_locations.get(block_ref, {}).get(
                    "node_ids", [])
                node_id = node_ids[0] if node_ids else None
                block_refs_by_node_id[node_id].append(block_ref)
            return block_refs_by_node_id

        def build_node_id_by_actor(actors: List[Any]) -> Dict[Any, str]:
            """Build a map from a actor to its node_id.
            """
            actors_state = ray.state.actors()
            return {
                actor: actors_state.get(actor._actor_id.hex(), {}).get(
                    "Address", {}).get("NodeID")
                for actor in actors
            }

        # expected number of blocks to be allocated for each actor
        expected_block_count_by_actor = build_allocation_size_map(
            len(block_refs), locality_hints)
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
            itertools.chain.from_iterable(block_refs_by_node_id.values()))
        for actor in locality_hints:
            while len(allocation_per_actor[actor]
                      ) < expected_block_count_by_actor[actor]:
                allocation_per_actor[actor].append(remaining_block_refs.pop())

        assert len(remaining_block_refs) == 0, len(remaining_block_refs)

        return equalize([
            Dataset(
                BlockList(
                    allocation_per_actor[actor],
                    [metadata_mapping[b]
                     for b in allocation_per_actor[actor]]))
            for actor in locality_hints
        ])

    def sort(self,
             key: Union[None, str, List[str], Callable[[T], Any]] = None,
             descending: bool = False) -> "Dataset[T]":
        """Sort the dataset by the specified key column or key function.
        (experimental support)

        This is a blocking operation.

        Examples:
            >>> # Sort using the entire record as the key.
            >>> ds.sort()

            >>> # Sort by a single column in descending order.
            >>> ds.sort("field1", descending=True)

            >>> # Sort by a key function.
            >>> ds.sort(lambda record: record["field1"] % 100)

            >>> # Sort by multiple columns (not yet supported).
            >>> ds.sort([("field1", "ascending"), ("field2", "descending)])

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
        return Dataset(sort_impl(self._blocks, key, descending))

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

        get_num_rows = cached_remote_fn(_get_num_rows)
        truncate = cached_remote_fn(_truncate, num_returns=2)

        count = 0
        out_blocks = []
        out_metadata = []
        for b, m in zip(self._blocks, self._blocks.get_metadata()):
            if m.num_rows is None:
                num_rows = ray.get(get_num_rows.remote(b))
            else:
                num_rows = m.num_rows
            if count + num_rows < limit:
                out_blocks.append(b)
                out_metadata.append(m)
            elif count + num_rows == limit:
                out_blocks.append(b)
                out_metadata.append(m)
                break
            else:
                new_block, new_metadata = truncate.remote(b, m, limit - count)
                out_blocks.append(new_block)
                out_metadata.append(ray.get(new_metadata))
                break
            count += num_rows

        return Dataset(BlockList(out_blocks, out_metadata))

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

        # For parquet, we can return the count directly from metadata.
        meta_count = self._meta_count()
        if meta_count is not None:
            return meta_count

        get_num_rows = cached_remote_fn(_get_num_rows)

        return sum(
            ray.get([get_num_rows.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        """Sum up the elements of this dataset.

        Time complexity: O(dataset size / parallelism)

        Returns:
            The sum of the records in the dataset.
        """

        get_sum = cached_remote_fn(_get_sum)

        return sum(ray.get([get_sum.remote(block) for block in self._blocks]))

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the schema of the dataset.

        For datasets of Arrow records, this will return the Arrow schema.
        For datasets of Python objects, this returns their Python type.

        Time complexity: O(1)

        Returns:
            The Python type or Arrow schema of the records, or None if the
            schema is not known.
        """
        metadata = self._blocks.get_metadata()
        # Some blocks could be empty, in which case we cannot get their schema.
        # TODO(ekl) validate schema is the same across different blocks.
        for m in metadata:
            if m.schema:
                return m.schema
        return None

    def num_blocks(self) -> int:
        """Return the number of blocks of this dataset.

        Time complexity: O(1)

        Returns:
            The number of blocks of this dataset.
        """
        return len(self._blocks)

    def size_bytes(self) -> int:
        """Return the in-memory size of the dataset.

        Time complexity: O(1)

        Returns:
            The in-memory size of the dataset in bytes, or None if the
            in-memory size is not known.
        """
        metadata = self._blocks.get_metadata()
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
        metadata = self._blocks.get_metadata()
        files = set()
        for m in metadata:
            for f in m.input_files:
                files.add(f)
        return list(files)

    def write_parquet(
            self,
            path: str,
            *,
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to parquet.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        The format of the output files will be {uuid}_{block_idx}.parquet,
        where ``uuid`` is an unique id for the dataset.

        Examples:
            >>> ds.write_parquet("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where Parquet
                files will be written to.
            filesystem: The filesystem implementation to write to.
        """
        import pyarrow.parquet as pq

        # TODO(ekl) remove once ported to datasource
        @ray.remote
        def parquet_write(write_path, block):
            block = BlockAccessor.for_block(block)
            logger.debug(
                f"Writing {block.num_rows()} records to {write_path}.")
            table = block.to_arrow()
            with pq.ParquetWriter(write_path, table.schema) as writer:
                writer.write_table(table)

        refs = [
            parquet_write.remote(
                os.path.join(path, f"{self._uuid}_{block_idx:06}.parquet"),
                block) for block_idx, block in enumerate(self._blocks)
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_json(
            self,
            path: str,
            *,
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to json.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        The format of the output files will be {self._uuid}_{block_idx}.json,
        where ``uuid`` is an unique id for the dataset.

        Examples:
            >>> ds.write_json("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where json
                files will be written to.
            filesystem: The filesystem implementation to write to.
        """

        if filesystem:
            raise NotImplementedError

        # TODO(ekl) remove once ported to datasource
        @ray.remote
        def json_write(write_path: str, block: Block):
            block = BlockAccessor.for_block(block)
            logger.debug(
                f"Writing {block.num_rows()} records to {write_path}.")
            block.to_pandas().to_json(write_path, orient="records")

        refs = [
            json_write.remote(
                os.path.join(path, f"{self._uuid}_{block_idx:06}.json"), block)
            for block_idx, block in enumerate(self._blocks)
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_csv(
            self,
            path: str,
            *,
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to csv.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        The format of the output files will be {uuid}_{block_idx}.csv, where
        ``uuid`` is an unique id for the dataset.

        Examples:
            >>> ds.write_csv("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where csv
                files will be written to.
            filesystem: The filesystem implementation to write to.
        """

        if filesystem:
            raise NotImplementedError

        # TODO(ekl) remove once ported to datasource
        @ray.remote
        def csv_write(write_path: str, block: Block):
            block = BlockAccessor.for_block(block)
            logger.debug(
                f"Writing {block.num_rows()} records to {write_path}.")
            block.to_pandas().to_csv(
                write_path, mode="a", header=True, index=False)

        refs = [
            csv_write.remote(
                os.path.join(path, f"{self._uuid}_{block_idx:06}.csv"), block)
            for block_idx, block in enumerate(self._blocks)
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_numpy(
            self,
            path: str,
            *,
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to npy files.

        This is only supported for datasets of Tensor records.
        To control the number of files, use ``.repartition()``.

        The format of the output files will be {self._uuid}_{block_idx}.npy,
        where ``uuid`` is an unique id for the dataset.

        Examples:
            >>> ds.write_numpy("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where npy
                files will be written to.
            filesystem: The filesystem implementation to write to.
        """

        if filesystem:
            raise NotImplementedError

        # TODO(ekl) remove once ported to datasource
        @ray.remote
        def numpy_write(write_path: str, block: Block):
            np.save(open(write_path, "wb"), block)

        refs = [
            numpy_write.remote(
                os.path.join(path, f"{self._uuid}_{block_idx:06}.npy"), block)
            for block_idx, block in enumerate(self._blocks)
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_datasource(self, datasource: Datasource[T],
                         **write_args) -> None:
        """Write the dataset to a custom datasource.

        Examples:
            >>> ds.write_datasource(CustomDatasourceImpl(...))

        Time complexity: O(dataset size / parallelism)

        Args:
            datasource: The datasource to write to.
            write_args: Additional write args to pass to the datasource.
        """

        write_tasks = datasource.prepare_write(self._blocks,
                                               self._blocks.get_metadata(),
                                               **write_args)
        progress = ProgressBar("Write Progress", len(write_tasks))
        remote_write = cached_remote_fn(_remote_write)

        write_task_outputs = [remote_write.remote(w) for w in write_tasks]
        try:
            progress.block_until_complete(write_task_outputs)
            datasource.on_write_complete(write_tasks,
                                         ray.get(write_task_outputs))
        except Exception as e:
            datasource.on_write_failed(write_tasks, e)
            raise
        finally:
            progress.close()

    def iter_rows(self, *, prefetch_blocks: int = 0) -> Iterator[T]:
        """Return a local row iterator over the dataset.

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
        for batch in self.iter_batches(
                prefetch_blocks=prefetch_blocks, batch_format="native"):
            batch = BlockAccessor.for_block(batch)
            for row in batch.iter_rows():
                yield row

    def iter_batches(self,
                     *,
                     prefetch_blocks: int = 0,
                     batch_size: int = None,
                     batch_format: str = "native",
                     drop_last: bool = False) -> Iterator[BatchType]:
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
                Specify "native" to use the current block format, "pandas" to
                select ``pandas.DataFrame`` or "pyarrow" to select
                ``pyarrow.Table/Tensor``. Default is "native".
            drop_last: Whether to drop the last batch if it's incomplete.

        Returns:
            A list of iterators over record batches.
        """

        def sliding_window(iterable: Iterable, n: int):
            """Creates an iterator consisting of n-width sliding windows over
            iterable. The sliding windows are constructed lazily such that an
            element on the base iterator (iterable) isn't consumed until the
            first sliding window containing that element is reached.

            Args:
                iterable: The iterable on which the sliding window will be
                    created.
                n: The width of the sliding window.

            Returns:
                An iterator of n-width windows over iterable.
            """
            iters = itertools.tee(iter(iterable), n)
            for i in range(1, n):
                for it in iters[i:]:
                    next(it, None)
            return zip(*iters)

        def format_batch(batch: Block, format: str) -> BatchType:
            if batch_format == "native":
                return batch
            elif batch_format == "pandas":
                batch = BlockAccessor.for_block(batch)
                return batch.to_pandas()
            elif batch_format == "pyarrow":
                batch = BlockAccessor.for_block(batch)
                return batch.to_arrow()
            else:
                raise ValueError(
                    f"The given batch format: {batch_format} "
                    f"is invalid. Supported batch type: {BatchType}")

        batcher = Batcher(batch_size=batch_size)
        for block_window in sliding_window(self._blocks, prefetch_blocks + 1):
            block_window = list(block_window)
            ray.wait(block_window, num_returns=1, fetch_local=True)
            block = ray.get(block_window[0])
            batcher.add(block)
            while batcher.has_batch():
                yield format_batch(batcher.next_batch(), batch_format)

        if batcher.has_any() and not drop_last:
            yield format_batch(batcher.next_batch(), batch_format)

    def to_torch(self,
                 *,
                 label_column: str,
                 feature_columns: Optional[List[str]] = None,
                 label_column_dtype: Optional["torch.dtype"] = None,
                 feature_column_dtypes: Optional[List["torch.dtype"]] = None,
                 batch_size: int = 1,
                 prefetch_blocks: int = 0,
                 drop_last: bool = False) -> \
            "torch.utils.data.IterableDataset":
        """Return a Torch IterableDataset over this dataset.

        It is recommended to use the returned ``IterableDataset`` directly
        instead of passing it into a torch ``DataLoader``.

        Each element in IterableDataset will be a tuple consisting of 2
        elements. The first item is a list of the feature tensors. The
        second item is the label tensor. Each tensor will be of shape (N,
        1), where N is the ``batch_size`` used by the DataLoader.

        Note that you probably want to call ``.split()`` on this dataset if
        there are to be multiple Torch workers consuming the data.

        Time complexity: O(1)

        Args:
            label_column (str): The name of the column used as the label
                (second element of the output list).
            feature_columns (Optional[List[str]]): The names of the columns
                to use as the features. If None, then use all columns
                except the label columns as the features.
            label_column_dtype (Optional[torch.dtype]): The torch dtype to
                use for the label column. If None, then automatically infer
                the dtype.
            feature_column_dtypes (Optional[List[torch.dtype]]): The dtypes
                to use for the feature columns. The len of this list must
                be equal to the len of ``feature_columns``. If None,
                then automatically infer the dtype.
            batch_size (int): How many samples per batch to yield at a time.
                Defaults to 1.
            prefetch_blocks (int): The number of blocks to prefetch ahead of
                the current block during the scan.
            drop_last (bool): Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of dataset is not divisible by the batch
                size, then the last batch will be smaller. Defaults to False.

        Returns:
            A torch IterableDataset.
        """
        import torch

        from ray.data.impl.torch_iterable_dataset import \
            TorchIterableDataset

        if feature_columns and feature_column_dtypes:
            if len(feature_columns) != len(feature_column_dtypes):
                raise ValueError("The lengths of `feature_columns` "
                                 f"({len(feature_columns)}) and "
                                 f"`feature_column_dtypes` ("
                                 f"{len(feature_column_dtypes)}) do not "
                                 "match!")

        def make_generator():
            for batch in self.iter_batches(
                    batch_size=batch_size,
                    batch_format="pandas",
                    prefetch_blocks=prefetch_blocks,
                    drop_last=drop_last):
                label_vals = batch.pop(label_column).values
                label_tensor = torch.as_tensor(
                    label_vals, dtype=label_column_dtype)
                label_tensor = label_tensor.view(-1, 1)

                feature_tensor = []
                if feature_columns:
                    batch = batch[feature_columns]

                if feature_column_dtypes:
                    dtypes = feature_column_dtypes
                else:
                    dtypes = [None] * len(batch.columns)

                for col, dtype in zip(batch.columns, dtypes):
                    col_vals = batch[col].values
                    t = torch.as_tensor(col_vals, dtype=dtype)
                    t = t.view(-1, 1)
                    feature_tensor.append(t)

                yield (feature_tensor, label_tensor)

        return TorchIterableDataset(make_generator)

    def to_tf(self,
              *,
              label_column: str,
              output_signature: List["tf.TypeSpec"],
              feature_columns: Optional[List[str]] = None,
              prefetch_blocks: int = 0,
              batch_size: int = 1) -> "tf.data.Dataset":
        """Return a TF Dataset over this dataset.

        The TF Dataset will be created from the generator returned by the
        ``iter_batches`` method. ``prefetch_blocks`` and ``batch_size``
        arguments will be passed to that method.

        This is only supported for datasets convertible to Arrow records.

        Requires all datasets to have the same columns.

        Note that you probably want to call ``.split()`` on this dataset if
        there are to be multiple TensorFlow workers consuming the data.

        The elements generated must be compatible with the given
        ``output_signature`` argument (same as in
        ``tf.data.Dataset.from_generator``).

        Time complexity: O(1)

        Args:
            label_column (str): The name of the column used as the label
                (second element of the output tuple).
            output_signature (List[tf.TypeSpec]): A 2-element list
                of `tf.TypeSpec` objects corresponding to (features, label).
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
                    batch_format="pandas"):
                target_col = batch.pop(label_column)
                if feature_columns:
                    batch = batch[feature_columns]
                yield batch.values, target_col.values

        return tf.data.Dataset.from_generator(
            make_generator, output_signature=output_signature)

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
                    "ray.util.dask).")
            return block.to_pandas()

        # TODO(Clark): Give Dask a Pandas-esque schema via the Pyarrow schema,
        # once that's implemented.
        ddf = dd.from_delayed([block_to_df(block) for block in self._blocks])
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

        Time complexity: O(dataset size / parallelism)

        Returns:
            A Modin dataframe created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_spark(self) -> "pyspark.sql.DataFrame":
        """Convert this dataset into a Spark dataframe.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A Spark dataframe created from this dataset.
        """
        raise NotImplementedError  # P2

    def to_pandas(self) -> List[ObjectRef["pandas.DataFrame"]]:
        """Convert this dataset into a distributed set of Pandas dataframes.

        This is only supported for datasets convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or ``.get_blocks()``.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A list of remote Pandas dataframes created from this dataset.
        """

        block_to_df = cached_remote_fn(_block_to_df)
        return [block_to_df.remote(block) for block in self._blocks]

    def to_arrow(self) -> List[ObjectRef["pyarrow.Table"]]:
        """Convert this dataset into a distributed set of Arrow tables.

        This is only supported for datasets convertible to Arrow records.
        This function is zero-copy if the existing data is already in Arrow
        format. Otherwise, the data will be converted to Arrow format.

        Time complexity: O(1) unless conversion is required.

        Returns:
            A list of remote Arrow tables created from this dataset.
        """

        check_is_arrow = cached_remote_fn(_check_is_arrow)
        blocks: List[ObjectRef[Block]] = list(self._blocks)
        is_arrow = ray.get(check_is_arrow.remote(blocks[0]))

        if is_arrow:
            return blocks  # Zero-copy path.

        block_to_arrow = cached_remote_fn(_block_to_arrow)
        return [block_to_arrow.remote(block) for block in self._blocks]

    def repeat(self, times: int = None) -> "DatasetPipeline[T]":
        """Convert this into a DatasetPipeline by looping over this dataset.

        Transformations prior to the call to ``repeat()`` are evaluated once.
        Transformations done on the returned pipeline are evaluated on each
        loop of the pipeline over the base dataset.

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

        if times is not None and times < 1:
            raise ValueError("`times` must be >= 1, got {}".format(times))

        class Iterator:
            def __init__(self, ds: "Dataset[T]"):
                self._ds = ds
                self._i = 0

            def __next__(self) -> "Dataset[T]":
                if times and self._i >= times:
                    raise StopIteration
                self._i += 1
                return lambda: self._ds

        class Iterable:
            def __init__(self, ds: "Dataset[T]"):
                self._ds = ds

            def __iter__(self):
                return Iterator(self._ds)

        return DatasetPipeline(Iterable(self), length=times)

    def pipeline(self, *, parallelism: int = 10) -> "DatasetPipeline[T]":
        """Pipeline the dataset execution by splitting its blocks into groups.

        Transformations prior to the call to ``pipeline()`` are evaluated in
        bulk on the entire dataset. Transformations done on the returned
        pipeline are evaluated incrementally per group of blocks as data is
        read from the output of the pipeline.

        Pipelining execution allows for output to be read sooner without
        waiting for all transformations to fully execute, and can also improve
        efficiency if transforms use different resources (e.g., GPUs).

        Without pipelining::

            [preprocessing......]
                                  [inference.......]
                                                     [write........]
            Time ----------------------------------------------------------->

        With pipelining::

            [prep1] [prep2] [prep3]
                    [infer1] [infer2] [infer3]
                             [write1] [write2] [write3]
            Time ----------------------------------------------------------->

        Examples:
            >>> # Create an inference pipeline.
            >>> ds = ray.data.read_binary_files(dir)
            >>> pipe = ds.pipeline(parallelism=10).map(infer)
            DatasetPipeline(num_stages=2, length=40)

            >>> # The higher the stage parallelism, the shorter the pipeline.
            >>> pipe = ds.pipeline(parallelism=20).map(infer)
            DatasetPipeline(num_stages=2, length=20)

            >>> # Outputs can be incrementally read from the pipeline.
            >>> for item in pipe.iter_rows():
            ...    print(item)

        Args:
            parallelism: The parallelism (number of blocks) per stage.
                Increasing parallelism increases pipeline throughput, but also
                increases the latency to initial output, since it decreases the
                length of the pipeline. Setting this to infinity effectively
                disables pipelining.
        """
        from ray.data.dataset_pipeline import DatasetPipeline

        class Iterator:
            def __init__(self, splits):
                self._splits = splits.copy()

            def __next__(self) -> "Dataset[T]":
                if not self._splits:
                    raise StopIteration

                blocks = self._splits.pop(0)

                def gen():
                    return Dataset(blocks)

                return gen

        class Iterable:
            def __init__(self, blocks):
                self._splits = blocks.split(split_size=parallelism)

            def __iter__(self):
                return Iterator(self._splits)

        it = Iterable(self._blocks)
        return DatasetPipeline(it, length=len(it._splits))

    @DeveloperAPI
    def get_blocks(self) -> List[ObjectRef[Block]]:
        """Get a list of references to the underlying blocks of this dataset.

        This function can be used for zero-copy access to the data.

        Time complexity: O(1)

        Returns:
            A list of references to this dataset's blocks.
        """
        return list(self._blocks)

    def __repr__(self) -> str:
        schema = self.schema()
        if schema is None:
            schema_str = "Unknown schema"
        elif isinstance(schema, dict):
            schema_str = "<Tensor: shape={}, dtype={}>".format(
                schema["shape"], schema["dtype"])
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
        if count is None:
            count = "?"
        return "Dataset(num_blocks={}, num_rows={}, schema={})".format(
            len(self._blocks), count, schema_str)

    def __str__(self) -> str:
        return repr(self)

    def _block_sizes(self) -> List[int]:
        get_num_rows = cached_remote_fn(_get_num_rows)
        return ray.get([get_num_rows.remote(b) for b in self._blocks])

    def _meta_count(self) -> Optional[int]:
        metadata = self._blocks.get_metadata()
        if metadata and metadata[0].num_rows is not None:
            return sum(m.num_rows for m in metadata)
        else:
            return None

    def _get_uuid(self) -> str:
        return self._uuid

    def _set_uuid(self, uuid: str) -> None:
        self._uuid = uuid


def _get_num_rows(block: Block) -> int:
    block = BlockAccessor.for_block(block)
    return block.num_rows()


def _get_sum(block: Block) -> int:
    block = BlockAccessor.for_block(block)
    return sum(block.iter_rows())


def _remote_write(task: WriteTask) -> Any:
    return task()


def _block_to_df(block: Block):
    block = BlockAccessor.for_block(block)
    return block.to_pandas()


def _block_to_arrow(block: Block):
    block = BlockAccessor.for_block(block)
    return block.to_arrow()


def _check_is_arrow(block: Block) -> bool:
    import pyarrow
    return isinstance(block, pyarrow.Table)


def _truncate(block: Block, meta: BlockMetadata,
              count: int) -> (Block, BlockMetadata):
    block = BlockAccessor.for_block(block)
    logger.debug("Truncating last block to size: {}".format(count))
    new_block = block.slice(0, count, copy=True)
    accessor = BlockAccessor.for_block(new_block)
    new_meta = BlockMetadata(
        num_rows=accessor.num_rows(),
        size_bytes=accessor.size_bytes(),
        schema=meta.schema,
        input_files=meta.input_files)
    return new_block, new_meta
