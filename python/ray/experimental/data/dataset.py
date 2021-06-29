from typing import List, Any, Callable, Iterator, Generic, TypeVar, \
    Generator, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import modin
    import dask
    import pyspark
    import ray.util.sgd

import ray
from ray.experimental.data.impl.compute import get_compute
from ray.experimental.data.impl.shuffle import simple_shuffle
from ray.experimental.data.impl.block import ObjectRef, Block, ListBlock
from ray.experimental.data.impl.arrow_block import (
    DelegatingArrowBlockBuilder, ArrowBlock)

T = TypeVar("T")
U = TypeVar("U")
BatchType = Union["pandas.DataFrame", "pyarrow.Table"]


class Dataset(Generic[T]):
    """Implements a distributed Arrow dataset.

    Datasets are implemented as a list of ``ObjectRef[Block[T]]``. The block
    also determines the unit of parallelism. The default block type is the
    ``ArrowBlock``, which is backed by a ``pyarrow.Table`` object. Other
    Python objects are represented with ``ListBlock`` (a plain Python list).

    Since Datasets are just lists of Ray object refs, they can be passed
    between Ray tasks and actors just like any other object. Datasets support
    conversion to/from several more featureful dataframe libraries
    (e.g., Spark, Dask), and are also compatible with TensorFlow / PyTorch.

    Dataset supports parallel transformations such as .map(), .map_batches(),
    and simple repartition, but currently not aggregations and joins.
    """

    def __init__(self, blocks: List[ObjectRef[Block[T]]]):
        self._blocks: List[ObjectRef[Block[T]]] = blocks

    def map(self, fn: Callable[[T], U], compute="tasks",
            **ray_remote_args) -> "Dataset[U]":
        """Apply the given function to each record of this dataset.

        This is a blocking operation. Note that mapping individual records
        can be quite slow. Consider using `.map_batches()` for performance.

        Examples:
            # Transform python objects.
            >>> ds.map(lambda x: x * 2)

            # Transform Arrow records.
            >>> ds.map(lambda record: {"v2": record["value"] * 2})

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(block: Block[T]) -> Block[U]:
            builder = DelegatingArrowBlockBuilder()
            for row in block.iter_rows():
                builder.add(fn(row))
            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def map_batches(self,
                    fn: Callable[[BatchType], BatchType],
                    batch_size: int = None,
                    compute: str = "tasks",
                    batch_format: str = "pandas",
                    **ray_remote_args) -> "Dataset[Any]":
        """Apply the given function to batches of records of this dataset.

        This is a blocking operation.

        Examples:
            # Transform batches in parallel.
            >>> ds.map_batches(lambda batch: [v * 2 for v in batch])

            # Transform batches in parallel on GPUs.
            >>> ds.map_batches(
            ...    batch_infer_fn,
            ...    batch_size=256, compute="actors", num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record batch.
            batch_size: Request a specific batch size, or leave unspecified
                to use entire blocks as batches.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            batch_format: Specify "pandas" to select ``pandas.DataFrame`` as
                the batch format, or "pyarrow" to select ``pyarrow.Table``.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if batch_size is not None and batch_size < 1:
            raise ValueError("Batch size cannot be negative or 0")
        import pyarrow as pa
        import pandas as pd

        def transform(block: Block[T]) -> Block[U]:
            total_rows = block.num_rows()
            max_batch_size = batch_size
            if max_batch_size is None:
                max_batch_size = total_rows

            builder = DelegatingArrowBlockBuilder()

            for start in range(0, total_rows, max_batch_size):
                # Build a block for each batch.
                end = min(total_rows, start + max_batch_size)
                # Note: if the block is a list, it doesn't support zero-copy.
                view = block.slice(start, end)
                if batch_format == "pandas":
                    view = view.to_pandas()
                elif batch_format == "pyarrow":
                    view = view._table
                else:
                    raise ValueError(
                        f"The given batch format: {batch_format} "
                        f"is invalid. Supported batch type: {BatchType}")

                applied = fn(view)
                if isinstance(applied, list):
                    applied = ListBlock(applied)
                elif isinstance(applied, pd.core.frame.DataFrame):
                    applied = ArrowBlock(pa.Table.from_pandas(applied))
                elif isinstance(applied, pa.Table):
                    applied = ArrowBlock(applied)
                else:
                    raise ValueError("The map batch UDF returns a type "
                                     f"{type(applied)}, which is not allowed. "
                                     "The return type must be either list, "
                                     "pandas.DataFrame, or pyarrow.Table")
                builder.add_block(applied)

            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def flat_map(self,
                 fn: Callable[[T], Iterator[U]],
                 compute="tasks",
                 **ray_remote_args) -> "Dataset[U]":
        """Apply the given function to each record and then flatten results.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (the batch size can be altered in map_batches).

        Examples:
            >>> ds.flat_map(lambda x: [x, x ** 2, x ** 3])

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(block: Block[T]) -> Block[U]:
            builder = DelegatingArrowBlockBuilder()
            for row in block.iter_rows():
                for r2 in fn(row):
                    builder.add(r2)
            return builder.build()

        compute = get_compute(compute)

        return Dataset(compute.apply(transform, ray_remote_args, self._blocks))

    def filter(self,
               fn: Callable[[T], bool],
               compute="tasks",
               **ray_remote_args) -> "Dataset[T]":
        """Filter out records that do not satisfy the given predicate.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (you can implement filter by dropping records).

        Examples:
            >>> ds.flat_map(lambda x: x % 2 == 0)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The predicate function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(block: Block[T]) -> Block[T]:
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
            # Set the number of output partitions to write to disk.
            >>> ds.repartition(100).write_parquet(...)

        Time complexity: O(dataset size / parallelism)

        Args:
            num_blocks: The number of blocks.

        Returns:
            The repartitioned dataset.
        """

        new_blocks = simple_shuffle(self._blocks, num_blocks)
        return Dataset(new_blocks)

    def sort(self,
             key: Union[None, str, List[str], Callable[[T], Any]],
             descending: bool = False) -> "Dataset[T]":
        """Sort the dataset by the specified key columns or key function.

        This is a blocking operation.

        Examples:
            # Sort using the entire record as the key.
            >>> ds.sort()

            # Sort by a single column.
            >>> ds.sort("field1")

            # Sort by multiple columns.
            >>> ds.sort(["field1", "field2"])

            # Sort by a key function.
            >>> ds.sort(lambda record: record["field1"] % 100)

        Time complexity: O(dataset size / parallelism)

        Args:
            key: Either a single Arrow column name, a list of Arrow column
                names, a function that returns a sortable key given each
                record as an input, or None to sort by the entire record.
            descending: Whether to sort in descending order.

        Returns:
            The sorted dataset.
        """
        raise NotImplementedError  # P2

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
        raise NotImplementedError  # P1

    def take(self, limit: int = 20) -> List[T]:
        """Take up to the given number of records from the dataset.

        Time complexity: O(limit specified)

        Args:
            limit: The max number of records to return.

        Returns:
            A list of up to ``limit`` records from the dataset.
        """
        output = []
        for row in self.to_local_iterator():
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

        Time complexity: O(1)

        Returns:
            The number of records in the dataset.
        """

        @ray.remote
        def count(block: Block[T]) -> int:
            return block.num_rows()

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        """Sum up the elements of this dataset.

        Time complexity: O(dataset size / parallelism)

        Returns:
            The sum of the records in the dataset.
        """

        @ray.remote
        def agg(block: Block[T]) -> int:
            return sum(block.iter_rows())

        return sum(ray.get([agg.remote(block) for block in self._blocks]))

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the schema of the dataset.

        For datasets of Arrow records, this will return the Arrow schema.
        For dataset of Python objects, this returns their Python type.

        Time complexity: O(1)

        Returns:
            The Python type or Arrow schema of the records.
        """
        raise NotImplementedError  # P0

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
            The in-memory size of the dataset in bytes.
        """
        raise NotImplementedError  # P0

    def input_files(self) -> List[str]:
        """Return the list of input files for the dataset.

        Time complexity: O(num input files)

        Returns:
            The list of input files used to create the dataset.
        """
        raise NotImplementedError  # P0

    def write_parquet(
            self,
            paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to parquet.

        This is only supported for datasets convertible to Arrow records.

        Examples:
            # Write to a single file.
            >>> ds.write_parquet("s3://bucket/path")

            # Write to multiple files.
            >>> ds.write_parquet(["/path/to/file1", "/path/to/file2"])

        Time complexity: O(dataset size / parallelism)

        Args:
            paths: A single file path or a list of file paths (or directories).
            filesystem: The filesystem implementation to write to.
        """
        import pyarrow.parquet as pq
        import numpy as np

        if isinstance(paths, str):
            paths = [paths]
        elif (not isinstance(paths, list)
              or any(not isinstance(p, str) for p in paths)):
            raise ValueError(
                "paths must be a path string or a list of path strings.")

        @ray.remote
        def parquet_write(writer_path, *blocks):
            print(f"Writing {len(blocks)} blocks to {writer_path}.")
            with pq.ParquetWriter(writer_path,
                                  blocks[0]._table.schema) as writer:
                for block in blocks:
                    writer.write_table(block._table)

        refs = [
            parquet_write.remote(writer_path, *blocks)
            for writer_path, blocks in zip(
                paths, np.array_split(self._blocks, len(paths)))
            if len(blocks) > 0
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_parquet_partitions(
            self,
            path: str,
            partition_cols: List[str],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to parquet as a partitioned dataset, consisting of
        multiple files.

        This is only supported for datasets convertible to Arrow records.

        Examples:
            >>> ds.write_parquet_partitions(
                    "s3://bucket/path", partition_cols=["foo", "bar"]))

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The root path to the parent directory in the filesystem to
                write to.
            partition_cols: Column names by which to partition the dataset.
            filesystem: The filesystem implementation to write to.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.concat_tables(
            [block._table for block in ray.get(self._blocks)])
        pq.write_to_dataset(
            table, path, partition_cols=partition_cols, filesystem=filesystem)

    def write_json(
            self,
            paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to json.

        This is only supported for datasets convertible to Arrow records.

        Examples:
            # Write to a single file.
            >>> ds.write_json("s3://bucket/path")

            # Write to multiple files.
            >>> ds.write_json(["/path/to/file1", "/path/to/file2"])

        Time complexity: O(dataset size / parallelism)

        Args:
            paths: A single file path or a list of file paths (or directories).
            filesystem: The filesystem implementation to write to.
        """
        import pandas as pd
        import numpy as np

        if isinstance(paths, str):
            paths = [paths]
        elif (not isinstance(paths, list)
              or any(not isinstance(p, str) for p in paths)):
            raise ValueError(
                "paths must be a path string or a list of path strings.")

        @ray.remote
        def json_write(writer_path, *blocks):
            print(f"Writing {len(blocks)} blocks to {writer_path}.")
            dfs = []
            for block in blocks:
                dfs.append(block.to_pandas())
            pd.concat(dfs).to_json(writer_path, orient="records")

        refs = [
            json_write.remote(writer_path, *blocks)
            for writer_path, blocks in zip(
                paths, np.array_split(self._blocks, len(paths)))
            if len(blocks) > 0
        ]

        # Block until writing is done.
        ray.get(refs)

    def write_csv(
            self,
            paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None) -> None:
        """Write the dataset to csv.

        This is only supported for datasets convertible to Arrow records.

        Examples:
            # Write to a single file.
            >>> ds.write_csv("s3://bucket/path")

            # Write to multiple files.
            >>> ds.write_csv(["/path/to/file1", "/path/to/file2"])

        Time complexity: O(dataset size / parallelism)

        Args:
            paths: A single file path or a list of file paths (or directories).
            filesystem: The filesystem implementation to write to.
        """
        import numpy as np

        if isinstance(paths, str):
            paths = [paths]
        elif (not isinstance(paths, list)
              or any(not isinstance(p, str) for p in paths)):
            raise ValueError(
                "paths must be a path string or a list of path strings.")

        @ray.remote
        def csv_write(writer_path, *blocks):
            print(f"Writing {len(blocks)} blocks to {writer_path}.")
            include_header = True
            for block in blocks:
                block.to_pandas().to_csv(
                    writer_path, mode="a", header=include_header)
                include_header = False

        refs = [
            csv_write.remote(writer_path, *blocks)
            for writer_path, blocks in zip(
                paths, np.array_split(self._blocks, len(paths)))
            if len(blocks) > 0
        ]

        # Block until writing is done.
        ray.get(refs)

    def to_local_iterator(self) -> Generator:
        """Return an iterator that can be used to scan the dataset serially.

        Time complexity: O(1)

        Returns:
            A local iterator over the entire dataset.
        """
        for b in self._blocks:
            block = ray.get(b)
            for row in block.iter_rows():
                yield row

    def to_batch_iterators(
            self,
            num_shards: int,
            batch_size: int = None,
            output_location_prefs: List[Any] = None,
            batch_format: str = "pandas",
            repeatable: bool = False) -> List[Iterator[BatchType]]:
        """Return a list of distributed iterators over record batches.

        This returns a list of iterators that can be passed to Ray tasks
        and actors, and used to read the dataset records in parallel.

        Time complexity: O(1)

        Args:
            num_shards: Number of iterators to return.
            batch_size: Record batch size, or None to let the system pick.
            output_location_prefs: A list of Ray actor handles of size
                ``num_shards``. The system will try to co-locate the objects
                given to the ith iterator with the ith actor to maximize data
                locality.
            batch_format: Specify "pandas" to select ``pandas.DataFrame`` as
                the batch format, or "pyarrow" to select ``pyarrow.Table``.
            repeatable: Whether each iterator should loop over data forever
                or stop after reading all records of the shard.

        Returns:
            A list of iterators over record batches.
        """
        raise NotImplementedError  # P1

    def to_torch(self, **todo) -> "ray.util.sgd.torch.TorchMLDataset":
        """Return a dataset that can be used for Torch distributed training.

        Time complexity: O(1)

        Returns:
            A TorchMLDataset.
        """
        raise NotImplementedError  # P1

    def to_tf(self, **todo) -> "ray.util.sgd.tf.TFMLDataset":
        """Return a dataset that can be used for TF distributed training.

        Time complexity: O(1)

        Returns:
            A TFMLDataset.
        """
        raise NotImplementedError  # P1

    def to_dask(self) -> "dask.DataFrame":
        """Convert this dataset into a Dask DataFrame.

        Time complexity: O(1)

        Returns:
            A Dask DataFrame created from this dataset.
        """
        import dask
        import dask.dataframe as dd
        from ray.util.client.common import ClientObjectRef

        def _block_to_df(block):
            return block._table.to_pandas()

        _remote_block_to_df = ray.remote(_block_to_df)

        @dask.delayed
        def block_to_df(block):
            if isinstance(block, (ray.ObjectRef, ClientObjectRef)):
                # The block Ray future hasn't been resolved to a concrete
                # value, so the end-user isn't using Dask-on-Ray. We therefore
                # launch a converting Ray task and block here to get the
                # underlying dataframe.
                df = ray.get(_remote_block_to_df.remote(block))
            else:
                # The block Ray future has been resolved, so the end-user is
                # using Dask-on-Ray. We therefore directly convert the block to
                # a dataframe since we're already running in a Ray task.
                df = _block_to_df(block)

            return df

        # TODO(Clark): Give Dask a Pandas-esque schema via the Pyarrow schema,
        # once that's implemented.
        return dd.from_delayed([block_to_df(block) for block in self._blocks])

    def to_modin(self) -> "modin.DataFrame":
        """Convert this dataset into a Modin dataframe.

        Time complexity: O(1)

        Returns:
            A Modin dataframe created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_pandas(self) -> Iterator[ObjectRef["pandas.DataFrame"]]:
        """Convert this dataset into a set of Pandas dataframes.

        Time complexity: O(1)

        Returns:
            A list of remote Pandas dataframes created from this dataset.
        """

        @ray.remote
        def block_to_df(block):
            return block._table.to_pandas()

        return [block_to_df.remote(block) for block in self._blocks]

    def to_spark(self) -> "pyspark.sql.DataFrame":
        """Convert this dataset into a Spark dataframe.

        Time complexity: O(1)

        Returns:
            A Spark dataframe created from this dataset.
        """
        raise NotImplementedError  # P2

    def __repr__(self) -> str:
        return "Dataset({} blocks)".format(len(self._blocks))

    def __str__(self) -> str:
        return repr(self)

    def _block_sizes(self) -> List[int]:
        @ray.remote
        def query(block: Block[T]) -> int:
            return block.num_rows()

        return ray.get([query.remote(b) for b in self._blocks])
