from typing import List, Any, Callable, Iterator, Generic, TypeVar, \
    Generator, Optional, Union, TYPE_CHECKING

import pyarrow

if TYPE_CHECKING:
    import pandas
    import modin
    import dask
    import pyspark
    import ray.util.sgd

import ray
from ray.experimental.data.impl.compute import get_compute
from ray.experimental.data.impl.shuffle import simple_shuffle
from ray.experimental.data.impl.block import ObjectRef, Block

T = TypeVar("T")
U = TypeVar("U")
BatchType = Union["pandas.DataFrame", pyarrow.Table]


class Dataset(Generic[T]):
    """A dataset represents a set of records in the Ray object store.

    Datasets are implemented as a list of ``ObjectRef[Block[T]]``. The block
    also determines the unit of parallelism. The most common type of block is
    the ``ArrowBlock``, which is backed by a ``pyarrow.Table`` object. Other
    Python objects are represented with ``ListBlock`` (a plain Python list).

    Since Datasets are just lists of Ray objects, they can be freely passed
    between Ray tasks and actors just like any other value. Datasets support
    conversion to/from several more featureful dataframe libraries
    (e.g., Spark, Dask), and are also compatible with TensorFlow / PyTorch.

    Dataset supports parallel transformations such as .map(), .map_batches(),
    and simple repartition, but currently not aggregations and joins.
    """

    def __init__(self, blocks: List[ObjectRef[Block[T]]], block_cls: Any):
        self._blocks: List[ObjectRef[Block[T]]] = blocks
        self._block_cls: type = block_cls

    def map(self, fn: Callable[[T], U], compute="tasks",
            **ray_remote_args) -> "Dataset[U]":
        """Apply the given function to each record of this dataset.

        This is a blocking operation. Note that mapping individual records
        can be quite slow. Consider using `.map_batches()` for performance.

        Args:
            fn: The function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                builder.add(fn(row))
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def map_batches(self,
                    fn: Callable[[BatchType], BatchType],
                    batch_size: int = None,
                    compute: str = "tasks",
                    batch_format: str = "pandas",
                    **ray_remote_args) -> "Dataset[dict]":
        """Apply the given function to batches of records of this dataset.

        This is a blocking operation.

        Args:
            fn: The function to apply to each record batch.
            batch_size: Request a specific batch size, or leave unspecified
                for the system to select a batch size.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            batch_format: Specify "pandas" to select ``pandas.DataFrame`` as
                the batch format, or "pyarrow" to select ``pyarrow.Table``.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        raise NotImplementedError  # P0

    def flat_map(self,
                 fn: Callable[[T], Iterator[U]],
                 compute="tasks",
                 **ray_remote_args) -> "Dataset[U]":
        """Apply the given function to each record and then flatten results.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (the batch size can be altered in map_batches).

        Args:
            fn: The function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                for r2 in fn(row):
                    builder.add(r2)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def filter(self,
               fn: Callable[[T], bool],
               compute="tasks",
               **ray_remote_args) -> "Dataset[T]":
        """Filter out records that do not satisfy the given predicate.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (you can implement filter by dropping records).

        Args:
            fn: The predicate function to apply to each record.
            compute: The compute strategy, either "tasks" to use Ray tasks,
                or "actors" to use an autoscaling Ray actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def repartition(self, num_blocks: int) -> "Dataset[T]":
        """Repartition the dataset into exactly this number of blocks.

        Args:
            num_blocks: The number of blocks.

        Returns:
            The repartitioned dataset.
        """

        new_blocks = simple_shuffle(self._block_cls, self._blocks, num_blocks)
        return Dataset(new_blocks, self._block_cls)

    def truncate(self, limit: int) -> "Dataset[T]":
        """Truncate the dataset to the given number of records.

        This operation is useful to limit the size of the dataset during
        development.

        Args:
            limit: The size of the dataset to truncate to.

        Returns:
            The truncated dataset.
        """
        raise NotImplementedError  # P1

    def take(self, limit: int = 20) -> List[T]:
        """Take up to the given number of records from the dataset.

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

        Args:
            limit: The max number of records to print.
        """
        for row in self.take(limit):
            print(row)

    def to_local_iterator(self) -> Generator:
        """Return an iterator that can be used to scan the dataset serially.

        Returns:
            A local iterator over the entire dataset.
        """
        for b in self._blocks:
            block = self._block_cls.deserialize(ray.get(b))
            for row in block.iter_rows():
                yield row

    def count(self) -> int:
        """Count the number of records in the dataset.

        Returns:
            The number of records in the dataset.
        """

        @ray.remote
        def count(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return block.num_rows()

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        """Sum up the elements of this dataset.

        Returns:
            The sum of the records in the dataset.
        """

        @ray.remote
        def agg(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return sum(block.iter_rows())

        return sum(ray.get([agg.remote(block) for block in self._blocks]))

    def schema(self) -> Union[type, pyarrow.lib.Schema]:
        """Return the schema of the dataset.

        For datasets of Arrow records, this will return the Arrow schema.
        For dataset of Python objects, this returns their Python type.

        Returns:
            The Python type or Arrow schema of the records.
        """
        raise NotImplementedError  # P0

    def num_blocks(self) -> int:
        """Return the number of blocks of this dataset.

        Returns:
            The number of blocks of this dataset.
        """
        return len(self._blocks)

    def size_bytes(self) -> int:
        """Return the in-memory size of the dataset.

        Returns:
            The in-memory size of the dataset in bytes.
        """
        raise NotImplementedError  # P0

    def input_files(self) -> List[str]:
        """Return the list of input files for the dataset.

        Returns:
            The list of input files used to create the dataset.
        """
        raise NotImplementedError  # P0

    def write_parquet(path: str,
                      filesystem: Optional[pyarrow.fs.FileSystem] = None
                      ) -> None:
        """Write the dataset to parquet.

        This is only supported for datasets convertible to Arrow records.

        Args:
            path: The path in the filesystem to write to.
            filesystem: The filesystem implementation to write to.
        """
        raise NotImplementedError  # P0

    def write_json(path: str,
                   filesystem: Optional[pyarrow.fs.FileSystem] = None) -> None:
        """Write the dataset to json.

        This is only supported for datasets convertible to Arrow records.

        Args:
            path: The path in the filesystem to write to.
            filesystem: The filesystem implementation to write to.
        """
        raise NotImplementedError  # P0

    def write_csv(path: str,
                  filesystem: Optional[pyarrow.fs.FileSystem] = None) -> None:
        """Write the dataset to csv.

        This is only supported for datasets convertible to Arrow records.

        Args:
            path: The path in the filesystem to write to.
            filesystem: The filesystem implementation to write to.
        """
        raise NotImplementedError  # P0

    def to_torch(self, **todo) -> "ray.util.sgd.torch.TorchMLDataset":
        """Return a dataset that can be used for Torch distributed training.

        Returns:
            A TorchMLDataset.
        """
        raise NotImplementedError  # P1

    def to_tf(self, **todo) -> "ray.util.sgd.tf.TFMLDataset":
        """Return a dataset that can be used for TF distributed training.

        Returns:
            A TFMLDataset.
        """
        raise NotImplementedError  # P1

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

    def to_dask(self) -> "dask.DataFrame":
        """Convert this dataset into a Dask dataframe.

        Returns:
            A Dask dataframe created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_modin(self) -> "modin.DataFrame":
        """Convert this dataset into a Modin dataframe.

        Returns:
            A Modin dataframe created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_pandas(self) -> Iterator[ObjectRef["pandas.DataFrame"]]:
        """Convert this dataset into a set of Pandas dataframes.

        Returns:
            A list of remote Pandas dataframes created from this dataset.
        """
        raise NotImplementedError  # P1

    def to_spark(self) -> "pyspark.sql.DataFrame":
        """Convert this dataset into a Spark dataframe.

        Returns:
            A Spark dataframe created from this dataset.
        """
        raise NotImplementedError  # P2

    def __repr__(self) -> str:
        return "Dataset({} blocks, {})".format(
            len(self._blocks), self._block_cls)

    def __str__(self) -> str:
        return repr(self)

    def _block_sizes(self) -> List[int]:
        @ray.remote
        def query(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return block.num_rows()

        return ray.get([query.remote(b) for b in self._blocks])
