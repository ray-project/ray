import functools
import time
from typing import Any, Callable, List, Iterator, Iterable, Generic, Union, \
    TYPE_CHECKING

import ray
from ray.data.dataset import Dataset, T, U, BatchType
from ray.data.impl.pipeline_executor import PipelineExecutor, \
    PipelineSplitExecutorCoordinator
from ray.data.impl import progress_bar
from ray.util.annotations import PublicAPI, DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

# Operations that can be naively applied per dataset in the pipeline.
PER_DATASET_OPS = [
    "map", "map_batches", "flat_map", "filter", "repartition",
    "random_shuffle", "sort", "write_json", "write_csv", "write_parquet",
    "write_datasource"
]

# Similar to above but we should force evaluation immediately.
PER_DATASET_OUTPUT_OPS = [
    "write_json", "write_csv", "write_parquet", "write_datasource"
]

# Operations that operate over the stream of output batches from the pipeline.
OUTPUT_ITER_OPS = ["take", "show", "iter_rows", "to_tf", "to_torch"]


@PublicAPI(stability="beta")
class DatasetPipeline(Generic[T]):
    """Implements a pipeline of Datasets.

    Unlike Datasets, which execute all transformations synchronously,
    DatasetPipelines implement pipelined execution. This allows for the
    overlapped execution of data input (e.g., reading files), computation
    (e.g. feature preprocessing), and output (e.g., distributed ML training).

    A DatasetPipeline can be created by either repeating a Dataset
    (``ds.repeat(times=None)``), by turning a single Dataset into a pipeline
    (``ds.pipeline(parallelism=10)``), or defined explicitly using
    ``DatasetPipeline.from_iterable()``.

    DatasetPipeline supports the all the per-record transforms of Datasets
    (e.g., map, flat_map, filter), holistic transforms (e.g., repartition),
    and output methods (e.g., iter_rows, to_tf, to_torch, write_datasource).
    """

    def __init__(self,
                 base_iterable: Iterable[Callable[[], Dataset[T]]],
                 stages: List[Callable[[Dataset[Any]], Dataset[Any]]] = None,
                 length: int = None,
                 progress_bars: bool = progress_bar._enabled):
        """Construct a DatasetPipeline (internal API).

        The constructor is not part of the DatasetPipeline API. Use the
        ``Dataset.repeat()``, ``Dataset.pipeline()``, or
        ``DatasetPipeline.from_iterable()`` methods to construct a pipeline.
        """
        self._base_iterable = base_iterable
        self._stages = stages or []
        self._length = length
        self._progress_bars = progress_bars
        self._uuid = None  # For testing only.

    def iter_batches(self,
                     *,
                     prefetch_blocks: int = 0,
                     batch_size: int = None,
                     batch_format: str = "pandas",
                     drop_last: bool = False) -> Iterator[BatchType]:
        """Return a local batched iterator over the data in the pipeline.

        Examples:
            >>> for pandas_df in ray.data.range(1000000).iter_batches():
            ...     print(pandas_df)

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: Record batch size, or None to let the system pick.
            batch_format: The format in which to return each batch.
                Specify "pandas" to select ``pandas.DataFrame`` or "pyarrow" to
                select ``pyarrow.Table``. Default is "pandas".
            drop_last: Whether to drop the last batch if it's incomplete.

        Returns:
            A list of iterators over record batches.
        """

        def gen_batches() -> Iterator[BatchType]:
            for ds in self.iter_datasets():
                for batch in ds.iter_batches(
                        prefetch_blocks=prefetch_blocks,
                        batch_size=batch_size,
                        batch_format=batch_format,
                        drop_last=drop_last):
                    yield batch

        return gen_batches()

    def split(self,
              n: int,
              *,
              equal: bool = False,
              locality_hints: List[Any] = None) -> List["DatasetPipeline[T]"]:
        """Split the pipeline into ``n`` disjoint pipeline shards.

        This returns a list of sub-pipelines that can be passed to Ray tasks
        and actors and used to read the pipeline records in parallel.

        Examples:
            >>> # Split up a pipeline to process over `n` worker actors.
            >>> shards = pipe.split(len(workers), locality_hints=workers)
            >>> for shard, worker in zip(shards, workers):
            ...     worker.consume.remote(shard)

        Time complexity: O(1)

        Implementation detail: this launches a coordinator actor that is used
        to execute the pipeline and push data blocks to each pipeline shard.
        Reading from an individual shard will be blocked if other shards are
        falling behind. A warning will be printed if a shard has been blocked
        on read for more than 10 seconds.

        Args:
            n: Number of child pipelines to return.
            equal: Whether to guarantee each split has an equal
                number of records. This may drop records if they cannot be
                divided equally among the splits.
            locality_hints: A list of Ray actor handles of size ``n``. The
                system will try to co-locate the blocks of the ith pipeline
                shard with the ith actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint pipeline splits.
        """
        coordinator = PipelineSplitExecutorCoordinator.remote(
            self, n, equal, locality_hints)

        class SplitIterator:
            def __init__(self, split_index):
                self.split_index = split_index
                self.warn_threshold = 100
                self.wait_delay_s = 0.1

            def __iter__(self):
                return self

            def __next__(self):
                ds = None
                tries = 0
                while ds is None:
                    ds = ray.get(
                        coordinator.next_dataset_if_ready.remote(
                            self.split_index))
                    # Wait for other shards to catch up reading.
                    if not ds:
                        time.sleep(self.wait_delay_s)
                        tries += 1
                    if tries > self.warn_threshold:
                        print("Warning: shard {} of the pipeline has been "
                              "stalled more than {}s waiting for other shards "
                              "to catch up.".format(
                                  self.split_index,
                                  self.wait_delay_s * self.warn_threshold))
                        self.warn_threshold *= 2
                return lambda: ds

        splits = []
        for i in range(n):
            # Disable progress bars for the split readers since they would
            # overwhelm the console.
            splits.append(
                DatasetPipeline(SplitIterator(i), progress_bars=False))

        return splits

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the schema of the dataset pipeline.

        For datasets of Arrow records, this will return the Arrow schema.
        For dataset of Python objects, this returns their Python type.

        Time complexity: O(1)

        Returns:
            The Python type or Arrow schema of the records, or None if the
            schema is not known.
        """
        return next(self.iter_datasets()).schema()

    def count(self) -> int:
        """Count the number of records in the dataset pipeline.

        This blocks until the entire pipeline is fully executed.

        Time complexity: O(dataset size / parallelism)

        Returns:
            The number of records in the dataset pipeline.
        """
        pipe = self.map_batches(lambda batch: [len(batch)])
        total = 0
        for elem in pipe.iter_rows():
            total += elem
        return total

    def sum(self) -> int:
        """Sum the records in the dataset pipeline.

        This blocks until the entire pipeline is fully executed.

        Time complexity: O(dataset size / parallelism)

        Returns:
            The sum of the records in the dataset pipeline.
        """
        pipe = self.map_batches(
            lambda batch: [batch.sum()[0]], batch_format="pandas")
        total = 0
        for elem in pipe.iter_rows():
            total += elem
        return total

    @DeveloperAPI
    def iter_datasets(self) -> Iterator[Dataset[T]]:
        """Iterate over the output datasets of this pipeline.

        Returns:
            Iterator over the datasets outputted from this pipeline.
        """
        return PipelineExecutor(self)

    @DeveloperAPI
    def foreach_dataset(self, fn: Callable[[Dataset[T]], Dataset[U]]
                        ) -> "DatasetPipeline[U]":
        """Apply a transform to each dataset in this pipeline.

        Args:
            fn: The function to transform each dataset with.

        Returns:
            The transformed DatasetPipeline.
        """
        return DatasetPipeline(self._base_iterable, self._stages + [fn],
                               self._length, self._progress_bars)

    @DeveloperAPI
    @staticmethod
    def from_iterable(iterable: Iterable[Callable[[], Dataset[T]]],
                      ) -> "DatasetPipeline[T]":
        """Create a pipeline from an sequence of Dataset producing functions.

        Args:
            iterable: A finite or infinite-length sequence of functions that
                each produce a Dataset when called.
        """
        if hasattr(iterable, "__len__"):
            length = len(iterable)
        else:
            length = None
        return DatasetPipeline(iterable, length=length)

    def __repr__(self) -> str:
        return "DatasetPipeline(length={}, num_stages={})".format(
            self._length, 1 + len(self._stages))

    def __str__(self) -> str:
        return repr(self)

    def _get_uuid(self) -> str:
        return self._uuid

    def _set_uuid(self, uuid: str) -> None:
        self._uuid = uuid


for method in PER_DATASET_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        @functools.wraps(delegate)
        def impl(self, *args, **kwargs):
            return self.foreach_dataset(
                lambda ds: getattr(ds, method)(*args, **kwargs))

        if impl.__annotations__.get("return"):
            impl.__annotations__["return"] = impl.__annotations__[
                "return"].replace("Dataset", "DatasetPipeline")

        return impl

    setattr(DatasetPipeline, method, make_impl(method))

for method in PER_DATASET_OUTPUT_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        @functools.wraps(delegate)
        def impl(self, *args, **kwargs):
            uuid = None
            for i, ds in enumerate(self.iter_datasets()):
                if uuid is None:
                    uuid = self._get_uuid() or ds._get_uuid()
                ds._set_uuid(f"{uuid}_{i:06}")
                getattr(ds, method)(*args, **kwargs)

        return impl

    setattr(DatasetPipeline, method, make_impl(method))

for method in OUTPUT_ITER_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        @functools.wraps(delegate)
        def impl(self, *args, **kwargs):
            return delegate(self, *args, **kwargs)

        return impl

    setattr(DatasetPipeline, method, make_impl(method))
