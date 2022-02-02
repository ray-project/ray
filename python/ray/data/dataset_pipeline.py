import inspect
import time
from typing import (
    Any,
    Callable,
    List,
    Iterator,
    Iterable,
    Generic,
    Union,
    Optional,
    TYPE_CHECKING,
)

import ray
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, T, U, BatchType
from ray.data.impl.pipeline_executor import (
    PipelineExecutor,
    PipelineSplitExecutorCoordinator,
)
from ray.data.impl import progress_bar
from ray.data.impl.stats import DatasetPipelineStats
from ray.util.annotations import PublicAPI, DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

# Operations that can be naively applied per dataset row in the pipeline.
PER_DATASET_OPS = ["map", "map_batches", "add_column", "flat_map", "filter"]

# Operations that apply to each dataset holistically in the pipeline.
HOLISTIC_PER_DATASET_OPS = ["repartition", "random_shuffle", "sort"]

# Similar to above but we should force evaluation immediately.
PER_DATASET_OUTPUT_OPS = [
    "write_json",
    "write_csv",
    "write_parquet",
    "write_datasource",
]

# Operations that operate over the stream of output batches from the pipeline.
OUTPUT_ITER_OPS = ["take", "take_all", "show", "iter_rows", "to_tf", "to_torch"]


@PublicAPI(stability="beta")
class DatasetPipeline(Generic[T]):
    """Implements a pipeline of Datasets.

    Unlike Datasets, which execute all transformations synchronously,
    DatasetPipelines implement pipelined execution. This allows for the
    overlapped execution of data input (e.g., reading files), computation
    (e.g. feature preprocessing), and output (e.g., distributed ML training).

    A DatasetPipeline can be created by either repeating a Dataset
    (``ds.repeat(times=None)``), by turning a single Dataset into a pipeline
    (``ds.window(blocks_per_window=10)``), or defined explicitly using
    ``DatasetPipeline.from_iterable()``.

    DatasetPipeline supports the all the per-record transforms of Datasets
    (e.g., map, flat_map, filter), holistic transforms (e.g., repartition),
    and output methods (e.g., iter_rows, to_tf, to_torch, write_datasource).
    """

    def __init__(
        self,
        base_iterable: Iterable[Callable[[], Dataset[T]]],
        stages: List[Callable[[Dataset[Any]], Dataset[Any]]] = None,
        length: int = None,
        progress_bars: bool = progress_bar._enabled,
        _executed: List[bool] = None,
    ):
        """Construct a DatasetPipeline (internal API).

        The constructor is not part of the DatasetPipeline API. Use the
        ``Dataset.repeat()``, ``Dataset.window()``, or
        ``DatasetPipeline.from_iterable()`` methods to construct a pipeline.
        """
        self._base_iterable = base_iterable
        self._stages = stages or []
        self._length = length
        self._progress_bars = progress_bars
        self._uuid = None  # For testing only.
        # Whether the pipeline execution has started.
        # This variable is shared across all pipelines descending from this.
        self._executed = _executed or [False]
        self._stats = DatasetPipelineStats()

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = None,
        batch_format: str = "pandas",
        drop_last: bool = False,
    ) -> Iterator[BatchType]:
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
            time_start = time.perf_counter()

            for ds in self.iter_datasets():
                wait_start = time.perf_counter()
                for batch in ds.iter_batches(
                    prefetch_blocks=prefetch_blocks,
                    batch_size=batch_size,
                    batch_format=batch_format,
                    drop_last=drop_last,
                ):
                    self._stats.iter_wait_s.add(time.perf_counter() - wait_start)
                    with self._stats.iter_user_s.timer():
                        yield batch
                    wait_start = time.perf_counter()

            self._stats.iter_total_s.add(time.perf_counter() - time_start)

        return gen_batches()

    def split(
        self, n: int, *, equal: bool = False, locality_hints: List[Any] = None
    ) -> List["DatasetPipeline[T]"]:
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
        return self._split(
            n, lambda ds: ds.split(n, equal=equal, locality_hints=locality_hints)
        )

    def split_at_indices(self, indices: List[int]) -> List["DatasetPipeline[T]"]:
        """Split the datasets within the pipeline at the given indices
        (like np.split).

        This will split each dataset contained within this pipeline, thereby
        producing len(indices) + 1 pipelines with the first pipeline containing
        the [0, indices[0]) slice from each dataset, the second pipeline
        containing the [indices[0], indices[1]) slice from each dataset, and so
        on, with the final pipeline will containing the
        [indices[-1], self.count()) slice from each dataset.

        Examples:
            >>> p1, p2, p3 = ray.data.range(
                    8).repeat(2).split_at_indices([2, 5])
            >>> p1.take()
            [0, 1, 0, 1]
            >>> p2.take()
            [2, 3, 4, 2, 3, 4]
            >>> p3.take()
            [5, 6, 7, 5, 6, 7]

        Time complexity: O(num splits)

        See also: ``DatasetPipeline.split``

        Args:
            indices: List of sorted integers which indicate where the pipeline
                will be split. If an index exceeds the length of the pipeline,
                an empty pipeline will be returned.

        Returns:
            The pipeline splits.
        """

        if len(indices) < 1:
            raise ValueError("indices must be at least of length 1")
        if sorted(indices) != indices:
            raise ValueError("indices must be sorted")
        if indices[0] < 0:
            raise ValueError("indices must be positive")

        return self._split(len(indices) + 1, lambda ds: ds.split_at_indices(indices))

    def _split(self, n: int, splitter: Callable[[Dataset], "DatasetPipeline[T]"]):

        coordinator = PipelineSplitExecutorCoordinator.remote(
            self, n, splitter, DatasetContext.get_current()
        )
        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        self._executed[0] = True

        class SplitIterator:
            def __init__(self, split_index, coordinator):
                self.split_index = split_index
                self.coordinator = coordinator
                self.warn_threshold = 100
                self.wait_delay_s = 0.1

            def __iter__(self):
                return self

            def __next__(self):
                ds = None
                tries = 0
                while ds is None:
                    ds = ray.get(
                        self.coordinator.next_dataset_if_ready.remote(self.split_index)
                    )
                    # Wait for other shards to catch up reading.
                    if not ds:
                        time.sleep(self.wait_delay_s)
                        tries += 1
                    if tries > self.warn_threshold:
                        print(
                            "Warning: reader on shard {} of the pipeline "
                            "has been blocked more than {}s waiting for "
                            "other readers to catch up. All pipeline shards "
                            "must be read from concurrently.".format(
                                self.split_index,
                                self.wait_delay_s * self.warn_threshold,
                            )
                        )
                        self.warn_threshold *= 2
                return lambda: ds

        return [
            # Disable progress bars for the split readers since they would
            # overwhelm the console.
            DatasetPipeline(
                SplitIterator(idx, coordinator),
                length=self._length,
                progress_bars=False,
            )
            for idx in range(n)
        ]

    def rewindow(
        self, *, blocks_per_window: int, preserve_epoch: bool = True
    ) -> "DatasetPipeline[T]":
        """Change the windowing (blocks per dataset) of this pipeline.

        Changes the windowing of this pipeline to the specified size. For
        example, if the current pipeline has two blocks per dataset, and
        `.rewindow(blocks_per_window=4)` is requested, adjacent datasets will
        be merged until each dataset is 4 blocks. If
        `.rewindow(blocks_per_window)` was requested the datasets will be
        split into smaller windows.

        Args:
            blocks_per_window: The new target blocks per window.
            preserve_epoch: Whether to preserve epoch boundaries. If set to
                False, then windows can contain data from two adjacent epochs.
        """

        class WindowIterator:
            def __init__(self, original_iter):
                self._original_iter = original_iter
                self._buffer: Optional[Dataset[T]] = None

            def __next__(self) -> Dataset[T]:
                try:
                    # Merge windows until we meet the requested window size.
                    if self._buffer is None:
                        self._buffer = next(self._original_iter)
                    while self._buffer.num_blocks() < blocks_per_window:
                        next_ds = next(self._original_iter)
                        if (
                            preserve_epoch
                            and self._buffer._get_epoch() != next_ds._get_epoch()
                        ):
                            partial_window = self._buffer
                            self._buffer = next_ds
                            return lambda: partial_window
                        else:
                            self._buffer = self._buffer.union(next_ds)
                    # Slice off the left-most chunk and return it.
                    res, self._buffer = self._buffer._divide(blocks_per_window)
                    assert res.num_blocks() <= blocks_per_window, res
                    if self._buffer.num_blocks() == 0:
                        self._buffer = None
                    return lambda: res
                except StopIteration:
                    # Return the left-over data as a single window.
                    if self._buffer and self._buffer.num_blocks() > 0:
                        res = self._buffer
                        assert res.num_blocks() <= blocks_per_window, res
                        self._buffer = None
                        return lambda: res
                    else:
                        raise

        class WindowIterable:
            def __init__(self, original_iter):
                self._original_iter = original_iter

            def __iter__(self):
                return WindowIterator(self._original_iter)

        if self._length == float("inf"):
            length = float("inf")
        else:
            length = None

        return DatasetPipeline(WindowIterable(self.iter_datasets()), length=length)

    def repeat(self, times: int = None) -> "DatasetPipeline[T]":
        """Repeat this pipeline a given number or times, or indefinitely.

        This operation is only allowed for pipelines of a finite length. An
        error will be raised for pipelines of infinite length.

        Transformations prior to the call to ``repeat()`` are evaluated once.
        Transformations done on the repeated pipeline are evaluated on each
        loop of the pipeline over the base pipeline.

        Note that every repeat of the pipeline is considered an "epoch" for
        the purposes of ``iter_epochs()``. If there are multiple repeat calls,
        the latest repeat takes precedence for the purpose of defining epochs.

        Args:
            times: The number of times to loop over this pipeline, or None
                to repeat indefinitely.
        """

        if self._length == float("inf"):
            raise ValueError("Cannot repeat a pipeline of infinite length.")

        class RepeatIterator:
            def __init__(self, original_iter):
                self._original_iter = original_iter
                # Holds results to repeat.
                self._results = []
                # Incrementing cursor over results.
                self._i = 0
                # This is calculated later.
                self._max_i = None

            def __next__(self) -> Dataset[T]:
                # Still going through the original pipeline.
                if self._original_iter:
                    try:
                        res = next(self._original_iter)
                        res._set_epoch(0)
                        self._results.append(res)
                        return lambda: res
                    except StopIteration:
                        self._original_iter = None
                        # Calculate the cursor limit.
                        if times:
                            self._max_i = len(self._results) * (times - 1)
                        else:
                            self._max_i = float("inf")
                # Going through a repeat of the pipeline.
                if self._i < self._max_i:
                    res = self._results[self._i % len(self._results)]
                    res._set_epoch(1 + self._i // len(self._results))
                    self._i += 1
                    return lambda: res
                else:
                    raise StopIteration

        class RepeatIterable:
            def __init__(self, original_iter):
                self._original_iter = original_iter

            def __iter__(self):
                return RepeatIterator(self._original_iter)

        if not times:
            length = float("inf")
        elif times and self._length:
            length = times * self._length
        else:
            length = None

        return DatasetPipeline(RepeatIterable(self.iter_datasets()), length=length)

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
        if self._length == float("inf"):
            raise ValueError("Cannot count a pipeline of infinite length.")

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
        if self._length == float("inf"):
            raise ValueError("Cannot sum a pipeline of infinite length.")

        pipe = self.map_batches(lambda batch: [batch.sum()[0]], batch_format="pandas")
        total = 0
        for elem in pipe.iter_rows():
            total += elem
        return total

    def show_windows(self, limit_per_dataset: int = 10) -> None:
        """Print up to the given number of records from each window/dataset.

        This is helpful as a debugging tool for understanding the structure of
        dataset pipelines.

        Args:
            limit_per_dataset: Rows to print per window/dataset.
        """
        epoch = None
        for i, ds in enumerate(self.iter_datasets()):
            if ds._get_epoch() != epoch:
                epoch = ds._get_epoch()
                print("------ Epoch {} ------".format(epoch))
            print("=== Window {} ===".format(i))
            ds.show(limit_per_dataset)

    def iter_epochs(self) -> Iterator["DatasetPipeline[T]"]:
        """Split this pipeline up by epoch.

        This allows reading of data per-epoch for repeated Datasets, which is
        useful for ML training. For example, ``ray.data.range(10).repeat(50)``
        generates a pipeline with 500 rows total split across 50 epochs. This
        method allows iterating over the data individually per epoch
        (repetition) of the original data.

        Examples:
            >>> epochs = ray.data.range(10).repeat(50).iter_epochs()
            >>> for i, epoch in enumerate(epochs):
            ...     print("Epoch", i)
            ...     for row in epoch.iter_rows():
            ...         print(row)

        Returns:
            Iterator over epoch objects, where each epoch is a DatasetPipeline
            containing data from that epoch only.
        """

        class Peekable:
            def __init__(self, base_iter: Iterator[T]):
                self._iter = base_iter
                self._buffer = None

            def _fill_buffer_if_possible(self):
                if self._buffer is None:
                    try:
                        self._buffer = next(self._iter)
                        assert self._buffer is not None
                    except StopIteration:
                        pass

            def peek(self) -> T:
                self._fill_buffer_if_possible()
                if self._buffer is None:
                    raise StopIteration
                return self._buffer

            def __next__(self) -> T:
                self._fill_buffer_if_possible()
                if self._buffer is None:
                    raise StopIteration
                item = self._buffer
                self._buffer = None
                return item

        class SingleEpochIterator:
            def __init__(self, peekable_iter: Iterator[Dataset[T]]):
                self._iter = peekable_iter
                self._epoch = None

            def __next__(self) -> Dataset[T]:
                if (
                    self._epoch is not None
                    and self._iter.peek()._get_epoch() != self._epoch
                ):
                    raise StopIteration
                ds = next(self._iter)
                self._epoch = ds._get_epoch()
                return lambda: ds

            def __iter__(self):
                return self

        class EpochDelimitedIterator:
            def __init__(self, pipe):
                self._iter = Peekable(pipe.iter_datasets())

            def __next__(self) -> "DatasetPipeline[T]":
                self._iter.peek()  # Raises StopIteration on end of data.
                epoch_pipe = DatasetPipeline.from_iterable(
                    SingleEpochIterator(self._iter)
                )
                return epoch_pipe

            def __iter__(self):
                return self

        return EpochDelimitedIterator(self)

    @DeveloperAPI
    def iter_datasets(self) -> Iterator[Dataset[T]]:
        """Iterate over the output datasets of this pipeline.

        Returns:
            Iterator over the datasets outputted from this pipeline.
        """
        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        self._executed[0] = True
        return PipelineExecutor(self)

    @DeveloperAPI
    def foreach_window(
        self, fn: Callable[[Dataset[T]], Dataset[U]]
    ) -> "DatasetPipeline[U]":
        """Apply a transform to each dataset/window in this pipeline.

        Args:
            fn: The function to transform each dataset with.

        Returns:
            The transformed DatasetPipeline.
        """
        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        return DatasetPipeline(
            self._base_iterable,
            self._stages + [fn],
            self._length,
            self._progress_bars,
            _executed=self._executed,
        )

    def foreach_dataset(self, *a, **kw) -> None:
        raise DeprecationWarning(
            "`foreach_dataset` has been renamed to `foreach_window`."
        )

    @DeveloperAPI
    def stats(self, exclude_first_window: bool = True) -> str:
        """Returns a string containing execution timing information.

        Args:
            exclude_first_window: Whether to exclude the first window from
                the pipeline time breakdown. This is generally a good idea
                since there is always a stall waiting for the first window to
                be initially computed, which can be misleading in the stats.
        """
        return self._stats.summary_string(exclude_first_window)

    @staticmethod
    def from_iterable(
        iterable: Iterable[Callable[[], Dataset[T]]],
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
        return "DatasetPipeline(num_windows={}, num_stages={})".format(
            self._length, 1 + len(self._stages)
        )

    def __str__(self) -> str:
        return repr(self)

    def _get_uuid(self) -> str:
        return self._uuid

    def _set_uuid(self, uuid: str) -> None:
        self._uuid = uuid


for method in PER_DATASET_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs) -> "DatasetPipeline[U]":
            return self.foreach_window(lambda ds: getattr(ds, method)(*args, **kwargs))

        impl.__name__ = delegate.__name__
        impl.__doc__ = """
Apply ``Dataset.{method}`` to each dataset/window in this pipeline.
""".format(
            method=method
        )
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(return_annotation="DatasetPipeline[U]"),
        )
        return impl

    setattr(DatasetPipeline, method, make_impl(method))

for method in HOLISTIC_PER_DATASET_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs) -> "DatasetPipeline[U]":
            return self.foreach_window(lambda ds: getattr(ds, method)(*args, **kwargs))

        impl.__name__ = delegate.__name__
        impl.__doc__ = """
Apply ``Dataset.{method}`` to each dataset/window in this pipeline.
""".format(
            method=method
        )
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(return_annotation="DatasetPipeline[U]"),
        )
        return impl

    def deprecation_warning(method: str):
        def impl(*a, **kw):
            raise DeprecationWarning(
                "`{}` has been renamed to `{}_each_window`.".format(method, method)
            )

        return impl

    setattr(DatasetPipeline, method, deprecation_warning(method))
    setattr(DatasetPipeline, method + "_each_window", make_impl(method))

for method in PER_DATASET_OUTPUT_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs):
            uuid = None
            for i, ds in enumerate(self.iter_datasets()):
                if uuid is None:
                    uuid = self._get_uuid() or ds._get_uuid()
                ds._set_uuid(f"{uuid}_{i:06}")
                getattr(ds, method)(*args, **kwargs)

        impl.__name__ = delegate.__name__
        impl.__doc__ = """
Call ``Dataset.{method}`` on each output dataset of this pipeline.
""".format(
            method=method
        )
        setattr(impl, "__signature__", inspect.signature(delegate))
        return impl

    setattr(DatasetPipeline, method, make_impl(method))

for method in OUTPUT_ITER_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs):
            return delegate(self, *args, **kwargs)

        impl.__name__ = delegate.__name__
        impl.__doc__ = """
Call ``Dataset.{method}`` over the stream of output batches from the pipeline.
""".format(
            method=method
        )
        setattr(impl, "__signature__", inspect.signature(delegate))
        return impl

    setattr(DatasetPipeline, method, make_impl(method))
