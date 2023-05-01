import itertools
import logging
import sys
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Union,
)
import warnings

import numpy as np

import ray
from ray.air.util.data_batch_conversion import BlockFormat
from ray.data._internal.block_batching import batch_block_refs
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.pipeline_executor import (
    PipelineExecutor,
    PipelineSplitExecutorCoordinator,
)
from ray.data._internal.iterator.pipelined_iterator import (
    PipelinedDataIterator,
)
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetPipelineStats, DatastreamStats
from ray.data.block import (
    UserDefinedFunction,
    Block,
    DataBatch,
    _apply_strict_mode_batch_format,
)
from ray.data.context import DataContext
from ray.data.datastream import Datastream
from ray.data.iterator import DataIterator
from ray.data.datasource import Datasource
from ray.data.datasource.file_based_datasource import (
    BlockWritePathProvider,
    DefaultBlockWritePathProvider,
)
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

if TYPE_CHECKING:
    import pandas
    import pyarrow
    import tensorflow as tf
    import torch
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType


logger = logging.getLogger(__name__)


@PublicAPI
class DatasetPipeline:
    """Implements a pipeline of Datastreams.

    DatasetPipelines implement pipelined execution. This allows for the
    overlapped execution of data input (e.g., reading files), computation
    (e.g. feature preprocessing), and output (e.g., distributed ML training).

    A DatasetPipeline can be created by either repeating a Datastream
    (``ds.repeat(times=None)``), by turning a single Datastream into a pipeline
    (``ds.window(blocks_per_window=10)``), or defined explicitly using
    ``DatasetPipeline.from_iterable()``.

    DatasetPipeline supports the all the per-record transforms of Datastreams
    (e.g., map, flat_map, filter), holistic transforms (e.g., repartition),
    and output methods (e.g., iter_rows, to_tf, to_torch, write_datasource).
    """

    def __init__(
        self,
        base_iterable: Iterable[Callable[[], Datastream]],
        stages: List[Callable[[Datastream], Datastream]] = None,
        length: Optional[int] = None,
        progress_bars: bool = DataContext.get_current().enable_progress_bars,
        _executed: List[bool] = None,
    ):
        """Construct a DatasetPipeline (internal API).

        The constructor is not part of the DatasetPipeline API. Use the
        ``Datastream.repeat()``, ``Datastream.window()``, or
        ``DatasetPipeline.from_iterable()`` methods to construct a pipeline.
        """
        self._base_iterable = base_iterable
        self._stages = stages or []
        self._optimized_stages = None
        self._length = length
        self._progress_bars = progress_bars
        self._uuid = None  # For testing only.
        # Whether the pipeline execution has started.
        # This variable is shared across all pipelines descending from this.
        self._executed = _executed or [False]
        self._first_datastream: Optional[Datastream] = None
        self._remaining_datastreams_iter: Optional[
            Iterator[Callable[[], Datastream]]
        ] = None
        self._schema = None
        self._stats = DatasetPipelineStats()

    def iterator(self) -> DataIterator:
        """Return a :class:`~ray.data.DataIterator` that
        can be used to repeatedly iterate over the datastream.

        Note that each pass iterates over the entire original Datastream, even if
        the datastream was windowed with ``.window()``.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(5).window(bytes_per_window=1).repeat()
            >>> ds
            DatasetPipeline(num_windows=inf, num_stages=2)
            >>> for batch in ds.iterator().iter_batches(batch_size=2):
            ...     print(batch) # doctest: +SKIP

        It is recommended to use ``DataIterator`` methods over directly
        calling methods such as ``iter_batches()``.
        """
        return PipelinedDataIterator(self)

    def iter_rows(self, *, prefetch_blocks: int = 0) -> Iterator[Dict[str, Any]]:
        """Return a local row iterator over the data in the pipeline.

        Examples:
            >>> import ray
            >>> for i in ray.data.range(1000000).repeat(5).iter_rows(): # doctest: +SKIP
            ...     print(i) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.

        Returns:
            A local iterator over the records in the pipeline.
        """

        def gen_rows() -> Iterator[Dict[str, Any]]:
            time_start = time.perf_counter()

            for ds in self.iter_datasets():
                wait_start = time.perf_counter()
                for row in ds.iter_rows(prefetch_blocks=prefetch_blocks):
                    self._stats.iter_wait_s.add(time.perf_counter() - wait_start)
                    with self._stats.iter_user_s.timer():
                        yield row
                    wait_start = time.perf_counter()

            self._stats.iter_total_s.add(time.perf_counter() - time_start)

        return gen_rows()

    def iter_batches(
        self,
        *,
        prefetch_batches: int = 1,
        # Deprecated.
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ) -> Iterator[DataBatch]:
        """Return a local batched iterator over the data in the pipeline.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000000).repeat(5) # doctest: +SKIP
            >>> for pandas_df in ds.iter_batches(): # doctest: +SKIP
            ...     print(pandas_df) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over record batches.
        """
        batch_format = _apply_strict_mode_batch_format(batch_format)
        if batch_format == "native":
            warnings.warn(
                "The 'native' batch format has been renamed 'default'.",
                DeprecationWarning,
            )

        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        time_start = time.perf_counter()
        if self._first_datastream is not None:
            blocks_owned_by_consumer = (
                self._first_datastream._plan.execute()._owned_by_consumer
            )
        else:
            blocks_owned_by_consumer = self._peek()._plan.execute()._owned_by_consumer
        yield from batch_block_refs(
            self._iter_blocks(),
            stats=self._stats,
            prefetch_blocks=prefetch_blocks,
            clear_block_after_read=blocks_owned_by_consumer,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            collate_fn=_collate_fn,
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )
        self._stats.iter_total_s.add(time.perf_counter() - time_start)

    def _iter_blocks(self) -> Iterator[ObjectRef[Block]]:
        ds_wait_start = time.perf_counter()
        for ds in self.iter_datasets():
            self._stats.iter_ds_wait_s.add(time.perf_counter() - ds_wait_start)
            yield from ds._plan.execute().iter_blocks()
            ds_wait_start = time.perf_counter()

    def split(
        self, n: int, *, equal: bool = False, locality_hints: List[Any] = None
    ) -> List["DatasetPipeline"]:
        """Split the pipeline into ``n`` disjoint pipeline shards.

        This returns a list of sub-pipelines that can be passed to Ray tasks
        and actors and used to read the pipeline records in parallel.

        Examples:
            >>> import ray
            >>> pipe = ray.data.range(10).repeat(50) # doctest: +SKIP
            >>> workers = ... # doctest: +SKIP
            >>> # Split up a pipeline to process over `n` worker actors.
            >>> shards = pipe.split( # doctest: +SKIP
            ...     len(workers), locality_hints=workers)
            >>> for shard, worker in zip(shards, workers): # doctest: +SKIP
            ...     worker.consume.remote(shard) # doctest: +SKIP

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
            locality_hints: [Experimental] A list of Ray actor handles of size ``n``.
                The system will try to co-locate the blocks of the ith pipeline
                shard with the ith actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint pipeline splits.
        """
        return self._split(
            n,
            lambda ds, equal=equal: ds.split(
                n, equal=equal, locality_hints=locality_hints
            ),
        )

    def split_at_indices(self, indices: List[int]) -> List["DatasetPipeline"]:
        """Split the datastreams within the pipeline at the given indices
        (like np.split).

        This will split each datastream contained within this pipeline, thereby
        producing len(indices) + 1 pipelines with the first pipeline containing
        the [0, indices[0]) slice from each datastream, the second pipeline
        containing the [indices[0], indices[1]) slice from each datastream, and so
        on, with the final pipeline will containing the
        [indices[-1], self.count()) slice from each datastream.

        Examples:
            >>> import ray
            >>> p1, p2, p3 = ray.data.range( # doctest: +SKIP
            ...     8).repeat(2).split_at_indices([2, 5]) # doctest: +SKIP
            >>> p1.take() # doctest: +SKIP
            [0, 1, 0, 1]
            >>> p2.take() # doctest: +SKIP
            [2, 3, 4, 2, 3, 4]
            >>> p3.take() # doctest: +SKIP
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

    def _split(
        self, n: int, splitter: Callable[[Datastream], List["Datastream"]]
    ) -> List["DatasetPipeline"]:
        ctx = DataContext.get_current()
        scheduling_strategy = ctx.scheduling_strategy
        if not ray.util.client.ray.is_connected():
            # Pin the coordinator (and any child actors) to the local node to avoid
            # errors during node failures. If the local node dies, then the driver
            # will fate-share with the coordinator anyway.
            scheduling_strategy = NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(),
                soft=False,
            )

        coordinator = PipelineSplitExecutorCoordinator.options(
            scheduling_strategy=scheduling_strategy,
        ).remote(self, n, splitter, DataContext.get_current())
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
                        self.coordinator.next_datastream_if_ready.remote(
                            self.split_index
                        )
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
    ) -> "DatasetPipeline":
        """Change the windowing (blocks per datastream) of this pipeline.

        Changes the windowing of this pipeline to the specified size. For
        example, if the current pipeline has two blocks per datastream, and
        `.rewindow(blocks_per_window=4)` is requested, adjacent datastreams will
        be merged until each datastream is 4 blocks. If
        `.rewindow(blocks_per_window)` was requested the datastreams will be
        split into smaller windows.

        Args:
            blocks_per_window: The new target blocks per window.
            preserve_epoch: Whether to preserve epoch boundaries. If set to
                False, then windows can contain data from two adjacent epochs.
        """

        class WindowIterator:
            def __init__(self, original_iter):
                self._original_iter = original_iter
                self._buffer: Optional[Datastream] = None

            def __next__(self) -> Datastream:
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

        # The newly created DatasetPipeline will contain a PipelineExecutor (because
        # this will execute the pipeline so far to iter the datastreams). In order to
        # make this new DatasetPipeline serializable, we need to make sure the
        # PipelineExecutor has not been iterated. So this uses
        # _iter_datasets_without_peek() instead of iter_datasets().
        return DatasetPipeline(
            WindowIterable(self._iter_datasets_without_peek()),
            length=length,
        )

    def repeat(self, times: int = None) -> "DatasetPipeline":
        """Repeat this pipeline a given number or times, or indefinitely.

        This operation is only allowed for pipelines of a finite length. An
        error will be raised for pipelines of infinite length.

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

            def __next__(self) -> Callable[[], Datastream]:
                # Still going through the original pipeline.
                if self._original_iter:
                    try:
                        make_ds = next(self._original_iter)
                        self._results.append(make_ds)

                        def gen():
                            res = make_ds()
                            res._set_epoch(0)
                            return res

                        return gen
                    except StopIteration:
                        self._original_iter = None
                        # Calculate the cursor limit.
                        if times:
                            self._max_i = len(self._results) * (times - 1)
                        else:
                            self._max_i = float("inf")
                # Going through a repeat of the pipeline.
                if self._i < self._max_i:
                    make_ds = self._results[self._i % len(self._results)]
                    epoch = 1 + self._i // len(self._results)

                    def gen():
                        res = make_ds()
                        res._set_epoch(epoch)
                        return res

                    self._i += 1
                    return gen
                else:
                    raise StopIteration

        class RepeatIterable:
            def __init__(self, original_iterable):
                self._original_iterable = original_iterable

            def __iter__(self):
                return RepeatIterator(iter(self._original_iterable))

        if not times:
            length = float("inf")
        elif times and self._length:
            length = times * self._length
        else:
            length = None

        return DatasetPipeline(
            RepeatIterable(self._base_iterable),
            stages=self._stages.copy(),
            length=length,
        )

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the schema of the datastream pipeline.

        For datastreams of Arrow records, this will return the Arrow schema.
        For datastream of Python objects, this returns their Python type.

        Note: This is intended to be a method for peeking schema before
        the execution of DatasetPipeline. If execution has already started,
        it will simply return the cached schema from the previous call.

        Time complexity: O(1)

        Args:
            fetch_if_missing: If True, synchronously fetch the schema if it's
                not known. Default is False, where None is returned if the
                schema is not known.

        Returns:
            The Python type or Arrow schema of the records, or None if the
            schema is not known.
        """
        if not self._executed[0]:
            self._schema = self._peek().schema(fetch_if_missing)
        return self._schema

    def dataset_format(self) -> BlockFormat:
        """The format of the datastream pipeline's underlying data blocks. Possible
        values are: "arrow", "pandas" and "simple".

        This may block; if the schema is unknown, this will synchronously fetch
        the schema for the first block.
        """
        # We need schema to properly validate, so synchronously
        # fetch it if necessary.
        schema = self.schema(fetch_if_missing=True)
        if schema is None:
            raise ValueError(
                "Datastream is empty or cleared, can't determine the format of "
                "the datastream."
            )

        try:
            import pyarrow as pa

            if isinstance(schema, pa.Schema):
                return BlockFormat.ARROW
        except ModuleNotFoundError:
            pass
        from ray.data._internal.pandas_block import PandasBlockSchema

        if isinstance(schema, PandasBlockSchema):
            return BlockFormat.PANDAS
        return BlockFormat.SIMPLE

    def count(self) -> int:
        """Count the number of records in the datastream pipeline.

        This blocks until the entire pipeline is fully executed.

        Time complexity: O(datastream size / parallelism)

        Returns:
            The number of records in the datastream pipeline.
        """
        if self._length == float("inf"):
            raise ValueError("Cannot count a pipeline of infinite length.")

        def batch_len(batch):
            key0 = list(batch.keys())[0]
            return len(batch[key0])

        pipe = self.map_batches(lambda batch: {"len": np.array([batch_len(batch)])})
        total = 0
        for elem in pipe.iter_rows():
            total += elem["len"]
        return total

    def sum(self) -> int:
        """Sum the records in the datastream pipeline.

        This blocks until the entire pipeline is fully executed.

        Time complexity: O(datastream size / parallelism)

        Returns:
            The sum of the records in the datastream pipeline.
        """
        if self._length == float("inf"):
            raise ValueError("Cannot sum a pipeline of infinite length.")

        pipe = self.map_batches(
            lambda batch: {"sum": np.array([batch.sum()[0]])}, batch_format="pandas"
        )
        total = 0
        for elem in pipe.iter_rows():
            total += elem["sum"]
        return total

    def show_windows(self, limit_per_datastream: int = 10) -> None:
        """Print up to the given number of records from each window/datastream.

        This is helpful as a debugging tool for understanding the structure of
        datastream pipelines.

        Args:
            limit_per_datastream: Rows to print per window/datastream.
        """
        epoch = None
        for i, ds in enumerate(self.iter_datasets()):
            if ds._get_epoch() != epoch:
                epoch = ds._get_epoch()
                print("------ Epoch {} ------".format(epoch))
            print("=== Window {} ===".format(i))
            ds.show(limit_per_datastream)

    def iter_epochs(self, max_epoch: int = -1) -> Iterator["DatasetPipeline"]:
        """Split this pipeline up by epoch.

        This allows reading of data per-epoch for repeated Datastreams, which is
        useful for ML training. For example, ``ray.data.range(10).repeat(50)``
        generates a pipeline with 500 rows total split across 50 epochs. This
        method allows iterating over the data individually per epoch
        (repetition) of the original data.

        Args:
            max_epoch: If greater than zero, stop after the given number of epochs.

        Examples:
            >>> import ray
            >>> epochs = ray.data.range(10).repeat(50).iter_epochs()  # doctest: +SKIP
            >>> for i, epoch in enumerate(epochs): # doctest: +SKIP
            ...     print("Epoch", i) # doctest: +SKIP
            ...     for row in epoch.iter_rows(): # doctest: +SKIP
            ...         print(row) # doctest: +SKIP

        Returns:
            Iterator over epoch objects, where each epoch is a DatasetPipeline
            containing data from that epoch only.
        """

        class Peekable:
            def __init__(self, base_iter: Iterator[Datastream]):
                self._iter = base_iter
                self._buffer = None

            def _fill_buffer_if_possible(self):
                if self._buffer is None:
                    try:
                        self._buffer = next(self._iter)
                        assert self._buffer is not None
                    except StopIteration:
                        pass

            def peek(self) -> Datastream:
                self._fill_buffer_if_possible()
                if self._buffer is None:
                    raise StopIteration
                return self._buffer

            def __next__(self) -> Datastream:
                self._fill_buffer_if_possible()
                if self._buffer is None:
                    raise StopIteration
                item = self._buffer
                self._buffer = None
                return item

        class SingleEpochIterator:
            def __init__(self, peekable_iter: Iterator[Datastream], epoch: int):
                self._iter = peekable_iter
                self._epoch = epoch

            def __next__(self) -> Datastream:
                if self._iter.peek()._get_epoch() > self._epoch:
                    raise StopIteration
                ds = next(self._iter)
                return lambda: ds

            def __iter__(self):
                return self

        class EpochDelimitedIterator:
            def __init__(self, pipe, max_epoch):
                self._iter = Peekable(pipe.iter_datasets())
                self._cur_epoch = None
                self._max_epoch = max_epoch

            def __next__(self) -> "DatasetPipeline":
                if self._cur_epoch is None:
                    self._cur_epoch = self._iter.peek()._get_epoch()
                else:
                    self._cur_epoch += 1
                if self._max_epoch > 0 and self._cur_epoch >= self._max_epoch:
                    raise StopIteration
                warned = False
                while self._iter.peek()._get_epoch() < self._cur_epoch:
                    if not warned:
                        warned = True
                        logger.warn(
                            "Data from epoch {} was not fully read, "
                            "skipping to next epoch.".format(self._cur_epoch - 1)
                        )
                    next(self._iter)
                epoch_pipe = DatasetPipeline.from_iterable(
                    SingleEpochIterator(self._iter, epoch=self._cur_epoch)
                )
                return epoch_pipe

            def __iter__(self):
                return self

        return EpochDelimitedIterator(self, max_epoch)

    def map(
        self,
        fn: UserDefinedFunction[Dict[str, Any], Dict[str, Any]],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.map <ray.data.Datastream.map>` to each datastream/window
        in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.map(fn, compute=compute, **ray_remote_args)
        )

    def map_batches(
        self,
        fn: UserDefinedFunction[DataBatch, DataBatch],
        *,
        batch_size: Optional[Union[int, Literal["default"]]] = "default",
        compute: Optional[Union[str, ComputeStrategy]] = None,
        batch_format: Optional[str] = "default",
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.map_batches <ray.data.Datastream.map_batches>` to each
        datastream/window in this pipeline."""

        batch_format = _apply_strict_mode_batch_format(batch_format)
        return self.foreach_window(
            lambda ds: ds.map_batches(
                fn,
                batch_size=batch_size,
                compute=compute,
                batch_format=batch_format,
                fn_args=fn_args,
                fn_kwargs=fn_kwargs,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
                **ray_remote_args,
            )
        )

    def flat_map(
        self,
        fn: UserDefinedFunction[Dict[str, Any], List[Dict[str, Any]]],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.flat_map <ray.data.Datastream.flat_map>` to each
        datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.flat_map(fn, compute=compute, **ray_remote_args)
        )

    def filter(
        self,
        fn: UserDefinedFunction[Dict[str, Any], bool],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.filter <ray.data.Datastream.filter>` to each
        datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.filter(fn, compute=compute, **ray_remote_args)
        )

    def add_column(
        self,
        col: str,
        fn: Callable[["pandas.DataFrame"], "pandas.Series"],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.add_column <ray.data.Datastream.add_column>` to each
        datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.add_column(col, fn, compute=compute, **ray_remote_args)
        )

    def drop_columns(
        self,
        cols: List[str],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.drop_columns <ray.data.Datastream.drop_columns>` to
        each datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.drop_columns(cols, compute=compute, **ray_remote_args)
        )

    def select_columns(
        self,
        cols: List[str],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.select_columns <ray.data.Datastream.select_columns>` to
        each datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.select_columns(cols, compute=compute, **ray_remote_args)
        )

    def repartition_each_window(
        self, num_blocks: int, *, shuffle: bool = False
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.repartition <ray.data.Datastream.repartition>` to each
        datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.repartition(num_blocks, shuffle=shuffle)
        )

    def random_shuffle_each_window(
        self,
        *,
        seed: Optional[int] = None,
        num_blocks: Optional[int] = None,
        **ray_remote_args,
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.random_shuffle <ray.data.Datastream.random_shuffle>` to
        each datastream/window in this pipeline."""
        return self.foreach_window(
            lambda ds: ds.random_shuffle(
                seed=seed, num_blocks=num_blocks, **ray_remote_args
            )
        )

    def sort_each_window(
        self, key: Optional[str] = None, descending: bool = False
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.sort <ray.data.Datastream.sort>` to each datastream/window
        in this pipeline."""
        return self.foreach_window(lambda ds: ds.sort(key, descending))

    def randomize_block_order_each_window(
        self, *, seed: Optional[int] = None
    ) -> "DatasetPipeline":
        """Apply :py:meth:`Datastream.randomize_block_order
        <ray.data.Datastream.randomize_block_order>` to each datastream/window in this
        pipeline."""
        return self.foreach_window(lambda ds: ds.randomize_block_order(seed=seed))

    def write_json(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        pandas_json_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        ray_remote_args: Dict[str, Any] = None,
        **pandas_json_args,
    ) -> None:
        """Call :py:meth:`Datastream.write_json <ray.data.Datastream.write_json>` on each
        output datastream of this pipeline."""
        self._write_each_datastream(
            lambda ds: ds.write_json(
                path,
                filesystem=filesystem,
                try_create_dir=try_create_dir,
                arrow_open_stream_args=arrow_open_stream_args,
                block_path_provider=block_path_provider,
                pandas_json_args_fn=pandas_json_args_fn,
                ray_remote_args=ray_remote_args,
                **pandas_json_args,
            )
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
        ray_remote_args: Dict[str, Any] = None,
        **arrow_csv_args,
    ) -> None:
        """Call :py:meth:`Datastream.write_csv <ray.data.Datastream.write_csv>` on each
        output datastream of this pipeline."""
        self._write_each_datastream(
            lambda ds: ds.write_csv(
                path,
                filesystem=filesystem,
                try_create_dir=try_create_dir,
                arrow_open_stream_args=arrow_open_stream_args,
                block_path_provider=block_path_provider,
                arrow_csv_args_fn=arrow_csv_args_fn,
                ray_remote_args=ray_remote_args,
                **arrow_csv_args,
            )
        )

    def write_parquet(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        ray_remote_args: Dict[str, Any] = None,
        **arrow_parquet_args,
    ) -> None:
        """Call :py:meth:`Datastream.write_parquet <ray.data.Datastream.write_parquet>` on
        each output datastream of this pipeline."""
        self._write_each_datastream(
            lambda ds: ds.write_parquet(
                path,
                filesystem=filesystem,
                try_create_dir=try_create_dir,
                arrow_open_stream_args=arrow_open_stream_args,
                block_path_provider=block_path_provider,
                arrow_parquet_args_fn=arrow_parquet_args_fn,
                ray_remote_args=ray_remote_args,
                **arrow_parquet_args,
            )
        )

    def write_tfrecords(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Call :py:meth:`Datastream.write_tfrecords <ray.data.Datastream.write_tfrecords>` on
        each output datastream of this pipeline."""
        self._write_each_datastream(
            lambda ds: ds.write_tfrecords(
                path,
                filesystem=filesystem,
                try_create_dir=try_create_dir,
                arrow_open_stream_args=arrow_open_stream_args,
                block_path_provider=block_path_provider,
                ray_remote_args=ray_remote_args,
            )
        )

    def write_datasource(
        self,
        datasource: Datasource,
        *,
        ray_remote_args: Dict[str, Any] = None,
        **write_args,
    ) -> None:
        """Call :py:meth:`Datastream.write_datasource <ray.data.Datastream.write_datasource>`
        on each output datastream of this pipeline."""
        self._write_each_datastream(
            lambda ds: ds.write_datasource(
                datasource,
                ray_remote_args=ray_remote_args,
                **write_args,
            )
        )

    def take(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Call :py:meth:`Datastream.take <ray.data.Datastream.take>` over the stream of
        output batches from the pipeline"""
        return Datastream.take(self, limit)

    def take_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Call :py:meth:`Datastream.take_all <ray.data.Datastream.take_all>` over the stream
        of output batches from the pipeline"""
        return Datastream.take_all(self, limit)

    def take_batch(
        self, batch_size: int = 20, *, batch_format: Optional[str] = "default"
    ) -> DataBatch:
        """Call :py:meth:`Datastream.take_batch <ray.data.Datastream.take_batch>`
        over the stream of output batches from the pipeline"""
        return Datastream.take_batch(self, batch_size, batch_format=batch_format)

    def show(self, limit: int = 20) -> None:
        """Call :py:meth:`Datastream.show <ray.data.Datastream.show>` over the stream of
        output batches from the pipeline"""
        return Datastream.show(self, limit)

    def iter_tf_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[Union["tf.Tensor", Dict[str, "tf.Tensor"]]]:
        """Call
        :py:meth:`Datastream.iter_tf_batches <ray.data.Datastream.iter_tf_batches>`
        over the stream of output batches from the pipeline."""
        batch_format = _apply_strict_mode_batch_format(batch_format)
        return DataIterator.iter_tf_batches(
            self,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def iter_torch_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        collate_fn: Optional[
            Callable[[Union[np.ndarray, Dict[str, np.ndarray]]], Any]
        ] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator["TorchTensorBatchType"]:
        """Call
        :py:meth:`Datastream.iter_torch_batches
        <ray.data.Datastream.iter_torch_batches>` over the stream of output batches
        from the pipeline."""
        return DataIterator.iter_torch_batches(
            self,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            dtypes=dtypes,
            device=device,
            collate_fn=collate_fn,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def to_tf(
        self,
        feature_columns: Union[str, List[str]],
        label_columns: Union[str, List[str]],
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> "tf.data.Dataset":
        """Call :py:meth:`Datastream.to_tf <ray.data.Datastream.to_tf>` over the stream of
        output batches from the pipeline"""
        return DataIterator.to_tf(
            self,
            feature_columns=feature_columns,
            label_columns=label_columns,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

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
        unsqueeze_feature_tensors: bool = True,
    ) -> "torch.utils.data.IterableDataset":
        """Call :py:meth:`Datastream.to_torch <ray.data.Datastream.to_torch>` over the stream
        of output batches from the pipeline"""
        return DataIterator.to_torch(
            self,
            label_column=label_column,
            feature_columns=feature_columns,
            label_column_dtype=label_column_dtype,
            feature_column_dtypes=feature_column_dtypes,
            batch_size=batch_size,
            prefetch_blocks=prefetch_blocks,
            drop_last=drop_last,
            unsqueeze_label_tensor=unsqueeze_label_tensor,
            unsqueeze_feature_tensors=unsqueeze_feature_tensors,
        )

    def _iter_datasets_without_peek(self):
        """This is similar to iter_datasets(), but without peeking PipelineExecutor."""
        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        self._executed[0] = True
        if self._first_datastream:
            raise RuntimeError("The pipeline has been peeked.")
        self._optimize_stages()
        return PipelineExecutor(self)

    @DeveloperAPI
    def iter_datasets(self) -> Iterator[Datastream]:
        """Iterate over the output datastreams of this pipeline.

        Returns:
            Iterator over the datastreams outputted from this pipeline.
        """
        if self._executed[0]:
            raise RuntimeError("Pipeline cannot be read multiple times.")
        self._executed[0] = True

        self._optimize_stages()

        # If the first datastream has already been executed (via a peek operation), then
        # we don't re-execute the first datastream when iterating through the pipeline.
        # We re-use the saved _first_datastream and _remaining_datastream_iter.
        if self._first_datastream is not None:

            class _IterableWrapper(Iterable):
                """Wrapper that takes an iterator and converts it to an
                iterable."""

                def __init__(self, base_iterator):
                    self.base_iterator = base_iterator

                def __iter__(self):
                    return self.base_iterator

            # Update the base iterable to skip the first datastream.
            # It is ok to update the base iterable here since
            # the pipeline can never be executed again.
            self._base_iterable = _IterableWrapper(self._remaining_datastreams_iter)

            iter = itertools.chain([self._first_datastream], PipelineExecutor(self))
            self._first_datastream = None
            self._remaining_datastreams_iter = None
            return iter
        else:
            return PipelineExecutor(self)

    @DeveloperAPI
    def foreach_window(
        self, fn: Callable[[Datastream], Datastream]
    ) -> "DatasetPipeline":
        """Apply a transform to each datastream/window in this pipeline.

        Args:
            fn: The function to transform each datastream with.

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
        iterable: Iterable[Callable[[], Datastream]],
    ) -> "DatasetPipeline":
        """Create a pipeline from an sequence of Datastream producing functions.

        Args:
            iterable: A finite or infinite-length sequence of functions that
                each produce a Datastream when called.
        """
        if hasattr(iterable, "__len__"):
            length = len(iterable)
        else:
            length = None
        return DatasetPipeline(iterable, False, length=length)

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

    def _optimize_stages(self):
        """Optimize this pipeline, fusing stages together as possible."""
        context = DataContext.get_current()

        if not context.optimize_fuse_stages:
            self._optimized_stages = self._stages
            return

        # This dummy datastream will be used to get a set of optimized stages.
        dummy_ds = Datastream(
            ExecutionPlan(
                BlockList([], [], owned_by_consumer=True),
                DatastreamStats(stages={}, parent=None),
                run_by_consumer=True,
            ),
            0,
            True,
        )
        # Apply all pipeline operations to the dummy datastream.
        for stage in self._stages:
            dummy_ds = stage(dummy_ds)
        # Get the optimized stages.
        _, _, stages = dummy_ds._plan._optimize()
        # Apply these optimized stages to the datastreams underlying the pipeline.
        # These optimized stages will be executed by the PipelineExecutor.
        optimized_stages = []
        for stage in stages:

            def add_stage(ds, stage):
                ds._plan._run_by_consumer = True
                return ds._plan.with_stage(stage)

            optimized_stages.append(
                lambda ds, stage=stage: Datastream(
                    add_stage(ds, stage), ds._epoch, True
                )
            )
        self._optimized_stages = optimized_stages

    def _peek(self) -> Datastream:
        if self._first_datastream is None:
            datastream_iter = iter(self._base_iterable)
            first_datastream_gen = next(datastream_iter)
            peek_pipe = DatasetPipeline(
                base_iterable=[first_datastream_gen],
                stages=self._stages.copy(),
                length=1,
                progress_bars=True,
            )
            # Cache the executed _first_datastream.
            self._first_datastream = next(peek_pipe.iter_datasets())
            self._remaining_datastreams_iter = datastream_iter

            # Store the stats from the peek pipeline.
            self._stats.add_pipeline_stats(peek_pipe._stats)

        return self._first_datastream

    def _write_each_datastream(self, write_fn: Callable[[Datastream], None]) -> None:
        """Write output for each datastream.

        This is utility method used for write_json,
        write_csv, write_parquet, write_datasource, etc.
        """
        uuid = None
        for i, ds in enumerate(self.iter_datasets()):
            if uuid is None:
                uuid = self._get_uuid() or ds._get_uuid()
            ds._set_uuid(f"{uuid}_{i:06}")
            write_fn(ds)

    def _synchronize_progress_bar(self):
        pass
