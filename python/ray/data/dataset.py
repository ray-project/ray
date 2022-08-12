import collections
import itertools
import logging
import os
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from uuid import uuid4

import numpy as np

import ray
import ray.cloudpickle as pickle
from ray._private.usage import usage_lib
from ray.data._internal.block_batching import BatchType, batch_blocks
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import (
    ActorPoolStrategy,
    CallableClass,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.equalize import _equalize
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _estimate_available_parallelism
from ray.data._internal.plan import (
    ExecutionPlan,
    OneToOneStage,
)
from ray.data._internal.stage_impl import (
    RandomizeBlocksStage,
    RepartitionStage,
    RandomShuffleStage,
    ZipStage,
    SortStage,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.split import _split_at_index, _split_at_indices, _get_num_rows
from ray.data._internal.stats import DatasetStats
from ray.data._internal.table_block import VALUE_COL_NAME
from ray.data.aggregate import AggregateFn, Max, Mean, Min, Std, Sum
from ray.data.block import (
    VALID_BATCH_FORMATS,
    BatchUDF,
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockPartition,
    BlockPartitionMetadata,
    KeyFn,
    RowUDF,
    T,
    U,
    _validate_key_fn,
)
from ray.data.context import (
    DatasetContext,
    WARN_PREFIX,
    OK_PREFIX,
    ESTIMATED_SAFE_MEMORY_FRACTION,
)
from ray.data.datasource import (
    BlockWritePathProvider,
    CSVDatasource,
    Datasource,
    DefaultBlockWritePathProvider,
    JSONDatasource,
    NumpyDatasource,
    ParquetDatasource,
    ReadTask,
    WriteResult,
)
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
    _wrap_and_register_arrow_serialization_workaround,
)
from ray.data.random_access_dataset import RandomAccessDataset
from ray.data.row import TableRow
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import dask
    import mars
    import modin
    import pandas
    import pyarrow
    import pyspark
    import tensorflow as tf
    import torch
    import torch.utils.data

    from ray.data.dataset_pipeline import DatasetPipeline
    from ray.data.grouped_dataset import GroupedDataset


logger = logging.getLogger(__name__)

TensorflowFeatureTypeSpec = Union[
    "tf.TypeSpec", List["tf.TypeSpec"], Dict[str, "tf.TypeSpec"]
]

TorchTensorBatchType = Union["torch.Tensor", Dict[str, "torch.Tensor"]]
TensorFlowTensorBatchType = Union["tf.Tensor", Dict[str, "tf.Tensor"]]


@PublicAPI
class Dataset(Generic[T]):
    """A Dataset is a distributed data collection for data loading and processing.

    Datasets are implemented as a list of ``ObjectRef[Block]``, where each block
    holds an ordered collection of items, representing a shard of the overall
    data collection. The block can be either a ``pyarrow.Table``, or Python list.
    The block also determines the unit of parallelism.

    Datasets can be created in multiple ways: from synthetic data via ``range_*()``
    APIs, from existing memory data via ``from_*()`` APIs, or from external storage
    systems such as local disk, S3, HDFS etc. via the ``read_*()`` APIs. The
    (potentially processed) Dataset can be saved back to external storage systems via
    the ``write_*()`` APIs.

    Examples:
        >>> import ray
        >>> # Create dataset from synthetic data.
        >>> ds = ray.data.range(1000) # doctest: +SKIP
        >>> # Create dataset from in-memory data.
        >>> ds = ray.data.from_items( # doctest: +SKIP
        ...     [{"col1": i, "col2": i * 2} for i in range(1000)])
        >>> # Create dataset from external storage system.
        >>> ds = ray.data.read_parquet("s3://bucket/path") # doctest: +SKIP
        >>> # Save dataset back to external storage system.
        >>> ds.write_csv("s3//bucket/output") # doctest: +SKIP

    Datasets has two kinds of operations: tranformation, which takes in Datasets and
    outputs a new Dataset (e.g. :py:meth:`.map_batches()`); and consumption, which
    produces values (not Dataset) as output (e.g. :py:meth:`.iter_batches()`).

    Datasets supports parallel processing at scale: transformations such as
    :py:meth:`.map_batches()`, aggregations such as
    :py:meth:`.min()`/:py:meth:`.max()`/:py:meth:`.mean()`, grouping via
    :py:meth:`.groupby()`, shuffling operations such as :py:meth:`.sort()`,
    :py:meth:`.random_shuffle()`, and :py:meth:`.repartition()`.

    Examples:
        >>> import ray
        >>> ds = ray.data.range(1000) # doctest: +SKIP
        >>> # Transform in parallel with map_batches().
        >>> ds.map_batches(lambda batch: [v * 2 for v in batch]) # doctest: +SKIP
        >>> # Compute max.
        >>> ds.max() # doctest: +SKIP
        >>> # Group the data.
        >>> ds.groupby(lambda x: x % 3).count() # doctest: +SKIP
        >>> # Shuffle this dataset randomly.
        >>> ds.random_shuffle() # doctest: +SKIP
        >>> # Sort it back in order.
        >>> ds.sort() # doctest: +SKIP

    Since Datasets are just lists of Ray object refs, they can be passed
    between Ray tasks and actors without incurring a copy. Datasets support
    conversion to/from several more featureful dataframe libraries
    (e.g., Spark, Dask, Modin, MARS), and are also compatible with distributed
    TensorFlow / PyTorch.
    """

    def __init__(
        self,
        plan: ExecutionPlan,
        epoch: int,
        lazy: bool,
        *,
        defer_execution: bool = False,
    ):
        """Construct a Dataset (internal API).

        The constructor is not part of the Dataset API. Use the ``ray.data.*``
        read methods to construct a dataset.
        """
        assert isinstance(plan, ExecutionPlan)
        usage_lib.record_library_usage("dataset")

        self._plan = plan
        self._uuid = uuid4().hex
        self._epoch = epoch
        self._lazy = lazy

        if not lazy and not defer_execution:
            self._plan.execute(allow_clear_input_blocks=False)

    @staticmethod
    def copy(dataset: "Dataset[T]") -> "Dataset[T]":
        return Dataset(dataset._plan, dataset._epoch, dataset._lazy)

    def map(
        self,
        fn: RowUDF[T, U],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Dataset[U]":
        """Apply the given function to each record of this dataset.

        This is a blocking operation. Note that mapping individual records
        can be quite slow. Consider using `.map_batches()` for performance.

        Examples:
            >>> import ray
            >>> # Transform python objects.
            >>> ds = ray.data.range(1000) # doctest: +SKIP
            >>> ds.map(lambda x: x * 2) # doctest: +SKIP
            >>> # Transform Arrow records.
            >>> ds = ray.data.from_items( # doctest: +SKIP
            ...     [{"value": i} for i in range(1000)])
            >>> ds.map(lambda record: {"v2": record["value"] * 2}) # doctest: +SKIP
            >>> # Define a callable class that persists state across
            >>> # function invocations for efficiency.
            >>> init_model = ... # doctest: +SKIP
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_model()
            ...    def __call__(self, batch):
            ...        return self.model(batch)
            >>> # Apply the transform in parallel on GPUs. Since
            >>> # compute=ActorPoolStrategy(2, 8) the transform will be applied on an
            >>> # autoscaling pool of 2-8 Ray actors, each allocated 1 GPU by Ray.
            >>> from ray.data._internal.compute import ActorPoolStrategy
            >>> ds.map(CachedModel, # doctest: +SKIP
            ...        compute=ActorPoolStrategy(2, 8),
            ...        num_gpus=1)

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling actor pool. If wanting to
                configure the min or max size of the autoscaling actor pool, you can
                provide an
                :class:`ActorPoolStrategy(min, max) <ray.data.ActorPoolStrategy>`
                instance. If using callable classes for fn, the actor compute strategy
                must be used.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}"
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )

        self._warn_slow()
        context = DatasetContext.get_current()

        def transform(block: Block, fn: RowUDF[T, U]) -> Iterable[Block]:
            DatasetContext._set_current(context)
            output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows():
                output_buffer.add(fn(row))
                if output_buffer.has_next():
                    yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        plan = self._plan.with_stage(
            OneToOneStage(
                "map",
                transform,
                compute,
                ray_remote_args,
                fn=fn,
            )
        )
        return Dataset(plan, self._epoch, self._lazy)

    def map_batches(
        self,
        fn: BatchUDF,
        *,
        batch_size: Optional[int] = 4096,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: str = "native",
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        **ray_remote_args,
    ) -> "Dataset[Any]":
        """Apply the given function to batches of records of this dataset.

        The format of the data batch provided to ``fn`` can be controlled via the
        ``batch_format`` argument, and the output of the UDF can be any batch type.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> # Transform python objects.
            >>> ds = ray.data.range(1000) # doctest: +SKIP
            >>> # Transform batches in parallel.
            >>> ds.map_batches(lambda batch: [v * 2 for v in batch]) # doctest: +SKIP
            >>> # Define a callable class that persists state across
            >>> # function invocations for efficiency.
            >>> init_model = ... # doctest: +SKIP
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_model()
            ...    def __call__(self, item):
            ...        return self.model(item)
            >>> # Apply the transform in parallel on GPUs. Since
            >>> # compute=ActorPoolStrategy(2, 8) the transform will be applied on an
            >>> # autoscaling pool of 2-8 Ray actors, each allocated 1 GPU by Ray.
            >>> from ray.data._internal.compute import ActorPoolStrategy
            >>> ds.map_batches( # doctest: +SKIP
            ...     CachedModel, # doctest: +SKIP
            ...     batch_size=256, # doctest: +SKIP
            ...     compute=ActorPoolStrategy(2, 8), # doctest: +SKIP
            ...     num_gpus=1) # doctest: +SKIP

            You can use ``map_batches`` to efficiently filter records.

            >>> import ray
            >>> ds = ray.data.range(10000)  # doctest: +SKIP
            >>> ds.count()  # doctest: +SKIP
            10000
            >>> ds = ds.map_batches(lambda batch: [x for x in batch if x % 2 == 0])  # doctest: +SKIP  # noqa: #501
            >>> ds.count()  # doctest: +SKIP
            5000

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record batch, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows.
                Defaults to 4096.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling actor pool. If wanting to
                configure the min or max size of the autoscaling actor pool, you can
                provide an
                :class:`ActorPoolStrategy(min, max) <ray.data.ActorPoolStrategy>`
                instance. If using callable classes for fn, the actor compute strategy
                must be used.
            batch_format: Specify "native" to use the native block format (promotes
                tables to Pandas and tensors to NumPy), "pandas" to select
                ``pandas.DataFrame``, "pyarrow" to select ``pyarrow.Table``, or "numpy"
                to select ``numpy.ndarray`` for tensor datasets and
                ``Dict[str, numpy.ndarray]`` for tabular datasets. Default is "native".
            fn_args: Positional arguments to pass to ``fn``, after the data batch. These
                arguments will be top-level arguments in the underlying Ray task that's
                submitted.
            fn_kwargs: Keyword arguments to pass to ``fn``. These arguments will be
                top-level arguments in the underlying Ray task that's submitted.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class and the actor
                compute strategy is being used. These arguments will be top-level
                arguments in the underlying Ray actor construction task that's
                submitted.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class and the actor
                compute strategy is being used. These arguments will be top-level
                arguments in the underlying Ray actor construction task that's
                submitted.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        import pandas as pd
        import pyarrow as pa

        if batch_size is not None and batch_size < 1:
            raise ValueError("Batch size cannot be negative or 0")

        if batch_format not in VALID_BATCH_FORMATS:
            raise ValueError(
                f"The batch format must be one of {VALID_BATCH_FORMATS}, got: "
                f"{batch_format}"
            )

        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}"
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )

        if fn_constructor_args is not None or fn_constructor_kwargs is not None:
            if compute is None or (
                compute != "actors" and not isinstance(compute, ActorPoolStrategy)
            ):
                raise ValueError(
                    "fn_constructor_args and fn_constructor_kwargs can only be "
                    "specified if using the actor pool compute strategy, but got: "
                    f"{compute}"
                )
            if not isinstance(fn, CallableClass):
                raise ValueError(
                    "fn_constructor_args and fn_constructor_kwargs can only be "
                    "specified if providing a CallableClass instance for fn, but got: "
                    f"{fn}"
                )

        context = DatasetContext.get_current()

        def transform(
            block: Block,
            batch_fn: BatchUDF,
            *fn_args,
            **fn_kwargs,
        ) -> Iterable[Block]:
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
                # Convert to batch format.
                view = BlockAccessor.for_block(view).to_batch_format(batch_format)

                applied = batch_fn(view, *fn_args, **fn_kwargs)
                if not (
                    isinstance(applied, list)
                    or isinstance(applied, pa.Table)
                    or isinstance(applied, np.ndarray)
                    or (
                        isinstance(applied, dict)
                        and all(isinstance(col, np.ndarray) for col in applied.values())
                    )
                    or isinstance(applied, pd.core.frame.DataFrame)
                ):
                    raise ValueError(
                        "The map batches UDF returned the value "
                        f"{applied} of type {type(applied)}, "
                        "which is not allowed. "
                        f"The return type must be one of: {BatchType}"
                    )
                output_buffer.add_batch(applied)
                if output_buffer.has_next():
                    yield output_buffer.next()

            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        plan = self._plan.with_stage(
            OneToOneStage(
                "map_batches",
                transform,
                compute,
                ray_remote_args,
                fn=fn,
                fn_args=fn_args,
                fn_kwargs=fn_kwargs,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
            )
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

        Examples:
            >>> import ray
            >>> ds = ray.data.range_table(100) # doctest: +SKIP
            >>> # Add a new column equal to value * 2.
            >>> ds = ds.add_column( # doctest: +SKIP
            ...     "new_col", lambda df: df["value"] * 2)
            >>> # Overwrite the existing "value" with zeros.
            >>> ds = ds.add_column("value", lambda df: 0) # doctest: +SKIP

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

    def drop_columns(
        self,
        cols: List[str],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset[U]":
        """Drop one or more columns from the dataset.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ds = ray.data.range_table(100) # doctest: +SKIP
            >>> # Add a new column equal to value * 2.
            >>> ds = ds.add_column( # doctest: +SKIP
            ...     "new_col", lambda df: df["value"] * 2)
            >>> # Drop the existing "value" column.
            >>> ds = ds.drop_columns(["value"]) # doctest: +SKIP


        Time complexity: O(dataset size / parallelism)

        Args:
            cols: Names of the columns to drop. If any name does not exist,
                an exception will be raised.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        return self.map_batches(
            lambda batch: batch.drop(columns=cols), compute=compute, **ray_remote_args
        )

    def flat_map(
        self,
        fn: RowUDF[T, U],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Dataset[U]":
        """Apply the given function to each record and then flatten results.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (the batch size can be altered in map_batches).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000) # doctest: +SKIP
            >>> ds.flat_map(lambda x: [x, x ** 2, x ** 3]) # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling actor pool. If wanting to
                configure the min or max size of the autoscaling actor pool, you can
                provide an
                :class:`ActorPoolStrategy(min, max) <ray.data.ActorPoolStrategy>`
                instance. If using callable classes for fn, the actor compute strategy
                must be used.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}"
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )

        self._warn_slow()
        context = DatasetContext.get_current()

        def transform(block: Block, fn: RowUDF[T, U]) -> Iterable[Block]:
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
            OneToOneStage("flat_map", transform, compute, ray_remote_args, fn=fn)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def filter(
        self,
        fn: RowUDF[T, U],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Dataset[T]":
        """Filter out records that do not satisfy the given predicate.

        This is a blocking operation. Consider using ``.map_batches()`` for
        better performance (you can implement filter by dropping records).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.filter(lambda x: x % 2 == 0) # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The predicate to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or "actors" to use an autoscaling actor pool. If wanting to
                configure the min or max size of the autoscaling actor pool, you can
                provide an
                :class:`ActorPoolStrategy(min, max) <ray.data.ActorPoolStrategy>`
                instance. If using callable classes for fn, the actor compute strategy
                must be used.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}"
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )

        self._warn_slow()
        context = DatasetContext.get_current()

        def transform(block: Block, fn: RowUDF[T, U]) -> Iterable[Block]:
            DatasetContext._set_current(context)
            block = BlockAccessor.for_block(block)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return [builder.build()]

        plan = self._plan.with_stage(
            OneToOneStage("filter", transform, compute, ray_remote_args, fn=fn)
        )
        return Dataset(plan, self._epoch, self._lazy)

    def repartition(self, num_blocks: int, *, shuffle: bool = False) -> "Dataset[T]":
        """Repartition the dataset into exactly this number of blocks.

        This is a blocking operation. After repartitioning, all blocks in the
        returned dataset will have approximately the same number of rows.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> # Set the number of output partitions to write to disk.
            >>> ds.repartition(10).write_parquet(...) # doctest: +SKIP

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

        plan = self._plan.with_stage(RepartitionStage(num_blocks, shuffle))
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
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> # Shuffle this dataset randomly.
            >>> ds.random_shuffle() # doctest: +SKIP
            >>> # Shuffle this dataset with a fixed random seed.
            >>> ds.random_shuffle(seed=12345) # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            seed: Fix the random seed to use, otherwise one will be chosen
                based on system randomness.
            num_blocks: The number of output blocks after the shuffle, or None
                to retain the number of blocks.

        Returns:
            The shuffled dataset.
        """

        plan = self._plan.with_stage(RandomShuffleStage(seed, num_blocks))
        return Dataset(plan, self._epoch, self._lazy)

    def randomize_block_order(
        self,
        *,
        seed: Optional[int] = None,
    ) -> "Dataset[T]":
        """Randomly shuffle the blocks of this dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> # Randomize the block order.
            >>> ds.randomize_block_order() # doctest: +SKIP
            >>> # Randomize the block order with a fixed random seed.
            >>> ds.randomize_block_order(seed=12345) # doctest: +SKIP

        Args:
            seed: Fix the random seed to use, otherwise one will be chosen
                based on system randomness.

        Returns:
            The block-shuffled dataset.
        """

        plan = self._plan.with_stage(RandomizeBlocksStage(seed))
        return Dataset(plan, self._epoch, self._lazy, defer_execution=True)

    def random_sample(
        self, fraction: float, *, seed: Optional[int] = None
    ) -> "Dataset[T]":
        """Randomly samples a fraction of the elements of this dataset.

        Note that the exact number of elements returned is not guaranteed,
        and that the number of elements being returned is roughly fraction * total_rows.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.random_sample(0.1) # doctest: +SKIP
            >>> ds.random_sample(0.2, seed=12345) # doctest: +SKIP

        Args:
            fraction: The fraction of elements to sample.
            seed: Seeds the python random pRNG generator.

        Returns:
            Returns a Dataset containing the sampled elements.
        """
        import random

        import pandas as pd
        import pyarrow as pa

        if self.num_blocks() == 0:
            raise ValueError("Cannot sample from an empty dataset.")

        if fraction < 0 or fraction > 1:
            raise ValueError("Fraction must be between 0 and 1.")

        if seed is not None:
            random.seed(seed)

        def process_batch(batch):
            if isinstance(batch, list):
                return [row for row in batch if random.random() <= fraction]
            if isinstance(batch, pa.Table):
                # Lets the item pass if weight generated for that item <= fraction
                return batch.filter(
                    pa.array(random.random() <= fraction for _ in range(len(batch)))
                )
            if isinstance(batch, pd.DataFrame):
                return batch.sample(frac=fraction)
            if isinstance(batch, np.ndarray):
                return np.array([row for row in batch if random.random() <= fraction])
            raise ValueError(f"Unsupported batch type: {type(batch)}")

        return self.map_batches(process_batch)

    def split(
        self, n: int, *, equal: bool = False, locality_hints: Optional[List[Any]] = None
    ) -> List["Dataset[T]"]:
        """Split the dataset into ``n`` disjoint pieces.

        This returns a list of sub-datasets that can be passed to Ray tasks
        and actors and used to read the dataset records in parallel.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> workers = ... # doctest: +SKIP
            >>> # Split up a dataset to process over `n` worker actors.
            >>> shards = ds.split(len(workers), locality_hints=workers) # doctest: +SKIP
            >>> for shard, worker in zip(shards, workers): # doctest: +SKIP
            ...     worker.consume.remote(shard) # doctest: +SKIP

        Time complexity: O(1)

        See also: ``Dataset.split_at_indices``, ``Dataset.split_proportionately``

        Args:
            n: Number of child datasets to return.
            equal: Whether to guarantee each split has an equal
                number of records. This may drop records if they cannot be
                divided equally among the splits.
            locality_hints: [Experimental] A list of Ray actor handles of size ``n``.
                The system will try to co-locate the blocks of the i-th dataset
                with the i-th actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint dataset splits.
        """
        if n <= 0:
            raise ValueError(f"The number of splits {n} is not positive.")

        # fallback to split_at_indices for equal split without locality hints.
        # simple benchmarks shows spilit_at_indices yields more stable performance.
        # https://github.com/ray-project/ray/pull/26641 for more context.
        if equal and locality_hints is None:
            count = self.count()
            split_index = count // n
            # we are creating n split_indices which will generate
            # n + 1 splits; the last split will at most contains (n - 1)
            # rows, which could be safely dropped.
            split_indices = [split_index * i for i in range(1, n + 1)]
            shards = self.split_at_indices(split_indices)
            return shards[:n]

        if locality_hints and len(locality_hints) != n:
            raise ValueError(
                f"The length of locality_hints {len(locality_hints)} "
                f"doesn't equal the number of splits {n}."
            )
            # TODO: this is unreachable code.
            if len(set(locality_hints)) != len(locality_hints):
                raise ValueError(
                    "locality_hints must not contain duplicate actor handles"
                )

        blocks = self._plan.execute()
        owned_by_consumer = blocks._owned_by_consumer
        stats = self._plan.stats()
        block_refs, metadata = zip(*blocks.get_blocks_with_metadata())

        if locality_hints is None:
            blocks = np.array_split(block_refs, n)
            meta = np.array_split(metadata, n)
            return [
                Dataset(
                    ExecutionPlan(
                        BlockList(
                            b.tolist(), m.tolist(), owned_by_consumer=owned_by_consumer
                        ),
                        stats,
                        run_by_consumer=owned_by_consumer,
                    ),
                    self._epoch,
                    self._lazy,
                )
                for b, m in zip(blocks, meta)
            ]

        metadata_mapping = {b: m for b, m in zip(block_refs, metadata)}

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
            actors_state = ray._private.state.actors()
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

        per_split_block_lists = [
            BlockList(
                allocation_per_actor[actor],
                [metadata_mapping[b] for b in allocation_per_actor[actor]],
                owned_by_consumer=owned_by_consumer,
            )
            for actor in locality_hints
        ]

        if equal:
            # equalize the splits
            per_split_block_lists = _equalize(per_split_block_lists, owned_by_consumer)

        return [
            Dataset(
                ExecutionPlan(
                    block_split,
                    stats,
                    run_by_consumer=owned_by_consumer,
                ),
                self._epoch,
                self._lazy,
            )
            for block_split in per_split_block_lists
        ]

    def split_at_indices(self, indices: List[int]) -> List["Dataset[T]"]:
        """Split the dataset at the given indices (like np.split).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10) # doctest: +SKIP
            >>> d1, d2, d3 = ds.split_at_indices([2, 5]) # doctest: +SKIP
            >>> d1.take() # doctest: +SKIP
            [0, 1]
            >>> d2.take() # doctest: +SKIP
            [2, 3, 4]
            >>> d3.take() # doctest: +SKIP
            [5, 6, 7, 8, 9]

        Time complexity: O(num splits)

        See also: ``Dataset.split``, ``Dataset.split_proportionately``

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
        start_time = time.perf_counter()
        block_list = self._plan.execute()
        blocks, metadata = _split_at_indices(block_list, indices)
        split_duration = time.perf_counter() - start_time
        parent_stats = self._plan.stats()
        splits = []
        for bs, ms in zip(blocks, metadata):
            stats = DatasetStats(stages={"split": ms}, parent=parent_stats)
            stats.time_total_s = split_duration
            splits.append(
                Dataset(
                    ExecutionPlan(
                        BlockList(
                            bs, ms, owned_by_consumer=block_list._owned_by_consumer
                        ),
                        stats,
                        run_by_consumer=block_list._owned_by_consumer,
                    ),
                    self._epoch,
                    self._lazy,
                )
            )
        return splits

    def split_proportionately(self, proportions: List[float]) -> List["Dataset[T]"]:
        """Split the dataset using proportions.

        A common use case for this would be splitting the dataset into train
        and test sets (equivalent to eg. scikit-learn's ``train_test_split``).
        See also ``Dataset.train_test_split`` for a higher level abstraction.

        The indices to split at will be calculated in such a way so that all splits
        always contains at least one element. If that is not possible,
        an exception will be raised.

        This is equivalent to caulculating the indices manually and calling
        ``Dataset.split_at_indices``.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10) # doctest: +SKIP
            >>> d1, d2, d3 = ds.split_proportionately([0.2, 0.5]) # doctest: +SKIP
            >>> d1.take() # doctest: +SKIP
            [0, 1]
            >>> d2.take() # doctest: +SKIP
            [2, 3, 4, 5, 6]
            >>> d3.take() # doctest: +SKIP
            [7, 8, 9]

        Time complexity: O(num splits)

        See also: ``Dataset.split``, ``Dataset.split_at_indices``,
        ``Dataset.train_test_split``

        Args:
            proportions: List of proportions to split the dataset according to.
                Must sum up to less than 1, and each proportion has to be bigger
                than 0.

        Returns:
            The dataset splits.
        """

        if len(proportions) < 1:
            raise ValueError("proportions must be at least of length 1")
        if sum(proportions) >= 1:
            raise ValueError("proportions must sum to less than 1")
        if any(p <= 0 for p in proportions):
            raise ValueError("proportions must be bigger than 0")

        dataset_length = self.count()
        cumulative_proportions = np.cumsum(proportions)
        split_indices = [
            int(dataset_length * proportion) for proportion in cumulative_proportions
        ]

        # Ensure each split has at least one element
        subtract = 0
        for i in range(len(split_indices) - 2, -1, -1):
            split_indices[i] -= subtract
            if split_indices[i] == split_indices[i + 1]:
                subtract += 1
                split_indices[i] -= 1
        if any(i <= 0 for i in split_indices):
            raise ValueError(
                "Couldn't create non-empty splits with the given proportions."
            )

        return self.split_at_indices(split_indices)

    def train_test_split(
        self,
        test_size: Union[int, float],
        *,
        shuffle: bool = False,
        seed: Optional[int] = None,
    ) -> Tuple["Dataset[T]", "Dataset[T]"]:
        """Split the dataset into train and test subsets.

        Example:
            .. code-block:: python

                import ray

                ds = ray.data.range(8)
                train, test = ds.train_test_split(test_size=0.25)
                print(train.take())  # [0, 1, 2, 3, 4, 5]
                print(test.take())  # [6, 7]

        Args:
            test_size: If float, should be between 0.0 and 1.0 and represent the
                proportion of the dataset to include in the test split. If int,
                represents the absolute number of test samples. The train split will
                always be the compliment of the test split.
            shuffle: Whether or not to globally shuffle the dataset before splitting.
                Defaults to False. This may be a very expensive operation with large
                datasets.
            seed: Fix the random seed to use for shuffle, otherwise one will be chosen
                based on system randomness. Ignored if ``shuffle=False``.

        Returns:
            Train and test subsets as two Datasets.
        """
        dataset = self

        if shuffle:
            dataset = dataset.random_shuffle(seed=seed)

        if not isinstance(test_size, (int, float)):
            raise TypeError(f"`test_size` must be int or float got {type(test_size)}.")
        if isinstance(test_size, float):
            if test_size <= 0 or test_size >= 1:
                raise ValueError(
                    "If `test_size` is a float, it must be bigger than 0 and smaller "
                    f"than 1. Got {test_size}."
                )
            return dataset.split_proportionately([1 - test_size])
        else:
            dataset_length = dataset.count()
            if test_size <= 0 or test_size >= dataset_length:
                raise ValueError(
                    "If `test_size` is an int, it must be bigger than 0 and smaller "
                    f"than the size of the dataset ({dataset_length}). "
                    f"Got {test_size}."
                )
            return dataset.split_at_indices([dataset_length - test_size])

    def union(self, *other: List["Dataset[T]"]) -> "Dataset[T]":
        """Combine this dataset with others of the same type.

        The order of the blocks in the datasets is preserved, as is the
        relative ordering between the datasets passed in the argument list.

        NOTE: Unioned datasets are not lineage-serializable, i.e. they can not be used
        as a tunable hyperparameter in Ray Tune.

        Args:
            other: List of datasets to combine with this one. The datasets
                must have the same schema as this dataset, otherwise the
                behavior is undefined.

        Returns:
            A new dataset holding the union of their data.
        """

        start_time = time.perf_counter()

        owned_by_consumer = self._plan.execute()._owned_by_consumer
        datasets = [self] + list(other)
        bls = []
        has_nonlazy = False
        for ds in datasets:
            bl = ds._plan.execute()
            if not isinstance(bl, LazyBlockList):
                has_nonlazy = True
            bls.append(bl)
        if has_nonlazy:
            blocks = []
            metadata = []
            for bl in bls:
                if isinstance(bl, LazyBlockList):
                    bs, ms = bl._get_blocks_with_metadata()
                else:
                    bs, ms = bl._blocks, bl._metadata
                blocks.extend(bs)
                metadata.extend(ms)
            blocklist = BlockList(blocks, metadata, owned_by_consumer=owned_by_consumer)
        else:
            tasks: List[ReadTask] = []
            block_partition_refs: List[ObjectRef[BlockPartition]] = []
            block_partition_meta_refs: List[ObjectRef[BlockPartitionMetadata]] = []
            for bl in bls:
                tasks.extend(bl._tasks)
                block_partition_refs.extend(bl._block_partition_refs)
                block_partition_meta_refs.extend(bl._block_partition_meta_refs)
            blocklist = LazyBlockList(
                tasks,
                block_partition_refs,
                block_partition_meta_refs,
                owned_by_consumer=owned_by_consumer,
            )

        epochs = [ds._get_epoch() for ds in datasets]
        max_epoch = max(*epochs)
        if len(set(epochs)) > 1:
            if ray.util.log_once("datasets_epoch_warned"):
                logger.warning(
                    "Dataset contains data from multiple epochs: {}, "
                    "likely due to a `rewindow()` call. The higher epoch "
                    "number {} will be used. This warning will not "
                    "be shown again.".format(set(epochs), max_epoch)
                )
        dataset_stats = DatasetStats(
            stages={"union": []},
            parent=[d._plan.stats() for d in datasets],
        )
        dataset_stats.time_total_s = time.perf_counter() - start_time
        return Dataset(
            ExecutionPlan(blocklist, dataset_stats, run_by_consumer=owned_by_consumer),
            max_epoch,
            self._lazy,
        )

    def groupby(self, key: Optional[KeyFn]) -> "GroupedDataset[T]":
        """Group the dataset by the key function or column name.

        This is a lazy operation.

        Examples:
            >>> import ray
            >>> # Group by a key function and aggregate.
            >>> ray.data.range(100).groupby(lambda x: x % 3).count() # doctest: +SKIP
            >>> # Group by an Arrow table column and aggregate.
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby( # doctest: +SKIP
            ...     "A").count() # doctest: +SKIP

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
            >>> import ray
            >>> from ray.data.aggregate import Max, Mean
            >>> ray.data.range(100).aggregate(Max()) # doctest: +SKIP
            >>> ray.data.range_table(100).aggregate( # doctest: +SKIP
            ...    Max("value"), Mean("value")) # doctest: +SKIP

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
            >>> import ray
            >>> ray.data.range(100).sum() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i, i**2) # doctest: +SKIP
            ...     for i in range(100)]).sum(lambda x: x[1]) # doctest: +SKIP
            >>> ray.data.range_table(100).sum("value") # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i, "B": i**2} # doctest: +SKIP
            ...     for i in range(100)]).sum(["A", "B"]) # doctest: +SKIP

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
            >>> import ray
            >>> ray.data.range(100).min() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i, i**2) # doctest: +SKIP
            ...     for i in range(100)]).min(lambda x: x[1]) # doctest: +SKIP
            >>> ray.data.range_table(100).min("value") # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i, "B": i**2} # doctest: +SKIP
            ...     for i in range(100)]).min(["A", "B"]) # doctest: +SKIP

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
            >>> import ray
            >>> ray.data.range(100).max() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i, i**2) # doctest: +SKIP
            ...     for i in range(100)]).max(lambda x: x[1]) # doctest: +SKIP
            >>> ray.data.range_table(100).max("value") # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i, "B": i**2} # doctest: +SKIP
            ...     for i in range(100)]).max(["A", "B"]) # doctest: +SKIP

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
            >>> import ray
            >>> ray.data.range(100).mean() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i, i**2) # doctest: +SKIP
            ...     for i in range(100)]).mean(lambda x: x[1]) # doctest: +SKIP
            >>> ray.data.range_table(100).mean("value") # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i, "B": i**2} # doctest: +SKIP
            ...     for i in range(100)]).mean(["A", "B"]) # doctest: +SKIP

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
            >>> import ray # doctest: +SKIP
            >>> ray.data.range(100).std() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i, i**2) # doctest: +SKIP
            ...     for i in range(100)]).std(lambda x: x[1]) # doctest: +SKIP
            >>> ray.data.range_table(100).std("value", ddof=0) # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i, "B": i**2} # doctest: +SKIP
            ...     for i in range(100)]).std(["A", "B"]) # doctest: +SKIP

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
        # TODO ds.sort(lambda ...) fails with:
        #  Callable key '<function <lambda> at 0x1b07a4cb0>' requires
        #  dataset format to be 'simple', was 'arrow'.
        #  How do I create something "simple" here?
        """Sort the dataset by the specified key column or key function.

        This is a blocking operation.

        Examples:
            >>> import ray # doctest: +SKIP
            >>> # Sort using the entire record as the key.
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.sort() # doctest: +SKIP
            >>> # Sort by a single column in descending order.
            >>> ds = ray.data.from_items( # doctest: +SKIP
            ...     [{"value": i} for i in range(1000)])
            >>> ds.sort("value", descending=True) # doctest: +SKIP
            >>> # Sort by a key function.
            >>> ds.sort(lambda record: record["value"]) # doctest: +SKIP

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

        plan = self._plan.with_stage(SortStage(self, key, descending))
        return Dataset(plan, self._epoch, self._lazy)

    def zip(self, other: "Dataset[U]") -> "Dataset[(T, U)]":
        """Zip this dataset with the elements of another.

        The datasets must have identical num rows, block types, and block sizes
        (e.g., one was produced from a ``.map()`` of another). For Arrow
        blocks, the schema will be concatenated, and any duplicate column
        names disambiguated with _1, _2, etc. suffixes.

        NOTE: Zipped datasets are not lineage-serializable, i.e. they can not be used
        as a tunable hyperparameter in Ray Tune.

        Time complexity: O(dataset size / parallelism)

        Args:
            other: The dataset to zip with on the right hand side.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(5) # doctest: +SKIP
            >>> ds.zip(ds).take() # doctest: +SKIP
            [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]

        Returns:
            A Dataset with (k, v) pairs (or concatenated Arrow schema) where k
            comes from the first dataset and v comes from the second.
        """

        plan = self._plan.with_stage(ZipStage(other))
        return Dataset(plan, self._epoch, self._lazy)

    def limit(self, limit: int) -> "Dataset[T]":
        """Truncate the dataset to the first ``limit`` records.

        Contrary to :meth`.take`, this will not move any data to the caller's
        machine. Instead, it will return a new ``Dataset`` pointing to the truncated
        distributed data.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000) # doctest: +SKIP
            >>> ds.limit(100).map(lambda x: x * 2).take() # doctest: +SKIP

        Time complexity: O(limit specified)

        Args:
            limit: The size of the dataset to truncate to.

        Returns:
            The truncated dataset.
        """
        start_time = time.perf_counter()
        # Truncate the block list to the minimum number of blocks that contains at least
        # `limit` rows.
        block_list = self._plan.execute().truncate_by_rows(limit)
        blocks, metadata, _, _ = _split_at_index(block_list, limit)
        split_duration = time.perf_counter() - start_time
        meta_for_stats = [
            BlockMetadata(
                num_rows=m.num_rows,
                size_bytes=m.size_bytes,
                schema=m.schema,
                input_files=m.input_files,
                exec_stats=None,
            )
            for m in metadata
        ]
        dataset_stats = DatasetStats(
            stages={"limit": meta_for_stats},
            parent=self._plan.stats(),
        )
        dataset_stats.time_total_s = split_duration
        return Dataset(
            ExecutionPlan(
                BlockList(
                    blocks,
                    metadata,
                    owned_by_consumer=block_list._owned_by_consumer,
                ),
                dataset_stats,
                run_by_consumer=block_list._owned_by_consumer,
            ),
            self._epoch,
            self._lazy,
        )

    def take(self, limit: int = 20) -> List[T]:
        """Return up to ``limit`` records from the dataset.

        This will move up to ``limit`` records to the caller's machine; if
        ``limit`` is very large, this can result in an OutOfMemory crash on
        the caller.

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
        """Return all of the records in the dataset.

        This will move the entire dataset to the caller's machine; if the
        dataset is very large, this can result in an OutOfMemory crash on
        the caller.

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
        ray_remote_args: Dict[str, Any] = None,
        **arrow_parquet_args,
    ) -> None:
        """Write the dataset to parquet.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.parquet, where ``uuid`` is an unique
        id for the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_parquet("s3://bucket/path") # doctest: +SKIP

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
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            arrow_parquet_args: Options to pass to
                pyarrow.parquet.write_table(), which is used to write out each
                block to a file.
        """
        self.write_datasource(
            ParquetDatasource(),
            ray_remote_args=ray_remote_args,
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
        ray_remote_args: Dict[str, Any] = None,
        **pandas_json_args,
    ) -> None:
        """Write the dataset to json.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.json, where ``uuid`` is an
        unique id for the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_json("s3://bucket/path") # doctest: +SKIP

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
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            pandas_json_args: These args will be passed to
                pandas.DataFrame.to_json(), which we use under the hood to
                write out each Datasets block. These
                are dict(orient="records", lines=True) by default.
        """
        self.write_datasource(
            JSONDatasource(),
            ray_remote_args=ray_remote_args,
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
        ray_remote_args: Dict[str, Any] = None,
        **arrow_csv_args,
    ) -> None:
        """Write the dataset to csv.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.csv, where ``uuid`` is an unique id
        for the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_csv("s3://bucket/path") # doctest: +SKIP

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
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            arrow_csv_args: Other CSV write options to pass to pyarrow.
        """
        self.write_datasource(
            CSVDatasource(),
            ray_remote_args=ray_remote_args,
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
        column: str = VALUE_COL_NAME,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Write a tensor column of the dataset to npy files.

        This is only supported for datasets convertible to Arrow records that
        contain a TensorArray column. To control the number of files, use
        ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.npy, where ``uuid`` is an unique
        id for the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_numpy("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where npy
                files will be written to.
            column: The name of the table column that contains the tensor to
                be written. The default is ``"__value__"``, the column name that
                Datasets uses for storing tensors in single-column tables.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each dataset block to a custom output path.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
        """
        self.write_datasource(
            NumpyDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            dataset_uuid=self._uuid,
            column=column,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
        )

    def write_datasource(
        self,
        datasource: Datasource[T],
        *,
        ray_remote_args: Dict[str, Any] = None,
        **write_args,
    ) -> None:
        """Write the dataset to a custom datasource.

        Examples:
            >>> import ray
            >>> from ray.data.datasource import Datasource
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> class CustomDatasource(Datasource): # doctest: +SKIP
            ...     # define custom data source
            ...     pass # doctest: +SKIP
            >>> ds.write_datasource(CustomDatasource(...)) # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            datasource: The datasource to write to.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            write_args: Additional write args to pass to the datasource.
        """

        ctx = DatasetContext.get_current()
        blocks, metadata = zip(*self._plan.execute().get_blocks_with_metadata())

        # TODO(ekl) remove this feature flag.
        if "RAY_DATASET_FORCE_LOCAL_METADATA" in os.environ:
            write_results: List[ObjectRef[WriteResult]] = datasource.do_write(
                blocks, metadata, ray_remote_args=ray_remote_args, **write_args
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
                    ray_remote_args,
                    _wrap_and_register_arrow_serialization_workaround(write_args),
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
            >>> import ray
            >>> for i in ray.data.range(1000000).iter_rows(): # doctest: +SKIP
            ...     print(i) # doctest: +SKIP

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
            batch_size=None, prefetch_blocks=prefetch_blocks, batch_format=batch_format
        ):
            batch = BlockAccessor.for_block(batch)
            for row in batch.iter_rows():
                yield row

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "native",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[BatchType]:
        """Return a local batched iterator over the dataset.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(1000000).iter_batches(): # doctest: +SKIP
            ...     print(batch) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            batch_format: The format in which to return each batch.
                Specify "native" to use the native block format (promoting
                tables to Pandas and tensors to NumPy), "pandas" to select
                ``pandas.DataFrame``, "pyarrow" to select ``pyarrow.Table``, or "numpy"
                to select ``numpy.ndarray`` for tensor datasets and
                ``Dict[str, numpy.ndarray]`` for tabular datasets. Default is "native".
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained.
            local_shuffle_seed: The seed to use for the local random shuffle.

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
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )

        stats.iter_total_s.add(time.perf_counter() - time_start)

    def iter_torch_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[TorchTensorBatchType]:
        """Return a local batched iterator of Torch Tensors over the dataset.

        This iterator will yield single-tensor batches if the underlying dataset
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors. If looking for more flexibility in the tensor conversion (e.g.
        casting dtypes) or the batch format, try use `.iter_batches` directly, which is
        a lower-level API.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range( # doctest: +SKIP
            ...     12,
            ... ).iter_torch_batches(batch_size=4):
            ...     print(batch.shape) # doctest: +SKIP
            torch.Size([4, 1])
            torch.Size([4, 1])
            torch.Size([4, 1])

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The Torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data.
            device: The device on which the tensor should be placed; if None, the Torch
                tensor will be constructed on the CPU.
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
            An iterator over Torch Tensor batches.
        """
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        for batch in self.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format="numpy",
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        ):
            yield convert_ndarray_batch_to_torch_tensor_batch(
                batch,
                dtypes=dtypes,
                device=device,
            )

    def iter_tf_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["tf.dtypes.DType", Dict[str, "tf.dtypes.DType"]]] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[TensorFlowTensorBatchType]:
        """Return a local batched iterator of TensorFlow Tensors over the dataset.

        This iterator will yield single-tensor batches of the underlying dataset
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors. If looking for more flexibility in the tensor conversion (e.g.
        casting dtypes) or the batch format, try using `.to_tf`, which has a
        declarative API for tensor casting and batch formatting, or use `.iter_batches`
        directly, which is a lower-level API.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range( # doctest: +SKIP
            ...     12,
            ... ).iter_torch_batches(batch_size=4):
            ...     print(batch.shape) # doctest: +SKIP
            (4, 1)
            (4, 1)
            (4, 1)

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The TensorFlow dtype(s) for the created tensor(s); if None, the
                dtype will be inferred from the tensor data.
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
            An iterator over TensorFlow Tensor batches.
        """
        from ray.air._internal.tensorflow_utils import (
            convert_ndarray_batch_to_tf_tensor_batch,
        )

        for batch in self.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format="numpy",
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        ):
            yield convert_ndarray_batch_to_tf_tensor_batch(batch, dtypes=dtypes)

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
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        unsqueeze_label_tensor: bool = True,
        unsqueeze_feature_tensors: bool = True,
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
            label_column: The name of the column used as the
                label (second element of the output list). Can be None for
                prediction, in which case the second element of returned
                tuple will also be None.
            feature_columns: The names of the columns
                to use as the features. Can be a list of lists or
                a dict of string-list pairs for multi-tensor output.
                If None, then use all columns except the label column as
                the features.
            label_column_dtype: The torch dtype to
                use for the label column. If None, then automatically infer
                the dtype.
            feature_column_dtypes: The dtypes to use for the feature
                tensors. This should match the format of ``feature_columns``,
                or be a single dtype, in which case it will be applied to
                all tensors. If None, then automatically infer the dtype.
            batch_size: How many samples per batch to yield at a time.
                Defaults to 1.
            prefetch_blocks: The number of blocks to prefetch ahead of
                the current block during the scan.
            drop_last: Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of dataset is not divisible by the batch
                size, then the last batch will be smaller. Defaults to False.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.
            unsqueeze_label_tensor: If set to True, the label tensor
                will be unsqueezed (reshaped to (N, 1)). Otherwise, it will
                be left as is, that is (N, ). In general, regression loss
                functions expect an unsqueezed tensor, while classification
                loss functions expect a squeezed one. Defaults to True.
            unsqueeze_feature_tensors: If set to True, the features tensors
                will be unsqueezed (reshaped to (N, 1)) before being concatenated into
                the final features tensor. Otherwise, they will be left as is, that is
                (N, ). Defaults to True.

        Returns:
            A torch IterableDataset.
        """
        import torch

        from ray.air._internal.torch_utils import convert_pandas_to_torch_tensor
        from ray.data._internal.torch_iterable_dataset import TorchIterableDataset

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
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed,
            ):
                if label_column:
                    label_tensor = convert_pandas_to_torch_tensor(
                        batch,
                        [label_column],
                        label_column_dtype,
                        unsqueeze=unsqueeze_label_tensor,
                    )
                    batch.pop(label_column)
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
                            unsqueeze=unsqueeze_feature_tensors,
                        )
                        for key in feature_columns
                    }
                else:
                    features_tensor = convert_pandas_to_torch_tensor(
                        batch,
                        columns=feature_columns,
                        column_dtypes=feature_column_dtypes,
                        unsqueeze=unsqueeze_feature_tensors,
                    )

                yield (features_tensor, label_tensor)

        return TorchIterableDataset(make_generator)

    def to_tf(
        self,
        *,
        output_signature: Union[
            TensorflowFeatureTypeSpec, Tuple[TensorflowFeatureTypeSpec, "tf.TypeSpec"]
        ],
        label_column: Optional[str] = None,
        feature_columns: Optional[
            Union[List[str], List[List[str]], Dict[str, List[str]]]
        ] = None,
        prefetch_blocks: int = 0,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> "tf.data.Dataset":
        """Return a TF Dataset over this dataset.

        The TF Dataset will be created from the generator returned by the
        ``iter_batches`` method. ``prefetch_blocks`` and ``batch_size``
        arguments will be passed to that method.

        For the features tensor (N is the ``batch_size`` and n1, ..., nk
        are the number of features per tensor):

        * If ``feature_columns`` is a ``List[str]``, the features will be
          a tensor of shape (N, n), with columns corresponding to
          ``feature_columns``

        * If ``feature_columns`` is a ``List[List[str]]``, the features will be
          a list of tensors of shape [(N, n1),...,(N, nk)], with columns of each
          tensor corresponding to the elements of ``feature_columns``

        * If ``feature_columns`` is a ``Dict[str, List[str]]``, the features
          will be a dict of key-tensor pairs of shape
          {key1: (N, n1),..., keyN: (N, nk)}, with columns of each
          tensor corresponding to the value of ``feature_columns`` under the
          key.

        This is only supported for datasets convertible to Arrow records.

        Requires all datasets to have the same columns.

        It is recommended to call ``.split()`` on this dataset if
        there are to be multiple TensorFlow workers consuming the data.

        The elements generated must be compatible with the given
        ``output_signature`` argument (same as in
        ``tf.data.Dataset.from_generator``).

        Time complexity: O(1)

        Args:
            output_signature: If ``label_column`` is specified,
                a two-element tuple containing a ``FeatureTypeSpec`` and
                ``tf.TypeSpec`` object corresponding to (features, label). Otherwise, a
                single ``TensorflowFeatureTypeSpec`` corresponding to features tensor.
                A ``TensorflowFeatureTypeSpec`` is a ``tf.TypeSpec``,
                ``List["tf.TypeSpec"]``, or ``Dict[str, "tf.TypeSpec"]``.
            label_column: The name of the column used as the label
                (second element of the output tuple). If not specified, output
                will be just one tensor instead of a tuple.
            feature_columns: The names of the columns to use as the features. Can be a
                list of lists or a dict of string-list pairs for multi-tensor output.
                If None, then use all columns except the label columns as the features.
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: Record batch size. Defaults to 1.
            drop_last: Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of dataset is not divisible by the batch
                size, then the last batch will be smaller. Defaults to False.
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
            A tf.data.Dataset.
        """

        # argument exception checking is done in from_generator

        try:
            import tensorflow as tf
        except ImportError:
            raise ValueError("tensorflow must be installed!")

        from ray.air._internal.tensorflow_utils import convert_pandas_to_tf_tensor

        # `output_signature` can be a tuple but not a list. See
        # https://stackoverflow.com/questions/59092423/what-is-a-nested-structure-in-tensorflow.
        if isinstance(output_signature, list):
            output_signature = tuple(output_signature)

        def make_generator():
            for batch in self.iter_batches(
                prefetch_blocks=prefetch_blocks,
                batch_size=batch_size,
                batch_format="pandas",
                drop_last=drop_last,
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed,
            ):
                if label_column:
                    targets = convert_pandas_to_tf_tensor(batch[[label_column]])
                    if targets.ndim == 2 and targets.shape[1] == 1:
                        targets = tf.squeeze(targets, axis=1)
                    batch.pop(label_column)

                features = None
                if feature_columns is None:
                    features = convert_pandas_to_tf_tensor(batch)
                elif isinstance(feature_columns, list):
                    if all(isinstance(column, str) for column in feature_columns):
                        features = convert_pandas_to_tf_tensor(batch[feature_columns])
                    elif all(isinstance(columns, list) for columns in feature_columns):
                        features = tuple(
                            convert_pandas_to_tf_tensor(batch[columns])
                            for columns in feature_columns
                        )
                    else:
                        raise ValueError(
                            "Expected `feature_columns` to be a list of strings or a "
                            "list of lists."
                        )
                elif isinstance(feature_columns, dict):
                    features = {
                        key: convert_pandas_to_tf_tensor(batch[columns])
                        for key, columns in feature_columns.items()
                    }
                else:
                    raise ValueError(
                        "Expected `feature_columns` to be a list or a dictionary, "
                        f"but got a `{type(feature_columns).__name__}` instead."
                    )

                if label_column:
                    yield features, targets
                else:
                    yield features

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
        import pandas as pd
        import pyarrow as pa
        from mars.dataframe.datasource.read_raydataset import DataFrameReadRayDataset
        from mars.dataframe.utils import parse_index

        from ray.data._internal.pandas_block import PandasBlockSchema

        refs = self.to_pandas_refs()
        # remove this when https://github.com/mars-project/mars/issues/2945 got fixed
        schema = self.schema()
        if isinstance(schema, PandasBlockSchema):
            dtypes = pd.Series(schema.types, index=schema.names)
        elif isinstance(schema, pa.Schema):
            dtypes = schema.empty_table().to_pandas().dtypes
        else:
            raise NotImplementedError(f"Unsupported format of schema {schema}")
        index_value = parse_index(pd.RangeIndex(-1))
        columns_value = parse_index(dtypes.index, store_data=True)
        op = DataFrameReadRayDataset(refs=refs)
        return op(index_value=index_value, columns_value=columns_value, dtypes=dtypes)

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

        return raydp.spark.ray_dataset_to_spark_dataframe(
            spark, self.schema(), self.get_internal_block_refs()
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

        count = self.count()
        if count > limit:
            raise ValueError(
                f"The dataset has more than the given limit of {limit} "
                f"records: {count}. If you are sure that a DataFrame with "
                f"{count} rows will fit in local memory, use "
                f"ds.to_pandas(limit={count})."
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
            column: The name of the column to convert to numpy, or None to specify the
            entire row. If not specified for Arrow or Pandas blocks, each returned
            future will represent a dict of column ndarrays.

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
            >>> import ray
            >>> # Infinite pipeline of numbers [0, 5)
            >>> ray.data.range(5).repeat().take() # doctest: +SKIP
            [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...]
            >>> # Can apply transformations to the pipeline.
            >>> ray.data.range(5).repeat().map(lambda x: -x).take() # doctest: +SKIP
            [0, -1, -2, -3, -4, 0, -1, -2, -3, -4, ...]
            >>> # Can shuffle each epoch (dataset) in the pipeline.
            >>> ray.data.range(5).repeat().random_shuffle().take() # doctest: +SKIP
            [2, 3, 0, 4, 1, 4, 0, 2, 1, 3, ...]

        Args:
            times: The number of times to loop over this dataset, or None
                to repeat indefinitely.
        """
        from ray.data._internal.plan import _rewrite_read_stage
        from ray.data.dataset_pipeline import DatasetPipeline

        ctx = DatasetContext.get_current()
        if self._plan.is_read_stage_equivalent() and ctx.optimize_fuse_read_stages:
            blocks, _, stages = self._plan._get_source_blocks_and_stages()
            blocks.clear()
            blocks, outer_stats, stages = _rewrite_read_stage(blocks, stages)
            read_stage = stages[0]
        else:
            blocks = self._plan.execute()
            outer_stats = self._plan.stats()
            read_stage = None
        uuid = self._get_uuid()
        outer_stats.dataset_uuid = uuid

        if times is not None and times < 1:
            raise ValueError("`times` must be >= 1, got {}".format(times))

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
                        ExecutionPlan(
                            blocks, outer_stats, dataset_uuid=uuid, run_by_consumer=True
                        ),
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

        pipe = DatasetPipeline(Iterable(blocks), False, length=times or float("inf"))
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
            >>> import ray
            >>> # Create an inference pipeline.
            >>> ds = ray.data.read_binary_files(dir) # doctest: +SKIP
            >>> infer = ... # doctest: +SKIP
            >>> pipe = ds.window(blocks_per_window=10).map(infer) # doctest: +SKIP
            DatasetPipeline(num_windows=40, num_stages=2)
            >>> # The higher the stage parallelism, the shorter the pipeline.
            >>> pipe = ds.window(blocks_per_window=20).map(infer) # doctest: +SKIP
            DatasetPipeline(num_windows=20, num_stages=2)
            >>> # Outputs can be incrementally read from the pipeline.
            >>> for item in pipe.iter_rows(): # doctest: +SKIP
            ...    print(item) # doctest: +SKIP

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
        from ray.data._internal.plan import _rewrite_read_stage
        from ray.data.dataset_pipeline import DatasetPipeline

        if blocks_per_window is not None and bytes_per_window is not None:
            raise ValueError("Only one windowing scheme can be specified.")

        if blocks_per_window is None:
            blocks_per_window = 10

        ctx = DatasetContext.get_current()
        if self._plan.is_read_stage_equivalent() and ctx.optimize_fuse_read_stages:
            blocks, _, stages = self._plan._get_source_blocks_and_stages()
            blocks.clear()
            blocks, outer_stats, stages = _rewrite_read_stage(blocks, stages)
            read_stage = stages[0]
        else:
            blocks = self._plan.execute()
            outer_stats = self._plan.stats()
            read_stage = None

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
                        ExecutionPlan(blocks, outer_stats, run_by_consumer=True),
                        self._epoch,
                        lazy=True,
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
                    num_blocks = [s.initial_num_blocks() for s in self._splits]
                    assert [s > 0 for s in sizes], sizes

                    def fmt(size_bytes):
                        if size_bytes > 1024 * 1024 * 1024:
                            return "{}GiB".format(
                                round(size_bytes / (1024 * 1024 * 1024), 2)
                            )
                        elif size_bytes > 10 * 1024:
                            return "{}MiB".format(round(size_bytes / (1024 * 1024), 2))
                        else:
                            return "{}b".format(size_bytes)

                    mean_bytes = int(np.mean(sizes))
                    logger.info(
                        "Created DatasetPipeline with {} windows: "
                        "{} min, {} max, {} mean".format(
                            len(self._splits),
                            fmt(min(sizes)),
                            fmt(max(sizes)),
                            fmt(mean_bytes),
                        )
                    )
                    mean_num_blocks = int(np.mean(num_blocks))
                    logger.info(
                        "Blocks per window: "
                        "{} min, {} max, {} mean".format(
                            min(num_blocks),
                            max(num_blocks),
                            mean_num_blocks,
                        )
                    )
                    # TODO(ekl) we should try automatically choosing the default
                    # windowing settings to meet these best-practice constraints.
                    avail_parallelism = _estimate_available_parallelism()
                    if mean_num_blocks < avail_parallelism:
                        logger.warning(
                            f"{WARN_PREFIX} This pipeline's parallelism is limited "
                            f"by its blocks per window to ~{mean_num_blocks} "
                            "concurrent tasks per window. To maximize "
                            "performance, increase the blocks per window to at least "
                            f"{avail_parallelism}. This may require increasing the "
                            "base dataset's parallelism and/or adjusting the "
                            "windowing parameters."
                        )
                    else:
                        logger.info(
                            f"{OK_PREFIX} This pipeline's per-window parallelism "
                            "is high enough to fully utilize the cluster."
                        )
                    obj_store_mem = ray.cluster_resources().get(
                        "object_store_memory", 0
                    )
                    safe_mem_bytes = int(obj_store_mem * ESTIMATED_SAFE_MEMORY_FRACTION)
                    if mean_bytes > safe_mem_bytes:
                        logger.warning(
                            f"{WARN_PREFIX} This pipeline's windows are "
                            f"~{fmt(mean_bytes)} in size each and may not fit in "
                            "object store memory without spilling. To improve "
                            "performance, consider reducing the size of each window "
                            f"to {fmt(safe_mem_bytes)} or less."
                        )
                    else:
                        logger.info(
                            f"{OK_PREFIX} This pipeline's windows likely fit in "
                            "object store memory without spilling."
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
        pipe = DatasetPipeline(it, False, length=len(it._splits))
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
        self._plan.execute(force_read=True)
        return self

    def is_fully_executed(self) -> bool:
        """Returns whether this Dataset has been fully executed.

        This will return False if this Dataset is lazy and if the output of its final
        stage hasn't been computed yet.
        """
        return self._plan.has_computed_output()

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

    def lazy(self) -> "Dataset[T]":
        """Enable lazy evaluation.

        The returned dataset is a lazy dataset, where all subsequent operations on the
        dataset won't be executed until the dataset is consumed (e.g. ``.take()``,
        ``.iter_batches()``, ``.to_torch()``, ``.to_tf()``, etc.) or execution is
        manually triggered via ``.fully_executed()``.
        """
        ds = Dataset(self._plan, self._epoch, lazy=True)
        ds._set_uuid(self._get_uuid())
        return ds

    def experimental_lazy(self) -> "Dataset[T]":
        raise DeprecationWarning("Use self.lazy().")

    def has_serializable_lineage(self) -> bool:
        """Whether this dataset's lineage is able to be serialized for storage and
        later deserialized, possibly on a different cluster.

        Only datasets that are created from data that we know will still exist at
        deserialization time, e.g. data external to this Ray cluster such as persistent
        cloud object stores, support lineage-based serialization. All of the
        ray.data.read_*() APIs support lineage-based serialization.
        """
        return self._plan.has_lazy_input()

    @DeveloperAPI
    def serialize_lineage(self) -> bytes:
        """
        Serialize this dataset's lineage, not the actual data or the existing data
        futures, to bytes that can be stored and later deserialized, possibly on a
        different cluster.

        Note that this will drop all computed data, and that everything will be
        recomputed from scratch after deserialization.

        Use :py:meth:`Dataset.deserialize_lineage` to deserialize the serialized bytes
        returned from this method into a Dataset.

        NOTE: Unioned and zipped datasets, produced by :py:meth`Dataset.union` and
        :py:meth:`Dataset.zip`, are not lineage-serializable.

        Returns:
            Serialized bytes containing the lineage of this dataset.
        """
        if not self.has_serializable_lineage():
            raise ValueError(
                "Lineage-based serialization is not supported for this dataset, which "
                "means that it cannot be used as a tunable hyperparameter. "
                "Lineage-based serialization is explicitly NOT supported for unioned "
                "or zipped datasets (see docstrings for those methods), and is only "
                "supported for Datasets created from data that we know will still "
                "exist at deserialization time, e.g. external data in persistent cloud "
                "object stores or in-memory data from long-lived clusters. Concretely, "
                "all ray.data.read_*() APIs should support lineage-based "
                "serialization, while all of the ray.data.from_*() APIs do not. To "
                "allow this Dataset to be serialized to storage, write the data to an "
                "external store (such as AWS S3, GCS, or Azure Blob Storage) using the "
                "Dataset.write_*() APIs, and serialize a new dataset reading from the "
                "external store using the ray.data.read_*() APIs."
            )
        # Copy Dataset and clear the blocks from the execution plan so only the
        # Dataset's lineage is serialized.
        plan_copy = self._plan.deep_copy(preserve_uuid=True)
        ds = Dataset(plan_copy, self._get_epoch(), self._lazy)
        ds._plan.clear_block_refs()
        ds._set_uuid(self._get_uuid())

        def _reduce_remote_fn(rf: ray.remote_function.RemoteFunction):
            # Custom reducer for Ray remote function handles that allows for
            # cross-cluster serialization.
            # This manually unsets the last export session and job to force re-exporting
            # of the function when the handle is deserialized on a new cluster.
            # TODO(Clark): Fix this in core Ray, see issue:
            # https://github.com/ray-project/ray/issues/24152.
            reconstructor, args, state = rf.__reduce__()
            state["_last_export_session_and_job"] = None
            return reconstructor, args, state

        context = ray._private.worker.global_worker.get_serialization_context()
        try:
            context._register_cloudpickle_reducer(
                ray.remote_function.RemoteFunction, _reduce_remote_fn
            )
            serialized = pickle.dumps(ds)
        finally:
            context._unregister_cloudpickle_reducer(ray.remote_function.RemoteFunction)
        return serialized

    @staticmethod
    @DeveloperAPI
    def deserialize_lineage(serialized_ds: bytes) -> "Dataset":
        """
        Deserialize the provided lineage-serialized Dataset.

        This assumes that the provided serialized bytes were serialized using
        :py:meth:`Dataset.serialize_lineage`.

        Args:
            serialized_ds: The serialized Dataset that we wish to deserialize.

        Returns:
            A deserialized ``Dataset`` instance.
        """
        return pickle.loads(serialized_ds)

    def _divide(self, block_idx: int) -> ("Dataset[T]", "Dataset[T]"):
        block_list = self._plan.execute()
        left, right = block_list.divide(block_idx)
        l_ds = Dataset(
            ExecutionPlan(
                left, self._plan.stats(), run_by_consumer=block_list._owned_by_consumer
            ),
            self._epoch,
            self._lazy,
        )
        r_ds = Dataset(
            ExecutionPlan(
                right, self._plan.stats(), run_by_consumer=block_list._owned_by_consumer
            ),
            self._epoch,
            self._lazy,
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
        from ray.data._internal.pandas_block import PandasBlockSchema

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
                assert schema is not None
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
                schema_str.append(f"{n}: {t}")
            schema_str = ", ".join(schema_str)
            schema_str = "{" + schema_str + "}"
        count = self._meta_count()
        return "Dataset(num_blocks={}, num_rows={}, schema={})".format(
            self._plan.initial_num_blocks(), count, schema_str
        )

    def __str__(self) -> str:
        return repr(self)

    def __bool__(self) -> bool:
        # Prevents `__len__` from being called to check if it is None
        # see: issue #25152
        return True

    def __len__(self) -> int:
        raise AttributeError(
            "Use `ds.count()` to compute the length of a distributed Dataset. "
            "This may be an expensive operation."
        )

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

    def _warn_slow(self):
        if ray.util.log_once("datasets_slow_warned"):
            logger.warning(
                "The `map`, `flat_map`, and `filter` operations are unvectorized and "
                "can be very slow. Consider using `.map_batches()` instead."
            )


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


def _do_write(
    ds: Datasource,
    ctx: DatasetContext,
    blocks: List[Block],
    meta: List[BlockMetadata],
    ray_remote_args: Dict[str, Any],
    write_args: Dict[str, Any],
) -> List[ObjectRef[WriteResult]]:
    write_args = _unwrap_arrow_serialization_workaround(write_args)
    DatasetContext._set_current(ctx)
    return ds.do_write(blocks, meta, ray_remote_args=ray_remote_args, **write_args)
