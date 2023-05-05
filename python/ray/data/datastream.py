import collections
import itertools
import logging
import sys
import time
import html
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Type,
    Optional,
    Tuple,
    Union,
    Mapping,
)
from uuid import uuid4

import numpy as np

import ray
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
import ray.cloudpickle as pickle
from ray._private.usage import usage_lib
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import BlockFormat
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomShuffle,
    RandomizeBlocks,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.optimizers import LogicalPlan
from ray.data._internal.logical.operators.limit_operator import Limit
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapRows,
    MapBatches,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.planner.filter import generate_filter_fn
from ray.data._internal.planner.flat_map import generate_flat_map_fn
from ray.data._internal.planner.map_batches import generate_map_batches_fn
from ray.data._internal.planner.map_rows import generate_map_rows_fn
from ray.data._internal.planner.write import generate_write_fn
from ray.data.iterator import DataIterator
from ray.data._internal.block_list import BlockList
from ray.data._internal.iterator.iterator_impl import (
    DataIteratorImpl,
)
from ray.data._internal.iterator.stream_split_iterator import (
    StreamSplitDataIterator,
)
from ray.data._internal.compute import (
    ActorPoolStrategy,
    CallableClass,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.equalize import _equalize
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.util import (
    _estimate_available_parallelism,
    _is_local_scheme,
    ConsumptionAPI,
)
from ray.data._internal.pandas_block import PandasBlockSchema
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
    LimitStage,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.split import _split_at_indices, _get_num_rows
from ray.data._internal.stats import DatastreamStats, DatastreamStatsSummary
from ray.data.aggregate import AggregateFn, Max, Mean, Min, Std, Sum
from ray.data.block import (
    VALID_BATCH_FORMATS,
    STRICT_MODE_EXPLANATION,
    _apply_strict_mode_batch_format,
    _apply_strict_mode_batch_size,
    UserDefinedFunction,
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockPartition,
    DataBatch,
    StrictModeError,
    T,
    U,
    _validate_key_fn,
)
from ray.data.context import (
    DataContext,
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
    TFRecordDatasource,
    WriteResult,
)
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
    _wrap_arrow_serialization_workaround,
)
from ray.data.random_access_dataset import RandomAccessDataset
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI, Deprecated
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.widgets import Template
from ray.widgets.util import ensure_notebook_deps, fallback_if_colab

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

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
    from ray.data.grouped_data import GroupedData
    from ray.data._internal.execution.interfaces import Executor, NodeIdStr
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
    from tensorflow_metadata.proto.v0 import schema_pb2


logger = logging.getLogger(__name__)

TensorflowFeatureTypeSpec = Union[
    "tf.TypeSpec", List["tf.TypeSpec"], Dict[str, "tf.TypeSpec"]
]

TensorFlowTensorBatchType = Union["tf.Tensor", Dict[str, "tf.Tensor"]]


@PublicAPI
class Datastream:
    """A Datastream is a distributed data collection for data loading and processing.

    Datastreams are distributed pipelines that produce ``ObjectRef[Block]`` outputs,
    where each block holds data in Arrow format, representing a shard of the overall
    data collection. The block also determines the unit of parallelism.

    Datastreams can be created in multiple ways: from synthetic data via ``range_*()``
    APIs, from existing memory data via ``from_*()`` APIs (this creates a subclass
    of Datastream called ``MaterializedDatastream``), or from external storage
    systems such as local disk, S3, HDFS etc. via the ``read_*()`` APIs. The
    (potentially processed) Datastream can be saved back to external storage systems
    via the ``write_*()`` APIs.

    Examples:
        >>> import ray
        >>> # Create datastream from synthetic data.
        >>> ds = ray.data.range(1000)
        >>> # Create datastream from in-memory data.
        >>> ds = ray.data.from_items(
        ...     [{"col1": i, "col2": i * 2} for i in range(1000)])
        >>> # Create datastream from external storage system.
        >>> ds = ray.data.read_parquet("s3://bucket/path") # doctest: +SKIP
        >>> # Save datastream back to external storage system.
        >>> ds.write_csv("s3://bucket/output") # doctest: +SKIP

    Datastream has two kinds of operations: transformation, which takes in Datastream
    and outputs a new Datastream (e.g. :py:meth:`.map_batches()`); and consumption,
    which produces values (not Datatream) as output (e.g. :py:meth:`.iter_batches()`).

    Datastream transformations are lazy, with execution of the transformations being
    triggered by downstream consumption.

    Datastream supports parallel processing at scale: transformations such as
    :py:meth:`.map_batches()`, aggregations such as
    :py:meth:`.min()`/:py:meth:`.max()`/:py:meth:`.mean()`, grouping via
    :py:meth:`.groupby()`, shuffling operations such as :py:meth:`.sort()`,
    :py:meth:`.random_shuffle()`, and :py:meth:`.repartition()`.

    Examples:
        >>> import ray
        >>> ds = ray.data.range(1000)
        >>> # Transform batches (Dict[str, np.ndarray]) with map_batches().
        >>> ds.map_batches(lambda batch: {"id": batch["id"] * 2})
        MapBatches(<lambda>)
        +- Datastream(num_blocks=17, num_rows=1000, schema={id: int64})
        >>> # Compute the maximum.
        >>> ds.max("id")
        999
        >>> # Shuffle this datastream randomly.
        >>> ds.random_shuffle()
        RandomShuffle
        +- Datastream(num_blocks=..., num_rows=1000, schema={id: int64})
        >>> # Sort it back in order.
        >>> ds.sort("id")
        Sort
        +- Datastream(num_blocks=..., num_rows=1000, schema={id: int64})

    Both unexecuted and materialized Datastreams can be passed between Ray tasks and
    actors without incurring a copy. Datastream supports conversion to/from several
    more featureful dataframe libraries (e.g., Spark, Dask, Modin, MARS), and are also
    compatible with distributed TensorFlow / PyTorch.
    """

    def __init__(
        self,
        plan: ExecutionPlan,
        epoch: int,
        lazy: bool = True,
        logical_plan: Optional[LogicalPlan] = None,
    ):
        """Construct a Datastream (internal API).

        The constructor is not part of the Datastream API. Use the ``ray.data.*``
        read methods to construct a datastream.
        """
        assert isinstance(plan, ExecutionPlan)
        usage_lib.record_library_usage("dataset")  # Legacy telemetry name.

        if ray.util.log_once("strict_mode_explanation"):
            logger.warning(STRICT_MODE_EXPLANATION)

        self._plan = plan
        self._uuid = uuid4().hex
        self._epoch = epoch
        self._lazy = lazy
        self._logical_plan = logical_plan
        if logical_plan is not None:
            self._plan.link_logical_plan(logical_plan)

        if not lazy:
            self._plan.execute(allow_clear_input_blocks=False)

        # Handle to currently running executor for this datastream.
        self._current_executor: Optional["Executor"] = None

    @staticmethod
    def copy(
        ds: "Datastream", _deep_copy: bool = False, _as: Optional[type] = None
    ) -> "Datastream":
        if not _as:
            _as = Datastream
        if _deep_copy:
            return _as(ds._plan.deep_copy(), ds._epoch, ds._lazy, ds._logical_plan)
        else:
            return _as(ds._plan.copy(), ds._epoch, ds._lazy, ds._logical_plan)

    def map(
        self,
        fn: UserDefinedFunction[Dict[str, Any], Dict[str, Any]],
        *,
        compute: Optional[ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Apply the given function to each record of this datastream.

        Note that mapping individual records can be quite slow. Consider using
        `.map_batches()` for performance.

        Examples:
            >>> import ray
            >>> # Transform python objects.
            >>> ds = ray.data.range(1000)
            >>> # The function goes from record (Dict[str, Any]) to record.
            >>> ds.map(lambda record: {"id": record["id"] * 2})
            Map
            +- Datastream(num_blocks=..., num_rows=1000, schema={id: int64})
            >>> # Transform Arrow records.
            >>> ds = ray.data.from_items(
            ...     [{"value": i} for i in range(1000)])
            >>> ds.map(lambda record: {"v2": record["value"] * 2})
            Map
            +- Datastream(num_blocks=200, num_rows=1000, schema={value: int64})
            >>> # Define a callable class that persists state across
            >>> # function invocations for efficiency.
            >>> init_model = ... # doctest: +SKIP
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_model()
            ...    def __call__(self, batch):
            ...        return self.model(batch)
            >>> # Apply the transform in parallel on GPUs. Since
            >>> # compute=ActorPoolStrategy(size=8) the transform will be applied on a
            >>> # pool of 8 Ray actors, each allocated 1 GPU by Ray.
            >>> ds.map(CachedModel, # doctest: +SKIP
            ...        compute=ray.data.ActorPoolStrategy(size=8),
            ...        num_gpus=1)

        Time complexity: O(datastream size / parallelism)

        Args:
            fn: The function to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either None (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).

        .. seealso::

            :meth:`~Datastream.flat_map`:
                Call this method to create new records from existing ones. Unlike
                :meth:`~Datastream.map`, a function passed to
                :meth:`~Datastream.flat_map` can return multiple records.

                :meth:`~Datastream.flat_map` isn't recommended because it's slow; call
                :meth:`~Datastream.map_batches` instead.

            :meth:`~Datastream.map_batches`
                Call this method to transform batches of data. It's faster and more
                flexible than :meth:`~Datastream.map` and :meth:`~Datastream.flat_map`.
        """
        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}. "
                "For example, use ``compute=ActorPoolStrategy(size=n)``."
            )

        self._warn_slow()

        transform_fn = generate_map_rows_fn()

        plan = self._plan.with_stage(
            OneToOneStage(
                "Map",
                transform_fn,
                compute,
                ray_remote_args,
                fn=fn,
            )
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            map_op = MapRows(
                logical_plan.dag,
                fn,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(map_op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def map_batches(
        self,
        fn: UserDefinedFunction[DataBatch, DataBatch],
        *,
        batch_size: Union[int, None, Literal["default"]] = "default",
        compute: Optional[ComputeStrategy] = None,
        batch_format: Optional[str] = "default",
        zero_copy_batch: bool = False,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Apply the given function to batches of data.

        This applies the ``fn`` in parallel with map tasks, with each task handling
        a batch of data (typically Dict[str, np.ndarray] or pd.DataFrame).

        To learn more about writing functions for :meth:`~Datastream.map_batches`, read
        :ref:`writing user-defined functions <transform_datastreams_writing_udfs>`.

        .. tip::
            If ``fn`` does not mutate its input, set ``zero_copy_batch=True`` to elide a
            batch copy, which can improve performance and decrease memory utilization.
            ``fn`` will then receive zero-copy read-only batches.
            If ``fn`` mutates its input, you will need to ensure that the batch provided
            to ``fn`` is writable by setting ``zero_copy_batch=False`` (default). This
            will create an extra, mutable copy of each batch before handing it to
            ``fn``.

        .. note::
            The size of the batches provided to ``fn`` may be smaller than the provided
            ``batch_size`` if ``batch_size`` doesn't evenly divide the block(s) sent to
            a given map task. When ``batch_size`` is specified, each map task will be
            sent a single block if the block is equal to or larger than ``batch_size``,
            and will be sent a bundle of blocks up to (but not exceeding)
            ``batch_size`` if blocks are smaller than ``batch_size``.

        Examples:

            >>> import numpy as np
            >>> import ray
            >>> ds = ray.data.from_items([
            ...     {"name": "Luna", "age": 4},
            ...     {"name": "Rory", "age": 14},
            ...     {"name": "Scout", "age": 9},
            ... ])
            >>> ds  # doctest: +SKIP
            MaterializedDatastream(
                num_blocks=3,
                num_rows=3,
                schema={name: string, age: int64}
            )

            Here ``fn`` returns the same batch type as the input, but your ``fn`` can
            also return a different batch type (e.g., pd.DataFrame). Read more about
            :ref:`Transforming Data <transforming_data>`.

            >>> from typing import Dict
            >>> def map_fn(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            ...     batch["age_in_dog_years"] = 7 * batch["age"]
            ...     return batch
            >>> ds = ds.map_batches(map_fn)
            >>> ds
            MapBatches(map_fn)
            +- Datastream(num_blocks=3, num_rows=3, schema={name: string, age: int64})

            :ref:`Actors <actor-guide>` can improve the performance of some workloads.
            For example, you can use :ref:`actors <actor-guide>` to load a model once
            per worker instead of once per inference.

            To transform batches with :ref:`actors <actor-guide>`, pass a callable type
            to ``fn`` and specify an :class:`~ray.data.ActorPoolStrategy>`.

            In the example below, ``CachedModel`` is called on an autoscaling pool of
            two to eight :ref:`actors <actor-guide>`, each allocated one GPU by Ray.

            >>> init_large_model = ... # doctest: +SKIP
            >>> class CachedModel:
            ...    def __init__(self):
            ...        self.model = init_large_model()
            ...    def __call__(self, item):
            ...        return self.model(item)
            >>> ds.map_batches( # doctest: +SKIP
            ...     CachedModel, # doctest: +SKIP
            ...     batch_size=256, # doctest: +SKIP
            ...     compute=ray.data.ActorPoolStrategy(size=8), # doctest: +SKIP
            ...     num_gpus=1,
            ... ) # doctest: +SKIP

            ``fn`` can also be a generator, yielding multiple batches in a single
            invocation. This is useful when returning large objects. Instead of
            returning a very large output batch, ``fn`` can instead yield the
            output batch in chunks.

            >>> def map_fn_with_large_output(batch):
            ...     for i in range(3):
            ...         yield {"large_output": np.ones((100, 1000))}
            >>> ds = ray.data.from_items([1])
            >>> ds = ds.map_batches(map_fn_with_large_output)
            >>> ds
            MapBatches(map_fn_with_large_output)
            +- Datastream(num_blocks=1, num_rows=1, schema={item: int64})


        Args:
            fn: The function or generator to apply to each record batch, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy. Note ``fn`` must be
                pickle-able.
            batch_size: The desired number of rows in each batch, or None to use entire
                blocks as batches (blocks may contain different number of rows).
                The actual size of the batch provided to ``fn`` may be smaller than
                ``batch_size`` if ``batch_size`` doesn't evenly divide the block(s) sent
                to a given map task. Default batch_size is 4096 with "default".
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.
            zero_copy_batch: Whether ``fn`` should be provided zero-copy, read-only
                batches. If this is ``True`` and no copy is required for the
                ``batch_format`` conversion, the batch will be a zero-copy, read-only
                view on data in Ray's object store, which can decrease memory
                utilization and improve performance. If this is ``False``, the batch
                will be writable, which will require an extra copy to guarantee.
                If ``fn`` mutates its input, this will need to be ``False`` in order to
                avoid "assignment destination is read-only" or "buffer source array is
                read-only" errors. Default is ``False``.
            fn_args: Positional arguments to pass to ``fn`` after the first argument.
                These arguments are top-level arguments to the underlying Ray task.
            fn_kwargs: Keyword arguments to pass to ``fn``. These arguments are
                top-level arguments to the underlying Ray task.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., ``num_gpus=1`` to request GPUs for the map tasks).

        .. seealso::

            :meth:`~Datastream.iter_batches`
                Call this function to iterate over batches of data.

            :meth:`~Datastream.flat_map`:
                Call this method to create new records from existing ones. Unlike
                :meth:`~Datastream.map`, a function passed to :meth:`~Datastream.flat_map`
                can return multiple records.

                :meth:`~Datastream.flat_map` isn't recommended because it's slow; call
                :meth:`~Datastream.map_batches` instead.

            :meth:`~Datastream.map`
                Call this method to transform one record at time.

                This method isn't recommended because it's slow; call
                :meth:`~Datastream.map_batches` instead.
        """  # noqa: E501

        batch_format = _apply_strict_mode_batch_format(batch_format)
        if batch_format == "native":
            logger.warning("The 'native' batch format has been renamed 'default'.")

        target_block_size = None
        if batch_size is not None and batch_size != "default":
            if batch_size < 1:
                raise ValueError("Batch size cannot be negative or 0")
            # Enable blocks bundling when batch_size is specified by caller.
            target_block_size = batch_size

        batch_size = _apply_strict_mode_batch_size(
            batch_size, use_gpu="num_gpus" in ray_remote_args
        )

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
                f"specify the actor compute strategy, but got: {compute}. "
                "For example, use ``compute=ActorPoolStrategy(size=n)``."
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

        transform_fn = generate_map_batches_fn(
            batch_size=batch_size,
            batch_format=batch_format,
            zero_copy_batch=zero_copy_batch,
        )

        # TODO(chengsu): pass function name to MapBatches logical operator.
        if hasattr(fn, "__self__") and isinstance(
            fn.__self__, ray.data.preprocessor.Preprocessor
        ):
            stage_name = fn.__self__.__class__.__name__
        else:
            stage_name = f'MapBatches({getattr(fn, "__name__", type(fn))})'

        stage = OneToOneStage(
            stage_name,
            transform_fn,
            compute,
            ray_remote_args,
            # TODO(Clark): Add a strict cap here.
            target_block_size=target_block_size,
            fn=fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        plan = self._plan.with_stage(stage)

        logical_plan = self._logical_plan
        if logical_plan is not None:
            map_batches_op = MapBatches(
                logical_plan.dag,
                fn,
                batch_size=batch_size,
                batch_format=batch_format,
                zero_copy_batch=zero_copy_batch,
                target_block_size=target_block_size,
                fn_args=fn_args,
                fn_kwargs=fn_kwargs,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(map_batches_op)

        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def add_column(
        self,
        col: str,
        fn: Callable[["pandas.DataFrame"], "pandas.Series"],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Add the given column to the datastream.

        This is only supported for datastreams convertible to pandas format.
        A function generating the new column values given the batch in pandas
        format must be specified.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> # Add a new column equal to value * 2.
            >>> ds = ds.add_column("new_col", lambda df: df["id"] * 2)
            >>> # Overwrite the existing "value" with zeros.
            >>> ds = ds.add_column("id", lambda df: 0)

        Time complexity: O(datastream size / parallelism)

        Args:
            col: Name of the column to add. If the name already exists, the
                column will be overwritten.
            fn: Map function generating the column values given a batch of
                records in pandas format.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        def process_batch(batch: "pandas.DataFrame") -> "pandas.DataFrame":
            batch.loc[:, col] = fn(batch)
            return batch

        if not callable(fn):
            raise ValueError("`fn` must be callable, got {}".format(fn))

        return self.map_batches(
            process_batch,
            batch_format="pandas",  # TODO(ekl) we should make this configurable.
            compute=compute,
            zero_copy_batch=False,
            **ray_remote_args,
        )

    def drop_columns(
        self,
        cols: List[str],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Drop one or more columns from the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> # Add a new column equal to value * 2.
            >>> ds = ds.add_column("new_col", lambda df: df["id"] * 2)
            >>> # Drop the existing "value" column.
            >>> ds = ds.drop_columns(["id"])


        Time complexity: O(datastream size / parallelism)

        Args:
            cols: Names of the columns to drop. If any name does not exist,
                an exception will be raised.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """

        return self.map_batches(
            lambda batch: batch.drop(columns=cols),
            batch_format="pandas",
            zero_copy_batch=True,
            compute=compute,
            **ray_remote_args,
        )

    def select_columns(
        self,
        cols: List[str],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Select one or more columns from the datastream.

        All input columns used to select need to be in the schema of the datastream.

        Examples:
            >>> import ray
            >>> # Create a datastream with 3 columns
            >>> ds = ray.data.from_items([{"col1": i, "col2": i+1, "col3": i+2}
            ...      for i in range(10)])
            >>> # Select only "col1" and "col2" columns.
            >>> ds = ds.select_columns(cols=["col1", "col2"])
            >>> ds
            MapBatches(<lambda>)
            +- Datastream(
                  num_blocks=10,
                  num_rows=10,
                  schema={col1: int64, col2: int64, col3: int64}
               )


        Time complexity: O(datastream size / parallelism)

        Args:
            cols: Names of the columns to select. If any name is not included in the
                datastream schema, an exception will be raised.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """  # noqa: E501
        return self.map_batches(
            lambda batch: BlockAccessor.for_block(batch).select(columns=cols),
            batch_format="pandas",
            zero_copy_batch=True,
            compute=compute,
            **ray_remote_args,
        )

    def flat_map(
        self,
        fn: UserDefinedFunction[Dict[str, Any], List[Dict[str, Any]]],
        *,
        compute: Optional[ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Apply the given function to each record and then flatten results.

        Consider using ``.map_batches()`` for better performance (the batch size can be
        altered in map_batches).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000)
            >>> ds.flat_map(lambda x: [{"id": 1}, {"id": 2}, {"id": 4}])
            FlatMap
            +- Datastream(num_blocks=..., num_rows=1000, schema={id: int64})

        Time complexity: O(datastream size / parallelism)

        Args:
            fn: The function or generator to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).

        .. seealso::

            :meth:`~Datastream.map_batches`
                Call this method to transform batches of data. It's faster and more
                flexible than :meth:`~Datastream.map` and :meth:`~Datastream.flat_map`.

            :meth:`~Datastream.map`
                Call this method to transform one record at time.

                This method isn't recommended because it's slow; call
                :meth:`~Datastream.map_batches` instead.
        """
        if isinstance(fn, CallableClass) and (
            compute is None
            or compute == "tasks"
            or isinstance(compute, TaskPoolStrategy)
        ):
            raise ValueError(
                "``compute`` must be specified when using a CallableClass, and must "
                f"specify the actor compute strategy, but got: {compute}. "
                "For example, use ``compute=ActorPoolStrategy(size=n)``."
            )

        self._warn_slow()

        transform_fn = generate_flat_map_fn()

        plan = self._plan.with_stage(
            OneToOneStage("FlatMap", transform_fn, compute, ray_remote_args, fn=fn)
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = FlatMap(
                input_op=logical_plan.dag,
                fn=fn,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def filter(
        self,
        fn: UserDefinedFunction[Dict[str, Any], bool],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Filter out records that do not satisfy the given predicate.

        Consider using ``.map_batches()`` for better performance (you can implement
        filter by dropping records).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.filter(lambda x: x["id"] % 2 == 0)
            Filter
            +- Datastream(num_blocks=..., num_rows=100, schema={id: int64})

        Time complexity: O(datastream size / parallelism)

        Args:
            fn: The predicate to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
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
                f"specify the actor compute strategy, but got: {compute}. "
                "For example, use ``compute=ActorPoolStrategy(size=n)``."
            )

        self._warn_slow()

        transform_fn = generate_filter_fn()

        plan = self._plan.with_stage(
            OneToOneStage("Filter", transform_fn, compute, ray_remote_args, fn=fn)
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Filter(
                input_op=logical_plan.dag,
                fn=fn,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(op)

        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def repartition(self, num_blocks: int, *, shuffle: bool = False) -> "Datastream":
        """Repartition the datastream into exactly this number of blocks.

        After repartitioning, all blocks in the returned datastream will have
        approximately the same number of rows.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> # Set the number of output partitions to write to disk.
            >>> ds.repartition(10).write_parquet("/tmp/test")

        Time complexity: O(datastream size / parallelism)

        Args:
            num_blocks: The number of blocks.
            shuffle: Whether to perform a distributed shuffle during the
                repartition. When shuffle is enabled, each output block
                contains a subset of data rows from each input block, which
                requires all-to-all data movement. When shuffle is disabled,
                output blocks are created from adjacent input blocks,
                minimizing data movement.

        Returns:
            The repartitioned datastream.
        """

        plan = self._plan.with_stage(RepartitionStage(num_blocks, shuffle))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Repartition(
                logical_plan.dag,
                num_outputs=num_blocks,
                shuffle=shuffle,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def random_shuffle(
        self,
        *,
        seed: Optional[int] = None,
        num_blocks: Optional[int] = None,
        **ray_remote_args,
    ) -> "Datastream":
        """Randomly shuffle the elements of this datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> # Shuffle this datastream randomly.
            >>> ds.random_shuffle()
            RandomShuffle
            +- Datastream(num_blocks=..., num_rows=100, schema={id: int64})
            >>> # Shuffle this datastream with a fixed random seed.
            >>> ds.random_shuffle(seed=12345)
            RandomShuffle
            +- Datastream(num_blocks=..., num_rows=100, schema={id: int64})

        Time complexity: O(datastream size / parallelism)

        Args:
            seed: Fix the random seed to use, otherwise one will be chosen
                based on system randomness.
            num_blocks: The number of output blocks after the shuffle, or None
                to retain the number of blocks.

        Returns:
            The shuffled datastream.
        """

        plan = self._plan.with_stage(
            RandomShuffleStage(seed, num_blocks, ray_remote_args)
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = RandomShuffle(
                logical_plan.dag,
                seed=seed,
                num_outputs=num_blocks,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def randomize_block_order(
        self,
        *,
        seed: Optional[int] = None,
    ) -> "Datastream":
        """Randomly shuffle the blocks of this datastream.

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
            The block-shuffled datastream.
        """

        plan = self._plan.with_stage(RandomizeBlocksStage(seed))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = RandomizeBlocks(
                logical_plan.dag,
                seed=seed,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def random_sample(
        self, fraction: float, *, seed: Optional[int] = None
    ) -> "Datastream":
        """Randomly samples a fraction of the elements of this datastream.

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
            Returns a Datastream containing the sampled elements.
        """
        import random

        import pandas as pd
        import pyarrow as pa

        if self.num_blocks() == 0:
            raise ValueError("Cannot sample from an empty Datastream.")

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
                return _create_possibly_ragged_ndarray(
                    [row for row in batch if random.random() <= fraction]
                )
            raise ValueError(f"Unsupported batch type: {type(batch)}")

        return self.map_batches(process_batch, batch_format=None)

    @ConsumptionAPI
    def streaming_split(
        self,
        n: int,
        *,
        equal: bool = False,
        locality_hints: Optional[List["NodeIdStr"]] = None,
    ) -> List[DataIterator]:
        """Returns ``n`` :class:`DataIterators <ray.data.DataIterator>` that can
        be used to read disjoint subsets of the datastream in parallel.

        This method is the recommended way to consume Datastreams from multiple
        processes (e.g., for distributed training), and requires streaming execution
        mode.

        Streaming split works by delegating the execution of this Datastream to a
        coordinator actor. The coordinator pulls block references from the executed
        stream, and divides those blocks among `n` output iterators. Iterators pull
        blocks from the coordinator actor to return to their caller on `next`.

        The returned iterators are also repeatable; each iteration will trigger a
        new execution of the Datastream. There is an implicit barrier at the start of
        each iteration, which means that `next` must be called on all iterators before
        the iteration starts.

        Warning: because iterators are pulling blocks from the same Datastream
        execution, if one iterator falls behind other iterators may be stalled.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000000)
            >>> it1, it2 = ds.streaming_split(2, equal=True)

            >>> # Can consume from both iterators in parallel.
            >>> @ray.remote
            ... def consume(it):
            ...    for batch in it.iter_batches():
            ...        print(batch)
            >>> ray.get([consume.remote(it1), consume.remote(it2)])  # doctest: +SKIP

            >>> # Can loop over the iterators multiple times (multiple epochs).
            >>> @ray.remote
            ... def train(it):
            ...    NUM_EPOCHS = 100
            ...    for _ in range(NUM_EPOCHS):
            ...        for batch in it.iter_batches():
            ...            print(batch)
            >>> ray.get([train.remote(it1), train.remote(it2)])  # doctest: +SKIP

            >>> # ERROR: this will block waiting for a read on `it2` to start.
            >>> ray.get(train.remote(it1))  # doctest: +SKIP

        Args:
            n: Number of output iterators to return.
            equal: If True, each output iterator will see an exactly equal number
                of rows, dropping data if necessary. If False, some iterators may see
                slightly more or less rows than other, but no data will be dropped.
            locality_hints: Specify the node ids corresponding to each iterator
                location. Datastream will try to minimize data movement based on the
                iterator output locations. This list must have length ``n``. You can
                get the current node id of a task or actor by calling
                ``ray.get_runtime_context().get_node_id()``.

        Returns:
            The output iterator splits. These iterators are Ray-serializable and can
            be freely passed to any Ray task or actor.
        """
        return StreamSplitDataIterator.create(self, n, equal, locality_hints)

    @ConsumptionAPI
    def split(
        self, n: int, *, equal: bool = False, locality_hints: Optional[List[Any]] = None
    ) -> List["MaterializedDatastream"]:
        """Materialize and split the datastream into ``n`` disjoint pieces.

        This returns a list of MaterializedDatastreams that can be passed to Ray tasks
        and actors and used to read the datastream records in parallel.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> workers = ... # doctest: +SKIP
            >>> # Split up a datastream to process over `n` worker actors.
            >>> shards = ds.split(len(workers), locality_hints=workers) # doctest: +SKIP
            >>> for shard, worker in zip(shards, workers): # doctest: +SKIP
            ...     worker.consume.remote(shard) # doctest: +SKIP

        Time complexity: O(1)

        See also: ``Datastream.split_at_indices``, ``Datastream.split_proportionately``,
            and ``Datastream.streaming_split``.

        Args:
            n: Number of child datastreams to return.
            equal: Whether to guarantee each split has an equal
                number of records. This may drop records if they cannot be
                divided equally among the splits.
            locality_hints: [Experimental] A list of Ray actor handles of size ``n``.
                The system will try to co-locate the blocks of the i-th datastream
                with the i-th actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint datastream splits.
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
                MaterializedDatastream(
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
            MaterializedDatastream(
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

    @ConsumptionAPI
    def split_at_indices(self, indices: List[int]) -> List["MaterializedDatastream"]:
        """Materialize and split the datastream at the given indices (like np.split).

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> d1, d2, d3 = ds.split_at_indices([2, 5])
            >>> d1.take_batch()
            {'id': array([0, 1])}
            >>> d2.take_batch()
            {'id': array([2, 3, 4])}
            >>> d3.take_batch()
            {'id': array([5, 6, 7, 8, 9])}

        Time complexity: O(num splits)

        See also: ``Datastream.split_at_indices``, ``Datastream.split_proportionately``,
            and ``Datastream.streaming_split``.

        Args:
            indices: List of sorted integers which indicate where the datastream
                will be split. If an index exceeds the length of the datastream,
                an empty datastream will be returned.

        Returns:
            The datastream splits.
        """

        if len(indices) < 1:
            raise ValueError("indices must be at least of length 1")
        if sorted(indices) != indices:
            raise ValueError("indices must be sorted")
        if indices[0] < 0:
            raise ValueError("indices must be positive")
        start_time = time.perf_counter()
        block_list = self._plan.execute()
        blocks, metadata = _split_at_indices(
            block_list.get_blocks_with_metadata(),
            indices,
            block_list._owned_by_consumer,
        )
        split_duration = time.perf_counter() - start_time
        parent_stats = self._plan.stats()
        splits = []
        for bs, ms in zip(blocks, metadata):
            stats = DatastreamStats(stages={"Split": ms}, parent=parent_stats)
            stats.time_total_s = split_duration
            splits.append(
                MaterializedDatastream(
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

    @ConsumptionAPI
    def split_proportionately(
        self, proportions: List[float]
    ) -> List["MaterializedDatastream"]:
        """Materialize and split the datastream using proportions.

        A common use case for this would be splitting the datastream into train
        and test sets (equivalent to eg. scikit-learn's ``train_test_split``).
        See also ``Datastream.train_test_split`` for a higher level abstraction.

        The indices to split at will be calculated in such a way so that all splits
        always contains at least one element. If that is not possible,
        an exception will be raised.

        This is equivalent to caulculating the indices manually and calling
        ``Datastream.split_at_indices``.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> d1, d2, d3 = ds.split_proportionately([0.2, 0.5])
            >>> d1.take_batch()
            {'id': array([0, 1])}
            >>> d2.take_batch()
            {'id': array([2, 3, 4, 5, 6])}
            >>> d3.take_batch()
            {'id': array([7, 8, 9])}

        Time complexity: O(num splits)

        See also: ``Datastream.split``, ``Datastream.split_at_indices``,
        ``Datastream.train_test_split``

        Args:
            proportions: List of proportions to split the datastream according to.
                Must sum up to less than 1, and each proportion has to be bigger
                than 0.

        Returns:
            The datastream splits.
        """

        if len(proportions) < 1:
            raise ValueError("proportions must be at least of length 1")
        if sum(proportions) >= 1:
            raise ValueError("proportions must sum to less than 1")
        if any(p <= 0 for p in proportions):
            raise ValueError("proportions must be bigger than 0")

        datastream_length = self.count()
        cumulative_proportions = np.cumsum(proportions)
        split_indices = [
            int(datastream_length * proportion) for proportion in cumulative_proportions
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

    @ConsumptionAPI
    def train_test_split(
        self,
        test_size: Union[int, float],
        *,
        shuffle: bool = False,
        seed: Optional[int] = None,
    ) -> Tuple["MaterializedDatastream", "MaterializedDatastream"]:
        """Materialize and split the datastream into train and test subsets.

        Examples:

            >>> import ray
            >>> ds = ray.data.range(8)
            >>> train, test = ds.train_test_split(test_size=0.25)
            >>> train.take_batch()
            {'id': array([0, 1, 2, 3, 4, 5])}
            >>> test.take_batch()
            {'id': array([6, 7])}

        Args:
            test_size: If float, should be between 0.0 and 1.0 and represent the
                proportion of the datastream to include in the test split. If int,
                represents the absolute number of test samples. The train split will
                always be the compliment of the test split.
            shuffle: Whether or not to globally shuffle the datastream before splitting.
                Defaults to False. This may be a very expensive operation with large
                datastream.
            seed: Fix the random seed to use for shuffle, otherwise one will be chosen
                based on system randomness. Ignored if ``shuffle=False``.

        Returns:
            Train and test subsets as two MaterializedDatastreams.
        """
        ds = self

        if shuffle:
            ds = ds.random_shuffle(seed=seed)

        if not isinstance(test_size, (int, float)):
            raise TypeError(f"`test_size` must be int or float got {type(test_size)}.")
        if isinstance(test_size, float):
            if test_size <= 0 or test_size >= 1:
                raise ValueError(
                    "If `test_size` is a float, it must be bigger than 0 and smaller "
                    f"than 1. Got {test_size}."
                )
            return ds.split_proportionately([1 - test_size])
        else:
            ds_length = ds.count()
            if test_size <= 0 or test_size >= ds_length:
                raise ValueError(
                    "If `test_size` is an int, it must be bigger than 0 and smaller "
                    f"than the size of the datastream ({ds_length}). "
                    f"Got {test_size}."
                )
            return ds.split_at_indices([ds_length - test_size])

    @ConsumptionAPI(pattern="Args:")
    def union(self, *other: List["Datastream"]) -> "Datastream":
        """Materialize and combine this datastream with others of the same type.

        The order of the blocks in the datastreams is preserved, as is the
        relative ordering between the datastreams passed in the argument list.

        .. note::
            Unioned datastreams are not lineage-serializable, i.e. they can not be
            used as a tunable hyperparameter in Ray Tune.

        Args:
            other: List of datastreams to combine with this one. The datastreams
                must have the same schema as this datastream, otherwise the
                behavior is undefined.

        Returns:
            A new datastream holding the union of their data.
        """

        start_time = time.perf_counter()

        owned_by_consumer = self._plan.execute()._owned_by_consumer
        datastreams = [self] + list(other)
        bls = []
        has_nonlazy = False
        for ds in datastreams:
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
            block_partition_meta_refs: List[ObjectRef[BlockMetadata]] = []

            # Gather read task names from input blocks of unioned Datastreams,
            # and concat them before passing to resulting LazyBlockList
            read_task_names = []
            self_read_name = self._plan._in_blocks._read_stage_name or "Read"
            read_task_names.append(self_read_name)
            other_read_names = [
                o._plan._in_blocks._read_stage_name or "Read" for o in other
            ]
            read_task_names.extend(other_read_names)

            for bl in bls:
                tasks.extend(bl._tasks)
                block_partition_refs.extend(bl._block_partition_refs)
                block_partition_meta_refs.extend(bl._block_partition_meta_refs)
            blocklist = LazyBlockList(
                tasks,
                f"Union({','.join(read_task_names)})",
                block_partition_refs,
                block_partition_meta_refs,
                owned_by_consumer=owned_by_consumer,
            )

        epochs = [ds._get_epoch() for ds in datastreams]
        max_epoch = max(*epochs)
        if len(set(epochs)) > 1:
            if ray.util.log_once("datastream_epoch_warned"):
                logger.warning(
                    "Datastream contains data from multiple epochs: {}, "
                    "likely due to a `rewindow()` call. The higher epoch "
                    "number {} will be used. This warning will not "
                    "be shown again.".format(set(epochs), max_epoch)
                )
        stats = DatastreamStats(
            stages={"Union": []},
            parent=[d._plan.stats() for d in datastreams],
        )
        stats.time_total_s = time.perf_counter() - start_time
        return Datastream(
            ExecutionPlan(blocklist, stats, run_by_consumer=owned_by_consumer),
            max_epoch,
            self._lazy,
        )

    def groupby(self, key: Optional[str]) -> "GroupedData":
        """Group the datastream by the key function or column name.

        Examples:
            >>> import ray
            >>> # Group by a table column and aggregate.
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").count()
            Aggregate
            +- Datastream(num_blocks=100, num_rows=100, schema={A: int64, B: int64})

        Time complexity: O(datastream size * log(datastream size / parallelism))

        Args:
            key: A column name. If this is None, the grouping is global.

        Returns:
            A lazy GroupedData that can be aggregated later.
        """
        from ray.data.grouped_data import GroupedData

        # Always allow None since groupby interprets that as grouping all
        # records into a single global group.
        if key is not None:
            _validate_key_fn(self.schema(fetch_if_missing=True), key)

        return GroupedData(self, key)

    @ConsumptionAPI
    def aggregate(self, *aggs: AggregateFn) -> Union[Any, Dict[str, Any]]:
        """Aggregate the entire datastream as one group.

        Examples:
            >>> import ray
            >>> from ray.data.aggregate import Max, Mean
            >>> ray.data.range(100).aggregate(Max("id"), Mean("id"))
            {'max(id)': 99, 'mean(id)': 49.5}

        Time complexity: O(datastream size / parallelism)

        Args:
            aggs: Aggregations to do.

        Returns:
            If the input datastream is a simple datastream then the output is
            a tuple of ``(agg1, agg2, ...)`` where each tuple element is
            the corresponding aggregation result.
            If the input datastream is an Arrow datastream then the output is
            an dict where each column is the corresponding aggregation result.
            If the datastream is empty, return ``None``.
        """
        ret = self.groupby(None).aggregate(*aggs).take(1)
        return ret[0] if len(ret) > 0 else None

    @ConsumptionAPI
    def sum(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute sum over entire datastream.

        Examples:
            >>> import ray
            >>> ray.data.range(100).sum("id")
            4950
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).sum(["A", "B"])
            {'sum(A)': 4950, 'sum(B)': 328350}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the sum; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The sum result.

            For different values of ``on``, the return varies:

            - ``on=None``: a dict containing the column-wise sum of all
              columns,
            - ``on="col"``: a scalar representing the sum of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``dict``
              containing the column-wise sum of the provided columns.

            If the datastream is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Sum, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def min(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute minimum over entire datastream.

        Examples:
            >>> import ray
            >>> ray.data.range(100).min("id")
            0
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).min(["A", "B"])
            {'min(A)': 0, 'min(B)': 0}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the min; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The min result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise min of
              all columns,
            - ``on="col"``: a scalar representing the min of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise min of the provided columns.

            If the datastream is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Min, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def max(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute maximum over entire datastream.

        Examples:
            >>> import ray
            >>> ray.data.range(100).max("id")
            99
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).max(["A", "B"])
            {'max(A)': 99, 'max(B)': 9801}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the max; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The max result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise max of
              all columns,
            - ``on="col"``: a scalar representing the max of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise max of the provided columns.

            If the datastream is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Max, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def mean(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute mean over entire datastream.

        Examples:
            >>> import ray
            >>> ray.data.range(100).mean("id")
            49.5
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).mean(["A", "B"])
            {'mean(A)': 49.5, 'mean(B)': 3283.5}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the mean; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The mean result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise mean of
              all columns,
            - ``on="col"``: a scalar representing the mean of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise mean of the provided columns.

            If the datastream is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Mean, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def std(
        self,
        on: Optional[Union[str, List[str]]] = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> Union[Any, Dict[str, Any]]:
        """Compute standard deviation over entire datastream.

        Examples:
            >>> import ray
            >>> round(ray.data.range(100).std("id", ddof=0), 5)
            28.86607
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)]).std(["A", "B"])
            {'std(A)': 29.011491975882016, 'std(B)': 2968.1748039269296}

        .. note:: This uses Welford's online method for an accumulator-style computation
            of the standard deviation. This method was chosen due to it's numerical
            stability, and it being computable in a single pass. This may give different
            (but more accurate) results than NumPy, Pandas, and sklearn, which use a
            less numerically stable two-pass algorithm.
            See
            https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        Args:
            on: a column name or a list of column names to aggregate.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is ``N - ddof``, where ``N`` represents the number of elements.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the std; if ``False``,
                if a null value is encountered, the output will be None.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The standard deviation result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise std of
              all columns,
            - ``on="col"``: a scalar representing the std of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise std of the provided columns.

            If the datastream is empty, all values are null, or any value is null
            AND ``ignore_nulls`` is ``False``, then the output will be None.
        """
        ret = self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)
        return self._aggregate_result(ret)

    def sort(self, key: Optional[str] = None, descending: bool = False) -> "Datastream":
        """Sort the datastream by the specified key column or key function.

        Examples:
            >>> import ray
            >>> # Sort by a single column in descending order.
            >>> ds = ray.data.from_items(
            ...     [{"value": i} for i in range(1000)])
            >>> ds.sort("value", descending=True)
            Sort
            +- Datastream(num_blocks=200, num_rows=1000, schema={value: int64})

        Time complexity: O(datastream size * log(datastream size / parallelism))

        Args:
            key: The column to sort by. To sort by multiple columns, use a map function
                to generate the sort column beforehand.
            descending: Whether to sort in descending order.

        Returns:
            A new, sorted datastream.
        """

        plan = self._plan.with_stage(SortStage(self, key, descending))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Sort(
                logical_plan.dag,
                key=key,
                descending=descending,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    def zip(self, other: "Datastream") -> "Datastream":
        """Materialize and zip this datastream with the elements of another.

        The datastreams must have the same number of rows. Their column sets will be
        merged, and any duplicate column names disambiguated with _1, _2, etc. suffixes.

        .. note::
            The smaller of the two datastreams will be repartitioned to align the number
            of rows per block with the larger datastream.

        .. note::
            Zipped datastreams are not lineage-serializable, i.e. they can not be used
            as a tunable hyperparameter in Ray Tune.

        Examples:
            >>> import ray
            >>> ds1 = ray.data.range(5)
            >>> ds2 = ray.data.range(5)
            >>> ds1.zip(ds2).take_batch()
            {'id': array([0, 1, 2, 3, 4]), 'id_1': array([0, 1, 2, 3, 4])}

        Time complexity: O(datastream size / parallelism)

        Args:
            other: The datastream to zip with on the right hand side.

        Returns:
            A ``Datastream`` containing the columns of the second datastream
            concatenated horizontally with the columns of the first datastream,
            with duplicate column names disambiguated with _1, _2, etc. suffixes.
        """

        plan = self._plan.with_stage(ZipStage(other))

        logical_plan = self._logical_plan
        other_logical_plan = other._logical_plan
        if logical_plan is not None and other_logical_plan is not None:
            op = Zip(logical_plan.dag, other_logical_plan.dag)
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    @ConsumptionAPI
    def limit(self, limit: int) -> "Datastream":
        """Materialize and truncate the datastream to the first ``limit`` records.

        Contrary to :meth`.take`, this will not move any data to the caller's
        machine. Instead, it will return a new ``Datastream`` pointing to the truncated
        distributed data.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000)
            >>> ds.limit(5).take_batch()
            {'id': array([0, 1, 2, 3, 4])}

        Time complexity: O(limit specified)

        Args:
            limit: The size of the datastream to truncate to.

        Returns:
            The truncated datastream.
        """
        plan = self._plan.with_stage(LimitStage(limit))
        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Limit(logical_plan.dag, limit=limit)
            logical_plan = LogicalPlan(op)
        return Datastream(plan, self._epoch, self._lazy, logical_plan)

    @ConsumptionAPI(pattern="Time complexity:")
    def take_batch(
        self, batch_size: int = 20, *, batch_format: Optional[str] = "default"
    ) -> DataBatch:
        """Return up to ``batch_size`` records from the datastream in a batch.

        Unlike take(), the records are returned in the same format as used for
        `iter_batches` and `map_batches`.

        This will move up to ``batch_size`` records to the caller's machine; if
        ``batch_size`` is very large, this can result in an OutOfMemory crash on
        the caller.

        Time complexity: O(batch_size specified)

        Args:
            batch_size: The max number of records to return.
            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.

        Returns:
            A batch of up to ``batch_size`` records from the datastream.

        Raises:
            ValueError if the datastream is empty.
        """
        batch_format = _apply_strict_mode_batch_format(batch_format)
        try:
            res = next(
                self.iter_batches(
                    batch_size=batch_size, prefetch_batches=0, batch_format=batch_format
                )
            )
        except StopIteration:
            raise ValueError("The datastream is empty.")
        self._synchronize_progress_bar()
        return res

    @ConsumptionAPI(pattern="Time complexity:")
    def take(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Return up to ``limit`` records from the datastream.

        This will move up to ``limit`` records to the caller's machine; if
        ``limit`` is very large, this can result in an OutOfMemory crash on
        the caller.

        Time complexity: O(limit specified)

        Args:
            limit: The max number of records to return.

        Returns:
            A list of up to ``limit`` records from the datastream.
        """
        if ray.util.log_once("datastream_take"):
            logger.info(
                "Tip: Use `take_batch()` instead of `take() / show()` to return "
                "records in pandas or numpy batch format."
            )
        output = []
        for row in self.iter_rows():
            output.append(row)
            if len(output) >= limit:
                break
        self._synchronize_progress_bar()
        return output

    @ConsumptionAPI(pattern="Time complexity:")
    def take_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return all of the records in the datastream.

        This will move the entire datastream to the caller's machine; if the
        datastream is very large, this can result in an OutOfMemory crash on
        the caller.

        Time complexity: O(datastream size)

        Args:
            limit: Raise an error if the size exceeds the specified limit.

        Returns:
            A list of all the records in the datastream.
        """
        output = []
        for row in self.iter_rows():
            output.append(row)
            if limit is not None and len(output) > limit:
                raise ValueError(
                    f"The datastream has more than the given limit of {limit} records."
                )
        self._synchronize_progress_bar()
        return output

    @ConsumptionAPI(pattern="Time complexity:")
    def show(self, limit: int = 20) -> None:
        """Print up to the given number of records from the datastream.

        Time complexity: O(limit specified)

        Args:
            limit: The max number of records to print.
        """
        for row in self.take(limit):
            print(row)

    @ConsumptionAPI(
        if_more_than_read=True,
        datasource_metadata="row count",
        pattern="Time complexity:",
    )
    def count(self) -> int:
        """Count the number of records in the datastream.

        Time complexity: O(datastream size / parallelism), O(1) for parquet

        Returns:
            The number of records in the datastream.
        """
        # Handle empty datastream.
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

    @ConsumptionAPI(
        if_more_than_read=True,
        datasource_metadata="schema",
        extra_condition="or if ``fetch_if_missing=True`` (the default)",
        pattern="Time complexity:",
    )
    def schema(self, fetch_if_missing: bool = True) -> Optional["Schema"]:
        """Return the schema of the datastream.

        Time complexity: O(1)

        Args:
            fetch_if_missing: If True, synchronously fetch the schema if it's
                not known. If False, None is returned if the schema is not known.
                Default is True.

        Returns:
            The ``ray.data.Schema`` class of the records, or None if the
            schema is not known and fetch_if_missing is False.
        """
        ctx = DataContext.get_current()
        base_schema = self._plan.schema(fetch_if_missing=fetch_if_missing)
        if ctx.strict_mode:
            if base_schema:
                return Schema(base_schema)
            else:
                return None
        else:
            return base_schema

    def num_blocks(self) -> int:
        """Return the number of blocks of this datastream.

        Note that during read and transform operations, the number of blocks
        may be dynamically adjusted to respect memory limits, increasing the
        number of blocks at runtime.

        Time complexity: O(1)

        Returns:
            The number of blocks of this datastream.
        """
        return self._plan.initial_num_blocks()

    @ConsumptionAPI(if_more_than_read=True, pattern="Time complexity:")
    def size_bytes(self) -> int:
        """Return the in-memory size of the datastream.

        Time complexity: O(1)

        Returns:
            The in-memory size of the datastream in bytes, or None if the
            in-memory size is not known.
        """
        metadata = self._plan.execute().get_metadata()
        if not metadata or metadata[0].size_bytes is None:
            return None
        return sum(m.size_bytes for m in metadata)

    @ConsumptionAPI(if_more_than_read=True, pattern="Time complexity:")
    def input_files(self) -> List[str]:
        """Return the list of input files for the datastream.

        Time complexity: O(num input files)

        Returns:
            The list of input files used to create the datastream, or an empty
            list if the input files is not known.
        """
        metadata = self._plan.execute().get_metadata()
        files = set()
        for m in metadata:
            for f in m.input_files:
                files.add(f)
        return list(files)

    @ConsumptionAPI
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
        """Write the datastream to parquet.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.parquet, where ``uuid`` is an unique
        id for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_parquet("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where Parquet
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            arrow_parquet_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from arrow_parquet_args. This should be used
                instead of arrow_parquet_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each datastream block.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            arrow_parquet_args: Options to pass to
                pyarrow.parquet.write_table(), which is used to write out each
                block to a file.
        """
        self.write_datasource(
            ParquetDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=arrow_parquet_args_fn,
            **arrow_parquet_args,
        )

    @ConsumptionAPI
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
        """Write the datastream to json.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.json, where ``uuid`` is an
        unique id for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_json("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where json
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            pandas_json_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from pandas_json_args. This should be used
                instead of pandas_json_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each datastream block.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            pandas_json_args: These args will be passed to
                pandas.DataFrame.to_json(), which we use under the hood to
                write out each Datastream block. These
                are dict(orient="records", lines=True) by default.
        """
        self.write_datasource(
            JSONDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=pandas_json_args_fn,
            **pandas_json_args,
        )

    @ConsumptionAPI
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
        """Write the datastream to csv.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.csv, where ``uuid`` is an unique id
        for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_csv("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where csv
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            arrow_csv_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from arrow_csv_args. This should be used
                instead of arrow_csv_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each datastream block.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            arrow_csv_args: Other CSV write options to pass to pyarrow.
        """
        self.write_datasource(
            CSVDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            write_args_fn=arrow_csv_args_fn,
            **arrow_csv_args,
        )

    @ConsumptionAPI
    def write_tfrecords(
        self,
        path: str,
        *,
        tf_schema: Optional["schema_pb2.Schema"] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Write the datastream to TFRecord files.

        The `TFRecord <https://www.tensorflow.org/tutorials/load_data/tfrecord>`_
        files will contain
        `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_ # noqa: E501
        records, with one Example record for each row in the datastream.

        .. warning::
            tf.train.Feature only natively stores ints, floats, and bytes,
            so this function only supports datastreams with these data types,
            and will error if the datastream contains unsupported types.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.tfrecords, where ``uuid`` is an unique id
        for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.from_items([
            ...     { "name": "foo", "score": 42 },
            ...     { "name": "bar", "score": 43 },
            ... ])
            >>> ds.write_tfrecords("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where tfrecords
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.

        """

        self.write_datasource(
            TFRecordDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            tf_schema=tf_schema,
        )

    @PublicAPI(stability="alpha")
    @ConsumptionAPI
    def write_webdataset(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
        encoder: Optional[Union[bool, str, callable, list]] = True,
    ) -> None:
        """Write the datastream to WebDataset files.

        The `TFRecord <https://www.tensorflow.org/tutorials/load_data/tfrecord>`_
        files will contain
        `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_ # noqa: E501
        records, with one Example record for each row in the datastream.

        .. warning::
            tf.train.Feature only natively stores ints, floats, and bytes,
            so this function only supports datastreams with these data types,
            and will error if the datastream contains unsupported types.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.tfrecords, where ``uuid`` is an unique id
        for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.from_items([
            ...     { "name": "foo", "score": 42 },
            ...     { "name": "bar", "score": 43 },
            ... ])
            >>> ds.write_webdataset("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where tfrecords
                files will be written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.

        """

        from ray.data.datasource.webdataset_datasource import WebDatasetDatasource

        self.write_datasource(
            WebDatasetDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
            encoder=encoder,
        )

    @ConsumptionAPI
    def write_numpy(
        self,
        path: str,
        *,
        column: Optional[str] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Write a tensor column of the datastream to npy files.

        This is only supported for datastreams convertible to Arrow records that
        contain a TensorArray column. To control the number of files, use
        ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {self._uuid}_{block_idx}.npy, where ``uuid`` is an unique
        id for the datastream.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.write_numpy("s3://bucket/path") # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            path: The path to the destination root directory, where npy
                files will be written to.
            column: The name of the table column that contains the tensor to
                be written.
            filesystem: The filesystem implementation to write to.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
            block_path_provider: BlockWritePathProvider implementation to
                write each datastream block to a custom output path.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
        """
        context = DataContext.get_current()
        if context.strict_mode and not column:
            raise StrictModeError(
                "In strict mode, the column must be specified "
                "(e.g., `write_numpy(column='data')`)."
            )
        column = column or TENSOR_COLUMN_NAME

        self.write_datasource(
            NumpyDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            datastream_uuid=self._uuid,
            column=column,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            block_path_provider=block_path_provider,
        )

    @ConsumptionAPI
    def write_mongo(
        self,
        uri: str,
        database: str,
        collection: str,
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Write the datastream to a MongoDB datasource.

        This is only supported for datastreams convertible to Arrow records.
        To control the number of parallel write tasks, use ``.repartition()``
        before calling this method.

        .. note::
            Currently, this supports only a subset of the pyarrow's types, due to the
            limitation of pymongoarrow which is used underneath. Writing unsupported
            types will fail on type checking. See all the supported types at:
            https://mongo-arrow.readthedocs.io/en/latest/data_types.html.

        .. note::
            The records will be inserted into MongoDB as new documents. If a record has
            the _id field, this _id must be non-existent in MongoDB, otherwise the write
            will be rejected and fail (hence preexisting documents are protected from
            being mutated). It's fine to not have _id field in record and MongoDB will
            auto generate one at insertion.

        Examples:
            >>> import ray
            >>> import pandas as pd
            >>> docs = [{"title": "MongoDB Datasource test"} for key in range(4)]
            >>> ds = ray.data.from_pandas(pd.DataFrame(docs))
            >>> ds.write_mongo( # doctest: +SKIP
            >>>     MongoDatasource(), # doctest: +SKIP
            >>>     uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin", # noqa: E501 # doctest: +SKIP
            >>>     database="my_db", # doctest: +SKIP
            >>>     collection="my_collection", # doctest: +SKIP
            >>> ) # doctest: +SKIP

        Args:
            uri: The URI to the destination MongoDB where the datastream will be
                written to. For the URI format, see details in
                https://www.mongodb.com/docs/manual/reference/connection-string/.
            database: The name of the database. This database must exist otherwise
                ValueError will be raised.
            collection: The name of the collection in the database. This collection
                must exist otherwise ValueError will be raised.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
        """
        from ray.data.datasource import MongoDatasource

        self.write_datasource(
            MongoDatasource(),
            ray_remote_args=ray_remote_args,
            uri=uri,
            database=database,
            collection=collection,
        )

    @ConsumptionAPI
    def write_datasource(
        self,
        datasource: Datasource,
        *,
        ray_remote_args: Dict[str, Any] = None,
        **write_args,
    ) -> None:
        """Write the datastream to a custom datasource.

        Examples:
            >>> import ray
            >>> from ray.data.datasource import Datasource
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> class CustomDatasource(Datasource): # doctest: +SKIP
            ...     # define custom data source
            ...     pass # doctest: +SKIP
            >>> ds.write_datasource(CustomDatasource(...)) # doctest: +SKIP

        Time complexity: O(datastream size / parallelism)

        Args:
            datasource: The datasource to write to.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            write_args: Additional write args to pass to the datasource.
        """
        if ray_remote_args is None:
            ray_remote_args = {}
        path = write_args.get("path", None)
        if path and _is_local_scheme(path):
            if ray.util.client.ray.is_connected():
                raise ValueError(
                    f"The local scheme paths {path} are not supported in Ray Client."
                )
            ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(),
                soft=False,
            )

        if type(datasource).write != Datasource.write:
            write_fn = generate_write_fn(datasource, **write_args)

            def write_fn_wrapper(blocks: Iterator[Block], ctx, fn) -> Iterator[Block]:
                return write_fn(blocks, ctx)

            plan = self._plan.with_stage(
                OneToOneStage(
                    "Write",
                    write_fn_wrapper,
                    TaskPoolStrategy(),
                    ray_remote_args,
                    fn=lambda x: x,
                )
            )

            logical_plan = self._logical_plan
            if logical_plan is not None:
                write_op = Write(
                    logical_plan.dag,
                    datasource,
                    ray_remote_args=ray_remote_args,
                    **write_args,
                )
                logical_plan = LogicalPlan(write_op)

            try:
                import pandas as pd

                self._write_ds = Datastream(
                    plan, self._epoch, self._lazy, logical_plan
                ).materialize()
                blocks = ray.get(self._write_ds._plan.execute().get_blocks())
                assert all(
                    isinstance(block, pd.DataFrame) and len(block) == 1
                    for block in blocks
                )
                write_results = [block["write_result"][0] for block in blocks]
                datasource.on_write_complete(write_results)
            except Exception as e:
                datasource.on_write_failed([], e)
                raise
        else:
            logger.warning(
                "The Datasource.do_write() is deprecated in "
                "Ray 2.4 and will be removed in future release. Use "
                "Datasource.write() instead."
            )

            ctx = DataContext.get_current()
            blocks, metadata = zip(*self._plan.execute().get_blocks_with_metadata())
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

    @ConsumptionAPI(
        delegate=(
            "Calling any of the consumption methods on the returned ``DataIterator``"
        )
    )
    def iterator(self) -> DataIterator:
        """Return a :class:`~ray.data.DataIterator` that
        can be used to repeatedly iterate over the datastream.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(
            ...     1000000
            ... ).iterator().iter_batches(): # doctest: +SKIP
            ...     print(batch) # doctest: +SKIP

        .. note::
            It is recommended to use ``DataIterator`` methods over directly
            calling methods such as ``iter_batches()``.
        """
        return DataIteratorImpl(self)

    @ConsumptionAPI
    def iter_rows(self, *, prefetch_blocks: int = 0) -> Iterator[Dict[str, Any]]:
        """Return a local row iterator over the datastream.

        Examples:
            >>> import ray
            >>> for i in ray.data.range(1000000).iter_rows(): # doctest: +SKIP
            ...     print(i) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.

        Returns:
            A local iterator over the entire datastream.
        """

        return self.iterator().iter_rows(prefetch_blocks=prefetch_blocks)

    @ConsumptionAPI
    def iter_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
        # Deprecated.
        prefetch_blocks: int = 0,
    ) -> Iterator[DataBatch]:
        """Return a local batched iterator over the datastream.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(1000000).iter_batches(): # doctest: +SKIP
            ...     print(batch) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datastreamContext.
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
                the buffer, the remaining rows in the buffer will be drained.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over record batches.
        """
        batch_format = _apply_strict_mode_batch_format(batch_format)
        if batch_format == "native":
            logger.warning("The 'native' batch format has been renamed 'default'.")
        return self.iterator().iter_batches(
            prefetch_batches=prefetch_batches,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            _collate_fn=_collate_fn,
        )

    @ConsumptionAPI
    def iter_torch_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        collate_fn: Optional[
            Callable[[Union[np.ndarray, Dict[str, np.ndarray]]], Any]
        ] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        # Deprecated
        prefetch_blocks: int = 0,
    ) -> Iterator["TorchTensorBatchType"]:
        """Return a local batched iterator of Torch Tensors over the datastream.

        This iterator will yield single-tensor batches if the underlying datastream
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
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datastreamContext.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The Torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data.
            device: The device on which the tensor should be placed; if None, the Torch
                tensor will be constructed on the CPU.
            collate_fn: A function to convert a Numpy batch to a PyTorch tensor batch.
                Potential use cases include collating along a dimension other than the
                first, padding sequences of various lengths, or generally handling
                batches of different length tensors. If not provided, the default
                collate function is used which simply converts the batch of numpy
                arrays to a batch of PyTorch tensors. This API is still experimental
                and is subject to change.
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
        return self.iterator().iter_torch_batches(
            prefetch_batches=prefetch_batches,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            dtypes=dtypes,
            device=device,
            collate_fn=collate_fn,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    @ConsumptionAPI
    def iter_tf_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["tf.dtypes.DType", Dict[str, "tf.dtypes.DType"]]] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        # Deprecated
        prefetch_blocks: int = 0,
    ) -> Iterator[TensorFlowTensorBatchType]:
        """Return a local batched iterator of TensorFlow Tensors over the datastream.

        This iterator will yield single-tensor batches of the underlying datastream
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors.

        .. tip::
            If you don't need the additional flexibility provided by this method,
            consider using :meth:`~ray.data.Datastream.to_tf` instead. It's easier
            to use.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range( # doctest: +SKIP
            ...     12,
            ... ).iter_tf_batches(batch_size=4):
            ...     print(batch.shape) # doctest: +SKIP
            (4, 1)
            (4, 1)
            (4, 1)

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datastreamContext.
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
        return self.iterator().iter_tf_batches(
            prefetch_batches=prefetch_batches,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            dtypes=dtypes,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    @ConsumptionAPI(pattern="Time complexity:")
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
        prefetch_batches: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        unsqueeze_label_tensor: bool = True,
        unsqueeze_feature_tensors: bool = True,
        # Deprecated
        prefetch_blocks: int = 0,
    ) -> "torch.utils.data.IterableDataset":
        """Return a Torch IterableDataset over this datastream.

        This is only supported for datastreams convertible to Arrow records.

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
        ``Datastream`` will be treated as the label, and the output label tensor
        will be ``None``.

        Note that you probably want to call ``.split()`` on this datastream if
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
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datastreamContext.
            drop_last: Set to True to drop the last incomplete batch,
                if the datastream size is not divisible by the batch size. If
                False and the size of the stream is not divisible by the batch
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

        return self.iterator().to_torch(
            label_column=label_column,
            feature_columns=feature_columns,
            label_column_dtype=label_column_dtype,
            feature_column_dtypes=feature_column_dtypes,
            batch_size=batch_size,
            prefetch_blocks=prefetch_blocks,
            prefetch_batches=prefetch_batches,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            unsqueeze_label_tensor=unsqueeze_label_tensor,
            unsqueeze_feature_tensors=unsqueeze_feature_tensors,
        )

    @ConsumptionAPI
    def to_tf(
        self,
        feature_columns: Union[str, List[str]],
        label_columns: Union[str, List[str]],
        *,
        prefetch_batches: int = 1,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        # Deprecated
        prefetch_blocks: int = 0,
    ) -> "tf.data.Dataset":
        """Return a TF Dataset over this datastream.

        .. warning::
            If your datastream contains ragged tensors, this method errors. To prevent
            errors, resize tensors or
            :ref:`disable tensor extension casting <disable_tensor_extension_casting>`.

        Examples:
            >>> import ray
            >>> ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
            >>> ds
            Datastream(
               num_blocks=1,
               num_rows=150,
               schema={
                  sepal length (cm): double,
                  sepal width (cm): double,
                  petal length (cm): double,
                  petal width (cm): double,
                  target: int64
               }
            )

            If your model accepts a single tensor as input, specify a single feature column.

            >>> ds.to_tf(feature_columns="sepal length (cm)", label_columns="target")  # doctest: +SKIP
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your model accepts a dictionary as input, specify a list of feature columns.

            >>> ds.to_tf(["sepal length (cm)", "sepal width (cm)"], "target")  # doctest: +SKIP
            <_OptionsDataset element_spec=({'sepal length (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), 'sepal width (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal width (cm)')}, TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your datastream contains multiple features but your model accepts a single
            tensor as input, combine features with
            :class:`~ray.data.preprocessors.Concatenator`.

            >>> from ray.data.preprocessors import Concatenator
            >>> preprocessor = Concatenator(output_column_name="features", exclude="target")
            >>> ds = preprocessor.transform(ds)
            >>> ds
            Concatenator
            +- Datastream(
                  num_blocks=1,
                  num_rows=150,
                  schema={
                     sepal length (cm): double,
                     sepal width (cm): double,
                     petal length (cm): double,
                     petal width (cm): double,
                     target: int64
                  }
               )
            >>> ds.to_tf("features", "target")  # doctest: +SKIP
            <_OptionsDataset element_spec=(TensorSpec(shape=(None, 4), dtype=tf.float64, name='features'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

        Args:
            feature_columns: Columns that correspond to model inputs. If this is a
                string, the input data is a tensor. If this is a list, the input data
                is a ``dict`` that maps column names to their tensor representation.
            label_column: Columns that correspond to model targets. If this is a
                string, the target data is a tensor. If this is a list, the target data
                is a ``dict`` that maps column names to their tensor representation.
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datastreamContext.
            batch_size: Record batch size. Defaults to 1.
            drop_last: Set to True to drop the last incomplete batch,
                if the datastream size is not divisible by the batch size. If
                False and the size of the stream is not divisible by the batch
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
            A ``tf.data.Dataset`` that yields inputs and targets.

        .. seealso::

            :meth:`~ray.data.Datastream.iter_tf_batches`
                Call this method if you need more flexibility.

        """  # noqa: E501

        return self.iterator().to_tf(
            feature_columns=feature_columns,
            label_columns=label_columns,
            prefetch_batches=prefetch_batches,
            prefetch_blocks=prefetch_blocks,
            drop_last=drop_last,
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    @ConsumptionAPI(pattern="Time complexity:")
    def to_dask(
        self,
        meta: Union[
            "pandas.DataFrame",
            "pandas.Series",
            Dict[str, Any],
            Iterable[Any],
            Tuple[Any],
            None,
        ] = None,
    ) -> "dask.DataFrame":
        """Convert this datastream into a Dask DataFrame.

        This is only supported for datastreams convertible to Arrow records.

        Note that this function will set the Dask scheduler to Dask-on-Ray
        globally, via the config.

        Time complexity: O(datastream size / parallelism)

        Args:
            meta: An empty pandas DataFrame or Series that matches the dtypes and column
                names of the stream. This metadata is necessary for many algorithms in
                dask dataframe to work. For ease of use, some alternative inputs are
                also available. Instead of a DataFrame, a dict of ``{name: dtype}`` or
                iterable of ``(name, dtype)`` can be provided (note that the order of
                the names should match the order of the columns). Instead of a series, a
                tuple of ``(name, dtype)`` can be used.
                By default, this will be inferred from the underlying Datastream schema,
                with this argument supplying an optional override.

        Returns:
            A Dask DataFrame created from this datastream.
        """
        import dask
        import dask.dataframe as dd
        import pandas as pd

        try:
            import pyarrow as pa
        except Exception:
            pa = None

        from ray.data._internal.pandas_block import PandasBlockSchema
        from ray.util.client.common import ClientObjectRef
        from ray.util.dask import ray_dask_get

        dask.config.set(scheduler=ray_dask_get)

        @dask.delayed
        def block_to_df(block: Block):
            if isinstance(block, (ray.ObjectRef, ClientObjectRef)):
                raise ValueError(
                    "Datastream.to_dask() must be used with Dask-on-Ray, please "
                    "set the Dask scheduler to ray_dask_get (located in "
                    "ray.util.dask)."
                )
            return _block_to_df(block)

        if meta is None:
            from ray.data.extensions import TensorDtype

            # Infer Dask metadata from Datastream schema.
            schema = self.schema(fetch_if_missing=True)
            if isinstance(schema, PandasBlockSchema):
                meta = pd.DataFrame(
                    {
                        col: pd.Series(
                            dtype=(
                                dtype
                                if not isinstance(dtype, TensorDtype)
                                else np.object_
                            )
                        )
                        for col, dtype in zip(schema.names, schema.types)
                    }
                )
            elif pa is not None and isinstance(schema, pa.Schema):
                from ray.data.extensions import ArrowTensorType

                if any(isinstance(type_, ArrowTensorType) for type_ in schema.types):
                    meta = pd.DataFrame(
                        {
                            col: pd.Series(
                                dtype=(
                                    dtype.to_pandas_dtype()
                                    if not isinstance(dtype, ArrowTensorType)
                                    else np.object_
                                )
                            )
                            for col, dtype in zip(schema.names, schema.types)
                        }
                    )
                else:
                    meta = schema.empty_table().to_pandas()

        ddf = dd.from_delayed(
            [block_to_df(block) for block in self.get_internal_block_refs()],
            meta=meta,
        )
        return ddf

    @ConsumptionAPI(pattern="Time complexity:")
    def to_mars(self) -> "mars.DataFrame":
        """Convert this datastream into a MARS dataframe.

        Time complexity: O(datastream size / parallelism)

        Returns:
            A MARS dataframe created from this datastream.
        """
        import pandas as pd
        import pyarrow as pa
        from mars.dataframe.datasource.read_raydataset import DataFrameReadRayDataset
        from mars.dataframe.utils import parse_index

        from ray.data._internal.pandas_block import PandasBlockSchema

        refs = self.to_pandas_refs()
        # remove this when https://github.com/mars-project/mars/issues/2945 got fixed
        schema = self.schema()
        if isinstance(schema, Schema):
            schema = schema.base_schema  # Backwards compat with non strict mode.
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

    @ConsumptionAPI(pattern="Time complexity:")
    def to_modin(self) -> "modin.DataFrame":
        """Convert this datastream into a Modin dataframe.

        This works by first converting this datastream into a distributed set of
        Pandas dataframes (using ``.to_pandas_refs()``). Please see caveats
        there. Then the individual dataframes are used to create the modin
        DataFrame using
        ``modin.distributed.dataframe.pandas.partitions.from_partitions()``.

        This is only supported for datastreams convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(datastream size / parallelism)

        Returns:
            A Modin dataframe created from this datastream.
        """

        from modin.distributed.dataframe.pandas.partitions import from_partitions

        pd_objs = self.to_pandas_refs()
        return from_partitions(pd_objs, axis=0)

    @ConsumptionAPI(pattern="Time complexity:")
    def to_spark(self, spark: "pyspark.sql.SparkSession") -> "pyspark.sql.DataFrame":
        """Convert this datastream into a Spark dataframe.

        Time complexity: O(datastream size / parallelism)

        Returns:
            A Spark dataframe created from this datastream.
        """
        import raydp

        schema = self.schema()
        if isinstance(schema, Schema):
            schema = schema.base_schema  # Backwards compat with non strict mode.
        return raydp.spark.ray_dataset_to_spark_dataframe(
            spark, schema, self.get_internal_block_refs()
        )

    @ConsumptionAPI(pattern="Time complexity:")
    def to_pandas(self, limit: int = 100000) -> "pandas.DataFrame":
        """Convert this datastream into a single Pandas DataFrame.

        This is only supported for datastreams convertible to Arrow or Pandas
        records. An error is raised if the number of records exceeds the
        provided limit. Note that you can use ``.limit()`` on the datastream
        beforehand to truncate the datastream manually.

        Time complexity: O(datastream size)

        Args:
            limit: The maximum number of records to return. An error will be
                raised if the limit is exceeded.

        Returns:
            A Pandas DataFrame created from this datastream, containing a limited
            number of records.
        """
        count = self.count()
        if count > limit:
            raise ValueError(
                f"the datastream has more than the given limit of {limit} "
                f"records: {count}. If you are sure that a DataFrame with "
                f"{count} rows will fit in local memory, use "
                f"ds.to_pandas(limit={count})."
            )
        blocks = self.get_internal_block_refs()
        output = DelegatingBlockBuilder()
        for block in blocks:
            output.add_block(ray.get(block))
        block = output.build()
        return _block_to_df(block)

    @ConsumptionAPI(pattern="Time complexity:")
    @DeveloperAPI
    def to_pandas_refs(self) -> List[ObjectRef["pandas.DataFrame"]]:
        """Convert this datastream into a distributed set of Pandas dataframes.

        This is only supported for datastreams convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(datastream size / parallelism)

        Returns:
            A list of remote Pandas dataframes created from this datastream.
        """

        block_to_df = cached_remote_fn(_block_to_df)
        return [block_to_df.remote(block) for block in self.get_internal_block_refs()]

    @DeveloperAPI
    def to_numpy_refs(
        self, *, column: Optional[str] = None
    ) -> List[ObjectRef[np.ndarray]]:
        """Convert this datastream into a distributed set of NumPy ndarrays.

        This is only supported for datastreams convertible to NumPy ndarrays.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using ``.to_arrow()`` or
        ``.get_internal_block_refs()``.

        Time complexity: O(datastream size / parallelism)

        Args:
            column: The name of the column to convert to numpy, or None to specify the
            entire row. If not specified for Arrow or Pandas blocks, each returned
            future will represent a dict of column ndarrays.

        Returns:
            A list of remote NumPy ndarrays created from this datastream.
        """
        block_to_ndarray = cached_remote_fn(_block_to_ndarray)
        return [
            block_to_ndarray.remote(block, column=column)
            for block in self.get_internal_block_refs()
        ]

    @ConsumptionAPI(pattern="Time complexity:")
    @DeveloperAPI
    def to_arrow_refs(self) -> List[ObjectRef["pyarrow.Table"]]:
        """Convert this datastream into a distributed set of Arrow tables.

        This is only supported for datastreams convertible to Arrow records.
        This function is zero-copy if the existing data is already in Arrow
        format. Otherwise, the data will be converted to Arrow format.

        Time complexity: O(1) unless conversion is required.

        Returns:
            A list of remote Arrow tables created from this datastream.
        """
        import pyarrow as pa

        blocks: List[ObjectRef["pyarrow.Table"]] = self.get_internal_block_refs()
        # Schema is safe to call since we have already triggered execution with
        # get_internal_block_refs.
        schema = self.schema(fetch_if_missing=True)
        if isinstance(schema, Schema):
            schema = schema.base_schema  # Backwards compat with non strict mode.
        if isinstance(schema, pa.Schema):
            # Zero-copy path.
            return blocks

        block_to_arrow = cached_remote_fn(_block_to_arrow)
        return [block_to_arrow.remote(block) for block in blocks]

    @ConsumptionAPI(pattern="Args:")
    def to_random_access_dataset(
        self,
        key: str,
        num_workers: Optional[int] = None,
    ) -> RandomAccessDataset:
        """Convert this datastream into a distributed RandomAccessDataset (EXPERIMENTAL).

        RandomAccessDataset partitions the datastream across the cluster by the given
        sort key, providing efficient random access to records via binary search. A
        number of worker actors are created, each of which has zero-copy access to the
        underlying sorted data blocks of the datastream.

        Note that the key must be unique in the datastream. If there are duplicate keys,
        an arbitrary value is returned.

        This is only supported for Arrow-format datastreams.

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

    @ConsumptionAPI
    def repeat(self, times: Optional[int] = None) -> "DatasetPipeline":
        """Convert this into a DatasetPipeline by looping over this datastream.

        Transformations prior to the call to ``repeat()`` are evaluated once.
        Transformations done on the returned pipeline are evaluated on each
        loop of the pipeline over the base datastream.

        Note that every repeat of the datastream is considered an "epoch" for
        the purposes of ``DatasetPipeline.iter_epochs()``.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(5, parallelism=1)
            >>> # Infinite pipeline of numbers [0, 5)
            >>> ds.repeat().take_batch()
            {'id': array([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...])}
            >>> # Can shuffle each epoch (datastream) in the pipeline.
            >>> ds.repeat().random_shuffle().take_batch() # doctest: +SKIP
            {'id': array([2, 3, 0, 4, 1, 4, 0, 2, 1, 3, ...])}

        Args:
            times: The number of times to loop over this datastream, or None
                to repeat indefinitely.
        """
        from ray.data._internal.plan import _rewrite_read_stage
        from ray.data.dataset_pipeline import DatasetPipeline

        ctx = DataContext.get_current()
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
        outer_stats.datastream_uuid = uuid

        if times is not None and times < 1:
            raise ValueError("`times` must be >= 1, got {}".format(times))

        class Iterator:
            def __init__(self, blocks):
                self._blocks = blocks
                self._i = 0

            def __next__(self) -> Callable[[], "Datastream"]:
                if times and self._i >= times:
                    raise StopIteration
                epoch = self._i
                blocks = self._blocks
                self._i += 1

                def gen():
                    ds = Datastream(
                        ExecutionPlan(
                            blocks,
                            outer_stats,
                            datastream_uuid=uuid,
                            run_by_consumer=True,
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
                lambda ds, read_stage=read_stage: Datastream(
                    ds._plan.with_stage(read_stage), ds._epoch, True
                )
            )
        return pipe

    def window(
        self,
        *,
        blocks_per_window: Optional[int] = None,
        bytes_per_window: Optional[int] = None,
    ) -> "DatasetPipeline":
        """Convert this into a DatasetPipeline by windowing over data blocks.

        Transformations prior to the call to ``window()`` are evaluated in
        bulk on the entire datastream. Transformations done on the returned
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

        ctx = DataContext.get_current()
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

            def __next__(self) -> "Datastream":
                if not self._splits:
                    raise StopIteration

                blocks = self._splits.pop(0)

                def gen():
                    ds = Datastream(
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
                            "base datastream's parallelism and/or adjusting the "
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
                lambda ds, read_stage=read_stage: Datastream(
                    ds._plan.with_stage(read_stage), ds._epoch, True
                )
            )
        return pipe

    @Deprecated(message="Use `Datastream.materialize()` instead.")
    def fully_executed(self) -> "MaterializedDatastream":
        logger.warning(
            "Deprecation warning: use Datastream.materialize() instead of "
            "fully_executed()."
        )
        self._plan.execute(force_read=True)
        return self

    @Deprecated(
        message="Check `isinstance(Datastream, MaterializedDatastream)` instead."
    )
    def is_fully_executed(self) -> bool:
        logger.warning(
            "Deprecation warning: Check "
            "`isinstance(Datastream, MaterializedDatastream)` "
            "instead of using is_fully_executed()."
        )
        return self._plan.has_computed_output()

    @ConsumptionAPI(pattern="store memory.", insert_after=True)
    def materialize(self) -> "MaterializedDatastream":
        """Execute and materialize this datastream into object store memory.

        This can be used to read all blocks into memory. By default, Datastream
        doesn't read blocks from the datasource until the first transform.

        Note that this does not mutate the original Datastream. Only the blocks of the
        returned MaterializedDatastream class are pinned in memory.

        Returns:
            A MaterializedDatastream holding the materialized data blocks.
        """
        copy = Datastream.copy(self, _deep_copy=True, _as=MaterializedDatastream)
        copy._plan.execute(force_read=True)
        return copy

    @ConsumptionAPI(pattern="timing information.", insert_after=True)
    def stats(self) -> str:
        """Returns a string containing execution timing information.

        Note that this does not trigger execution, so if the datastream has not yet
        executed, an empty string will be returned.
        """
        return self._get_stats_summary().to_string()

    def _get_stats_summary(self) -> DatastreamStatsSummary:
        return self._plan.stats_summary()

    @ConsumptionAPI(pattern="Time complexity:")
    @DeveloperAPI
    def get_internal_block_refs(self) -> List[ObjectRef[Block]]:
        """Get a list of references to the underlying blocks of this datastream.

        This function can be used for zero-copy access to the data. It blocks
        until the underlying blocks are computed.

        Time complexity: O(1)

        Returns:
            A list of references to this datastream's blocks.
        """
        blocks = self._plan.execute().get_blocks()
        self._synchronize_progress_bar()
        return blocks

    @Deprecated(
        message="Datastream is lazy by default, so this conversion call is no longer "
        "needed and this API will be removed in a future release"
    )
    def lazy(self) -> "Datastream":
        """Enable lazy evaluation.

        Datastream is lazy by default, so this is only useful for datastreams created
        from :func:`ray.data.from_items() <ray.data.read_api.from_items>`, which is
        eager.

        The returned datastream is a lazy datastream, where all subsequent operations
        on the stream won't be executed until the datastream is consumed
        (e.g. ``.take()``, ``.iter_batches()``, ``.to_torch()``, ``.to_tf()``, etc.)
        or execution is manually triggered via ``.materialize()``.
        """
        ds = Datastream(
            self._plan, self._epoch, lazy=True, logical_plan=self._logical_plan
        )
        ds._set_uuid(self._get_uuid())
        return ds

    def has_serializable_lineage(self) -> bool:
        """Whether this datastream's lineage is able to be serialized for storage and
        later deserialized, possibly on a different cluster.

        Only datastreams that are created from data that we know will still exist at
        deserialization time, e.g. data external to this Ray cluster such as persistent
        cloud object stores, support lineage-based serialization. All of the
        ray.data.read_*() APIs support lineage-based serialization.
        """
        return self._plan.has_lazy_input()

    @DeveloperAPI
    def serialize_lineage(self) -> bytes:
        """
        Serialize this datastream's lineage, not the actual data or the existing data
        futures, to bytes that can be stored and later deserialized, possibly on a
        different cluster.

        Note that this will drop all computed data, and that everything will be
        recomputed from scratch after deserialization.

        Use :py:meth:`Datastream.deserialize_lineage` to deserialize the serialized
        bytes returned from this method into a Datastream.

        .. note::
            Unioned and zipped datastreams, produced by :py:meth`Datastream.union` and
            :py:meth:`Datastream.zip`, are not lineage-serializable.

        Returns:
            Serialized bytes containing the lineage of this datastream.
        """
        if not self.has_serializable_lineage():
            raise ValueError(
                "Lineage-based serialization is not supported for this stream, which "
                "means that it cannot be used as a tunable hyperparameter. "
                "Lineage-based serialization is explicitly NOT supported for unioned "
                "or zipped datastreams (see docstrings for those methods), and is only "
                "supported for datastreams created from data that we know will still "
                "exist at deserialization time, e.g. external data in persistent cloud "
                "object stores or in-memory data from long-lived clusters. Concretely, "
                "all ray.data.read_*() APIs should support lineage-based "
                "serialization, while all of the ray.data.from_*() APIs do not. To "
                "allow this stream to be serialized to storage, write the data to an "
                "external store (such as AWS S3, GCS, or Azure Blob Storage) using the "
                "Datastream.write_*() APIs, and serialize a new datastream reading "
                "from the external store using the ray.data.read_*() APIs."
            )
        # Copy Datastream and clear the blocks from the execution plan so only the
        # Datastream's lineage is serialized.
        plan_copy = self._plan.deep_copy(preserve_uuid=True)
        ds = Datastream(plan_copy, self._get_epoch(), self._lazy)
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
    def deserialize_lineage(serialized_ds: bytes) -> "Datastream":
        """
        Deserialize the provided lineage-serialized Datastream.

        This assumes that the provided serialized bytes were serialized using
        :py:meth:`Datastream.serialize_lineage`.

        Args:
            serialized_ds: The serialized Datastream that we wish to deserialize.

        Returns:
            A deserialized ``Datastream`` instance.
        """
        return pickle.loads(serialized_ds)

    def _divide(self, block_idx: int) -> ("Datastream", "Datastream"):
        block_list = self._plan.execute()
        left, right = block_list.divide(block_idx)
        l_ds = Datastream(
            ExecutionPlan(
                left, self._plan.stats(), run_by_consumer=block_list._owned_by_consumer
            ),
            self._epoch,
            self._lazy,
        )
        r_ds = Datastream(
            ExecutionPlan(
                right, self._plan.stats(), run_by_consumer=block_list._owned_by_consumer
            ),
            self._epoch,
            self._lazy,
        )
        return l_ds, r_ds

    @Deprecated(message="The batch format is no longer exposed as a public API.")
    def default_batch_format(self) -> Type:
        context = DataContext.get_current()
        if context.strict_mode:
            raise StrictModeError(
                "default_batch_format() is not allowed in strict mode"
            )

        import pandas as pd
        import pyarrow as pa

        schema = self.schema()
        assert isinstance(schema, (type, PandasBlockSchema, pa.Schema))

        if isinstance(schema, type):
            return list

        if isinstance(schema, (PandasBlockSchema, pa.Schema)):
            if schema.names == [TENSOR_COLUMN_NAME]:
                return np.ndarray
            return pd.DataFrame

    @Deprecated(message="The dataset format is no longer exposed as a public API.")
    def dataset_format(self) -> BlockFormat:
        context = DataContext.get_current()
        if context.strict_mode:
            raise StrictModeError("dataset_format() is not allowed in strict mode")

        if context.use_streaming_executor:
            raise DeprecationWarning(
                "`dataset_format` is deprecated for streaming execution. To use "
                "`dataset_format`, you must explicitly enable bulk execution by "
                "setting `use_streaming_executor` to False in the `DataContext`"
            )

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

    def _aggregate_on(
        self, agg_cls: type, on: Optional[Union[str, List[str]]], *args, **kwargs
    ):
        """Helper for aggregating on a particular subset of the datastream.

        This validates the `on` argument, and converts a list of column names
        or lambdas to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Datastream, and a single
        aggregation on the entire row for a simple Datastream.
        """
        aggs = self._build_multicolumn_aggs(agg_cls, on, *args, **kwargs)
        return self.aggregate(*aggs)

    def _build_multicolumn_aggs(
        self,
        agg_cls: type,
        on: Optional[Union[str, List[str]]],
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
            schema = self.schema(fetch_if_missing=True)
            if schema is not None and not isinstance(schema, type):
                if not skip_cols:
                    skip_cols = []
                if len(schema.names) > 0:
                    on = [col for col in schema.names if col not in skip_cols]

        if not isinstance(on, list):
            on = [on]
        return [agg_cls(on_, *args, ignore_nulls=ignore_nulls, **kwargs) for on_ in on]

    def _aggregate_result(self, result: Union[Tuple, Mapping]) -> U:
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

    @ensure_notebook_deps(
        ["ipywidgets", "8"],
    )
    @fallback_if_colab
    def _ipython_display_(self):
        from ipywidgets import HTML, VBox, Layout
        from IPython.display import display

        title = HTML(f"<h2>{self.__class__.__name__}</h2>")
        tab = self._tab_repr_()

        if tab:
            display(VBox([title, tab], layout=Layout(width="100%")))

    @ensure_notebook_deps(
        ["tabulate", None],
        ["ipywidgets", "8"],
    )
    def _tab_repr_(self):
        from ray._private.thirdparty.tabulate.tabulate import tabulate
        from ipywidgets import Tab, HTML

        metadata = {
            "num_blocks": self._plan.initial_num_blocks(),
            "num_rows": self._meta_count(),
        }
        # Show metadata if available, but don't trigger execution.
        schema = self.schema(fetch_if_missing=False)
        if schema is None:
            schema_repr = Template("rendered_html_common.html.j2").render(
                content="<h5>Unknown schema</h5>"
            )
        elif isinstance(schema, type):
            schema_repr = Template("rendered_html_common.html.j2").render(
                content=f"<h5>Data type: <code>{html.escape(str(schema))}</code></h5>"
            )
        else:
            schema_data = {}
            for sname, stype in zip(schema.names, schema.types):
                schema_data[sname] = getattr(stype, "__name__", str(stype))

            schema_repr = Template("scrollableTable.html.j2").render(
                table=tabulate(
                    tabular_data=schema_data.items(),
                    tablefmt="html",
                    showindex=False,
                    headers=["Name", "Type"],
                ),
                max_height="300px",
            )

        children = []
        children.append(
            HTML(
                Template("scrollableTable.html.j2").render(
                    table=tabulate(
                        tabular_data=metadata.items(),
                        tablefmt="html",
                        showindex=False,
                        headers=["Field", "Value"],
                    ),
                    max_height="300px",
                )
            )
        )
        children.append(HTML(schema_repr))
        return Tab(children, titles=["Metadata", "Schema"])

    def __repr__(self) -> str:
        return self._plan.get_plan_as_string(self.__class__.__name__)

    def __str__(self) -> str:
        return repr(self)

    def __bool__(self) -> bool:
        # Prevents `__len__` from being called to check if it is None
        # see: issue #25152
        return True

    def __len__(self) -> int:
        raise AttributeError(
            "Use `ds.count()` to compute the length of a distributed Datastream. "
            "This may be an expensive operation."
        )

    def __iter__(self):
        raise TypeError(
            "`Datastream` objects aren't iterable. To iterate records, call "
            "`ds.iter_rows()` or `ds.iter_batches()`. For more information, read "
            "https://docs.ray.io/en/latest/data/consuming-datastreams.html."
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
        if ray.util.log_once("datastream_slow_warned"):
            logger.warning(
                "The `map`, `flat_map`, and `filter` operations are unvectorized and "
                "can be very slow. Consider using `.map_batches()` instead."
            )

    def _synchronize_progress_bar(self):
        """Flush progress bar output by shutting down the current executor.

        This should be called at the end of all blocking APIs (e.g., `take`), but not
        async APIs (e.g., `iter_batches`).

        The streaming executor runs in a separate generator / thread, so it is
        possible the shutdown logic runs even after a call to retrieve rows from the
        stream has finished. Explicit shutdown avoids this, which can clobber console
        output (https://github.com/ray-project/ray/issues/32414).
        """
        if self._current_executor:
            self._current_executor.shutdown()
            self._current_executor = None

    def __getstate__(self):
        # Note: excludes _current_executor which is not serializable.
        return {
            "plan": self._plan,
            "uuid": self._uuid,
            "epoch": self._epoch,
            "lazy": self._lazy,
            "logical_plan": self._logical_plan,
        }

    def __setstate__(self, state):
        self._plan = state["plan"]
        self._uuid = state["uuid"]
        self._epoch = state["epoch"]
        self._lazy = state["lazy"]
        self._logical_plan = state["logical_plan"]
        self._current_executor = None

    def __del__(self):
        if sys.meta_path is None:
            return
        if self._current_executor and ray is not None and ray.is_initialized():
            self._current_executor.shutdown()


# Backwards compatibility alias.
Dataset = Datastream


@PublicAPI
class MaterializedDatastream(Datastream, Generic[T]):
    """A Datastream materialized in Ray memory, e.g., via `.materialize()`.

    The blocks of a MaterializedDatastream object are materialized into Ray object store
    memory, which means that this class can be shared or iterated over by multiple Ray
    tasks without re-executing the underlying computations for producing the stream.
    """

    pass


@PublicAPI(stability="beta")
class Schema:
    """Datastream schema.

    Attributes:
        names: List of column names of this Datastream.
        types: List of Arrow types of the Datastream. Note that the "object" type is
            not Arrow compatible and hence will be returned as `object`.
        base_schema: The underlying Arrow or Pandas schema.
    """

    def __init__(self, base_schema: Union["pyarrow.lib.Schema", "PandasBlockSchema"]):
        self.base_schema = base_schema

    @property
    def names(self) -> List[str]:
        """Lists the columns of this Datastream."""
        return self.base_schema.names

    @property
    def types(self) -> List[Union[Literal[object], "pyarrow.DataType"]]:
        """Lists the types of this Datastream in Arrow format

        For non-Arrow compatible types, we return "object".
        """
        import pyarrow as pa
        from ray.data.extensions import TensorDtype, ArrowTensorType

        if isinstance(self.base_schema, pa.lib.Schema):
            return list(self.base_schema.types)

        arrow_types = []
        for dtype in self.base_schema.types:
            if isinstance(dtype, TensorDtype):
                # Manually convert our Pandas tensor extension type to Arrow.
                arrow_types.append(
                    ArrowTensorType(
                        shape=dtype._shape, dtype=pa.from_numpy_dtype(dtype._dtype)
                    )
                )
            else:
                try:
                    arrow_types.append(pa.from_numpy_dtype(dtype))
                except pa.ArrowNotImplementedError:
                    arrow_types.append(object)
                except Exception:
                    logger.exception(f"Error converting dtype {dtype} to Arrow.")
                    arrow_types.append(None)
        return arrow_types

    def __eq__(self, other):
        return isinstance(other, Schema) and other.base_schema == self.base_schema

    def __str__(self):
        return f"Schema({dict(zip(self.names, self.types))})"

    def __repr__(self):
        return str(self)


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
    ctx: DataContext,
    blocks: List[Block],
    meta: List[BlockMetadata],
    ray_remote_args: Dict[str, Any],
    write_args: Dict[str, Any],
) -> List[ObjectRef[WriteResult]]:
    write_args = _unwrap_arrow_serialization_workaround(write_args)
    DataContext._set_current(ctx)
    return ds.do_write(blocks, meta, ray_remote_args=ray_remote_args, **write_args)
