import collections
import copy
import html
import itertools
import logging
import sys
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
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import uuid4

import numpy as np

import ray
import ray.cloudpickle as pickle
from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray._private.usage import usage_lib
from ray.air.util.data_batch_conversion import BlockFormat
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import (
    ActorPoolStrategy,
    CallableClass,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.equalize import _equalize
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.legacy_compat import _block_list_to_bundles
from ray.data._internal.iterator.iterator_impl import DataIteratorImpl
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
)
from ray.data._internal.logical.operators.n_ary_operator import (
    Union as UnionLogicalOperator,
)
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.optimizers import LogicalPlan
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data._internal.plan import ExecutionPlan, OneToOneStage
from ray.data._internal.planner.plan_udf_map_op import (
    generate_filter_fn,
    generate_flat_map_fn,
    generate_map_batches_fn,
    generate_map_rows_fn,
)
from ray.data._internal.planner.plan_write_op import generate_write_fn
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.sort import SortKey
from ray.data._internal.split import _get_num_rows, _split_at_indices
from ray.data._internal.stage_impl import (
    LimitStage,
    RandomizeBlocksStage,
    RandomShuffleStage,
    RepartitionStage,
    SortStage,
    ZipStage,
)
from ray.data._internal.stats import DatasetStats, DatasetStatsSummary
from ray.data._internal.util import (
    ConsumptionAPI,
    _estimate_available_parallelism,
    _is_local_scheme,
    validate_compute,
)
from ray.data.aggregate import AggregateFn, Max, Mean, Min, Std, Sum
from ray.data.block import (
    VALID_BATCH_FORMATS,
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockPartition,
    DataBatch,
    T,
    U,
    UserDefinedFunction,
    _apply_strict_mode_batch_format,
    _apply_strict_mode_batch_size,
)
from ray.data.context import (
    ESTIMATED_SAFE_MEMORY_FRACTION,
    OK_PREFIX,
    WARN_PREFIX,
    DataContext,
)
from ray.data.datasource import (
    BlockWritePathProvider,
    Connection,
    CSVDatasource,
    Datasource,
    DefaultBlockWritePathProvider,
    ImageDatasource,
    JSONDatasource,
    NumpyDatasource,
    ParquetDatasource,
    ReadTask,
    SQLDatasource,
    TFRecordDatasource,
    WriteResult,
)
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
    _wrap_arrow_serialization_workaround,
)
from ray.data.iterator import DataIterator
from ray.data.random_access_dataset import RandomAccessDataset
from ray.types import ObjectRef
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.widgets import Template
from ray.widgets.util import repr_with_fallback

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
    from tensorflow_metadata.proto.v0 import schema_pb2

    from ray.data._internal.execution.interfaces import Executor, NodeIdStr
    from ray.data.dataset_pipeline import DatasetPipeline
    from ray.data.grouped_data import GroupedData


logger = logging.getLogger(__name__)

TensorflowFeatureTypeSpec = Union[
    "tf.TypeSpec", List["tf.TypeSpec"], Dict[str, "tf.TypeSpec"]
]

TensorFlowTensorBatchType = Union["tf.Tensor", Dict[str, "tf.Tensor"]]

CollatedData = TypeVar("CollatedData")
TorchBatchType = Union[Dict[str, "torch.Tensor"], CollatedData]


@PublicAPI
class Dataset:
    """A Dataset is a distributed data collection for data loading and processing.

    Datasets are distributed pipelines that produce ``ObjectRef[Block]`` outputs,
    where each block holds data in Arrow format, representing a shard of the overall
    data collection. The block also determines the unit of parallelism. For more
    details, see :ref:`Ray Data Internals <dataset_concept>`.

    Datasets can be created in multiple ways: from synthetic data via ``range_*()``
    APIs, from existing memory data via ``from_*()`` APIs (this creates a subclass
    of Dataset called ``MaterializedDataset``), or from external storage
    systems such as local disk, S3, HDFS etc. via the ``read_*()`` APIs. The
    (potentially processed) Dataset can be saved back to external storage systems
    via the ``write_*()`` APIs.

    Examples:
        .. testcode::
            :skipif: True

            import ray
            # Create dataset from synthetic data.
            ds = ray.data.range(1000)
            # Create dataset from in-memory data.
            ds = ray.data.from_items(
                [{"col1": i, "col2": i * 2} for i in range(1000)]
            )
            # Create dataset from external storage system.
            ds = ray.data.read_parquet("s3://bucket/path")
            # Save dataset back to external storage system.
            ds.write_csv("s3://bucket/output")

    Dataset has two kinds of operations: transformation, which takes in Dataset
    and outputs a new Dataset (e.g. :py:meth:`.map_batches()`); and consumption,
    which produces values (not a data stream) as output
    (e.g. :meth:`.iter_batches()`).

    Dataset transformations are lazy, with execution of the transformations being
    triggered by downstream consumption.

    Dataset supports parallel processing at scale: transformations such as
    :py:meth:`.map_batches()`, aggregations such as
    :py:meth:`.min()`/:py:meth:`.max()`/:py:meth:`.mean()`, grouping via
    :py:meth:`.groupby()`, shuffling operations such as :py:meth:`.sort()`,
    :py:meth:`.random_shuffle()`, and :py:meth:`.repartition()`.

    Examples:
        >>> import ray
        >>> ds = ray.data.range(1000)
        >>> # Transform batches (Dict[str, np.ndarray]) with map_batches().
        >>> ds.map_batches(lambda batch: {"id": batch["id"] * 2})  # doctest: +ELLIPSIS
        MapBatches(<lambda>)
        +- Dataset(num_blocks=..., num_rows=1000, schema={id: int64})
        >>> # Compute the maximum.
        >>> ds.max("id")
        999
        >>> # Shuffle this dataset randomly.
        >>> ds.random_shuffle()  # doctest: +ELLIPSIS
        RandomShuffle
        +- Dataset(num_blocks=..., num_rows=1000, schema={id: int64})
        >>> # Sort it back in order.
        >>> ds.sort("id")  # doctest: +ELLIPSIS
        Sort
        +- Dataset(num_blocks=..., num_rows=1000, schema={id: int64})

    Both unexecuted and materialized Datasets can be passed between Ray tasks and
    actors without incurring a copy. Dataset supports conversion to/from several
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
        """Construct a Dataset (internal API).

        The constructor is not part of the Dataset API. Use the ``ray.data.*``
        read methods to construct a dataset.
        """
        assert isinstance(plan, ExecutionPlan), type(plan)
        usage_lib.record_library_usage("dataset")  # Legacy telemetry name.

        self._plan = plan
        self._uuid = uuid4().hex
        self._epoch = epoch
        self._lazy = lazy
        self._logical_plan = logical_plan
        if logical_plan is not None:
            self._plan.link_logical_plan(logical_plan)

        if not lazy:
            self._plan.execute(allow_clear_input_blocks=False)

        # Handle to currently running executor for this dataset.
        self._current_executor: Optional["Executor"] = None

    @staticmethod
    def copy(
        ds: "Dataset", _deep_copy: bool = False, _as: Optional[type] = None
    ) -> "Dataset":
        if not _as:
            _as = type(ds)
        if _deep_copy:
            return _as(ds._plan.deep_copy(), ds._epoch, ds._lazy, ds._logical_plan)
        else:
            return _as(ds._plan.copy(), ds._epoch, ds._lazy, ds._logical_plan)

    def map(
        self,
        fn: UserDefinedFunction[Dict[str, Any], Dict[str, Any]],
        *,
        compute: Optional[ComputeStrategy] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Apply the given function to each row of this dataset.

        Use this method to transform your data. To learn more, see
        :ref:`Transforming rows <transforming_rows>`.

        .. tip::

            If your transformation is vectorized like most NumPy or pandas operations,
            :meth:`~Dataset.map_batches` might be faster.

        Examples:

            .. testcode::

                import os
                from typing import Any, Dict
                import ray

                def parse_filename(row: Dict[str, Any]) -> Dict[str, Any]:
                    row["filename"] = os.path.basename(row["path"])
                    return row

                ds = (
                    ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple", include_paths=True)
                    .map(parse_filename)
                )
                print(ds.schema())

            .. testoutput::

                Column    Type
                ------    ----
                image     numpy.ndarray(shape=(32, 32, 3), dtype=uint8)
                path      string
                filename  string

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function to apply to each row, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either None (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPUs to reserve for each parallel map worker.
            num_gpus: The number of GPUs to reserve for each parallel map worker. For
                example, specify `num_gpus=1` to request 1 GPU for each parallel map
                worker.
            ray_remote_args: Additional resource requirements to request from
                Ray for each map worker.

        .. seealso::

            :meth:`~Dataset.flat_map`
                Call this method to create new rows from existing ones. Unlike
                :meth:`~Dataset.map`, a function passed to
                :meth:`~Dataset.flat_map` can return multiple rows.

            :meth:`~Dataset.map_batches`
                Call this method to transform batches of data.
        """  # noqa: E501
        validate_compute(fn, compute, fn_constructor_args)

        transform_fn = generate_map_rows_fn()

        if num_cpus is not None:
            ray_remote_args["num_cpus"] = num_cpus

        if num_gpus is not None:
            ray_remote_args["num_gpus"] = num_gpus

        plan = self._plan.with_stage(
            OneToOneStage(
                "Map",
                transform_fn,
                compute,
                ray_remote_args,
                fn=fn,
                fn_constructor_args=fn_constructor_args,
            )
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            map_op = MapRows(
                logical_plan.dag,
                fn,
                fn_constructor_args=fn_constructor_args,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(map_op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

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
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Apply the given function to batches of data.

        This method is useful for preprocessing data and performing inference. To learn
        more, see :ref:`Transforming batches <transforming_batches>`.

        You can use either Ray Tasks or Ray Actors to perform the transformation. By
        default, Ray Data uses Tasks. To use Actors, see
        :ref:`Transforming batches with actors <transforming_data_actors>`.

        .. tip::
            If ``fn`` doesn't mutate its input, set ``zero_copy_batch=True`` to improve
            performance and decrease memory utilization.

        Examples:

            Call :meth:`~Dataset.map_batches` to transform your data.

            .. testcode::

                from typing import Dict
                import numpy as np
                import ray

                def add_dog_years(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    batch["age_in_dog_years"] = 7 * batch["age"]
                    return batch

                ds = (
                    ray.data.from_items([
                        {"name": "Luna", "age": 4},
                        {"name": "Rory", "age": 14},
                        {"name": "Scout", "age": 9},
                    ])
                    .map_batches(add_dog_years)
                )
                ds.show()

            .. testoutput::

                {'name': 'Luna', 'age': 4, 'age_in_dog_years': 28}
                {'name': 'Rory', 'age': 14, 'age_in_dog_years': 98}
                {'name': 'Scout', 'age': 9, 'age_in_dog_years': 63}

            If your function returns large objects, yield outputs in chunks.

            .. testcode::

                from typing import Dict
                import ray
                import numpy as np

                def map_fn_with_large_output(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    for i in range(3):
                        yield {"large_output": np.ones((100, 1000))}

                ds = (
                    ray.data.from_items([1])
                    .map_batches(map_fn_with_large_output)
                )

            You can also use :meth:`~Dataset.map_batches` to perform offline inference.
            To learn more, see
            :ref:`End-to-end: Offline Batch Inference <batch_inference_home>`.

        Args:
            fn: The function or generator to apply to a record batch, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy. Note ``fn`` must be
                pickle-able.
            batch_size: The desired number of rows in each batch, or ``None`` to use
                entire blocks as batches (blocks may contain different numbers of rows).
                The actual size of the batch provided to ``fn`` may be smaller than
                ``batch_size`` if ``batch_size`` doesn't evenly divide the block(s) sent
                to a given map task. Default batch_size is 4096 with "default".
            compute: Either "tasks" (default) to use Ray Tasks or an
                :class:`~ray.data.ActorPoolStrategy` to use an autoscaling actor pool.
            batch_format: If ``"default"`` or ``"numpy"``, batches are
                ``Dict[str, numpy.ndarray]``. If ``"pandas"``, batches are
                ``pandas.DataFrame``.
            zero_copy_batch: Whether ``fn`` should be provided zero-copy, read-only
                batches. If this is ``True`` and no copy is required for the
                ``batch_format`` conversion, the batch is a zero-copy, read-only
                view on data in Ray's object store, which can decrease memory
                utilization and improve performance. If this is ``False``, the batch
                is writable, which requires an extra copy to guarantee.
                If ``fn`` mutates its input, this needs to be ``False`` in order to
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
            num_cpus: The number of CPUs to reserve for each parallel map worker.
            num_gpus: The number of GPUs to reserve for each parallel map worker. For
                example, specify `num_gpus=1` to request 1 GPU for each parallel map worker.
            ray_remote_args: Additional resource requirements to request from
                ray for each map worker.

        .. note::

            The size of the batches provided to ``fn`` might be smaller than the
            specified ``batch_size`` if ``batch_size`` doesn't evenly divide the
            block(s) sent to a given map task.

        .. seealso::

            :meth:`~Dataset.iter_batches`
                Call this function to iterate over batches of data.

            :meth:`~Dataset.flat_map`
                Call this method to create new records from existing ones. Unlike
                :meth:`~Dataset.map`, a function passed to :meth:`~Dataset.flat_map`
                can return multiple records.

            :meth:`~Dataset.map`
                Call this method to transform one record at time.

        """  # noqa: E501

        if num_cpus is not None:
            ray_remote_args["num_cpus"] = num_cpus

        if num_gpus is not None:
            ray_remote_args["num_gpus"] = num_gpus

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

        validate_compute(fn, compute, fn_constructor_args)

        if fn_constructor_kwargs is not None:
            if compute is None or (
                compute != "actors" and not isinstance(compute, ActorPoolStrategy)
            ):
                raise ValueError(
                    "fn_constructor_kwargs can only be specified if using the actor "
                    f"pool compute strategy, but got: {compute}"
                )
            if not isinstance(fn, CallableClass):
                raise ValueError(
                    "fn_constructor_kwargs can only be specified if providing a "
                    f"CallableClass instance for fn, but got: {fn}"
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

        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def add_column(
        self,
        col: str,
        fn: Callable[["pandas.DataFrame"], "pandas.Series"],
        *,
        compute: Optional[str] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Add the given column to the dataset.

        A function generating the new column values given the batch in pandas
        format must be specified.

        Examples:


            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.schema()
            Column  Type
            ------  ----
            id      int64

            Add a new column equal to ``id * 2``.

            >>> ds.add_column("new_id", lambda df: df["id"] * 2).schema()
            Column  Type
            ------  ----
            id      int64
            new_id  int64

            Overwrite the existing values with zeros.

            >>> ds.add_column("id", lambda df: 0).take(3)
            [{'id': 0}, {'id': 0}, {'id': 0}]

        Time complexity: O(dataset size / parallelism)

        Args:
            col: Name of the column to add. If the name already exists, the
                column is overwritten.
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
    ) -> "Dataset":
        """Drop one or more columns from the dataset.

        Examples:

            >>> import ray
            >>> ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
            >>> ds.schema()
            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string
            >>> ds.drop_columns(["variety"]).schema()
            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double

        Time complexity: O(dataset size / parallelism)

        Args:
            cols: Names of the columns to drop. If any name does not exist,
                an exception is raised.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """  # noqa: E501

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
    ) -> "Dataset":
        """Select one or more columns from the dataset.

        Specified columns must be in the dataset schema.

        Examples:

            >>> import ray
            >>> ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
            >>> ds.schema()
            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string
            >>> ds.select_columns(["sepal.length", "sepal.width"]).schema()
            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double

        Time complexity: O(dataset size / parallelism)

        Args:
            cols: Names of the columns to select. If a name isn't in the
                dataset schema, an exception is raised.
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
        fn_constructor_args: Optional[Iterable[Any]] = None,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Apply the given function to each row and then flatten results.

        Use this method if your transformation returns multiple rows for each input
        row.

        .. tip::
            :meth:`~Dataset.map_batches` can also modify the number of rows. If your
            transformation is vectorized like most NumPy and pandas operations,
            it might be faster.

        Examples:

            .. testcode::

                from typing import Any, Dict, List
                import ray

                def duplicate_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
                    return [row] * 2

                print(
                    ray.data.range(3)
                    .flat_map(duplicate_row)
                    .take_all()
                )

            .. testoutput::

                [{'id': 0}, {'id': 0}, {'id': 1}, {'id': 1}, {'id': 2}, {'id': 2}]

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The function or generator to apply to each record, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPUs to reserve for each parallel map worker.
            num_gpus: The number of GPUs to reserve for each parallel map worker. For
                example, specify `num_gpus=1` to request 1 GPU for each parallel map
                worker.
            ray_remote_args: Additional resource requirements to request from
                ray for each map worker.

        .. seealso::

            :meth:`~Dataset.map_batches`
                Call this method to transform batches of data.

            :meth:`~Dataset.map`
                Call this method to transform one row at time.
        """
        validate_compute(fn, compute, fn_constructor_args)

        transform_fn = generate_flat_map_fn()

        if num_cpus is not None:
            ray_remote_args["num_cpus"] = num_cpus

        if num_gpus is not None:
            ray_remote_args["num_gpus"] = num_gpus

        plan = self._plan.with_stage(
            OneToOneStage(
                "FlatMap",
                transform_fn,
                compute,
                ray_remote_args,
                fn=fn,
                fn_constructor_args=fn_constructor_args,
            )
        )

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = FlatMap(
                input_op=logical_plan.dag,
                fn=fn,
                fn_constructor_args=fn_constructor_args,
                compute=compute,
                ray_remote_args=ray_remote_args,
            )
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def filter(
        self,
        fn: UserDefinedFunction[Dict[str, Any], bool],
        *,
        compute: Union[str, ComputeStrategy] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Filter out rows that don't satisfy the given predicate.

        .. tip::
            If you can represent your predicate with NumPy or pandas operations,
            :meth:`Dataset.map_batches` might be faster. You can implement filter by
            dropping rows.

        Examples:

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.filter(lambda row: row["id"] % 2 == 0).take_all()
            [{'id': 0}, {'id': 2}, {'id': 4}, ...]

        Time complexity: O(dataset size / parallelism)

        Args:
            fn: The predicate to apply to each row, or a class type
                that can be instantiated to create such a callable. Callable classes are
                only supported for the actor compute strategy.
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
        """
        validate_compute(fn, compute)

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

        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def repartition(self, num_blocks: int, *, shuffle: bool = False) -> "Dataset":
        """Repartition the :class:`Dataset` into exactly this number of :ref:`blocks <dataset_concept>`.

        This method can be useful to tune the performance of your pipeline. To learn
        more, see :ref:`Advanced: Performance Tips and Tuning <data_performance_tips>`.

        If you're writing data to files, you can also use this method to change the
        number of output files. To learn more, see
        :ref:`Changing the number of output files <changing-number-output-files>`.

        .. note::

            Repartition has two modes. If ``shuffle=False``, Ray Data performs the
            minimal data movement needed to equalize block sizes. Otherwise, Ray Data
            performs a full distributed shuffle.

            .. image:: /data/images/dataset-shuffle.svg
                :align: center

            ..
                https://docs.google.com/drawings/d/132jhE3KXZsf29ho1yUdPrCHB9uheHBWHJhDQMXqIVPA/edit

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.repartition(10).num_blocks()
            10

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
            The repartitioned :class:`Dataset`.
        """  # noqa: E501

        plan = self._plan.with_stage(RepartitionStage(num_blocks, shuffle))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Repartition(
                logical_plan.dag,
                num_outputs=num_blocks,
                shuffle=shuffle,
            )
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def random_shuffle(
        self,
        *,
        seed: Optional[int] = None,
        num_blocks: Optional[int] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Randomly shuffle the rows of this :class:`Dataset`.

        .. tip::

            This method can be slow. For better performance, try
            `Iterating over batches with shuffling <iterating-over-data#iterating-over-batches-with-shuffling>`_.
            Also, see :ref:`Optimizing shuffles <optimizing_shuffles>`.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.random_shuffle().take(3)  # doctest: +SKIP
            {'id': 41}, {'id': 21}, {'id': 92}]
            >>> ds.random_shuffle(seed=42).take(3)  # doctest: +SKIP
            {'id': 77}, {'id': 21}, {'id': 63}]

        Time complexity: O(dataset size / parallelism)

        Args:
            seed: Fix the random seed to use, otherwise one is chosen
                based on system randomness.
            num_blocks: The number of output blocks after the shuffle, or ``None``
                to retain the number of blocks.

        Returns:
            The shuffled :class:`Dataset`.
        """  # noqa: E501

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
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def randomize_block_order(
        self,
        *,
        seed: Optional[int] = None,
    ) -> "Dataset":
        """Randomly shuffle the :ref:`blocks <dataset_concept>` of this :class:`Dataset`.

        This method is useful if you :meth:`~Dataset.split` your dataset into shards and
        want to randomize the data in each shard without performing a full
        :meth:`~Dataset.random_shuffle`.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.take(5)
            [{'id': 0}, {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]
            >>> ds.randomize_block_order().take(5)  # doctest: +SKIP
            {'id': 15}, {'id': 16}, {'id': 17}, {'id': 18}, {'id': 19}]

        Args:
            seed: Fix the random seed to use, otherwise one is chosen
                based on system randomness.

        Returns:
            The block-shuffled :class:`Dataset`.
        """

        plan = self._plan.with_stage(RandomizeBlocksStage(seed))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = RandomizeBlocks(
                logical_plan.dag,
                seed=seed,
            )
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def random_sample(
        self, fraction: float, *, seed: Optional[int] = None
    ) -> "Dataset":
        """Returns a new :class:`Dataset` containing a random fraction of the rows.

        .. note::

            This method returns roughly ``fraction * total_rows`` rows. An exact number
            of rows isn't guaranteed.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.random_sample(0.1).count()  # doctest: +SKIP
            10

        Args:
            fraction: The fraction of elements to sample.
            seed: Seeds the python random pRNG generator.

        Returns:
            Returns a :class:`Dataset` containing the sampled rows.
        """
        import random

        import pandas as pd
        import pyarrow as pa

        if self.num_blocks() == 0:
            raise ValueError("Cannot sample from an empty Dataset.")

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
        be used to read disjoint subsets of the dataset in parallel.

        This method is the recommended way to consume :class:`Datasets <Dataset>` for
        distributed training.

        Streaming split works by delegating the execution of this :class:`Dataset` to a
        coordinator actor. The coordinator pulls block references from the executed
        stream, and divides those blocks among ``n`` output iterators. Iterators pull
        blocks from the coordinator actor to return to their caller on ``next``.

        The returned iterators are also repeatable; each iteration will trigger a
        new execution of the Dataset. There is an implicit barrier at the start of
        each iteration, which means that `next` must be called on all iterators before
        the iteration starts.

        .. warning::

            Because iterators are pulling blocks from the same :class:`Dataset`
            execution, if one iterator falls behind, other iterators may be stalled.

        Examples:

            .. testcode::

                import ray

                ds = ray.data.range(100)
                it1, it2 = ds.streaming_split(2, equal=True)

            Consume data from iterators in parallel.

            .. testcode::

                @ray.remote
                def consume(it):
                    for batch in it.iter_batches():
                       pass

                ray.get([consume.remote(it1), consume.remote(it2)])

            You can loop over the iterators multiple times (multiple epochs).

            .. testcode::

                @ray.remote
                def train(it):
                    NUM_EPOCHS = 2
                    for _ in range(NUM_EPOCHS):
                        for batch in it.iter_batches():
                            pass

                ray.get([train.remote(it1), train.remote(it2)])

            The following remote function call blocks waiting for a read on ``it2`` to
            start.

            .. testcode::
                :skipif: True

                ray.get(train.remote(it1))

        Args:
            n: Number of output iterators to return.
            equal: If ``True``, each output iterator sees an exactly equal number
                of rows, dropping data if necessary. If ``False``, some iterators may
                see slightly more or less rows than others, but no data is dropped.
            locality_hints: Specify the node ids corresponding to each iterator
                location. Dataset will try to minimize data movement based on the
                iterator output locations. This list must have length ``n``. You can
                get the current node id of a task or actor by calling
                ``ray.get_runtime_context().get_node_id()``.

        Returns:
            The output iterator splits. These iterators are Ray-serializable and can
            be freely passed to any Ray task or actor.

        .. seealso::

            :meth:`Dataset.split`
                Unlike :meth:`~Dataset.streaming_split`, :meth:`~Dataset.split`
                materializes the dataset in memory.
        """
        return StreamSplitDataIterator.create(self, n, equal, locality_hints)

    @ConsumptionAPI
    def split(
        self, n: int, *, equal: bool = False, locality_hints: Optional[List[Any]] = None
    ) -> List["MaterializedDataset"]:
        """Materialize and split the dataset into ``n`` disjoint pieces.

        This method returns a list of ``MaterializedDataset`` that can be passed to Ray
        Tasks and Actors and used to read the dataset rows in parallel.

        Examples:

            .. testcode::

                @ray.remote
                class Worker:

                    def train(self, data_iterator):
                        for batch in data_iterator.iter_batches(batch_size=8):
                            pass

                workers = [Worker.remote() for _ in range(4)]
                shards = ray.data.range(100).split(n=4, equal=True)
                ray.get([w.train.remote(s) for w, s in zip(workers, shards)])

        Time complexity: O(1)

        Args:
            n: Number of child datasets to return.
            equal: Whether to guarantee each split has an equal
                number of records. This might drop records if the rows can't be
                divided equally among the splits.
            locality_hints: [Experimental] A list of Ray actor handles of size ``n``.
                The system tries to co-locate the blocks of the i-th dataset
                with the i-th actor to maximize data locality.

        Returns:
            A list of ``n`` disjoint dataset splits.

        .. seealso::

            :meth:`Dataset.split_at_indices`
                Unlike :meth:`~Dataset.split`, which splits a dataset into approximately
                equal splits, :meth:`Dataset.split_proportionately` lets you split a
                dataset into different sizes.

            :meth:`Dataset.split_proportionately`
                This method is equivalent to :meth:`Dataset.split_at_indices` if
                you compute indices manually.

            :meth:`Dataset.streaming_split`.
                Unlike :meth:`~Dataset.split`, :meth:`~Dataset.streaming_split`
                doesn't materialize the dataset in memory.
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

            split_datasets = []
            for b, m in zip(blocks, meta):
                block_list = BlockList(
                    b.tolist(), m.tolist(), owned_by_consumer=owned_by_consumer
                )
                logical_plan = self._plan._logical_plan
                if logical_plan is not None:
                    ref_bundles = _block_list_to_bundles(block_list, owned_by_consumer)
                    logical_plan = LogicalPlan(InputData(input_data=ref_bundles))
                split_datasets.append(
                    MaterializedDataset(
                        ExecutionPlan(
                            block_list,
                            stats,
                            run_by_consumer=owned_by_consumer,
                        ),
                        self._epoch,
                        self._lazy,
                        logical_plan,
                    )
                )
            return split_datasets

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

        split_datasets = []
        for block_split in per_split_block_lists:
            logical_plan = self._plan._logical_plan
            if logical_plan is not None:
                ref_bundles = _block_list_to_bundles(block_split, owned_by_consumer)
                logical_plan = LogicalPlan(InputData(input_data=ref_bundles))
            split_datasets.append(
                MaterializedDataset(
                    ExecutionPlan(
                        block_split,
                        stats,
                        run_by_consumer=owned_by_consumer,
                    ),
                    self._epoch,
                    self._lazy,
                    logical_plan,
                )
            )
        return split_datasets

    @ConsumptionAPI
    def split_at_indices(self, indices: List[int]) -> List["MaterializedDataset"]:
        """Materialize and split the dataset at the given indices (like ``np.split``).

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

        Args:
            indices: List of sorted integers which indicate where the dataset
                are split. If an index exceeds the length of the dataset,
                an empty dataset is returned.

        Returns:
            The dataset splits.

        .. seealso::

            :meth:`Dataset.split`
                Unlike :meth:`~Dataset.split_at_indices`, which lets you split a
                dataset into different sizes, :meth:`Dataset.split` splits a dataset
                into approximately equal splits.

            :meth:`Dataset.split_proportionately`
                This method is equivalent to :meth:`Dataset.split_at_indices` if
                you compute indices manually.

            :meth:`Dataset.streaming_split`.
                Unlike :meth:`~Dataset.split`, :meth:`~Dataset.streaming_split`
                doesn't materialize the dataset in memory.
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
            stats = DatasetStats(stages={"Split": ms}, parent=parent_stats)
            stats.time_total_s = split_duration

            split_block_list = BlockList(
                bs, ms, owned_by_consumer=block_list._owned_by_consumer
            )
            logical_plan = self._plan._logical_plan
            if logical_plan is not None:
                ref_bundles = _block_list_to_bundles(
                    split_block_list, block_list._owned_by_consumer
                )
                logical_plan = LogicalPlan(InputData(input_data=ref_bundles))

            splits.append(
                MaterializedDataset(
                    ExecutionPlan(
                        split_block_list,
                        stats,
                        run_by_consumer=block_list._owned_by_consumer,
                    ),
                    self._epoch,
                    self._lazy,
                    logical_plan,
                )
            )
        return splits

    @ConsumptionAPI
    def split_proportionately(
        self, proportions: List[float]
    ) -> List["MaterializedDataset"]:
        """Materialize and split the dataset using proportions.

        A common use case for this is splitting the dataset into train
        and test sets (equivalent to eg. scikit-learn's ``train_test_split``).
        For a higher level abstraction, see :meth:`Dataset.train_test_split`.

        This method splits datasets so that all splits
        always contains at least one row. If that isn't possible,
        an exception is raised.

        This is equivalent to caulculating the indices manually and calling
        :meth:`Dataset.split_at_indices`.

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

        Args:
            proportions: List of proportions to split the dataset according to.
                Must sum up to less than 1, and each proportion must be bigger
                than 0.

        Returns:
            The dataset splits.

        .. seealso::

            :meth:`Dataset.split`
                Unlike :meth:`~Dataset.split_proportionately`, which lets you split a
                dataset into different sizes, :meth:`Dataset.split` splits a dataset
                into approximately equal splits.

            :meth:`Dataset.split_at_indices`
                :meth:`Dataset.split_proportionately` uses this method under the hood.

            :meth:`Dataset.streaming_split`.
                Unlike :meth:`~Dataset.split`, :meth:`~Dataset.streaming_split`
                doesn't materialize the dataset in memory.
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

    @ConsumptionAPI
    def train_test_split(
        self,
        test_size: Union[int, float],
        *,
        shuffle: bool = False,
        seed: Optional[int] = None,
    ) -> Tuple["MaterializedDataset", "MaterializedDataset"]:
        """Materialize and split the dataset into train and test subsets.

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
                proportion of the dataset to include in the test split. If int,
                represents the absolute number of test samples. The train split
                always complements the test split.
            shuffle: Whether or not to globally shuffle the dataset before splitting.
                Defaults to ``False``. This may be a very expensive operation with a
                large dataset.
            seed: Fix the random seed to use for shuffle, otherwise one is chosen
                based on system randomness. Ignored if ``shuffle=False``.

        Returns:
            Train and test subsets as two ``MaterializedDatasets``.

        .. seealso::

            :meth:`Dataset.split_proportionately`
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
                    f"than the size of the dataset ({ds_length}). "
                    f"Got {test_size}."
                )
            return ds.split_at_indices([ds_length - test_size])

    @ConsumptionAPI
    def union(self, *other: List["Dataset"]) -> "Dataset":
        """Materialize and concatenate :class:`Datasets <ray.data.Dataset>` across rows.

        The order of the blocks in the datasets is preserved, as is the
        relative ordering between the datasets passed in the argument list.

        .. caution::
            Unioned datasets aren't lineage-serializable. As a result, they can't be
            used as a tunable hyperparameter in Ray Tune.

        Examples:

            >>> import ray
            >>> ds1 = ray.data.range(2)
            >>> ds2 = ray.data.range(3)
            >>> ds1.union(ds2).take_all()
            [{'id': 0}, {'id': 1}, {'id': 0}, {'id': 1}, {'id': 2}]

        Args:
            other: List of datasets to combine with this one. The datasets
                must have the same schema as this dataset, otherwise the
                behavior is undefined.

        Returns:
            A new dataset holding the rows of the input datasets.
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
            ops_to_union = []
            for idx, bl in enumerate(bls):
                if isinstance(bl, LazyBlockList):
                    bs, ms = bl._get_blocks_with_metadata()
                else:
                    assert isinstance(bl, BlockList), type(bl)
                    bs, ms = bl._blocks, bl._metadata
                op_logical_plan = getattr(datasets[idx]._plan, "_logical_plan", None)
                if isinstance(op_logical_plan, LogicalPlan):
                    ops_to_union.append(op_logical_plan.dag)
                else:
                    ops_to_union.append(None)
                blocks.extend(bs)
                metadata.extend(ms)
            blocklist = BlockList(blocks, metadata, owned_by_consumer=owned_by_consumer)

            logical_plan = None
            if all(ops_to_union):
                logical_plan = LogicalPlan(UnionLogicalOperator(*ops_to_union))
        else:
            tasks: List[ReadTask] = []
            block_partition_refs: List[ObjectRef[BlockPartition]] = []
            block_partition_meta_refs: List[ObjectRef[BlockMetadata]] = []

            # Gather read task names from input blocks of unioned Datasets,
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

            logical_plan = self._logical_plan
            logical_plans = [
                getattr(union_ds, "_logical_plan", None) for union_ds in datasets
            ]
            if all(logical_plans):
                op = UnionLogicalOperator(
                    *[plan.dag for plan in logical_plans],
                )
                logical_plan = LogicalPlan(op)

        epochs = [ds._get_epoch() for ds in datasets]
        max_epoch = max(*epochs)
        if len(set(epochs)) > 1:
            if ray.util.log_once("dataset_epoch_warned"):
                logger.warning(
                    "Dataset contains data from multiple epochs: {}, "
                    "likely due to a `rewindow()` call. The higher epoch "
                    "number {} will be used. This warning will not "
                    "be shown again.".format(set(epochs), max_epoch)
                )
        stats = DatasetStats(
            stages={"Union": []},
            parent=[d._plan.stats() for d in datasets],
        )
        stats.time_total_s = time.perf_counter() - start_time
        return Dataset(
            ExecutionPlan(blocklist, stats, run_by_consumer=owned_by_consumer),
            max_epoch,
            self._lazy,
            logical_plan,
        )

    def groupby(self, key: Optional[str]) -> "GroupedData":
        """Group rows of a :class:`Dataset` according to a column.

        Use this method to transform data based on a
        categorical variable.

        Examples:

            .. testcode::

                import pandas as pd
                import ray

                def normalize_variety(group: pd.DataFrame) -> pd.DataFrame:
                    for feature in group.drop("variety").columns:
                        group[feature] = group[feature] / group[feature].abs().max()
                    return group

                ds = (
                    ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
                    .groupby("variety")
                    .map_groups(normalize_variety, batch_format="pandas")
                )

        Time complexity: O(dataset size * log(dataset size / parallelism))

        Args:
            key: A column name. If this is ``None``, place all rows in a single group.

        Returns:
            A lazy :class:`~ray.data.grouped_data.GroupedData`.

        .. seealso::

            :meth:`~ray.data.grouped_data.GroupedData.map_groups`
                Call this method to transform groups of data.
        """
        from ray.data.grouped_data import GroupedData

        # Always allow None since groupby interprets that as grouping all
        # records into a single global group.
        if key is not None:
            SortKey(key).validate_schema(self.schema(fetch_if_missing=True))

        return GroupedData(self, key)

    def unique(self, column: str) -> List[Any]:
        """List the unique elements in a given column.

        Examples:

            >>> import ray
            >>> ds = ray.data.from_items([1, 2, 3, 2, 3])
            >>> ds.unique("item")
            [1, 2, 3]

            This function is very useful for computing labels
            in a machine learning dataset:

            >>> import ray
            >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
            >>> ds.unique("target")
            [0, 1, 2]

            One common use case is to convert the class labels
            into integers for training and inference:

            >>> classes = {0: 'Setosa', 1: 'Versicolor', 2: 'Virginica'}
            >>> def preprocessor(df, classes):
            ...     df["variety"] = df["target"].map(classes)
            ...     return df
            >>> train_ds = ds.map_batches(
            ...     preprocessor, fn_kwargs={"classes": classes}, batch_format="pandas")
            >>> train_ds.sort("sepal length (cm)").take(1)  # Sort to make it deterministic
            [{'sepal length (cm)': 4.3, ..., 'variety': 'Setosa'}]

        Time complexity: O(dataset size * log(dataset size / parallelism))

        Args:
            column: The column to collect unique elements over.

        Returns:
            A list with unique elements in the given column.
        """  # noqa: E501
        ds = self.groupby(column).count().select_columns([column])
        return [item[column] for item in ds.take_all()]

    @ConsumptionAPI
    def aggregate(self, *aggs: AggregateFn) -> Union[Any, Dict[str, Any]]:
        """Aggregate values using one or more functions.

        Use this method to compute metrics like the product of a column.

        Examples:

            .. testcode::

                import ray
                from ray.data.aggregate import AggregateFn

                ds = ray.data.from_items([{"number": i} for i in range(1, 10)])
                aggregation = AggregateFn(
                    init=lambda column: 1,
                    accumulate_row=lambda a, row: a * row["number"],
                    merge = lambda a1, a2: a1 + a2,
                    name="prod"
                )
                print(ds.aggregate(aggregation))

            .. testoutput::

                {'prod': 45}

        Time complexity: O(dataset size / parallelism)

        Args:
            *aggs: :class:`Aggregations <ray.data.aggregate.AggregateFn>` to perform.

        Returns:
            A ``dict`` where each each value is an aggregation for a given column.
        """
        ret = self.groupby(None).aggregate(*aggs).take(1)
        return ret[0] if len(ret) > 0 else None

    @ConsumptionAPI
    def sum(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute the sum of one or more columns.

        Examples:
            >>> import ray
            >>> ray.data.range(100).sum("id")
            4950
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)
            ... ]).sum(["A", "B"])
            {'sum(A)': 4950, 'sum(B)': 328350}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values are ignored when computing the sum. If ``False``,
                when a null value is encountered, the output is ``None``.
                Ray Data considers ``np.nan``, ``None``, and ``pd.NaT`` to be null
                values. Default is ``True``.

        Returns:
            The sum result.

            For different values of ``on``, the return varies:

            - ``on=None``: a dict containing the column-wise sum of all
              columns,
            - ``on="col"``: a scalar representing the sum of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column ``dict``
              containing the column-wise sum of the provided columns.

            If the dataset is empty, all values are null. If ``ignore_nulls`` is
            ``False`` and any value is null, then the output is ``None``.
        """
        ret = self._aggregate_on(Sum, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def min(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Return the minimum of one or more columns.

        Examples:
            >>> import ray
            >>> ray.data.range(100).min("id")
            0
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)
            ... ]).min(["A", "B"])
            {'min(A)': 0, 'min(B)': 0}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values are ignored when computing the min; if ``False``,
                when a null value is encountered, the output is ``None``.
                This method considers ``np.nan``, ``None``, and ``pd.NaT`` to be null
                values. Default is ``True``.

        Returns:
            The min result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise min of
              all columns,
            - ``on="col"``: a scalar representing the min of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise min of the provided columns.

            If the dataset is empty, all values are null. If ``ignore_nulls`` is
            ``False`` and any value is null, then the output is ``None``.
        """
        ret = self._aggregate_on(Min, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def max(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Return the maximum of one or more columns.

        Examples:
            >>> import ray
            >>> ray.data.range(100).max("id")
            99
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)
            ... ]).max(["A", "B"])
            {'max(A)': 99, 'max(B)': 9801}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values are ignored when computing the max; if ``False``,
                when a null value is encountered, the output is ``None``.
                This method considers ``np.nan``, ``None``, and ``pd.NaT`` to be null
                values. Default is ``True``.

        Returns:
            The max result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise max of
              all columns,
            - ``on="col"``: a scalar representing the max of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise max of the provided columns.

            If the dataset is empty, all values are null. If ``ignore_nulls`` is
            ``False`` and any value is null, then the output is ``None``.
        """
        ret = self._aggregate_on(Max, on, ignore_nulls)
        return self._aggregate_result(ret)

    @ConsumptionAPI
    def mean(
        self, on: Optional[Union[str, List[str]]] = None, ignore_nulls: bool = True
    ) -> Union[Any, Dict[str, Any]]:
        """Compute the mean of one or more columns.

        Examples:
            >>> import ray
            >>> ray.data.range(100).mean("id")
            49.5
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)
            ... ]).mean(["A", "B"])
            {'mean(A)': 49.5, 'mean(B)': 3283.5}

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values are ignored when computing the mean; if ``False``,
                when a null value is encountered, the output is ``None``.
                This method considers ``np.nan``, ``None``, and ``pd.NaT`` to be null
                values. Default is ``True``.

        Returns:
            The mean result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise mean of
              all columns,
            - ``on="col"``: a scalar representing the mean of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise mean of the provided columns.

            If the dataset is empty, all values are null. If ``ignore_nulls`` is
            ``False`` and any value is null, then the output is ``None``.
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
        """Compute the standard deviation of one or more columns.

        .. note::
            This method uses Welford's online method for an accumulator-style
            computation of the standard deviation. This method has
            numerical stability, and is computable in a single pass. This may give
            different (but more accurate) results than NumPy, Pandas, and sklearn, which
            use a less numerically stable two-pass algorithm.
            To learn more, see
            `the Wikapedia article <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>`_.

        Examples:
            >>> import ray
            >>> round(ray.data.range(100).std("id", ddof=0), 5)
            28.86607
            >>> ray.data.from_items([
            ...     {"A": i, "B": i**2}
            ...     for i in range(100)
            ... ]).std(["A", "B"])
            {'std(A)': 29.011491975882016, 'std(B)': 2968.1748039269296}

        Args:
            on: a column name or a list of column names to aggregate.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is ``N - ddof``, where ``N`` represents the number of elements.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values are ignored when computing the std; if ``False``,
                when a null value is encountered, the output is ``None``.
                This method considers ``np.nan``, ``None``, and ``pd.NaT`` to be null
                values. Default is ``True``.

        Returns:
            The standard deviation result.

            For different values of ``on``, the return varies:

            - ``on=None``: an dict containing the column-wise std of
              all columns,
            - ``on="col"``: a scalar representing the std of all items in
              column ``"col"``,
            - ``on=["col_1", ..., "col_n"]``: an n-column dict
              containing the column-wise std of the provided columns.

            If the dataset is empty, all values are null. If ``ignore_nulls`` is
            ``False`` and any value is null, then the output is ``None``.
        """  # noqa: E501
        ret = self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)
        return self._aggregate_result(ret)

    def sort(
        self,
        key: Union[str, List[str], None] = None,
        descending: Union[bool, List[bool]] = False,
    ) -> "Dataset":
        """Sort the dataset by the specified key column or key function.

        .. note::
            The `descending` parameter must be a boolean, or a list of booleans.
            If it is a list, all items in the list must share the same direction.
            Multi-directional sort is not supported yet.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.sort("id", descending=True).take(3)
            [{'id': 99}, {'id': 98}, {'id': 97}]

        Time complexity: O(dataset size * log(dataset size / parallelism))

        Args:
            key: The column or a list of columns to sort by.
            descending: Whether to sort in descending order. Must be a boolean or a list
                of booleans matching the number of the columns.

        Returns:
            A new, sorted :class:`Dataset`.
        """

        sort_key = SortKey(key, descending)
        plan = self._plan.with_stage(SortStage(self, sort_key))

        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Sort(
                logical_plan.dag,
                sort_key=sort_key,
            )
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    def zip(self, other: "Dataset") -> "Dataset":
        """Materialize and zip the columns of this dataset with the columns of another.

        The datasets must have the same number of rows. Their column sets are
        merged, and any duplicate column names are disambiguated with suffixes like
        ``"_1"``.

        .. note::
            The smaller of the two datasets is repartitioned to align the number
            of rows per block with the larger dataset.

        .. note::
            Zipped datasets aren't lineage-serializable. As a result, they can't be used
            as a tunable hyperparameter in Ray Tune.

        Examples:
            >>> import ray
            >>> ds1 = ray.data.range(5)
            >>> ds2 = ray.data.range(5)
            >>> ds1.zip(ds2).take_batch()
            {'id': array([0, 1, 2, 3, 4]), 'id_1': array([0, 1, 2, 3, 4])}

        Time complexity: O(dataset size / parallelism)

        Args:
            other: The dataset to zip with on the right hand side.

        Returns:
            A :class:`Dataset` containing the columns of the second dataset
            concatenated horizontally with the columns of the first dataset,
            with duplicate column names disambiguated with suffixes like ``"_1"``.
        """

        plan = self._plan.with_stage(ZipStage(other))

        logical_plan = self._logical_plan
        other_logical_plan = other._logical_plan
        if logical_plan is not None and other_logical_plan is not None:
            op = Zip(logical_plan.dag, other_logical_plan.dag)
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    @ConsumptionAPI
    def limit(self, limit: int) -> "Dataset":
        """Truncate the dataset to the first ``limit`` rows.

        Unlike :meth:`~Dataset.take`, this method doesn't move data to the caller's
        machine. Instead, it returns a new :class:`Dataset` pointing to the truncated
        distributed data.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1000)
            >>> ds.limit(5).count()
            5

        Time complexity: O(limit specified)

        Args:
            limit: The size of the dataset to truncate to.

        Returns:
            The truncated dataset.
        """
        plan = self._plan.with_stage(LimitStage(limit))
        logical_plan = self._logical_plan
        if logical_plan is not None:
            op = Limit(logical_plan.dag, limit=limit)
            logical_plan = LogicalPlan(op)
        return Dataset(plan, self._epoch, self._lazy, logical_plan)

    @ConsumptionAPI
    def take_batch(
        self, batch_size: int = 20, *, batch_format: Optional[str] = "default"
    ) -> DataBatch:
        """Return up to ``batch_size`` rows from the :class:`Dataset` in a batch.

        Ray Data represents batches as NumPy arrays or pandas DataFrames. You can
        configure the batch type by specifying ``batch_format``.

        This method is useful for inspecting inputs to :meth:`~Dataset.map_batches`.

        .. warning::

            :meth:`~Dataset.take_batch` moves up to ``batch_size`` rows to the caller's
            machine. If ``batch_size`` is large, this method can cause an `
            ``OutOfMemory`` error on the caller.

        Examples:

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.take_batch(5)
            {'id': array([0, 1, 2, 3, 4])}

        Time complexity: O(batch_size specified)

        Args:
            batch_size: The maximum number of rows to return.
            batch_format: If ``"default"`` or ``"numpy"``, batches are
                ``Dict[str, numpy.ndarray]``. If ``"pandas"``, batches are
                ``pandas.DataFrame``.

        Returns:
            A batch of up to ``batch_size`` rows from the dataset.

        Raises:
            ``ValueError``: if the dataset is empty.
        """
        batch_format = _apply_strict_mode_batch_format(batch_format)
        limited_ds = self.limit(batch_size)

        try:
            res = next(
                iter(
                    limited_ds.iter_batches(
                        batch_size=batch_size,
                        prefetch_batches=0,
                        batch_format=batch_format,
                    )
                )
            )
        except StopIteration:
            raise ValueError("The dataset is empty.")
        self._synchronize_progress_bar()

        # Save the computed stats to the original dataset.
        self._plan._snapshot_stats = limited_ds._plan.stats()
        return res

    @ConsumptionAPI
    def take(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Return up to ``limit`` rows from the :class:`Dataset`.

        This method is useful for inspecting data.

        .. warning::

            :meth:`~Dataset.take` moves up to ``limit`` rows to the caller's machine. If
            ``limit`` is large, this method can cause an ``OutOfMemory`` error on the
            caller.

        Examples:

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.take(3)
            [{'id': 0}, {'id': 1}, {'id': 2}]

        Time complexity: O(limit specified)

        Args:
            limit: The maximum number of rows to return.

        Returns:
            A list of up to ``limit`` rows from the dataset.

        .. seealso::

            :meth:`~Dataset.take_all`
                Call this method to return all rows.
        """
        if ray.util.log_once("dataset_take"):
            logger.info(
                "Tip: Use `take_batch()` instead of `take() / show()` to return "
                "records in pandas or numpy batch format."
            )
        output = []

        limited_ds = self.limit(limit)
        for row in limited_ds.iter_rows():
            output.append(row)
            if len(output) >= limit:
                break
        self._synchronize_progress_bar()

        # Save the computed stats to the original dataset.
        self._plan._snapshot_stats = limited_ds._plan.stats()
        return output

    @ConsumptionAPI
    def take_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return all of the rows in this :class:`Dataset`.

        This method is useful for inspecting small datasets.

        .. warning::

            :meth:`~Dataset.take_all` moves the entire dataset to the caller's
            machine. If the dataset is large, this method can cause an
            ``OutOfMemory`` error on the caller.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(5)
            >>> ds.take_all()
            [{'id': 0}, {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]

        Time complexity: O(dataset size)

        Args:
            limit: Raise an error if the size exceeds the specified limit.

        Returns:
            A list of all the rows in the dataset.

        .. seealso::

            :meth:`~Dataset.take`
                Call this method to return a specific number of rows.
        """
        output = []
        for row in self.iter_rows():
            output.append(row)
            if limit is not None and len(output) > limit:
                raise ValueError(
                    f"The dataset has more than the given limit of {limit} records."
                )
        self._synchronize_progress_bar()
        return output

    @ConsumptionAPI
    def show(self, limit: int = 20) -> None:
        """Print up to the given number of rows from the :class:`Dataset`.

        This method is useful for inspecting data.

        Examples:

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.show(3)
            {'id': 0}
            {'id': 1}
            {'id': 2}

        Time complexity: O(limit specified)

        Args:
            limit: The maximum number of row to print.

        .. seealso::

            :meth:`~Dataset.take`
                Call this method to get (not print) a given number of rows.
        """
        for row in self.take(limit):
            print(row)

    @ConsumptionAPI(
        if_more_than_read=True,
        datasource_metadata="row count",
        pattern="Time complexity:",
    )
    def count(self) -> int:
        """Count the number of records in the dataset.

        Time complexity: O(dataset size / parallelism), O(1) for parquet

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> ds.count()
            10

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

    @ConsumptionAPI(
        if_more_than_read=True,
        datasource_metadata="schema",
        extra_condition="or if ``fetch_if_missing=True`` (the default)",
        pattern="Time complexity:",
    )
    def schema(self, fetch_if_missing: bool = True) -> Optional["Schema"]:
        """Return the schema of the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> ds.schema()
            Column  Type
            ------  ----
            id      int64

        Time complexity: O(1)

        Args:
            fetch_if_missing: If True, synchronously fetch the schema if it's
                not known. If False, None is returned if the schema is not known.
                Default is True.

        Returns:
            The :class:`ray.data.Schema` class of the records, or None if the
            schema is not known and fetch_if_missing is False.
        """

        # First check if the schema is already known from materialized blocks.
        base_schema = self._plan.schema(fetch_if_missing=False)
        if base_schema is not None:
            return Schema(base_schema)
        if not fetch_if_missing:
            return None

        # Lazily execute only the first block to minimize computation.
        # We achieve this by appending a Limit[1] operation to a copy
        # of this Dataset, which we then execute to get its schema.
        base_schema = self.limit(1)._plan.schema(fetch_if_missing=fetch_if_missing)
        if base_schema:
            self._plan.cache_schema(base_schema)
            return Schema(base_schema)
        else:
            return None

    @ConsumptionAPI(
        if_more_than_read=True,
        datasource_metadata="schema",
        extra_condition="or if ``fetch_if_missing=True`` (the default)",
        pattern="Time complexity:",
    )
    def columns(self, fetch_if_missing: bool = True) -> Optional[List[str]]:
        """Returns the columns of this Dataset.

        Time complexity: O(1)

        Example:
            >>> import ray
            >>> # Create dataset from synthetic data.
            >>> ds = ray.data.range(1000)
            >>> ds.columns()
            ['id']

        Args:
            fetch_if_missing: If True, synchronously fetch the column names from the
                schema if it's not known. If False, None is returned if the schema is
                not known. Default is True.

        Returns:
            A list of the column names for this Dataset or None if schema is not known
            and `fetch_if_missing` is False.

        """
        schema = self.schema(fetch_if_missing=fetch_if_missing)
        if schema is not None:
            return schema.names
        return None

    def num_blocks(self) -> int:
        """Return the number of blocks of this dataset.

        Note that during read and transform operations, the number of blocks
        may be dynamically adjusted to respect memory limits, increasing the
        number of blocks at runtime.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100).repartition(10)
            >>> ds.num_blocks()
            10

        Time complexity: O(1)

        Returns:
            The number of blocks of this dataset.
        """
        return self._plan.initial_num_blocks()

    @ConsumptionAPI(if_more_than_read=True, pattern="Time complexity:")
    def size_bytes(self) -> int:
        """Return the in-memory size of the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> ds.size_bytes()
            80

        Time complexity: O(1)

        Returns:
            The in-memory size of the dataset in bytes, or None if the
            in-memory size is not known.
        """
        metadata = self._plan.execute().get_metadata()
        if not metadata or metadata[0].size_bytes is None:
            return None
        return sum(m.size_bytes for m in metadata)

    @ConsumptionAPI(if_more_than_read=True, pattern="Time complexity:")
    def input_files(self) -> List[str]:
        """Return the list of input files for the dataset.

        Examples:
            >>> import ray
            >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
            >>> ds.input_files()
            ['ray-example-data/iris.csv']

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
        """Writes the :class:`~ray.data.Dataset` to parquet files under the provided ``path``.

        The number of files is determined by the number of blocks in the dataset.
        To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        If pyarrow can't represent your data, this method errors.

        By default, the format of the output files is ``{uuid}_{block_idx}.parquet``,
        where ``uuid`` is a unique
        id for the dataset. To modify this behavior, implement a custom
        :class:`~ray.data.datasource.BlockWritePathProvider`
        and pass it in as the ``block_path_provider`` argument.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.write_parquet("local:///tmp/data/")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where
                parquet files are written to.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in the
                destination path. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            block_path_provider: A
                :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation specifying the filename structure for each output
                parquet file. By default, the format of the output files is
                ``{uuid}_{block_idx}.parquet``, where ``uuid`` is a unique id for the
                dataset.
            arrow_parquet_args_fn: Callable that returns a dictionary of write
                arguments that are provided to `pyarrow.parquet.write_table() <https:/\
                    /arrow.apache.org/docs/python/generated/\
                        pyarrow.parquet.write_table.html#pyarrow.parquet.write_table>`_
                when writing each block to a file. Overrides
                any duplicate keys from ``arrow_parquet_args``. Use this argument
                instead of ``arrow_parquet_args`` if any of your write arguments
                can't pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            ray_remote_args: Kwargs passed to :meth:`~ray.remote` in the write tasks.
            arrow_parquet_args: Options to pass to
                `pyarrow.parquet.write_table() <https://arrow.apache.org/docs/python\
                    /generated/pyarrow.parquet.write_table.html\
                        #pyarrow.parquet.write_table>`_, which is used to write out each
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
        """Writes the :class:`~ray.data.Dataset` to JSON and JSONL files.

        The number of files is determined by the number of blocks in the dataset.
        To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        This method is only supported for datasets with records that are convertible to
        pandas dataframes.

        By default, the format of the output files is ``{uuid}_{block_idx}.json``,
        where ``uuid`` is a unique id for the dataset. To modify this behavior,
        implement a custom
        :class:`~ray.data.file_based_datasource.BlockWritePathProvider`
        and pass it in as the ``block_path_provider`` argument.

        Examples:
            Write the dataset as JSON file to a local directory.

            >>> import ray
            >>> import pandas as pd
            >>> ds = ray.data.from_pandas([pd.DataFrame({"one": [1], "two": ["a"]})])
            >>> ds.write_json("local:///tmp/data")

            Write the dataset as JSONL files to a local directory.

            >>> ds = ray.data.read_json("s3://anonymous@ray-example-data/train.jsonl")
            >>> ds.write_json("local:///tmp/data")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where
                the JSON files are written to.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in the
                destination path. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            block_path_provider: A
                :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation specifying the filename structure for each output
                parquet file. By default, the format of the output files is
                ``{uuid}_{block_idx}.json``, where ``uuid`` is a unique id for the
                dataset.
            pandas_json_args_fn: Callable that returns a dictionary of write
                arguments that are provided to
                `pandas.DataFrame.to_json() <https://pandas.pydata.org/docs/reference/\
                    api/pandas.DataFrame.to_json.html>`_
                when writing each block to a file. Overrides
                any duplicate keys from ``pandas_json_args``. Use this parameter
                instead of ``pandas_json_args`` if any of your write arguments
                can't be pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.
            pandas_json_args: These args are passed to
                `pandas.DataFrame.to_json() <https://pandas.pydata.org/docs/reference/\
                    api/pandas.DataFrame.to_json.html>`_,
                which is used under the hood to write out each
                :class:`~ray.data.Dataset` block. These
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

    @PublicAPI(stability="alpha")
    @ConsumptionAPI
    def write_images(
        self,
        path: str,
        column: str,
        file_format: str = "png",
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Writes the :class:`~ray.data.Dataset` to images.

        Examples:
            >>> import ray
            >>> ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            >>> ds.write_images("local:///tmp/images", column="image")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where
                the images are written to.
            column: The column containing the data you want to write to images.
            file_format: The image file format to write with. For available options,
                see `Image file formats <https://pillow.readthedocs.io/en/latest\
                /handbook/image-file-formats.html>`_.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in the
                destination path. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.
        """  # noqa: E501
        self.write_datasource(
            ImageDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            dataset_uuid=self._uuid,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            column=column,
            file_format=file_format,
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
        """Writes the :class:`~ray.data.Dataset` to CSV files.

        The number of files is determined by the number of blocks in the dataset.
        To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        This method is only supported for datasets with records that are convertible to
        pyarrow tables.

        By default, the format of the output files is ``{uuid}_{block_idx}.csv``,
        where ``uuid`` is a unique id for the dataset. To modify this behavior,
        implement a custom
        :class:`~ray.data.datasource.BlockWritePathProvider`
        and pass it in as the ``block_path_provider`` argument.

        Examples:
            Write the dataset as CSV files to a local directory.

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.write_csv("local:///tmp/data")

            Write the dataset as CSV files to S3.

            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.write_csv("s3://bucket/folder/)  # doctest: +SKIP

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where
                the CSV files are written to.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in the
                destination path if ``True``. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            block_path_provider: A
                :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation specifying the filename structure for each output
                parquet file. By default,  the format of the output files is
                ``{uuid}_{block_idx}.csv``, where ``uuid`` is a unique id for the
                dataset.
            arrow_csv_args_fn: Callable that returns a dictionary of write
                arguments that are provided to `pyarrow.write.write_csv <https://\
                arrow.apache.org/docs/python/generated/\
                pyarrow.csv.write_csv.html#pyarrow.csv.write_csv>`_ when writing each
                block to a file. Overrides any duplicate keys from ``arrow_csv_args``.
                Use this argument instead of ``arrow_csv_args`` if any of your write
                arguments cannot be pickled, or if you'd like to lazily resolve the
                write arguments for each dataset block.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.
            arrow_csv_args: Options to pass to `pyarrow.write.write_csv <https://\
                arrow.apache.org/docs/python/generated/pyarrow.csv.write_csv.html\
                    #pyarrow.csv.write_csv>`_
                when writing each block to a file.
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
        """Write the :class:`~ray.data.Dataset` to TFRecord files.

        The `TFRecord <https://www.tensorflow.org/tutorials/load_data/tfrecord>`_
        files contain
        `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/\
            Example>`_
        records, with one Example record for each row in the dataset.

        .. warning::
            tf.train.Feature only natively stores ints, floats, and bytes,
            so this function only supports datasets with these data types,
            and will error if the dataset contains unsupported types.

        The number of files is determined by the number of blocks in the dataset.
        To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        This method is only supported for datasets with records that are convertible to
        pyarrow tables.

        By default, the format of the output files is ``{uuid}_{block_idx}.tfrecords``,
        where ``uuid`` is a unique id for the dataset. To modify this behavior,
        implement a custom
        :class:`~ray.data.file_based_datasource.BlockWritePathProvider`
        and pass it in as the ``block_path_provider`` argument.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.write_tfrecords("local:///tmp/data/")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where tfrecords
                files are written to.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in the
                destination path. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            block_path_provider: A
                :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation specifying the filename structure for each output
                parquet file. By default, the format of the output files is
                ``{uuid}_{block_idx}.tfrecords``, where ``uuid`` is a unique id for the
                dataset.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.

        """

        self.write_datasource(
            TFRecordDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            dataset_uuid=self._uuid,
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
        """Writes the dataset to `WebDataset <https://webdataset.github.io/webdataset/>`_ files.

        The `TFRecord <https://www.tensorflow.org/tutorials/load_data/tfrecord>`_
        files will contain
        `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_ # noqa: E501
        records, with one Example record for each row in the dataset.

        .. warning::
            tf.train.Feature only natively stores ints, floats, and bytes,
            so this function only supports datasets with these data types,
            and will error if the dataset contains unsupported types.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use :meth:`Dataset.repartition`.

        Unless a custom block path provider is given, the format of the output
        files is ``{uuid}_{block_idx}.tfrecords``, where ``uuid`` is a unique id
        for the dataset.

        Examples:

            .. testcode::
                :skipif: True

                import ray

                ds = ray.data.range(100)
                ds.write_webdataset("s3://bucket/folder/")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where tfrecords
                files are written to.
            filesystem: The filesystem implementation to write to.
            try_create_dir: If ``True``, attempts to create all
                directories in the destination path. Does nothing if all directories
                already exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                ``pyarrow.fs.FileSystem.open_output_stream``
            block_path_provider: :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation to write each dataset block to a custom output path.
            ray_remote_args: Kwargs passed to ``ray.remote`` in the write tasks.

        """

        from ray.data.datasource.webdataset_datasource import WebDatasetDatasource

        self.write_datasource(
            WebDatasetDatasource(),
            ray_remote_args=ray_remote_args,
            path=path,
            dataset_uuid=self._uuid,
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
        column: str,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Writes a column of the :class:`~ray.data.Dataset` to .npy files.

        This is only supported for columns in the datasets that can be converted to
        NumPy arrays.

        The number of files is determined by the number of blocks in the dataset.
        To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        By default, the format of the output files is ``{uuid}_{block_idx}.npy``,
        where ``uuid`` is a unique id for the dataset. To modify this behavior,
        implement a custom
        :class:`~ray.data.datasource.BlockWritePathProvider`
        and pass it in as the ``block_path_provider`` argument.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(100)
            >>> ds.write_numpy("local:///tmp/data/", column="id")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory, where
                the npy files are written to.
            column: The name of the column that contains the data to
                be written.
            filesystem: The pyarrow filesystem implementation to write to.
                These filesystems are specified in the
                `pyarrow docs <https://arrow.apache.org/docs\
                /python/api/filesystems.html#filesystem-implementations>`_.
                Specify this if you need to provide specific configurations to the
                filesystem. By default, the filesystem is automatically selected based
                on the scheme of the paths. For example, if the path begins with
                ``s3://``, the ``S3FileSystem`` is used.
            try_create_dir: If ``True``, attempts to create all directories in
                destination path. Does nothing if all directories already
                exist. Defaults to ``True``.
            arrow_open_stream_args: kwargs passed to
                `pyarrow.fs.FileSystem.open_output_stream <https://arrow.apache.org\
                /docs/python/generated/pyarrow.fs.FileSystem.html\
                #pyarrow.fs.FileSystem.open_output_stream>`_, which is used when
                opening the file to write to.
            block_path_provider: A
                :class:`~ray.data.datasource.BlockWritePathProvider`
                implementation specifying the filename structure for each output
                parquet file. By default,  the format of the output files is
                ``{uuid}_{block_idx}.npy``, where ``uuid`` is a unique id for the
                dataset.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.
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

    @ConsumptionAPI
    def write_sql(
        self,
        sql: str,
        connection_factory: Callable[[], Connection],
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write to a database that provides a
        `Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

        .. note::

            This method writes data in parallel using the DB API2 ``executemany``
            method. To learn more about this method, see
            `PEP 249 <https://peps.python.org/pep-0249/#executemany>`_.

        Examples:

            .. testcode::

                import sqlite3
                import ray

                connection = sqlite3.connect("example.db")
                connection.cursor().execute("CREATE TABLE movie(title, year, score)")
                dataset = ray.data.from_items([
                    {"title": "Monty Python and the Holy Grail", "year": 1975, "score": 8.2},
                    {"title": "And Now for Something Completely Different", "year": 1971, "score": 7.5}
                ])

                dataset.write_sql(
                    "INSERT INTO movie VALUES(?, ?, ?)", lambda: sqlite3.connect("example.db")
                )

                result = connection.cursor().execute("SELECT * FROM movie ORDER BY year")
                print(result.fetchall())

            .. testoutput::

                [('And Now for Something Completely Different', 1971, 7.5), ('Monty Python and the Holy Grail', 1975, 8.2)]

            .. testcode::
                :hide:

                import os
                os.remove("example.db")

        Arguments:
            sql: An ``INSERT INTO`` statement that specifies the table to write to. The
                number of parameters must match the number of columns in the table.
            connection_factory: A function that takes no arguments and returns a
                Python DB API2
                `Connection object <https://peps.python.org/pep-0249/#connection-objects>`_.
            ray_remote_args: Keyword arguments passed to :meth:`~ray.remote` in the
                write tasks.
        """  # noqa: E501
        self.write_datasource(
            SQLDatasource(connection_factory),
            ray_remote_args=ray_remote_args,
            sql=sql,
        )

    @ConsumptionAPI
    def write_mongo(
        self,
        uri: str,
        database: str,
        collection: str,
        ray_remote_args: Dict[str, Any] = None,
    ) -> None:
        """Writes the :class:`~ray.data.Dataset` to a MongoDB database.

        This method is only supported for datasets convertible to pyarrow tables.

        The number of parallel writes is determined by the number of blocks in the
        dataset. To control the number of number of blocks, call
        :meth:`~ray.data.Dataset.repartition`.

        .. warning::
            This method supports only a subset of the PyArrow's types, due to the
            limitation of pymongoarrow which is used underneath. Writing unsupported
            types fails on type checking. See all the supported types at:
            https://mongo-arrow.readthedocs.io/en/latest/data_types.html.

        .. note::
            The records are inserted into MongoDB as new documents. If a record has
            the _id field, this _id must be non-existent in MongoDB, otherwise the write
            is rejected and fail (hence preexisting documents are protected from
            being mutated). It's fine to not have _id field in record and MongoDB will
            auto generate one at insertion.

        Examples:

            .. testcode::
                :skipif: True

                import ray

                ds = ray.data.range(100)
                ds.write_mongo(
                    uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
                    database="my_db",
                    collection="my_collection"
                )

        Args:
            uri: The URI to the destination MongoDB where the dataset is
                written to. For the URI format, see details in the
                `MongoDB docs <https://www.mongodb.com/docs/manual/reference\
                    /connection-string/>`_.
            database: The name of the database. This database must exist otherwise
                a ValueError is raised.
            collection: The name of the collection in the database. This collection
                must exist otherwise a ValueError is raised.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the write tasks.

        Raises:
            ValueError: if ``database`` doesn't exist.
            ValueError: if ``collection`` doesn't exist.
        """
        from ray.data.datasource import MongoDatasource

        self.write_datasource(
            MongoDatasource(),
            ray_remote_args=ray_remote_args,
            uri=uri,
            database=database,
            collection=collection,
        )

    @ConsumptionAPI(pattern="Time complexity:")
    def write_datasource(
        self,
        datasource: Datasource,
        *,
        ray_remote_args: Dict[str, Any] = None,
        **write_args,
    ) -> None:
        """Writes the dataset to a custom :class:`~ray.data.Datasource`.

        For an example of how to use this method, see
        :ref:`Implementing a Custom Datasource <custom_datasources>`.

        Time complexity: O(dataset size / parallelism)

        Args:
            datasource: The :class:`~ray.data.Datasource` to write to.
            ray_remote_args: Kwargs passed to ``ray.remote`` in the write tasks.
            write_args: Additional write args to pass to the :class:`~ray.data.Datasource`.
        """  # noqa: E501
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

                datasource.on_write_start(**write_args)
                self._write_ds = Dataset(
                    plan, self._epoch, self._lazy, logical_plan
                ).materialize()
                blocks = ray.get(self._write_ds._plan.execute().get_blocks())
                assert all(
                    isinstance(block, pd.DataFrame) and len(block) == 1
                    for block in blocks
                )
                write_results = [block["write_result"][0] for block in blocks]
                datasource.on_write_complete(write_results, **write_args)
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
        ),
        pattern="Returns:",
    )
    def iterator(self) -> DataIterator:
        """Return a :class:`~ray.data.DataIterator` over this dataset.

        Don't call this method directly. Use it internally.

        Returns:
            A :class:`~ray.data.DataIterator` over this dataset.
        """
        return DataIteratorImpl(self)

    @ConsumptionAPI
    def iter_rows(self, *, prefetch_blocks: int = 0) -> Iterator[Dict[str, Any]]:
        """Return an iterator over the rows in this dataset.

        Examples:
            >>> import ray
            >>> for row in ray.data.range(3).iter_rows():
            ...     print(row)
            {'id': 0}
            {'id': 1}
            {'id': 2}

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.

        Returns:
            An iterator over the rows in this dataset.
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
        _collate_fn: Optional[Callable[[DataBatch], CollatedData]] = None,
        # Deprecated.
        prefetch_blocks: int = 0,
    ) -> Iterator[DataBatch]:
        """Return an iterator over batches of data.

        This method is useful for model training.

        Examples:

            .. testcode::

                import ray

                ds = ray.data.read_images("example://image-datasets/simple")

                for batch in ds.iter_batches(batch_size=2, batch_format="numpy"):
                    print(batch)

            .. testoutput::
                :options: +MOCK

                {'image': array([[[[...]]]], dtype=uint8)}
                ...
                {'image': array([[[[...]]]], dtype=uint8)}

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool is used
                to fetch the objects to the local node and format the batches. Defaults
                to 1.
            batch_size: The number of rows in each batch, or ``None`` to use entire
                blocks as batches (blocks may contain different numbers of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            batch_format: If ``"default"`` or ``"numpy"``, batches are
                ``Dict[str, numpy.ndarray]``. If ``"pandas"``, batches are
                ``pandas.DataFrame``.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If not ``None``, the data is randomly shuffled
                using a local in-memory shuffle buffer, and this value serves as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer are drained.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over batches of data.
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
        device: str = "auto",
        collate_fn: Optional[Callable[[Dict[str, np.ndarray]], CollatedData]] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        # Deprecated
        prefetch_blocks: int = 0,
    ) -> Iterator[TorchBatchType]:
        """Return an iterator over batches of data represented as Torch tensors.

        This iterator yields batches of type ``Dict[str, torch.Tensor]``.
        For more flexibility, call :meth:`~Dataset.iter_batches` and manually convert
        your data to Torch tensors.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(
            ...     12,
            ... ).iter_torch_batches(batch_size=4):
            ...     print(batch)
            {'id': tensor([0, 1, 2, 3])}
            {'id': tensor([4, 5, 6, 7])}
            {'id': tensor([ 8,  9, 10, 11])}

            Use the ``collate_fn`` to customize how the tensor batch is created.

            >>> from typing import Any, Dict
            >>> import torch
            >>> import numpy as np
            >>> import ray
            >>> def collate_fn(batch: Dict[str, np.ndarray]) -> Any:
            ...     return torch.stack(
            ...         [torch.as_tensor(array) for array in batch.values()],
            ...         axis=1
            ...     )
            >>> dataset = ray.data.from_items([
            ...     {"col_1": 1, "col_2": 2},
            ...     {"col_1": 3, "col_2": 4}])
            >>> for batch in dataset.iter_torch_batches(collate_fn=collate_fn):
            ...     print(batch)
            tensor([[1, 2],
                    [3, 4]])


        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool is used
                to fetch the objects to the local node, format the batches, and apply
                the ``collate_fn``. Defaults to 1.
            batch_size: The number of rows in each batch, or ``None`` to use entire
                blocks as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The Torch dtype(s) for the created tensor(s); if ``None``, the dtype
                is inferred from the tensor data. You can't use this parameter with
                ``collate_fn``.
            device: The device on which the tensor should be placed. Defaults to
                "auto" which moves the tensors to the appropriate device when the
                Dataset is passed to Ray Train and ``collate_fn`` is not provided.
                Otherwise, defaults to CPU. You can't use this parameter with
                ``collate_fn``.
            collate_fn: A function to convert a Numpy batch to a PyTorch tensor batch.
                When this parameter is specified, the user should manually handle the
                host to device data transfer outside of collate_fn.
                This is useful for further processing the data after it has been
                batched. Potential use cases include collating along a dimension other
                than the first, padding sequences of various lengths, or generally
                handling batches of different length tensors. If not provided, the
                default collate function is used which simply converts the batch of
                numpy arrays to a batch of PyTorch tensors. This API is still
                experimental and is subject to change. You can't use this parameter in
                conjunction with ``dtypes`` or ``device``.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If not ``None``, the data is randomly shuffled
                using a local in-memory shuffle buffer, and this value serves as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer are drained.
                ``batch_size`` must also be specified when using local shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over Torch Tensor batches.

        .. seealso::
            :meth:`Dataset.iter_batches`
                Call this method to manually convert your data to Torch tensors.
        """  # noqa: E501
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
        """Return an iterator over batches of data represented as TensorFlow tensors.

        This iterator yields batches of type ``Dict[str, tf.Tensor]``.
        For more flexibility, call :meth:`~Dataset.iter_batches` and manually convert
        your data to TensorFlow tensors.

        .. tip::
            If you don't need the additional flexibility provided by this method,
            consider using :meth:`~ray.data.Dataset.to_tf` instead. It's easier
            to use.

        Examples:

            .. testcode::

                import ray

                ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

                tf_dataset = ds.to_tf(
                    feature_columns="sepal length (cm)",
                    label_columns="target",
                    batch_size=2
                )
                for features, labels in tf_dataset:
                    print(features, labels)

            .. testoutput::

                tf.Tensor([5.1 4.9], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)
                ...
                tf.Tensor([6.2 5.9], shape=(2,), dtype=float64) tf.Tensor([2 2], shape=(2,), dtype=int64)

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool is used
                to fetch the objects to the local node, format the batches, and apply
                the ``collate_fn``. Defaults to 1.
            batch_size: The number of rows in each batch, or ``None`` to use entire
                blocks as batches (blocks may contain different numbers of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The TensorFlow dtype(s) for the created tensor(s); if ``None``, the
                dtype is inferred from the tensor data.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If not ``None``, the data is randomly shuffled
                using a local in-memory shuffle buffer, and this value serves as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer are drained.
                ``batch_size`` must also be specified when using local shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over TensorFlow Tensor batches.

        .. seealso::
            :meth:`Dataset.iter_batches`
                Call this method to manually convert your data to TensorFlow tensors.
        """  # noqa: E501
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
        """Return a
        `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`_
        over this :class:`~ray.data.Dataset`.

        This is only supported for datasets convertible to Arrow records.

        It is recommended to use the returned ``IterableDataset`` directly
        instead of passing it into a torch ``DataLoader``.

        Each element in ``IterableDataset`` is a tuple consisting of 2
        elements. The first item contains the feature tensor(s), and the
        second item is the label tensor. Those can take on different
        forms, depending on the specified arguments.

        For the features tensor (N is the ``batch_size`` and n, m, k
        are the number of features per tensor):

        * If ``feature_columns`` is a ``List[str]``, the features is
          a tensor of shape (N, n), with columns corresponding to
          ``feature_columns``

        * If ``feature_columns`` is a ``List[List[str]]``, the features is
          a list of tensors of shape [(N, m),...,(N, k)], with columns of each
          tensor corresponding to the elements of ``feature_columns``

        * If ``feature_columns`` is a ``Dict[str, List[str]]``, the features
          is a dict of key-tensor pairs of shape
          {key1: (N, m),..., keyN: (N, k)}, with columns of each
          tensor corresponding to the value of ``feature_columns`` under the
          key.

        If ``unsqueeze_label_tensor=True`` (default), the label tensor is
        of shape (N, 1). Otherwise, it is of shape (N,).
        If ``label_column`` is specified as ``None``, then no column from the
        ``Dataset`` is treated as the label, and the output label tensor
        is ``None``.

        Note that you probably want to call :meth:`Dataset.split` on this dataset if
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
                If ``None``, then use all columns except the label column as
                the features.
            label_column_dtype: The torch dtype to
                use for the label column. If ``None``, then automatically infer
                the dtype.
            feature_column_dtypes: The dtypes to use for the feature
                tensors. This should match the format of ``feature_columns``,
                or be a single dtype, in which case it is applied to
                all tensors. If ``None``, then automatically infer the dtype.
            batch_size: How many samples per batch to yield at a time.
                Defaults to 1.
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool is used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the datasetContext.
            drop_last: Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of the stream is not divisible by the batch
                size, then the last batch is smaller. Defaults to False.
            local_shuffle_buffer_size: If non-None, the data is randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer is drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.
            unsqueeze_label_tensor: If set to True, the label tensor
                is unsqueezed (reshaped to (N, 1)). Otherwise, it will
                be left as is, that is (N, ). In general, regression loss
                functions expect an unsqueezed tensor, while classification
                loss functions expect a squeezed one. Defaults to True.
            unsqueeze_feature_tensors: If set to True, the features tensors
                are unsqueezed (reshaped to (N, 1)) before being concatenated into
                the final features tensor. Otherwise, they are left as is, that is
                (N, ). Defaults to True.

        Returns:
            A `Torch IterableDataset`_.
        """  # noqa: E501

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
        """Return a `TensorFlow Dataset <https://www.tensorflow.org/api_docs/python/tf/data/Dataset/>`_
        over this :class:`~ray.data.Dataset`.

        .. warning::
            If your :class:`~ray.data.Dataset` contains ragged tensors, this method errors.
            To prevent errors, :ref:`resize your tensors <transforming_tensors>`.

        Examples:
            >>> import ray
            >>> ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
            >>> ds
            Dataset(
               num_blocks=...,
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

            >>> ds.to_tf(feature_columns="sepal length (cm)", label_columns="target")
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your model accepts a dictionary as input, specify a list of feature columns.

            >>> ds.to_tf(["sepal length (cm)", "sepal width (cm)"], "target")
            <_OptionsDataset element_spec=({'sepal length (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), 'sepal width (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal width (cm)')}, TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your dataset contains multiple features but your model accepts a single
            tensor as input, combine features with
            :class:`~ray.data.preprocessors.Concatenator`.

            >>> from ray.data.preprocessors import Concatenator
            >>> preprocessor = Concatenator(output_column_name="features", exclude="target")
            >>> ds = preprocessor.transform(ds)
            >>> ds
            Concatenator
            +- Dataset(
                  num_blocks=...,
                  num_rows=150,
                  schema={
                     sepal length (cm): double,
                     sepal width (cm): double,
                     petal length (cm): double,
                     petal width (cm): double,
                     target: int64
                  }
               )
            >>> ds.to_tf("features", "target")
            <_OptionsDataset element_spec=(TensorSpec(shape=(None, 4), dtype=tf.float64, name='features'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

        Args:
            feature_columns: Columns that correspond to model inputs. If this is a
                string, the input data is a tensor. If this is a list, the input data
                is a ``dict`` that maps column names to their tensor representation.
            label_column: Columns that correspond to model targets. If this is a
                string, the target data is a tensor. If this is a list, the target data
                is a ``dict`` that maps column names to their tensor representation.
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool is used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1. You can revert back to the old
                prefetching behavior that uses `prefetch_blocks` by setting
                `use_legacy_iter_batches` to True in the :class:`~ray.data.DataContext`.
            batch_size: Record batch size. Defaults to 1.
            drop_last: Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of the stream is not divisible by the batch
                size, then the last batch is smaller. Defaults to False.
            local_shuffle_buffer_size: If non-None, the data is randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer is drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            A `TensorFlow Dataset`_ that yields inputs and targets.

        .. seealso::

            :meth:`~ray.data.Dataset.iter_tf_batches`
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
        verify_meta: bool = True,
    ) -> "dask.DataFrame":
        """Convert this :class:`~ray.data.Dataset` into a
        `Dask DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html#dask.dataframe.DataFrame>`_.

        This is only supported for datasets convertible to Arrow records.

        Note that this function will set the Dask scheduler to Dask-on-Ray
        globally, via the config.

        Time complexity: O(dataset size / parallelism)

        Args:
            meta: An empty `pandas DataFrame`_ or `Series`_ that matches the dtypes and column
                names of the stream. This metadata is necessary for many algorithms in
                dask dataframe to work. For ease of use, some alternative inputs are
                also available. Instead of a DataFrame, a dict of ``{name: dtype}`` or
                iterable of ``(name, dtype)`` can be provided (note that the order of
                the names should match the order of the columns). Instead of a series, a
                tuple of ``(name, dtype)`` can be used.
                By default, this is inferred from the underlying Dataset schema,
                with this argument supplying an optional override.
            verify_meta: If True, Dask will check that the partitions have consistent
                metadata. Defaults to True.

        Returns:
            A `Dask DataFrame`_ created from this dataset.

        .. _pandas DataFrame: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        .. _Series: https://pandas.pydata.org/docs/reference/api/pandas.Series.html
        """  # noqa: E501
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
                    "Dataset.to_dask() must be used with Dask-on-Ray, please "
                    "set the Dask scheduler to ray_dask_get (located in "
                    "ray.util.dask)."
                )
            return _block_to_df(block)

        if meta is None:
            from ray.data.extensions import TensorDtype

            # Infer Dask metadata from Dataset schema.
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
            verify_meta=verify_meta,
        )
        return ddf

    @ConsumptionAPI(pattern="Time complexity:")
    def to_mars(self) -> "mars.DataFrame":
        """Convert this :class:`~ray.data.Dataset` into a
        `Mars DataFrame <https://mars-project.readthedocs.io/en/latest/reference/dataframe/index.html>`_.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A `Mars DataFrame`_ created from this dataset.
        """  # noqa: E501
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
        """Convert this :class:`~ray.data.Dataset` into a
        `Modin DataFrame <https://modin.readthedocs.io/en/stable/flow/modin/pandas/dataframe.html>`_.

        This works by first converting this dataset into a distributed set of
        Pandas DataFrames (using :meth:`Dataset.to_pandas_refs`).
        See caveats there. Then the individual DataFrames are used to
        create the Modin DataFrame using
        ``modin.distributed.dataframe.pandas.partitions.from_partitions()``.

        This is only supported for datasets convertible to Arrow records.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using :meth:`.to_arrow_refs` or
        :meth:`.get_internal_block_refs`.

        Time complexity: O(dataset size / parallelism)

        Returns:
            A `Modin DataFrame`_ created from this dataset.
        """  # noqa: E501

        from modin.distributed.dataframe.pandas.partitions import from_partitions

        pd_objs = self.to_pandas_refs()
        return from_partitions(pd_objs, axis=0)

    @ConsumptionAPI(pattern="Time complexity:")
    def to_spark(self, spark: "pyspark.sql.SparkSession") -> "pyspark.sql.DataFrame":
        """Convert this :class:`~ray.data.Dataset` into a
        `Spark DataFrame <https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html>`_.

        Time complexity: O(dataset size / parallelism)

        Args:
            spark: A `SparkSession`_, which must be created by RayDP (Spark-on-Ray).

        Returns:
            A `Spark DataFrame`_ created from this dataset.

        .. _SparkSession: https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html
        """  # noqa: E501
        import raydp

        schema = self.schema()
        if isinstance(schema, Schema):
            schema = schema.base_schema  # Backwards compat with non strict mode.
        return raydp.spark.ray_dataset_to_spark_dataframe(
            spark, schema, self.get_internal_block_refs()
        )

    @ConsumptionAPI(pattern="Time complexity:")
    def to_pandas(self, limit: int = None) -> "pandas.DataFrame":
        """Convert this :class:`~ray.data.Dataset` to a single pandas DataFrame.

        This method errors if the number of rows exceeds the provided ``limit``.
        To truncate the dataset beforehand, call :meth:`.limit`.

        Examples:
            >>> import ray
            >>> ds = ray.data.from_items([{"a": i} for i in range(3)])
            >>> ds.to_pandas()
               a
            0  0
            1  1
            2  2

        Time complexity: O(dataset size)

        Args:
            limit: The maximum number of rows to return. An error is
                raised if the dataset has more rows than this limit. Defaults to
                ``None``, which means no limit.

        Returns:
            A pandas DataFrame created from this dataset, containing a limited
            number of rows.

        Raises:
            ValueError: if the number of rows in the :class:`~ray.data.Dataset` exceeds
                ``limit``.
        """
        count = self.count()
        if limit is not None and count > limit:
            raise ValueError(
                f"the dataset has more than the given limit of {limit} "
                f"rows: {count}. If you are sure that a DataFrame with "
                f"{count} rows will fit in local memory, set ds.to_pandas(limit=None) "
                "to disable limits."
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
        """Converts this :class:`~ray.data.Dataset` into a distributed set of Pandas
        dataframes.

        One DataFrame is created for each block in this Dataset.

        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using :meth:`Dataset.to_arrow` or
        :meth:`Dataset.get_internal_block_refs`.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10, parallelism=2)
            >>> refs = ds.to_pandas_refs()
            >>> len(refs)
            2

        Time complexity: O(dataset size / parallelism)

        Returns:
            A list of remote pandas DataFrames created from this dataset.
        """

        block_to_df = cached_remote_fn(_block_to_df)
        return [block_to_df.remote(block) for block in self.get_internal_block_refs()]

    @DeveloperAPI
    def to_numpy_refs(
        self, *, column: Optional[str] = None
    ) -> List[ObjectRef[np.ndarray]]:
        """Converts this :class:`~ray.data.Dataset` into a distributed set of NumPy
        ndarrays or dictionary of NumPy ndarrays.

        This is only supported for datasets convertible to NumPy ndarrays.
        This function induces a copy of the data. For zero-copy access to the
        underlying data, consider using :meth:`Dataset.to_arrow` or
        :meth:`Dataset.get_internal_block_refs`.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10, parallelism=2)
            >>> refs = ds.to_numpy_refs()
            >>> len(refs)
            2

        Time complexity: O(dataset size / parallelism)

        Args:
            column: The name of the column to convert to numpy. If ``None``, all columns
                are used. If multiple columns are specified, each returned
            future represents a dict of ndarrays. Defaults to None.

        Returns:
            A list of remote NumPy ndarrays created from this dataset.
        """
        block_to_ndarray = cached_remote_fn(_block_to_ndarray)
        return [
            block_to_ndarray.remote(block, column=column)
            for block in self.get_internal_block_refs()
        ]

    @ConsumptionAPI(pattern="Time complexity:")
    @DeveloperAPI
    def to_arrow_refs(self) -> List[ObjectRef["pyarrow.Table"]]:
        """Convert this :class:`~ray.data.Dataset` into a distributed set of PyArrow
        tables.

        One PyArrow table is created for each block in this Dataset.

        This method is only supported for datasets convertible to PyArrow tables.
        This function is zero-copy if the existing data is already in PyArrow
        format. Otherwise, the data is converted to PyArrow format.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10, parallelism=2)
            >>> refs = ds.to_arrow_refs()
            >>> len(refs)
            2

        Time complexity: O(1) unless conversion is required.

        Returns:
            A list of remote PyArrow tables created from this dataset.
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
        """Convert this dataset into a distributed RandomAccessDataset (EXPERIMENTAL).

        RandomAccessDataset partitions the dataset across the cluster by the given
        sort key, providing efficient random access to records via binary search. A
        number of worker actors are created, each of which has zero-copy access to the
        underlying sorted data blocks of the dataset.

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

    @Deprecated
    @ConsumptionAPI
    def repeat(self, times: Optional[int] = None) -> "DatasetPipeline":
        """Convert this into a DatasetPipeline by looping over this dataset.

        Transformations prior to the call to ``repeat()`` are evaluated once.
        Transformations done on the returned pipeline are evaluated on each
        loop of the pipeline over the base dataset.

        Note that every repeat of the dataset is considered an "epoch" for
        the purposes of ``DatasetPipeline.iter_epochs()``.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(5, parallelism=1)
            >>> # Infinite pipeline of numbers [0, 5)
            >>> ds.repeat().take_batch()
            {'id': array([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...])}
            >>> # Can shuffle each epoch (dataset) in the pipeline.
            >>> ds.repeat().random_shuffle().take_batch() # doctest: +SKIP
            {'id': array([2, 3, 0, 4, 1, 4, 0, 2, 1, 3, ...])}

        Args:
            times: The number of times to loop over this dataset, or None
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
        outer_stats.dataset_uuid = uuid

        if times is not None and times < 1:
            raise ValueError("`times` must be >= 1, got {}".format(times))

        class Iterator:
            def __init__(self, blocks):
                self._blocks = blocks
                self._i = 0

            def __next__(self) -> Callable[[], "Dataset"]:
                if times and self._i >= times:
                    raise StopIteration
                epoch = self._i
                blocks = self._blocks
                self._i += 1

                def gen():
                    ds = Dataset(
                        ExecutionPlan(
                            blocks,
                            outer_stats,
                            dataset_uuid=uuid,
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
                lambda ds, read_stage=read_stage: Dataset(
                    ds._plan.with_stage(read_stage), ds._epoch, True
                )
            )
        return pipe

    @Deprecated
    def window(
        self,
        *,
        blocks_per_window: Optional[int] = None,
        bytes_per_window: Optional[int] = None,
    ) -> "DatasetPipeline":
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
                This is treated as an upper bound for the window size, but each
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

            def __next__(self) -> "Dataset":
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

    @Deprecated(message="Use `Dataset.materialize()` instead.")
    def fully_executed(self) -> "MaterializedDataset":
        logger.warning(
            "Deprecation warning: use Dataset.materialize() instead of "
            "fully_executed()."
        )
        self._plan.execute(force_read=True)
        return self

    @Deprecated(message="Check `isinstance(Dataset, MaterializedDataset)` instead.")
    def is_fully_executed(self) -> bool:
        logger.warning(
            "Deprecation warning: Check "
            "`isinstance(Dataset, MaterializedDataset)` "
            "instead of using is_fully_executed()."
        )
        return self._plan.has_computed_output()

    @ConsumptionAPI(pattern="store memory.", insert_after=True)
    def materialize(self) -> "MaterializedDataset":
        """Execute and materialize this dataset into object store memory.

        This can be used to read all blocks into memory. By default, Dataset
        doesn't read blocks from the datasource until the first transform.

        Note that this does not mutate the original Dataset. Only the blocks of the
        returned MaterializedDataset class are pinned in memory.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(10)
            >>> materialized_ds = ds.materialize()
            >>> materialized_ds
            MaterializedDataset(num_blocks=..., num_rows=10, schema={id: int64})

        Returns:
            A MaterializedDataset holding the materialized data blocks.
        """
        copy = Dataset.copy(self, _deep_copy=True, _as=MaterializedDataset)
        copy._plan.execute(force_read=True)

        blocks = copy._plan._snapshot_blocks
        blocks_with_metadata = blocks.get_blocks_with_metadata() if blocks else []
        # TODO(hchen): Here we generate the same number of blocks as
        # the original Dataset. Because the old code path does this, and
        # some unit tests implicily depend on this behavior.
        # After we remove the old code path, we should consider merging
        # some blocks for better perf.
        ref_bundles = [
            RefBundle(
                blocks=[block_with_metadata],
                owns_blocks=False,
            )
            for block_with_metadata in blocks_with_metadata
        ]
        logical_plan = LogicalPlan(InputData(input_data=ref_bundles))
        output = MaterializedDataset(
            ExecutionPlan(
                blocks,
                copy._plan.stats(),
                run_by_consumer=False,
            ),
            copy._epoch,
            copy._lazy,
            logical_plan,
        )
        output._plan.execute()  # No-op that marks the plan as fully executed.
        output._plan._in_stats.dataset_uuid = self._get_uuid()
        return output

    @ConsumptionAPI(pattern="timing information.", insert_after=True)
    def stats(self) -> str:
        """Returns a string containing execution timing information.

        Note that this does not trigger execution, so if the dataset has not yet
        executed, an empty string is returned.

        Examples:

        .. testcode::

            import ray

            ds = ray.data.range(10)
            assert ds.stats() == ""

            ds = ds.materialize()
            print(ds.stats())

        .. testoutput::
            :options: +MOCK

            Stage 0 Read: 20/20 blocks executed in 0.3s
            * Remote wall time: 16.29us min, 7.29ms max, 1.21ms mean, 24.17ms total
            * Remote cpu time: 16.0us min, 2.54ms max, 810.45us mean, 16.21ms total
            * Peak heap memory usage (MiB): 137968.75 min, 142734.38 max, 139846 mean
            * Output num rows: 0 min, 1 max, 0 mean, 10 total
            * Output size bytes: 0 min, 8 max, 4 mean, 80 total
            * Tasks per node: 20 min, 20 max, 20 mean; 1 nodes used

        """
        return self._get_stats_summary().to_string()

    def _get_stats_summary(self) -> DatasetStatsSummary:
        return self._plan.stats_summary()

    @ConsumptionAPI(pattern="Time complexity:")
    @DeveloperAPI
    def get_internal_block_refs(self) -> List[ObjectRef[Block]]:
        """Get a list of references to the underlying blocks of this dataset.

        This function can be used for zero-copy access to the data. It blocks
        until the underlying blocks are computed.

        Examples:
            >>> import ray
            >>> ds = ray.data.range(1)
            >>> ds.get_internal_block_refs()
            [ObjectRef(...)]

        Time complexity: O(1)

        Returns:
            A list of references to this dataset's blocks.
        """
        blocks = self._plan.execute().get_blocks()
        self._synchronize_progress_bar()
        return blocks

    @Deprecated(
        message="Dataset is lazy by default, so this conversion call is no longer "
        "needed and this API will be removed in a future release"
    )
    def lazy(self) -> "Dataset":
        """Enable lazy evaluation.

        Dataset is lazy by default, so this is only useful for datasets created
        from :func:`ray.data.from_items() <ray.data.read_api.from_items>`, which is
        eager.

        The returned dataset is a lazy dataset, where all subsequent operations
        on the stream won't be executed until the dataset is consumed
        (e.g. ``.take()``, ``.iter_batches()``, ``.to_torch()``, ``.to_tf()``, etc.)
        or execution is manually triggered via ``.materialize()``.
        """
        ds = Dataset(
            self._plan, self._epoch, lazy=True, logical_plan=self._logical_plan
        )
        ds._set_uuid(self._get_uuid())
        return ds

    def has_serializable_lineage(self) -> bool:
        """Whether this dataset's lineage is able to be serialized for storage and
        later deserialized, possibly on a different cluster.

        Only datasets that are created from data that we know will still exist at
        deserialization time, e.g. data external to this Ray cluster such as persistent
        cloud object stores, support lineage-based serialization. All of the
        ray.data.read_*() APIs support lineage-based serialization.

        Examples:

            >>> import ray
            >>> ray.data.from_items(list(range(10))).has_serializable_lineage()
            False
            >>> ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv").has_serializable_lineage()
            True
        """  # noqa: E501
        return self._plan.has_lazy_input()

    @DeveloperAPI
    def serialize_lineage(self) -> bytes:
        """
        Serialize this dataset's lineage, not the actual data or the existing data
        futures, to bytes that can be stored and later deserialized, possibly on a
        different cluster.

        Note that this will drop all computed data, and that everything is
        recomputed from scratch after deserialization.

        Use :py:meth:`Dataset.deserialize_lineage` to deserialize the serialized
        bytes returned from this method into a Dataset.

        .. note::
            Unioned and zipped datasets, produced by :py:meth`Dataset.union` and
            :py:meth:`Dataset.zip`, are not lineage-serializable.

        Examples:

            .. testcode::

                import ray

                ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
                serialized_ds = ds.serialize_lineage()
                ds = ray.data.Dataset.deserialize_lineage(serialized_ds)
                print(ds)

            .. testoutput::

                Dataset(
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


        Returns:
            Serialized bytes containing the lineage of this dataset.
        """
        if not self.has_serializable_lineage():
            raise ValueError(
                "Lineage-based serialization is not supported for this stream, which "
                "means that it cannot be used as a tunable hyperparameter. "
                "Lineage-based serialization is explicitly NOT supported for unioned "
                "or zipped datasets (see docstrings for those methods), and is only "
                "supported for datasets created from data that we know will still "
                "exist at deserialization time, e.g. external data in persistent cloud "
                "object stores or in-memory data from long-lived clusters. Concretely, "
                "all ray.data.read_*() APIs should support lineage-based "
                "serialization, while all of the ray.data.from_*() APIs do not. To "
                "allow this stream to be serialized to storage, write the data to an "
                "external store (such as AWS S3, GCS, or Azure Blob Storage) using the "
                "Dataset.write_*() APIs, and serialize a new dataset reading "
                "from the external store using the ray.data.read_*() APIs."
            )
        # Copy Dataset and clear the blocks from the execution plan so only the
        # Dataset's lineage is serialized.
        plan_copy = self._plan.deep_copy(preserve_uuid=True)
        logical_plan_copy = copy.copy(self._plan._logical_plan)
        ds = Dataset(plan_copy, self._get_epoch(), self._lazy, logical_plan_copy)
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

        Examples:

            .. testcode::

                import ray

                ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
                serialized_ds = ds.serialize_lineage()
                ds = ray.data.Dataset.deserialize_lineage(serialized_ds)
                print(ds)

            .. testoutput::

                Dataset(
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

        Args:
            serialized_ds: The serialized Dataset that we wish to deserialize.

        Returns:
            A deserialized ``Dataset`` instance.
        """
        return pickle.loads(serialized_ds)

    @property
    @DeveloperAPI
    def context(self) -> DataContext:
        """Return the DataContext used to create this Dataset."""
        return self._plan._context

    def _divide(self, block_idx: int) -> ("Dataset", "Dataset"):
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

    @Deprecated(message="The batch format is no longer exposed as a public API.")
    def default_batch_format(self) -> Type:
        raise ValueError("default_batch_format() is not allowed in Ray 2.5")

    @Deprecated(message="The dataset format is no longer exposed as a public API.")
    def dataset_format(self) -> BlockFormat:
        raise ValueError("dataset_format() is not allowed in Ray 2.5")

    def _aggregate_on(
        self, agg_cls: type, on: Optional[Union[str, List[str]]], *args, **kwargs
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

    @repr_with_fallback(["ipywidgets", "8"])
    def _repr_mimebundle_(self, **kwargs):
        """Return a mimebundle with an ipywidget repr and a simple text repr.

        Depending on the frontend where the data is being displayed,
        different mimetypes are used from this bundle.
        See https://ipython.readthedocs.io/en/stable/config/integrating.html
        for information about this method, and
        https://ipywidgets.readthedocs.io/en/latest/embedding.html
        for more information about the jupyter widget mimetype.

        Returns:
            A mimebundle containing an ipywidget repr and a simple text repr.
        """
        import ipywidgets

        title = ipywidgets.HTML(f"<h2>{self.__class__.__name__}</h2>")
        tab = self._tab_repr_()
        widget = ipywidgets.VBox([title, tab], layout=ipywidgets.Layout(width="100%"))

        # Get the widget mime bundle, but replace the plaintext
        # with the Datastream repr
        bundle = widget._repr_mimebundle_(**kwargs)
        bundle.update(
            {
                "text/plain": repr(self),
            }
        )
        return bundle

    def _tab_repr_(self):
        from ipywidgets import HTML, Tab

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
            "Use `ds.count()` to compute the length of a distributed Dataset. "
            "This may be an expensive operation."
        )

    def __iter__(self):
        raise TypeError(
            "`Dataset` objects aren't iterable. To iterate records, call "
            "`ds.iter_rows()` or `ds.iter_batches()`. For more information, read "
            "https://docs.ray.io/en/latest/data/consuming-datasets.html."
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
        if self._current_executor and ray is not None and ray.is_initialized():
            self._current_executor.shutdown()


@PublicAPI
class MaterializedDataset(Dataset, Generic[T]):
    """A Dataset materialized in Ray memory, e.g., via `.materialize()`.

    The blocks of a MaterializedDataset object are materialized into Ray object store
    memory, which means that this class can be shared or iterated over by multiple Ray
    tasks without re-executing the underlying computations for producing the stream.
    """

    pass


@PublicAPI(stability="beta")
class Schema:
    """Dataset schema.

    Attributes:
        names: List of column names of this Dataset.
        types: List of Arrow types of the Dataset. Note that the "object" type is
            not Arrow compatible and hence is returned as `object`.
        base_schema: The underlying Arrow or Pandas schema.
    """

    def __init__(self, base_schema: Union["pyarrow.lib.Schema", "PandasBlockSchema"]):
        self.base_schema = base_schema

    @property
    def names(self) -> List[str]:
        """Lists the columns of this Dataset."""
        return self.base_schema.names

    @property
    def types(self) -> List[Union[Literal[object], "pyarrow.DataType"]]:
        """Lists the types of this Dataset in Arrow format

        For non-Arrow compatible types, we return "object".
        """
        import pyarrow as pa

        from ray.data.extensions import ArrowTensorType, TensorDtype

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

    def __repr__(self):
        column_width = max([len(name) for name in self.names] + [len("Column")])
        padding = 2

        output = "Column"
        output += " " * ((column_width + padding) - len("Column"))
        output += "Type\n"

        output += "-" * len("Column")
        output += " " * ((column_width + padding) - len("Column"))
        output += "-" * len("Type") + "\n"

        for name, type in zip(self.names, self.types):
            output += name
            output += " " * ((column_width + padding) - len(name))
            output += f"{type}\n"

        output = output.rstrip()
        return output


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
        iterable: The iterable on which the sliding window is
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
