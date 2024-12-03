import collections
import logging
import os
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

import ray
from ray._private.auto_init_hook import wrap_auto_init
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data._internal.datasource.avro_datasource import AvroDatasource
from ray.data._internal.datasource.bigquery_datasource import BigQueryDatasource
from ray.data._internal.datasource.binary_datasource import BinaryDatasource
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.delta_sharing_datasource import (
    DeltaSharingDatasource,
)
from ray.data._internal.datasource.hudi_datasource import HudiDatasource
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource
from ray.data._internal.datasource.image_datasource import (
    ImageDatasource,
    ImageFileMetadataProvider,
)
from ray.data._internal.datasource.json_datasource import JSONDatasource
from ray.data._internal.datasource.lance_datasource import LanceDatasource
from ray.data._internal.datasource.mongo_datasource import MongoDatasource
from ray.data._internal.datasource.numpy_datasource import NumpyDatasource
from ray.data._internal.datasource.parquet_bulk_datasource import ParquetBulkDatasource
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.datasource.range_datasource import RangeDatasource
from ray.data._internal.datasource.sql_datasource import SQLDatasource
from ray.data._internal.datasource.text_datasource import TextDatasource
from ray.data._internal.datasource.tfrecords_datasource import TFRecordDatasource
from ray.data._internal.datasource.torch_datasource import TorchDatasource
from ray.data._internal.datasource.webdataset_datasource import WebDatasetDatasource
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.logical.operators.from_operators import (
    FromArrow,
    FromBlocks,
    FromItems,
    FromNumpy,
    FromPandas,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.optimizers import LogicalPlan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import (
    _autodetect_parallelism,
    get_table_block_metadata,
    ndarray_to_block,
    pandas_df_to_arrow_block,
)
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import DataContext
from ray.data.dataset import Dataset, MaterializedDataset
from ray.data.datasource import (
    BaseFileMetadataProvider,
    Connection,
    Datasource,
    PathPartitionFilter,
)
from ray.data.datasource.datasource import Reader
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
)
from ray.data.datasource.file_meta_provider import (
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider
from ray.data.datasource.partitioning import Partitioning
from ray.types import ObjectRef
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import dask
    import datasets
    import mars
    import modin
    import pandas
    import pyarrow
    import pymongoarrow.api
    import pyspark
    import tensorflow as tf
    import torch
    from pyiceberg.expressions import BooleanExpression
    from tensorflow_metadata.proto.v0 import schema_pb2

    from ray.data._internal.datasource.tfrecords_datasource import TFXReadOptions


T = TypeVar("T")

logger = logging.getLogger(__name__)


@DeveloperAPI
def from_blocks(blocks: List[Block]):
    """Create a :class:`~ray.data.Dataset` from a list of blocks.

    This method is primarily used for testing. Unlike other methods like
    :func:`~ray.data.from_pandas` and :func:`~ray.data.from_arrow`, this method
    gaurentees that it won't modify the number of blocks.

    Args:
        blocks: List of blocks to create the dataset from.

    Returns:
        A :class:`~ray.data.Dataset` holding the blocks.
    """
    block_refs = [ray.put(block) for block in blocks]
    metadata = [BlockAccessor.for_block(block).get_metadata() for block in blocks]
    from_blocks_op = FromBlocks(block_refs, metadata)
    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"FromBlocks": metadata}, parent=None)
    )
    logical_plan = LogicalPlan(from_blocks_op, execution_plan._context)
    return MaterializedDataset(
        execution_plan,
        logical_plan,
    )


@PublicAPI
def from_items(
    items: List[Any],
    *,
    parallelism: int = -1,
    override_num_blocks: Optional[int] = None,
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of local Python objects.

    Use this method to create small datasets from data that fits in memory.

    Examples:

        >>> import ray
        >>> ds = ray.data.from_items([1, 2, 3, 4, 5])
        >>> ds
        MaterializedDataset(num_blocks=..., num_rows=5, schema={item: int64})
        >>> ds.schema()
        Column  Type
        ------  ----
        item    int64

    Args:
        items: List of local Python objects.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` holding the items.
    """
    import builtins

    parallelism = _get_num_output_blocks(parallelism, override_num_blocks)
    if parallelism == 0:
        raise ValueError(f"parallelism must be -1 or > 0, got: {parallelism}")

    detected_parallelism, _, _ = _autodetect_parallelism(
        parallelism,
        ray.util.get_current_placement_group(),
        DataContext.get_current(),
    )
    # Truncate parallelism to number of items to avoid empty blocks.
    detected_parallelism = min(len(items), detected_parallelism)

    if detected_parallelism > 0:
        block_size, remainder = divmod(len(items), detected_parallelism)
    else:
        block_size, remainder = 0, 0
    # NOTE: We need to explicitly use the builtins range since we override range below,
    # with the definition of ray.data.range.
    blocks: List[ObjectRef[Block]] = []
    metadata: List[BlockMetadata] = []
    for i in builtins.range(detected_parallelism):
        stats = BlockExecStats.builder()
        builder = DelegatingBlockBuilder()
        # Evenly distribute remainder across block slices while preserving record order.
        block_start = i * block_size + min(i, remainder)
        block_end = (i + 1) * block_size + min(i + 1, remainder)
        for j in builtins.range(block_start, block_end):
            item = items[j]
            if not isinstance(item, collections.abc.Mapping):
                item = {"item": item}
            builder.add(item)
        block = builder.build()
        blocks.append(ray.put(block))
        metadata.append(
            BlockAccessor.for_block(block).get_metadata(exec_stats=stats.build())
        )

    from_items_op = FromItems(blocks, metadata)
    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"FromItems": metadata}, parent=None)
    )
    logical_plan = LogicalPlan(from_items_op, execution_plan._context)
    return MaterializedDataset(
        execution_plan,
        logical_plan,
    )


@PublicAPI
def range(
    n: int,
    *,
    parallelism: int = -1,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from a range of integers [0..n).

    This function allows for easy creation of synthetic datasets for testing or
    benchmarking :ref:`Ray Data <data>`.

    Examples:

        >>> import ray
        >>> ds = ray.data.range(10000)
        >>> ds
        Dataset(num_rows=10000, schema={id: int64})
        >>> ds.map(lambda row: {"id": row["id"] * 2}).take(4)
        [{'id': 0}, {'id': 2}, {'id': 4}, {'id': 6}]

    Args:
        n: The upper bound of the range of integers.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing the integers from the range 0 to n.

    .. seealso::

        :meth:`~ray.data.range_tensor`
                    Call this method for creating synthetic datasets of tensor data.

    """
    datasource = RangeDatasource(n=n, block_format="arrow", column_name="id")
    return read_datasource(
        datasource,
        parallelism=parallelism,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def range_tensor(
    n: int,
    *,
    shape: Tuple = (1,),
    parallelism: int = -1,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` tensors of the provided shape from range
    [0...n].

    This function allows for easy creation of synthetic tensor datasets for testing or
    benchmarking :ref:`Ray Data <data>`.

    Examples:

        >>> import ray
        >>> ds = ray.data.range_tensor(1000, shape=(2, 2))
        >>> ds
        Dataset(num_rows=1000, schema={data: numpy.ndarray(shape=(2, 2), dtype=int64)})
        >>> ds.map_batches(lambda row: {"data": row["data"] * 2}).take(2)
        [{'data': array([[0, 0],
               [0, 0]])}, {'data': array([[2, 2],
               [2, 2]])}]

    Args:
        n: The upper bound of the range of tensor records.
        shape: The shape of each tensor in the dataset.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing the tensor data from range 0 to n.

    .. seealso::

        :meth:`~ray.data.range`
                    Call this method to create synthetic datasets of integer data.

    """
    datasource = RangeDatasource(
        n=n, block_format="tensor", column_name="data", tensor_shape=tuple(shape)
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
@wrap_auto_init
def read_datasource(
    datasource: Datasource,
    *,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **read_args,
) -> Dataset:
    """Read a stream from a custom :class:`~ray.data.Datasource`.

    Args:
        datasource: The :class:`~ray.data.Datasource` to read data from.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.
        read_args: Additional kwargs to pass to the :class:`~ray.data.Datasource`
            implementation.

    Returns:
        :class:`~ray.data.Dataset` that reads data from the :class:`~ray.data.Datasource`.
    """  # noqa: E501
    parallelism = _get_num_output_blocks(parallelism, override_num_blocks)

    ctx = DataContext.get_current()

    if ray_remote_args is None:
        ray_remote_args = {}

    if not datasource.supports_distributed_reads:
        ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )

    if "scheduling_strategy" not in ray_remote_args:
        ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy

    datasource_or_legacy_reader = _get_datasource_or_legacy_reader(
        datasource,
        ctx,
        read_args,
    )

    cur_pg = ray.util.get_current_placement_group()
    requested_parallelism, _, inmemory_size = _autodetect_parallelism(
        parallelism,
        ctx.target_max_block_size,
        DataContext.get_current(),
        datasource_or_legacy_reader,
        placement_group=cur_pg,
    )

    # TODO(hchen/chengsu): Remove the duplicated get_read_tasks call here after
    # removing LazyBlockList code path.
    read_tasks = datasource_or_legacy_reader.get_read_tasks(requested_parallelism)
    import uuid

    stats = DatasetStats(
        metadata={"Read": [read_task.metadata for read_task in read_tasks]},
        parent=None,
        needs_stats_actor=True,
        stats_uuid=uuid.uuid4(),
    )
    read_op = Read(
        datasource,
        datasource_or_legacy_reader,
        parallelism,
        inmemory_size,
        len(read_tasks) if read_tasks else 0,
        ray_remote_args,
        concurrency,
    )
    execution_plan = ExecutionPlan(stats)
    logical_plan = LogicalPlan(read_op, execution_plan._context)
    return Dataset(
        plan=execution_plan,
        logical_plan=logical_plan,
    )


@PublicAPI(stability="alpha")
def read_mongo(
    uri: str,
    database: str,
    collection: str,
    *,
    pipeline: Optional[List[Dict]] = None,
    schema: Optional["pymongoarrow.api.Schema"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **mongo_args,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from a MongoDB database.

    The data to read from is specified via the ``uri``, ``database`` and ``collection``
    of the MongoDB. The dataset is created from the results of executing
    ``pipeline`` against the ``collection``. If ``pipeline`` is None, the entire
    ``collection`` is read.

    .. tip::

        For more details about these MongoDB concepts, see the following:
        - URI: https://www.mongodb.com/docs/manual/reference/connection-string/
        - Database and Collection: https://www.mongodb.com/docs/manual/core/databases-and-collections/
        - Pipeline: https://www.mongodb.com/docs/manual/core/aggregation-pipeline/

    To read the MongoDB in parallel, the execution of the pipeline is run on partitions
    of the collection, with a Ray read task to handle a partition. Partitions are
    created in an attempt to evenly distribute the documents into the specified number
    of partitions. The number of partitions is determined by ``parallelism`` which can
    be requested from this interface or automatically chosen if unspecified (see the
    ``parallelism`` arg below).

    Examples:
        >>> import ray
        >>> from pymongoarrow.api import Schema # doctest: +SKIP
        >>> ds = ray.data.read_mongo( # doctest: +SKIP
        ...     uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin", # noqa: E501
        ...     database="my_db",
        ...     collection="my_collection",
        ...     pipeline=[{"$match": {"col2": {"$gte": 0, "$lt": 100}}}, {"$sort": "sort_field"}], # noqa: E501
        ...     schema=Schema({"col1": pa.string(), "col2": pa.int64()}),
        ...     override_num_blocks=10,
        ... )

    Args:
        uri: The URI of the source MongoDB where the dataset is
            read from. For the URI format, see details in the `MongoDB docs <https:/\
                /www.mongodb.com/docs/manual/reference/connection-string/>`_.
        database: The name of the database hosted in the MongoDB. This database
            must exist otherwise ValueError is raised.
        collection: The name of the collection in the database. This collection
            must exist otherwise ValueError is raised.
        pipeline: A `MongoDB pipeline <https://www.mongodb.com/docs/manual/core\
            /aggregation-pipeline/>`_, which is executed on the given collection
            with results used to create Dataset. If None, the entire collection will
            be read.
        schema: The schema used to read the collection. If None, it'll be inferred from
            the results of pipeline.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.
        mongo_args: kwargs passed to `aggregate_arrow_all() <https://mongo-arrow\
            .readthedocs.io/en/latest/api/api.html#pymongoarrow.api\
            aggregate_arrow_all>`_ in pymongoarrow in producing
            Arrow-formatted results.

    Returns:
        :class:`~ray.data.Dataset` producing rows from the results of executing the pipeline on the specified MongoDB collection.

    Raises:
        ValueError: if ``database`` doesn't exist.
        ValueError: if ``collection`` doesn't exist.
    """
    datasource = MongoDatasource(
        uri=uri,
        database=database,
        collection=collection,
        pipeline=pipeline,
        schema=schema,
        **mongo_args,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="alpha")
def read_bigquery(
    project_id: str,
    dataset: Optional[str] = None,
    query: Optional[str] = None,
    *,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a dataset from BigQuery.

    The data to read from is specified via the ``project_id``, ``dataset``
    and/or ``query`` parameters. The dataset is created from the results of
    executing ``query`` if a query is provided. Otherwise, the entire
    ``dataset`` is read.

    For more information about BigQuery, see the following concepts:

    - Project id: `Creating and Managing Projects <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_

    - Dataset: `Datasets Intro <https://cloud.google.com/bigquery/docs/datasets-intro>`_

    - Query: `Query Syntax <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax>`_

    This method uses the BigQuery Storage Read API which reads in parallel,
    with a Ray read task to handle each stream. The number of streams is
    determined by ``parallelism`` which can be requested from this interface
    or automatically chosen if unspecified (see the ``parallelism`` arg below).

    .. warning::
        The maximum query response size is 10GB. For more information, see `BigQuery response too large to return <https://cloud.google.com/knowledge/kb/bigquery-response-too-large-to-return-consider-setting-allowlargeresults-to-true-in-your-job-configuration-000004266>`_.

    Examples:
        .. testcode::
            :skipif: True

            import ray
            # Users will need to authenticate beforehand (https://cloud.google.com/sdk/gcloud/reference/auth/login)
            ds = ray.data.read_bigquery(
                project_id="my_project",
                query="SELECT * FROM `bigquery-public-data.samples.gsod` LIMIT 1000",
            )

    Args:
        project_id: The name of the associated Google Cloud Project that hosts the dataset to read.
            For more information, see `Creating and Managing Projects <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_.
        dataset: The name of the dataset hosted in BigQuery in the format of ``dataset_id.table_id``.
            Both the dataset_id and table_id must exist otherwise an exception will be raised.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        Dataset producing rows from the results of executing the query (or reading the entire dataset)
        on the specified BigQuery dataset.
    """  # noqa: E501
    datasource = BigQueryDatasource(project_id=project_id, dataset=dataset, query=query)
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    meta_provider: Optional[ParquetMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Optional[Partitioning] = Partitioning("hive"),
    shuffle: Union[Literal["files"], None] = None,
    include_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_parquet_args,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from parquet files.


    Examples:
        Read a file in remote storage.

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

        Read a directory in remote storage.

        >>> ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris-parquet/")

        Read multiple local files.

        >>> ray.data.read_parquet(
        ...    ["local:///path/to/file1", "local:///path/to/file2"]) # doctest: +SKIP

        Specify a schema for the parquet file.

        >>> import pyarrow as pa
        >>> fields = [("sepal.length", pa.float32()),
        ...           ("sepal.width", pa.float32()),
        ...           ("petal.length", pa.float32()),
        ...           ("petal.width", pa.float32()),
        ...           ("variety", pa.string())]
        >>> ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet",
        ...     schema=pa.schema(fields))
        >>> ds.schema()
        Column        Type
        ------        ----
        sepal.length  float
        sepal.width   float
        petal.length  float
        petal.width   float
        variety       string

        The Parquet reader also supports projection and filter pushdown, allowing column
        selection and row filtering to be pushed down to the file scan.

        .. testcode::

            import pyarrow as pa

            # Create a Dataset by reading a Parquet file, pushing column selection and
            # row filtering down to the file scan.
            ds = ray.data.read_parquet(
                "s3://anonymous@ray-example-data/iris.parquet",
                columns=["sepal.length", "variety"],
                filter=pa.dataset.field("sepal.length") > 5.0,
            )

            ds.show(2)

        .. testoutput::

            {'sepal.length': 5.1, 'variety': 'Setosa'}
            {'sepal.length': 5.4, 'variety': 'Setosa'}

        For further arguments you can pass to PyArrow as a keyword argument, see the
        `PyArrow API reference <https://arrow.apache.org/docs/python/generated/\
        pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment>`_.

    Args:
        paths: A single file path or directory, or a list of file paths. Multiple
            directories are not supported.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `pyarrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the ``S3FileSystem`` is
            used. If ``None``, this function uses a system-chosen implementation.
        columns: A list of column names to read. Only the specified columns are
            read during the file scan.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        tensor_column_schema: A dict of column name to PyArrow dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to PyArrow tensors. This function
            assumes that the tensors are serialized in the raw
            NumPy array format in C-contiguous order (e.g., via
            `arr.tobytes()`).
        meta_provider: A :ref:`file metadata provider <metadata_provider>`. Custom
            metadata providers may be able to resolve file metadata more quickly and/or
            accurately. In most cases you do not need to set this parameter.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`. Use
            with a custom callback to read only selected partitions of a dataset.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to HIVE partitioning.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        arrow_parquet_args: Other parquet read options to pass to PyArrow. For the full
            set of arguments, see the `PyArrow API <https://arrow.apache.org/docs/\
                python/generated/pyarrow.dataset.Scanner.html\
                    #pyarrow.dataset.Scanner.from_fragment>`_
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` producing records read from the specified parquet
        files.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)
    _validate_shuffle_arg(shuffle)

    if meta_provider is None:
        meta_provider = ParquetMetadataProvider()
    arrow_parquet_args = _resolve_parquet_args(
        tensor_column_schema,
        **arrow_parquet_args,
    )

    dataset_kwargs = arrow_parquet_args.pop("dataset_kwargs", None)
    _block_udf = arrow_parquet_args.pop("_block_udf", None)
    schema = arrow_parquet_args.pop("schema", None)
    datasource = ParquetDatasource(
        paths,
        columns=columns,
        dataset_kwargs=dataset_kwargs,
        to_batch_kwargs=arrow_parquet_args,
        _block_udf=_block_udf,
        filesystem=filesystem,
        schema=schema,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="beta")
def read_images(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = ImageDatasource._FILE_EXTENSIONS,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from image files.

    Examples:
        >>> import ray
        >>> path = "s3://anonymous@ray-example-data/batoidea/JPEGImages/"
        >>> ds = ray.data.read_images(path)
        >>> ds.schema()
        Column  Type
        ------  ----
        image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

        If you need image file paths, set ``include_paths=True``.

        >>> ds = ray.data.read_images(path, include_paths=True)
        >>> ds.schema()
        Column  Type
        ------  ----
        image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)
        path    string
        >>> ds.take(1)[0]["path"]
        'ray-example-data/batoidea/JPEGImages/1.jpeg'

        If your images are arranged like:

        .. code::

            root/dog/xxx.png
            root/dog/xxy.png

            root/cat/123.png
            root/cat/nsdf3.png

        Then you can include the labels by specifying a
        :class:`~ray.data.datasource.partitioning.Partitioning`.

        >>> import ray
        >>> from ray.data.datasource.partitioning import Partitioning
        >>> root = "s3://anonymous@ray-example-data/image-datasets/dir-partitioned"
        >>> partitioning = Partitioning("dir", field_names=["class"], base_dir=root)
        >>> ds = ray.data.read_images(root, size=(224, 224), partitioning=partitioning)
        >>> ds.schema()
        Column  Type
        ------  ----
        image   numpy.ndarray(shape=(224, 224, 3), dtype=uint8)
        class   string

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        filesystem: The pyarrow filesystem
            implementation to read from. These filesystems are specified in the
            `pyarrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        arrow_open_file_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_file>`_.
            when opening input files to read.
        partition_filter:  A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`. Use
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match ``*.png``, ``*.jpg``, ``*.jpeg``, ``*.tiff``, ``*.bmp``, or ``*.gif``.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        size: The desired height and width of loaded images. If unspecified, images
            retain their original shape.
        mode: A `Pillow mode <https://pillow.readthedocs.io/en/stable/handbook/concepts\
            .html#modes>`_
            describing the desired type and depth of pixels. If unspecified, image
            modes are inferred by
            `Pillow <https://pillow.readthedocs.io/en/stable/index.html>`_.
        include_paths: If ``True``, include the path to each image. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file/directory paths in ``paths``
            that are not found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing tensors that represent the images at
        the specified paths. For information on working with tensors, read the
        :ref:`tensor data guide <working_with_tensors>`.

    Raises:
        ValueError: if ``size`` contains non-positive numbers.
        ValueError: if ``mode`` is unsupported.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = ImageFileMetadataProvider()

    datasource = ImageDatasource(
        paths,
        size=size,
        mode=mode,
        include_paths=include_paths,
        filesystem=filesystem,
        meta_provider=meta_provider,
        open_stream_args=arrow_open_file_args,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@Deprecated
def read_parquet_bulk(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    shuffle: Union[Literal["files"], None] = None,
    include_paths: bool = False,
    file_extensions: Optional[List[str]] = ParquetBulkDatasource._FILE_EXTENSIONS,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_parquet_args,
) -> Dataset:
    """Create :class:`~ray.data.Dataset` from parquet files without reading metadata.

    Use :meth:`~ray.data.read_parquet` for most cases.

    Use :meth:`~ray.data.read_parquet_bulk` if all the provided paths point to files
    and metadata fetching using :meth:`~ray.data.read_parquet` takes too long or the
    parquet files do not all have a unified schema.

    Performance slowdowns are possible when using this method with parquet files that
    are very large.

    .. warning::

        Only provide file paths as input (i.e., no directory paths). An
        OSError is raised if one or more paths point to directories. If your
        use-case requires directory paths, use :meth:`~ray.data.read_parquet`
        instead.

    Examples:
        Read multiple local files. You should always provide only input file paths
        (i.e. no directory paths) when known to minimize read latency.

        >>> ray.data.read_parquet_bulk( # doctest: +SKIP
        ...     ["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are
            specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
                filesystems.html#filesystem-implementations>`_.
            Specify this parameter if you need to provide specific configurations to
            the filesystem. By default, the filesystem is automatically selected based
            on the scheme of the paths. For example, if the path begins with ``s3://``,
            the `S3FileSystem` is used.
        columns: A list of column names to read. Only the
            specified columns are read during the file scan.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        arrow_open_file_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_file>`_.
            when opening input files to read.
        tensor_column_schema: A dict of column name to PyArrow dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to PyArrow tensors. This function
            assumes that the tensors are serialized in the raw
            NumPy array format in C-contiguous order (e.g. via
            `arr.tobytes()`).
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`. Use
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.parquet*".
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        arrow_parquet_args: Other parquet read options to pass to PyArrow. For the full
            set of arguments, see
            the `PyArrow API <https://arrow.apache.org/docs/python/generated/\
                pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment>`_
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
       :class:`~ray.data.Dataset` producing records read from the specified paths.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    warnings.warn(
        "`read_parquet_bulk` is deprecated and will be removed after May 2025. Use "
        "`read_parquet` instead.",
        DeprecationWarning,
    )

    if meta_provider is None:
        meta_provider = FastFileMetadataProvider()
    read_table_args = _resolve_parquet_args(
        tensor_column_schema,
        **arrow_parquet_args,
    )
    if columns is not None:
        read_table_args["columns"] = columns

    datasource = ParquetBulkDatasource(
        paths,
        read_table_args=read_table_args,
        filesystem=filesystem,
        open_stream_args=arrow_open_file_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_json(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = JSONDatasource._FILE_EXTENSIONS,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_json_args,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from JSON and JSONL files.

    For JSON file, the whole file is read as one row.
    For JSONL file, each line of file is read as separate row.

    Examples:
        Read a JSON file in remote storage.

        >>> import ray
        >>> ds = ray.data.read_json("s3://anonymous@ray-example-data/log.json")
        >>> ds.schema()
        Column     Type
        ------     ----
        timestamp  timestamp[...]
        size       int64

        Read a JSONL file in remote storage.

        >>> ds = ray.data.read_json("s3://anonymous@ray-example-data/train.jsonl")
        >>> ds.schema()
        Column  Type
        ------  ----
        input   string

        Read multiple local files.

        >>> ray.data.read_json( # doctest: +SKIP
        ...    ["local:///path/to/file1", "local:///path/to/file2"])

        Read multiple directories.

        >>> ray.data.read_json( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

        By default, :meth:`~ray.data.read_json` parses
        `Hive-style partitions <https://athena.guide/articles/\
        hive-style-partitioning/>`_
        from file paths. If your data adheres to a different partitioning scheme, set
        the ``partitioning`` parameter.

        >>> ds = ray.data.read_json("s3://anonymous@ray-example-data/year=2022/month=09/sales.json")
        >>> ds.take(1)
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}]

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
            when opening input files to read.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.json" or "*.jsonl".
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. By default, this function parses
            `Hive-style partitions <https://athena.guide/articles/\
                hive-style-partitioning/>`_.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        arrow_json_args: JSON read options to pass to `pyarrow.json.read_json <https://\
            arrow.apache.org/docs/python/generated/pyarrow.json.read_json.html#pyarrow.\
            json.read_json>`_.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` producing records read from the specified paths.
    """  # noqa: E501
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = JSONDatasource(
        paths,
        arrow_json_args=arrow_json_args,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_csv_args,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from CSV files.

    Examples:
        Read a file in remote storage.

        >>> import ray
        >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        >>> ds.schema()
        Column             Type
        ------             ----
        sepal length (cm)  double
        sepal width (cm)   double
        petal length (cm)  double
        petal width (cm)   double
        target             int64

        Read multiple local files.

        >>> ray.data.read_csv( # doctest: +SKIP
        ...    ["local:///path/to/file1", "local:///path/to/file2"])

        Read a directory from remote storage.

        >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris-csv/")

        Read files that use a different delimiter. For more uses of ParseOptions see
        https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html  # noqa: #501

        >>> from pyarrow import csv
        >>> parse_options = csv.ParseOptions(delimiter="\\t")
        >>> ds = ray.data.read_csv(
        ...     "s3://anonymous@ray-example-data/iris.tsv",
        ...     parse_options=parse_options)
        >>> ds.schema()
        Column        Type
        ------        ----
        sepal.length  double
        sepal.width   double
        petal.length  double
        petal.width   double
        variety       string

        Convert a date column with a custom format from a CSV file. For more uses of ConvertOptions see https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html  # noqa: #501

        >>> from pyarrow import csv
        >>> convert_options = csv.ConvertOptions(
        ...     timestamp_parsers=["%m/%d/%Y"])
        >>> ds = ray.data.read_csv(
        ...     "s3://anonymous@ray-example-data/dow_jones.csv",
        ...     convert_options=convert_options)

        By default, :meth:`~ray.data.read_csv` parses
        `Hive-style partitions <https://athena.guide/\
        articles/hive-style-partitioning/>`_
        from file paths. If your data adheres to a different partitioning scheme, set
        the ``partitioning`` parameter.

        >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/year=2022/month=09/sales.csv")
        >>> ds.take(1)
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}]

        By default, :meth:`~ray.data.read_csv` reads all files from file paths. If you want to filter
        files by file extensions, set the ``file_extensions`` parameter.

        Read only ``*.csv`` files from a directory.

        >>> ray.data.read_csv("s3://anonymous@ray-example-data/different-extensions/",
        ...     file_extensions=["csv"])
        Dataset(num_rows=?, schema={a: int64, b: int64})

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `pyarrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
            when opening input files to read.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset. By default, no files are filtered.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. By default, this function parses
            `Hive-style partitions <https://athena.guide/articles/\
                hive-style-partitioning/>`_.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        arrow_csv_args: CSV read options to pass to
            `pyarrow.csv.open_csv <https://arrow.apache.org/docs/python/generated/\
            pyarrow.csv.open_csv.html#pyarrow.csv.open_csv>`_
            when opening CSV files.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` producing records read from the specified paths.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = CSVDatasource(
        paths,
        arrow_csv_args=arrow_csv_args,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_text(
    paths: Union[str, List[str]],
    *,
    encoding: str = "utf-8",
    drop_empty_lines: bool = True,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from lines stored in text files.

    Examples:
        Read a file in remote storage.

        >>> import ray
        >>> ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")
        >>> ds.schema()
        Column  Type
        ------  ----
        text    string

        Read multiple local files.

        >>> ray.data.read_text( # doctest: +SKIP
        ...    ["local:///path/to/file1", "local:///path/to/file2"])

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        encoding: The encoding of the files (e.g., "utf-8" or "ascii").
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks and
            in the subsequent text decoding map task.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
            when opening input files to read.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset. By default, no files are filtered.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` producing lines of text read from the specified
        paths.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = TextDatasource(
        paths,
        drop_empty_lines=drop_empty_lines,
        encoding=encoding,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_avro(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from records stored in Avro files.

    Examples:
        Read an Avro file in remote storage or local storage.

        >>> import ray
        >>> ds = ray.data.read_avro("s3://anonymous@ray-example-data/mnist.avro")
        >>> ds.schema()
        Column    Type
        ------    ----
        features  list<item: int64>
        label     int64
        dataType  string

        >>> ray.data.read_avro( # doctest: +SKIP
        ...    ["local:///path/to/file1", "local:///path/to/file2"])

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks and
            in the subsequent text decoding map task.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
            when opening input files to read.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset. By default, no files are filtered.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` holding records from the Avro files.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = AvroDatasource(
        paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_numpy(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = NumpyDatasource._FILE_EXTENSIONS,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **numpy_load_args,
) -> Dataset:
    """Create an Arrow dataset from numpy files.

    Examples:
        Read a directory of files in remote storage.

        >>> import ray
        >>> ray.data.read_numpy("s3://bucket/path") # doctest: +SKIP

        Read multiple local files.

        >>> ray.data.read_numpy(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        Read multiple directories.

        >>> ray.data.read_numpy( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_stream <https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`_.
        numpy_load_args: Other options to pass to np.load.
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately. If
            ``None``, this function uses a system-chosen implementation.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.npy*".
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        Dataset holding Tensor records read from the specified paths.
    """  # noqa: E501
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = NumpyDatasource(
        paths,
        numpy_load_args=numpy_load_args,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="alpha")
def read_tfrecords(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    tf_schema: Optional["schema_pb2.Schema"] = None,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    tfx_read_options: Optional["TFXReadOptions"] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from TFRecord files that contain
    `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_
    messages.

    .. tip::
        Using the ``tfx-bsl`` library is more performant when reading large
        datasets (for example, in production use cases). To use this
        implementation, you must first install ``tfx-bsl``:

        1. `pip install tfx_bsl --no-dependencies`
        2. Pass tfx_read_options to read_tfrecords, for example:
           `ds = read_tfrecords(path, ..., tfx_read_options=TFXReadOptions())`

    .. warning::
        This function exclusively supports ``tf.train.Example`` messages. If a file
        contains a message that isn't of type ``tf.train.Example``, then this function
        fails.

    Examples:
        >>> import ray
        >>> ray.data.read_tfrecords("s3://anonymous@ray-example-data/iris.tfrecords")
        Dataset(
           num_rows=?,
           schema={...}
        )

        We can also read compressed TFRecord files, which use one of the
        `compression types supported by Arrow <https://arrow.apache.org/docs/python/\
            generated/pyarrow.CompressedInputStream.html>`_:

        >>> ray.data.read_tfrecords(
        ...     "s3://anonymous@ray-example-data/iris.tfrecords.gz",
        ...     arrow_open_stream_args={"compression": "gzip"},
        ... )
        Dataset(
           num_rows=?,
           schema={...}
        )

    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
            when opening input files to read. To read a compressed TFRecord file,
            pass the corresponding compression type (e.g., for ``GZIP`` or ``ZLIB``),
            use ``arrow_open_stream_args={'compression': 'gzip'}``).
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths:  If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        tf_schema: Optional TensorFlow Schema which is used to explicitly set the schema
            of the underlying Dataset.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.
        tfx_read_options: Specifies read options when reading TFRecord files with TFX.
            When no options are provided, the default version without tfx-bsl will
            be used to read the tfrecords.
    Returns:
        A :class:`~ray.data.Dataset` that contains the example features.

    Raises:
        ValueError: If a file contains a message that isn't a ``tf.train.Example``.
    """
    import platform

    _emit_meta_provider_deprecation_warning(meta_provider)

    tfx_read = False

    if tfx_read_options and platform.processor() != "arm":
        try:
            import tfx_bsl  # noqa: F401

            tfx_read = True
        except ModuleNotFoundError:
            # override the tfx_read_options given that tfx-bsl is not installed
            tfx_read_options = None
            logger.warning(
                "Please install tfx-bsl package with"
                " `pip install tfx_bsl --no-dependencies`."
                " This can help speed up the reading of large TFRecord files."
            )

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()
    datasource = TFRecordDatasource(
        paths,
        tf_schema=tf_schema,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
        tfx_read_options=tfx_read_options,
    )
    ds = read_datasource(
        datasource,
        parallelism=parallelism,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )

    if (
        tfx_read_options
        and tfx_read_options.auto_infer_schema
        and tfx_read
        and not tf_schema
    ):
        from ray.data._internal.datasource.tfrecords_datasource import (
            _infer_schema_and_transform,
        )

        return _infer_schema_and_transform(ds)

    return ds


@PublicAPI(stability="alpha")
def read_webdataset(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    decoder: Optional[Union[bool, str, callable, list]] = True,
    fileselect: Optional[Union[list, callable]] = None,
    filerename: Optional[Union[list, callable]] = None,
    suffixes: Optional[Union[list, callable]] = None,
    verbose_open: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    include_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from
    `WebDataset <https://webdataset.github.io/webdataset/>`_ files.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        arrow_open_stream_args: Key-word arguments passed to
            `pyarrow.fs.FileSystem.open_input_stream <https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`_.
            To read a compressed TFRecord file,
            pass the corresponding compression type (e.g. for ``GZIP`` or ``ZLIB``, use
            ``arrow_open_stream_args={'compression': 'gzip'}``).
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately. If
            ``None``, this function uses a system-chosen implementation.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
        decoder: A function or list of functions to decode the data.
        fileselect: A callable or list of glob patterns to select files.
        filerename: A function or list of tuples to rename files prior to grouping.
        suffixes: A function or list of suffixes to select for creating samples.
        verbose_open: Whether to print the file names as they are opened.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` that contains the example features.

    Raises:
        ValueError: If a file contains a message that isn't a `tf.train.Example`_.

    .. _tf.train.Example: https://www.tensorflow.org/api_docs/python/tf/train/Example
    """  # noqa: E501
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = WebDatasetDatasource(
        paths,
        decoder=decoder,
        fileselect=fileselect,
        filerename=filerename,
        suffixes=suffixes,
        verbose_open=verbose_open,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def read_binary_files(
    paths: Union[str, List[str]],
    *,
    include_paths: bool = False,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from binary files of arbitrary contents.

    Examples:
        Read a file in remote storage.

        >>> import ray
        >>> path = "s3://anonymous@ray-example-data/pdf-sample_0.pdf"
        >>> ds = ray.data.read_binary_files(path)
        >>> ds.schema()
        Column  Type
        ------  ----
        bytes   binary

        Read multiple local files.

        >>> ray.data.read_binary_files( # doctest: +SKIP
        ...     ["local:///path/to/file1", "local:///path/to/file2"])

        Read a file with the filepaths included as a column in the dataset.

        >>> path = "s3://anonymous@ray-example-data/pdf-sample_0.pdf"
        >>> ds = ray.data.read_binary_files(path, include_paths=True)
        >>> ds.take(1)[0]["path"]
        'ray-example-data/pdf-sample_0.pdf'


    Args:
        paths: A single file or directory, or a list of file or directory paths.
            A list of paths can contain both files and directories.
        include_paths: If ``True``, include the path to each file. File paths are
            stored in the ``'path'`` column.
        filesystem: The PyArrow filesystem
            implementation to read from. These filesystems are specified in the
            `PyArrow docs <https://arrow.apache.org/docs/python/api/\
            filesystems.html#filesystem-implementations>`_. Specify this parameter if
            you need to provide specific configurations to the filesystem. By default,
            the filesystem is automatically selected based on the scheme of the paths.
            For example, if the path begins with ``s3://``, the `S3FileSystem` is used.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_stream>`_.
        meta_provider: [Deprecated] A :ref:`file metadata provider <metadata_provider>`.
            Custom metadata providers may be able to resolve file metadata more quickly
            and/or accurately. In most cases, you do not need to set this. If ``None``,
            this function uses a system-chosen implementation.
        partition_filter: A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`.
            Use with a custom callback to read only selected partitions of a
            dataset. By default, no files are filtered.
            By default, this does not filter out any files.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        shuffle: If setting to "files", randomly shuffle input files order before read.
            Defaults to not shuffle with ``None``.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` producing rows read from the specified paths.
    """
    _emit_meta_provider_deprecation_warning(meta_provider)

    if meta_provider is None:
        meta_provider = DefaultFileMetadataProvider()

    datasource = BinaryDatasource(
        paths,
        include_paths=include_paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        file_extensions=file_extensions,
    )
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="alpha")
def read_sql(
    sql: str,
    connection_factory: Callable[[], Connection],
    *,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Read from a database that provides a
    `Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

    .. note::

        By default, ``read_sql`` launches multiple read tasks, and each task executes a
        ``LIMIT`` and ``OFFSET`` to fetch a subset of the rows. However, for many
        databases, ``OFFSET`` is slow.

        As a workaround, set ``override_num_blocks=1`` to directly fetch all rows in a
        single task. Note that this approach requires all result rows to fit in the
        memory of single task. If the rows don't fit, your program may raise an out of
        memory error.

    Examples:

        For examples of reading from larger databases like MySQL and PostgreSQL, see
        :ref:`Reading from SQL Databases <reading_sql>`.

        .. testcode::

            import sqlite3

            import ray

            # Create a simple database
            connection = sqlite3.connect("example.db")
            connection.execute("CREATE TABLE movie(title, year, score)")
            connection.execute(
                \"\"\"
                INSERT INTO movie VALUES
                    ('Monty Python and the Holy Grail', 1975, 8.2),
                    ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
                    ("Monty Python's Life of Brian", 1979, 8.0),
                    ("Rocky II", 1979, 7.3)
                \"\"\"
            )
            connection.commit()
            connection.close()

            def create_connection():
                return sqlite3.connect("example.db")

            # Get all movies
            ds = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            ds = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            ds = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

        .. testcode::
            :hide:

            import os
            os.remove("example.db")

    Args:
        sql: The SQL query to execute.
        connection_factory: A function that takes no arguments and returns a
            Python DB API2
            `Connection object <https://peps.python.org/pep-0249/#connection-objects>`_.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`Dataset` containing the queried data.
    """
    if parallelism != -1 and parallelism != 1:
        raise ValueError(
            "To ensure correctness, 'read_sql' always launches one task. The "
            "'parallelism' argument you specified can't be used."
        )

    datasource = SQLDatasource(sql=sql, connection_factory=connection_factory)
    return read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="alpha")
def read_databricks_tables(
    *,
    warehouse_id: str,
    table: Optional[str] = None,
    query: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Read a Databricks unity catalog table or Databricks SQL execution result.

    Before calling this API, set the ``DATABRICKS_TOKEN`` environment
    variable to your Databricks warehouse access token.

    .. code-block:: console

        export DATABRICKS_TOKEN=...

    If you're not running your program on the Databricks runtime, also set the
    ``DATABRICKS_HOST`` environment variable.

    .. code-block:: console

        export DATABRICKS_HOST=adb-<workspace-id>.<random-number>.azuredatabricks.net

    .. note::

        This function is built on the
        `Databricks statement execution API <https://docs.databricks.com/api/workspace/statementexecution>`_.

    Examples:

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_databricks_tables(
                warehouse_id='...',
                catalog='catalog_1',
                schema='db_1',
                query='select id from table_1 limit 750000',
            )

    Args:
        warehouse_id: The ID of the Databricks warehouse. The query statement is
            executed on this warehouse.
        table: The name of UC table you want to read. If this argument is set,
            you can't set ``query`` argument, and the reader generates query
            of ``select * from {table_name}`` under the hood.
        query: The query you want to execute. If this argument is set,
            you can't set ``table_name`` argument.
        catalog: (Optional) The default catalog name used by the query.
        schema: (Optional) The default schema used by the query.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`Dataset` containing the queried data.
    """  # noqa: E501
    from ray.data._internal.datasource.databricks_uc_datasource import (
        DatabricksUCDatasource,
    )
    from ray.util.spark.utils import get_spark_session, is_in_databricks_runtime

    def get_dbutils():
        no_dbutils_error = RuntimeError("No dbutils module found.")
        try:
            import IPython

            ip_shell = IPython.get_ipython()
            if ip_shell is None:
                raise no_dbutils_error
            return ip_shell.ns_table["user_global"]["dbutils"]
        except ImportError:
            raise no_dbutils_error
        except KeyError:
            raise no_dbutils_error

    token = os.environ.get("DATABRICKS_TOKEN")

    if not token:
        raise ValueError(
            "Please set environment variable 'DATABRICKS_TOKEN' to "
            "databricks workspace access token."
        )

    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        if is_in_databricks_runtime():
            ctx = (
                get_dbutils().notebook.entry_point.getDbutils().notebook().getContext()
            )
            host = ctx.tags().get("browserHostName").get()
        else:
            raise ValueError(
                "You are not in databricks runtime, please set environment variable "
                "'DATABRICKS_HOST' to databricks workspace URL"
                '(e.g. "adb-<workspace-id>.<random-number>.azuredatabricks.net").'
            )

    if not catalog:
        catalog = get_spark_session().sql("SELECT CURRENT_CATALOG()").collect()[0][0]

    if not schema:
        schema = get_spark_session().sql("SELECT CURRENT_DATABASE()").collect()[0][0]

    if query is not None and table is not None:
        raise ValueError("Only one of 'query' and 'table' arguments can be set.")

    if table:
        query = f"select * from {table}"

    if query is None:
        raise ValueError("One of 'query' and 'table' arguments should be set.")

    datasource = DatabricksUCDatasource(
        host=host,
        token=token,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        query=query,
    )
    return read_datasource(
        datasource=datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI(stability="alpha")
def read_hudi(
    table_uri: str,
    *,
    storage_options: Optional[Dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """
    Create a :class:`~ray.data.Dataset` from an
    `Apache Hudi table <https://hudi.apache.org>`_.

    Examples:
        >>> import ray
        >>> ds = ray.data.read_hudi( # doctest: +SKIP
        ...     table_uri="/hudi/trips",
        ... )

    Args:
        table_uri: The URI of the Hudi table to read from. Local file paths, S3, and GCS
            are supported.
        storage_options: Extra options that make sense for a particular storage
            connection. This is used to store connection parameters like credentials,
            endpoint, etc. See more explanation
            `here <https://github.com/apache/hudi-rs?tab=readme-ov-file#work-with-cloud-storage>`_.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing records read from the Hudi table.
    """  # noqa: E501
    datasource = HudiDatasource(
        table_uri=table_uri,
        storage_options=storage_options,
    )

    return read_datasource(
        datasource=datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def from_dask(df: "dask.dataframe.DataFrame") -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a
    `Dask DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html#dask.dataframe.DataFrame>`_.

    Args:
        df: A `Dask DataFrame`_.

    Returns:
        A :class:`~ray.data.MaterializedDataset` holding rows read from the DataFrame.
    """  # noqa: E501
    import dask

    from ray.util.dask import ray_dask_get

    partitions = df.to_delayed()
    persisted_partitions = dask.persist(*partitions, scheduler=ray_dask_get)

    import pandas

    def to_ref(df):
        if isinstance(df, pandas.DataFrame):
            return ray.put(df)
        elif isinstance(df, ray.ObjectRef):
            return df
        else:
            raise ValueError(
                "Expected a Ray object ref or a Pandas DataFrame, " f"got {type(df)}"
            )

    ds = from_pandas_refs(
        [to_ref(next(iter(part.dask.values()))) for part in persisted_partitions],
    )
    return ds


@PublicAPI
def from_mars(df: "mars.dataframe.DataFrame") -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a
    `Mars DataFrame <https://mars-project.readthedocs.io/en/latest/reference/dataframe/index.html>`_.

    Args:
        df: A `Mars DataFrame`_, which must be executed by Mars-on-Ray.

    Returns:
        A :class:`~ray.data.MaterializedDataset` holding rows read from the DataFrame.
    """  # noqa: E501
    import mars.dataframe as md

    ds: Dataset = md.to_ray_dataset(df)
    return ds


@PublicAPI
def from_modin(df: "modin.pandas.dataframe.DataFrame") -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a
    `Modin DataFrame <https://modin.readthedocs.io/en/stable/flow/modin/pandas/dataframe.html>`_.

    Args:
        df: A `Modin DataFrame`_, which must be using the Ray backend.

    Returns:
        A :class:`~ray.data.MaterializedDataset` rows read from the DataFrame.
    """  # noqa: E501
    from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

    parts = unwrap_partitions(df, axis=0)
    ds = from_pandas_refs(parts)
    return ds


@PublicAPI
def from_pandas(
    dfs: Union["pandas.DataFrame", List["pandas.DataFrame"]],
    override_num_blocks: Optional[int] = None,
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of pandas dataframes.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> ray.data.from_pandas(df)
        MaterializedDataset(num_blocks=1, num_rows=3, schema={a: int64, b: int64})

       Create a Ray Dataset from a list of Pandas DataFrames.

        >>> ray.data.from_pandas([df, df])
        MaterializedDataset(num_blocks=2, num_rows=6, schema={a: int64, b: int64})

    Args:
        dfs: A pandas dataframe or a list of pandas dataframes.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        :class:`~ray.data.Dataset` holding data read from the dataframes.
    """
    import pandas as pd

    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

    if override_num_blocks is not None:
        if len(dfs) > 1:
            # I assume most users pass a single DataFrame as input. For simplicity, I'm
            # concatenating DataFrames, even though it's not efficient.
            ary = pd.concat(dfs, axis=0)
        else:
            ary = dfs[0]
        dfs = np.array_split(ary, override_num_blocks)

    from ray.air.util.data_batch_conversion import (
        _cast_ndarray_columns_to_tensor_extension,
    )

    context = DataContext.get_current()
    if context.enable_tensor_extension_casting:
        dfs = [_cast_ndarray_columns_to_tensor_extension(df.copy()) for df in dfs]

    return from_pandas_refs([ray.put(df) for df in dfs])


@DeveloperAPI
def from_pandas_refs(
    dfs: Union[ObjectRef["pandas.DataFrame"], List[ObjectRef["pandas.DataFrame"]]],
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of Ray object references to
    pandas dataframes.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> df_ref = ray.put(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
        >>> ray.data.from_pandas_refs(df_ref)
        MaterializedDataset(num_blocks=1, num_rows=3, schema={a: int64, b: int64})

        Create a Ray Dataset from a list of Pandas Dataframes references.

        >>> ray.data.from_pandas_refs([df_ref, df_ref])
        MaterializedDataset(num_blocks=2, num_rows=6, schema={a: int64, b: int64})

    Args:
        dfs: A Ray object reference to a pandas dataframe, or a list of
             Ray object references to pandas dataframes.

    Returns:
        :class:`~ray.data.Dataset` holding data read from the dataframes.
    """
    if isinstance(dfs, ray.ObjectRef):
        dfs = [dfs]
    elif isinstance(dfs, list):
        for df in dfs:
            if not isinstance(df, ray.ObjectRef):
                raise ValueError(
                    "Expected list of Ray object refs, "
                    f"got list containing {type(df)}"
                )
    else:
        raise ValueError(
            "Expected Ray object ref or list of Ray object refs, " f"got {type(df)}"
        )

    context = DataContext.get_current()
    if context.enable_pandas_block:
        get_metadata = cached_remote_fn(get_table_block_metadata)
        metadata = ray.get([get_metadata.remote(df) for df in dfs])
        execution_plan = ExecutionPlan(
            DatasetStats(metadata={"FromPandas": metadata}, parent=None)
        )
        logical_plan = LogicalPlan(FromPandas(dfs, metadata), execution_plan._context)
        return MaterializedDataset(
            execution_plan,
            logical_plan,
        )

    df_to_block = cached_remote_fn(pandas_df_to_arrow_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = map(list, zip(*res))
    metadata = ray.get(metadata)
    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"FromPandas": metadata}, parent=None)
    )
    logical_plan = LogicalPlan(FromPandas(blocks, metadata), execution_plan._context)
    return MaterializedDataset(
        execution_plan,
        logical_plan,
    )


@PublicAPI
def from_numpy(ndarrays: Union[np.ndarray, List[np.ndarray]]) -> MaterializedDataset:
    """Creates a :class:`~ray.data.Dataset` from a list of NumPy ndarrays.

    Examples:
        >>> import numpy as np
        >>> import ray
        >>> arr = np.array([1])
        >>> ray.data.from_numpy(arr)
        MaterializedDataset(num_blocks=1, num_rows=1, schema={data: int64})

        Create a Ray Dataset from a list of NumPy arrays.

        >>> ray.data.from_numpy([arr, arr])
        MaterializedDataset(num_blocks=2, num_rows=2, schema={data: int64})

    Args:
        ndarrays: A NumPy ndarray or a list of NumPy ndarrays.

    Returns:
        :class:`~ray.data.Dataset` holding data from the given ndarrays.
    """
    if isinstance(ndarrays, np.ndarray):
        ndarrays = [ndarrays]

    return from_numpy_refs([ray.put(ndarray) for ndarray in ndarrays])


@DeveloperAPI
def from_numpy_refs(
    ndarrays: Union[ObjectRef[np.ndarray], List[ObjectRef[np.ndarray]]],
) -> MaterializedDataset:
    """Creates a :class:`~ray.data.Dataset` from a list of Ray object references to
    NumPy ndarrays.

    Examples:
        >>> import numpy as np
        >>> import ray
        >>> arr_ref = ray.put(np.array([1]))
        >>> ray.data.from_numpy_refs(arr_ref)
        MaterializedDataset(num_blocks=1, num_rows=1, schema={data: int64})

        Create a Ray Dataset from a list of NumPy array references.

        >>> ray.data.from_numpy_refs([arr_ref, arr_ref])
        MaterializedDataset(num_blocks=2, num_rows=2, schema={data: int64})

    Args:
        ndarrays: A Ray object reference to a NumPy ndarray or a list of Ray object
            references to NumPy ndarrays.

    Returns:
        :class:`~ray.data.Dataset` holding data from the given ndarrays.
    """
    if isinstance(ndarrays, ray.ObjectRef):
        ndarrays = [ndarrays]
    elif isinstance(ndarrays, list):
        for ndarray in ndarrays:
            if not isinstance(ndarray, ray.ObjectRef):
                raise ValueError(
                    "Expected list of Ray object refs, "
                    f"got list containing {type(ndarray)}"
                )
    else:
        raise ValueError(
            f"Expected Ray object ref or list of Ray object refs, got {type(ndarray)}"
        )

    ctx = DataContext.get_current()
    ndarray_to_block_remote = cached_remote_fn(ndarray_to_block, num_returns=2)

    res = [ndarray_to_block_remote.remote(ndarray, ctx) for ndarray in ndarrays]
    blocks, metadata = map(list, zip(*res))
    metadata = ray.get(metadata)

    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"FromNumpy": metadata}, parent=None)
    )
    logical_plan = LogicalPlan(FromNumpy(blocks, metadata), execution_plan._context)

    return MaterializedDataset(
        execution_plan,
        logical_plan,
    )


@PublicAPI
def from_arrow(
    tables: Union["pyarrow.Table", bytes, List[Union["pyarrow.Table", bytes]]],
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of PyArrow tables.

    Examples:
        >>> import pyarrow as pa
        >>> import ray
        >>> table = pa.table({"x": [1]})
        >>> ray.data.from_arrow(table)
        MaterializedDataset(num_blocks=1, num_rows=1, schema={x: int64})

        Create a Ray Dataset from a list of PyArrow tables.

        >>> ray.data.from_arrow([table, table])
        MaterializedDataset(num_blocks=2, num_rows=2, schema={x: int64})


    Args:
        tables: A PyArrow table, or a list of PyArrow tables,
                or its streaming format in bytes.

    Returns:
        :class:`~ray.data.Dataset` holding data from the PyArrow tables.
    """
    import pyarrow as pa

    if isinstance(tables, (pa.Table, bytes)):
        tables = [tables]
    return from_arrow_refs([ray.put(t) for t in tables])


@DeveloperAPI
def from_arrow_refs(
    tables: Union[
        ObjectRef[Union["pyarrow.Table", bytes]],
        List[ObjectRef[Union["pyarrow.Table", bytes]]],
    ],
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of Ray object references to
    PyArrow tables.

    Examples:
        >>> import pyarrow as pa
        >>> import ray
        >>> table_ref = ray.put(pa.table({"x": [1]}))
        >>> ray.data.from_arrow_refs(table_ref)
        MaterializedDataset(num_blocks=1, num_rows=1, schema={x: int64})

        Create a Ray Dataset from a list of PyArrow table references

        >>> ray.data.from_arrow_refs([table_ref, table_ref])
        MaterializedDataset(num_blocks=2, num_rows=2, schema={x: int64})


    Args:
        tables: A Ray object reference to Arrow table, or list of Ray object
                references to Arrow tables, or its streaming format in bytes.

    Returns:
         :class:`~ray.data.Dataset` holding data read from the tables.
    """
    if isinstance(tables, ray.ObjectRef):
        tables = [tables]

    get_metadata = cached_remote_fn(get_table_block_metadata)
    metadata = ray.get([get_metadata.remote(t) for t in tables])
    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"FromArrow": metadata}, parent=None)
    )
    logical_plan = LogicalPlan(FromArrow(tables, metadata), execution_plan._context)

    return MaterializedDataset(
        execution_plan,
        logical_plan,
    )


@PublicAPI(stability="alpha")
def read_delta_sharing_tables(
    url: str,
    *,
    limit: Optional[int] = None,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
    json_predicate_hints: Optional[str] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """
    Read data from a Delta Sharing table.
    Delta Sharing projct https://github.com/delta-io/delta-sharing/tree/main

    This function reads data from a Delta Sharing table specified by the URL.
    It supports various options such as limiting the number of rows, specifying
    a version or timestamp, and configuring concurrency.

    Before calling this function, ensure that the URL is correctly formatted
    to point to the Delta Sharing table you want to access. Make sure you have
    a valid delta_share profile in the working directory.

    Examples:

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_delta_sharing_tables(
                url=f"your-profile.json#your-share-name.your-schema-name.your-table-name",
                limit=100000,
                version=1,
            )

    Args:
        url: A URL under the format
            "<profile-file-path>#<share-name>.<schema-name>.<table-name>".
            Example can be found at
            https://github.com/delta-io/delta-sharing/blob/main/README.md#quick-start
        limit: A non-negative integer. Load only the ``limit`` rows if the
            parameter is specified. Use this optional parameter to explore the
            shared table without loading the entire table into memory.
        version: A non-negative integer. Load the snapshot of the table at
            the specified version.
        timestamp: A timestamp to specify the version of the table to read.
        json_predicate_hints: Predicate hints to be applied to the table. For more
            details, see:
            https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#json-predicates-for-filtering.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control the number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`Dataset` containing the queried data.

    Raises:
        ValueError: If the URL is not properly formatted or if there is an issue
            with the Delta Sharing table connection.
    """

    datasource = DeltaSharingDatasource(
        url=url,
        json_predicate_hints=json_predicate_hints,
        limit=limit,
        version=version,
        timestamp=timestamp,
    )
    # DeltaSharing limit is at the add_files level, it will not return
    # exactly the limit number of rows but it will return less files and rows.
    return ray.data.read_datasource(
        datasource=datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@PublicAPI
def from_spark(
    df: "pyspark.sql.DataFrame",
    *,
    parallelism: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a
    `Spark DataFrame <https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html>`_.

    Args:
        df: A `Spark DataFrame`_, which must be created by RayDP (Spark-on-Ray).
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.MaterializedDataset` holding rows read from the DataFrame.
    """  # noqa: E501
    import raydp

    parallelism = _get_num_output_blocks(parallelism, override_num_blocks)
    return raydp.spark.spark_dataframe_to_ray_dataset(df, parallelism)


@PublicAPI
def from_huggingface(
    dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
    parallelism: int = -1,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Union[MaterializedDataset, Dataset]:
    """Create a :class:`~ray.data.MaterializedDataset` from a
    `Hugging Face Datasets Dataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.Dataset/>`_
    or a :class:`~ray.data.Dataset` from a `Hugging Face Datasets IterableDataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.IterableDataset/>`_.
    For an `IterableDataset`, we use a streaming implementation to read data.

    If the dataset is a public Hugging Face Dataset that is hosted on the Hugging Face Hub and
    no transformations have been applied, then the `hosted parquet files <https://huggingface.co/docs/datasets-server/parquet#list-parquet-files>`_
    will be passed to :meth:`~ray.data.read_parquet` to perform a distributed read. All
    other cases will be done with a single node read.

    Example:

        ..
            The following `testoutput` is mocked to avoid illustrating download
            logs like "Downloading and preparing dataset 162.17 MiB".

        .. testcode::

            import ray
            import datasets

            hf_dataset = datasets.load_dataset("tweet_eval", "emotion")
            ray_ds = ray.data.from_huggingface(hf_dataset["train"])
            print(ray_ds)

            hf_dataset_stream = datasets.load_dataset("tweet_eval", "emotion", streaming=True)
            ray_ds_stream = ray.data.from_huggingface(hf_dataset_stream["train"])
            print(ray_ds_stream)

        .. testoutput::
            :options: +MOCK

            MaterializedDataset(
                num_blocks=...,
                num_rows=3257,
                schema={text: string, label: int64}
            )
            Dataset(
                num_rows=3257,
                schema={text: string, label: int64}
            )

    Args:
        dataset: A `Hugging Face Datasets Dataset`_ or `Hugging Face Datasets IterableDataset`_.
            `DatasetDict <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.DatasetDict/>`_
            and `IterableDatasetDict <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.IterableDatasetDict/>`_
            are not supported.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` holding rows from the `Hugging Face Datasets Dataset`_.
    """  # noqa: E501
    import datasets
    from aiohttp.client_exceptions import ClientResponseError

    from ray.data._internal.datasource.huggingface_datasource import (
        HuggingFaceDatasource,
    )

    if isinstance(dataset, (datasets.IterableDataset, datasets.Dataset)):
        try:
            # Attempt to read data via Hugging Face Hub parquet files. If the
            # returned list of files is empty, attempt read via other methods.
            file_urls = HuggingFaceDatasource.list_parquet_urls_from_dataset(dataset)
            if len(file_urls) > 0:
                # If file urls are returned, the parquet files are available via API
                # TODO: Add support for reading from http filesystem in
                # FileBasedDatasource. GH Issue:
                # https://github.com/ray-project/ray/issues/42706
                import fsspec.implementations.http

                http = fsspec.implementations.http.HTTPFileSystem()
                return read_parquet(
                    file_urls,
                    parallelism=parallelism,
                    filesystem=http,
                    concurrency=concurrency,
                    override_num_blocks=override_num_blocks,
                    ray_remote_args={
                        "retry_exceptions": [FileNotFoundError, ClientResponseError]
                    },
                )
        except (FileNotFoundError, ClientResponseError):
            logger.warning(
                "Distrubuted read via Hugging Face Hub parquet files failed, "
                "falling back on single node read."
            )

    if isinstance(dataset, datasets.IterableDataset):
        # For an IterableDataset, we can use a streaming implementation to read data.
        return read_datasource(
            HuggingFaceDatasource(dataset=dataset),
            parallelism=parallelism,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )
    if isinstance(dataset, datasets.Dataset):
        # For non-streaming Hugging Face Dataset, we don't support override_num_blocks
        if override_num_blocks is not None:
            raise ValueError(
                "`override_num_blocks` parameter is not supported for "
                "streaming Hugging Face Datasets. Please omit the parameter or "
                "use non-streaming mode to read the dataset."
            )

        # To get the resulting Arrow table from a Hugging Face Dataset after
        # applying transformations (e.g., train_test_split(), shard(), select()),
        # we create a copy of the Arrow table, which applies the indices
        # mapping from the transformations.
        hf_ds_arrow = dataset.with_format("arrow")
        ray_ds = from_arrow(hf_ds_arrow[:])
        return ray_ds
    elif isinstance(dataset, (datasets.DatasetDict, datasets.IterableDatasetDict)):
        available_keys = list(dataset.keys())
        raise DeprecationWarning(
            "You provided a Hugging Face DatasetDict or IterableDatasetDict, "
            "which contains multiple datasets, but `from_huggingface` now "
            "only accepts a single Hugging Face Dataset. To convert just "
            "a single Hugging Face Dataset to a Ray Dataset, specify a split. "
            "For example, `ray.data.from_huggingface(my_dataset_dictionary"
            f"['{available_keys[0]}'])`. "
            f"Available splits are {available_keys}."
        )
    else:
        raise TypeError(
            f"`dataset` must be a `datasets.Dataset`, but got {type(dataset)}"
        )


@PublicAPI
def from_tf(
    dataset: "tf.data.Dataset",
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a
    `TensorFlow Dataset <https://www.tensorflow.org/api_docs/python/tf/data/Dataset/>`_.

    This function is inefficient. Use it to read small datasets or prototype.

    .. warning::
        If your dataset is large, this function may execute slowly or raise an
        out-of-memory error. To avoid issues, read the underyling data with a function
        like :meth:`~ray.data.read_images`.

    .. note::
        This function isn't parallelized. It loads the entire dataset into the local
        node's memory before moving the data to the distributed object store.

    Examples:
        >>> import ray
        >>> import tensorflow_datasets as tfds
        >>> dataset, _ = tfds.load('cifar10', split=["train", "test"])  # doctest: +SKIP
        >>> ds = ray.data.from_tf(dataset)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        MaterializedDataset(
            num_blocks=...,
            num_rows=50000,
            schema={
                id: binary,
                image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8),
                label: int64
            }
        )
        >>> ds.take(1)  # doctest: +SKIP
        [{'id': b'train_16399', 'image': array([[[143,  96,  70],
        [141,  96,  72],
        [135,  93,  72],
        ...,
        [ 96,  37,  19],
        [105,  42,  18],
        [104,  38,  20]],
        ...,
        [[195, 161, 126],
        [187, 153, 123],
        [186, 151, 128],
        ...,
        [212, 177, 147],
        [219, 185, 155],
        [221, 187, 157]]], dtype=uint8), 'label': 7}]

    Args:
        dataset: A `TensorFlow Dataset`_.

    Returns:
        A :class:`MaterializedDataset` that contains the samples stored in the `TensorFlow Dataset`_.
    """  # noqa: E501
    # FIXME: `as_numpy_iterator` errors if `dataset` contains ragged tensors.
    return from_items(list(dataset.as_numpy_iterator()))


@PublicAPI
def from_torch(
    dataset: "torch.utils.data.Dataset",
    local_read: bool = False,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from a
    `Torch Dataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.Dataset/>`_.

    .. note::
        The input dataset can either be map-style or iterable-style, and can have arbitrarily large amount of data.
        The data will be sequentially streamed with one single read task.

    Examples:
        >>> import ray
        >>> from torchvision import datasets
        >>> dataset = datasets.MNIST("data", download=True)  # doctest: +SKIP
        >>> ds = ray.data.from_torch(dataset)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        MaterializedDataset(num_blocks=..., num_rows=60000, schema={item: object})
        >>> ds.take(1)  # doctest: +SKIP
        {"item": (<PIL.Image.Image image mode=L size=28x28 at 0x...>, 5)}

    Args:
        dataset: A `Torch Dataset`_.
        local_read: If ``True``, perform the read as a local read.

    Returns:
        A :class:`~ray.data.Dataset` containing the Torch dataset samples.
    """  # noqa: E501

    # Files may not be accessible from all nodes, run the read task on current node.
    ray_remote_args = {}
    if local_read:
        ray_remote_args = {
            "scheduling_strategy": NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(),
                soft=False,
            ),
            # The user might have initialized Ray to have num_cpus = 0 for the head
            # node. For a local read we expect the read task to be executed on the
            # head node, so we should set num_cpus = 0 for the task to allow it to
            # run regardless of the user's head node configuration.
            "num_cpus": 0,
        }
    return read_datasource(
        TorchDatasource(dataset=dataset),
        ray_remote_args=ray_remote_args,
        # Only non-parallel, streaming read is currently supported
        override_num_blocks=1,
    )


@PublicAPI
def read_iceberg(
    *,
    table_identifier: str,
    row_filter: Union[str, "BooleanExpression"] = None,
    parallelism: int = -1,
    selected_fields: Tuple[str, ...] = ("*",),
    snapshot_id: Optional[int] = None,
    scan_kwargs: Optional[Dict[str, str]] = None,
    catalog_kwargs: Optional[Dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Create a :class:`~ray.data.Dataset` from an Iceberg table.

    The table to read from is specified using a fully qualified ``table_identifier``.
    Using PyIceberg, any intended row filters, selection of specific fields and
    picking of a particular snapshot ID are applied, and the files that satisfy
    the query are distributed across Ray read tasks.
    The number of output blocks is determined by ``override_num_blocks``
    which can be requested from this interface or automatically chosen if
    unspecified.

    .. tip::

        For more details on PyIceberg, see
        - URI: https://py.iceberg.apache.org/

    Examples:
        >>> import ray
        >>> from pyiceberg.expressions import EqualTo  #doctest: +SKIP
        >>> ds = ray.data.read_iceberg( #doctest: +SKIP
        ...     table_identifier="db_name.table_name",
        ...     row_filter=EqualTo("column_name", "literal_value"),
        ...     catalog_kwargs={"name": "default", "type": "glue"}
        ... )

    Args:
        table_identifier: Fully qualified table identifier (``db_name.table_name``)
        row_filter: A PyIceberg :class:`~pyiceberg.expressions.BooleanExpression`
            to use to filter the data *prior* to reading
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        selected_fields: Which columns from the data to read, passed directly to
            PyIceberg's load functions. Should be an tuple of string column names.
        snapshot_id: Optional snapshot ID for the Iceberg table, by default the latest
            snapshot is used
        scan_kwargs: Optional arguments to pass to PyIceberg's Table.scan() function
             (e.g., case_sensitive, limit, etc.)
        catalog_kwargs: Optional arguments to pass to PyIceberg's catalog.load_catalog()
             function (e.g., name, type, etc.). For the function definition, see
             `pyiceberg catalog
             <https://py.iceberg.apache.org/reference/pyiceberg/catalog/\
             #pyiceberg.catalog.load_catalog>`_.
        ray_remote_args: Optional arguments to pass to `ray.remote` in the read tasks
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources, and capped at the number of
            physical files to be read. You shouldn't manually set this value in most
            cases.

    Returns:
        :class:`~ray.data.Dataset` with rows from the Iceberg table.
    """

    # Setup the Datasource
    datasource = IcebergDatasource(
        table_identifier=table_identifier,
        row_filter=row_filter,
        selected_fields=selected_fields,
        snapshot_id=snapshot_id,
        scan_kwargs=scan_kwargs,
        catalog_kwargs=catalog_kwargs,
    )

    dataset = read_datasource(
        datasource=datasource,
        parallelism=parallelism,
        override_num_blocks=override_num_blocks,
        ray_remote_args=ray_remote_args,
    )

    return dataset


@PublicAPI
def read_lance(
    uri: str,
    *,
    columns: Optional[List[str]] = None,
    filter: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
    scanner_options: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """
    Create a :class:`~ray.data.Dataset` from a
    `Lance Dataset <https://lancedb.github.io/lance/api/python/lance.html#lance.LanceDataset>`_.

    Examples:
        >>> import ray
        >>> ds = ray.data.read_lance( # doctest: +SKIP
        ...     uri="./db_name.lance",
        ...     columns=["image", "label"],
        ...     filter="label = 2 AND text IS NOT NULL",
        ... )

    Args:
        uri: The URI of the Lance dataset to read from. Local file paths, S3, and GCS
            are supported.
        columns: The columns to read. By default, all columns are read.
        filter: Read returns only the rows matching the filter. By default, no
            filter is applied.
        storage_options: Extra options that make sense for a particular storage
            connection. This is used to store connection parameters like credentials,
            endpoint, etc. For more information, see `Object Store Configuration <https\
                ://lancedb.github.io/lance/read_and_write.html#object-store-configuration>`_.
        scanner_options: Additional options to configure the `LanceDataset.scanner()`
            method, such as `batch_size`. For more information,
            see `LanceDB API doc <https://lancedb.github.io\
            /lance/api/python/lance.html#lance.dataset.LanceDataset.scanner>`_
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing records read from the Lance dataset.
    """  # noqa: E501
    datasource = LanceDatasource(
        uri=uri,
        columns=columns,
        filter=filter,
        storage_options=storage_options,
        scanner_options=scanner_options,
    )

    return read_datasource(
        datasource=datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


def _get_datasource_or_legacy_reader(
    ds: Datasource,
    ctx: DataContext,
    kwargs: dict,
) -> Union[Datasource, Reader]:
    """Generates reader.

    Args:
        ds: Datasource to read from.
        ctx: Dataset config to use.
        kwargs: Additional kwargs to pass to the legacy reader if
            `Datasource.create_reader` is implemented.

    Returns:
        The datasource or a generated legacy reader.
    """
    kwargs = _unwrap_arrow_serialization_workaround(kwargs)

    DataContext._set_current(ctx)

    if ds.should_create_reader:
        warnings.warn(
            "`create_reader` has been deprecated in Ray 2.9. Instead of creating a "
            "`Reader`, implement `Datasource.get_read_tasks` and "
            "`Datasource.estimate_inmemory_data_size`.",
            DeprecationWarning,
        )
        datasource_or_legacy_reader = ds.create_reader(**kwargs)
    else:
        datasource_or_legacy_reader = ds

    return datasource_or_legacy_reader


def _resolve_parquet_args(
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    **arrow_parquet_args,
) -> Dict[str, Any]:
    if tensor_column_schema is not None:
        existing_block_udf = arrow_parquet_args.pop("_block_udf", None)

        def _block_udf(block: "pyarrow.Table") -> "pyarrow.Table":
            from ray.data.extensions import ArrowTensorArray

            for tensor_col_name, (dtype, shape) in tensor_column_schema.items():
                # NOTE(Clark): We use NumPy to consolidate these potentially
                # non-contiguous buffers, and to do buffer bookkeeping in
                # general.
                np_col = _create_possibly_ragged_ndarray(
                    [
                        np.ndarray(shape, buffer=buf.as_buffer(), dtype=dtype)
                        for buf in block.column(tensor_col_name)
                    ]
                )

                block = block.set_column(
                    block._ensure_integer_index(tensor_col_name),
                    tensor_col_name,
                    ArrowTensorArray.from_numpy(np_col, tensor_col_name),
                )
            if existing_block_udf is not None:
                # Apply UDF after casting the tensor columns.
                block = existing_block_udf(block)
            return block

        arrow_parquet_args["_block_udf"] = _block_udf
    return arrow_parquet_args


def _get_num_output_blocks(
    parallelism: int = -1,
    override_num_blocks: Optional[int] = None,
) -> int:
    if parallelism != -1:
        logger.warning(
            "The argument ``parallelism`` is deprecated in Ray 2.10. Please specify "
            "argument ``override_num_blocks`` instead."
        )
    elif override_num_blocks is not None:
        parallelism = override_num_blocks
    return parallelism


def _validate_shuffle_arg(shuffle: Optional[str]) -> None:
    if shuffle not in [None, "files"]:
        raise ValueError(
            f"Invalid value for 'shuffle': {shuffle}. "
            "Valid values are None, 'files'."
        )


def _emit_meta_provider_deprecation_warning(
    meta_provider: Optional[BaseFileMetadataProvider],
) -> None:
    if meta_provider is not None:
        warnings.warn(
            "The `meta_provider` argument is deprecated and will be removed after May "
            "2025.",
            DeprecationWarning,
        )
