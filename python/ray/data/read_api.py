import collections
import logging
import math
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

import ray
from ray._private.auto_init_hook import wrap_auto_init
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.logical.operators.from_operators import (
    FromArrow,
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
    _is_local_scheme,
    _lazy_import_pyarrow_dataset,
    get_table_block_metadata,
    ndarray_to_block,
    pandas_df_to_arrow_block,
)
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import WARN_PREFIX, DataContext
from ray.data.dataset import Dataset, MaterializedDataset
from ray.data.datasource import (
    BaseFileMetadataProvider,
    BinaryDatasource,
    Connection,
    CSVDatasource,
    Datasource,
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
    ImageDatasource,
    JSONDatasource,
    MongoDatasource,
    NumpyDatasource,
    ParquetBaseDatasource,
    ParquetDatasource,
    ParquetMetadataProvider,
    PathPartitionFilter,
    RangeDatasource,
    ReadTask,
    SQLDatasource,
    TextDatasource,
    TFRecordDatasource,
    WebDatasetDatasource,
)
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
    _wrap_arrow_serialization_workaround,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider
from ray.data.datasource.partitioning import Partitioning
from ray.types import ObjectRef
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray.util.placement_group import PlacementGroup
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
    from tensorflow_metadata.proto.v0 import schema_pb2


T = TypeVar("T")

logger = logging.getLogger(__name__)


@PublicAPI
def from_items(
    items: List[Any],
    *,
    parallelism: int = -1,
    output_arrow_format: bool = True,
) -> MaterializedDataset:
    """Create a :class:`~ray.data.Dataset` from a list of local Python objects.

    Use this method to create small datasets for testing and exploration.

    Examples:

        .. testcode::

            import ray

            ds = ray.data.from_items([1, 2, 3, 4, 5])

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            item    int64

    Args:
        items: List of local Python objects.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism might be limited by the number of items.

    Returns:
        A :class:`~ray.data.Dataset` holding the items.
    """
    import builtins

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
            BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
        )

    from_items_op = FromItems(blocks, metadata)
    logical_plan = LogicalPlan(from_items_op)
    return MaterializedDataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"FromItems": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        True,
        logical_plan,
    )


@PublicAPI
def range(n: int, *, parallelism: int = -1) -> Dataset:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range(10000) # doctest: +SKIP
        >>> ds # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=10000, schema={id: int64})
        >>> ds.map(lambda x: {"id": x["id"] * 2}).take(4) # doctest: +SKIP
        [{"id": 0}, {"id": 2}, {"id": 4}, {"id": 6}]

    Args:
        n: The upper bound of the range of integers.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset producing the integers.
    """
    return read_datasource(
        RangeDatasource(),
        parallelism=parallelism,
        n=n,
        block_format="arrow",
        column_name="id",
    )


@Deprecated
def range_table(n: int, *, parallelism: int = -1) -> Dataset:
    raise DeprecationWarning("In Ray 2.5, use range() instead of range_table().")


@PublicAPI
def range_tensor(n: int, *, shape: Tuple = (1,), parallelism: int = -1) -> Dataset:
    """Create a Tensor stream from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range_tensor(1000, shape=(2, 2))
        >>> ds  # doctest: +ELLIPSIS
        Dataset(
           num_blocks=...,
           num_rows=1000,
           schema={data: numpy.ndarray(shape=(2, 2), dtype=int64)}
        )
        >>> ds.map_batches(lambda arr: arr * 2).take(2) # doctest: +SKIP
        [array([[0, 0],
                [0, 0]]),
         array([[2, 2],
                [2, 2]])]

    This is similar to range_table(), but uses the ArrowTensorArray extension
    type. The dataset elements take the form {"data": array(N, shape=shape)}.

    Args:
        n: The upper bound of the range of integer records.
        shape: The shape of each record.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset producing the integers as Arrow tensor records.
    """
    return read_datasource(
        RangeDatasource(),
        parallelism=parallelism,
        n=n,
        block_format="tensor",
        column_name="data",
        tensor_shape=tuple(shape),
    )


@PublicAPI
@wrap_auto_init
def read_datasource(
    datasource: Datasource,
    *,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    **read_args,
) -> Dataset:
    """Read a stream from a custom data source.

    Args:
        datasource: The datasource to read data from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the available partitioning of the datasource. If set to -1,
            parallelism will be automatically chosen based on the available cluster
            resources and estimated in-memory data size.
        read_args: Additional kwargs to pass to the datasource impl.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.

    Returns:
        Dataset that reads data from the datasource.
    """
    ctx = DataContext.get_current()

    if ray_remote_args is None:
        ray_remote_args = {}

    local_uri = False
    paths = read_args.get("paths", None)
    if paths and _is_local_scheme(paths):
        if ray.util.client.ray.is_connected():
            raise ValueError(
                f"The local scheme paths {paths} are not supported in Ray Client."
            )
        ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )
        local_uri = True

    if "scheduling_strategy" not in ray_remote_args:
        ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy

    force_local = False
    cur_pg = ray.util.get_current_placement_group()
    pa_ds = _lazy_import_pyarrow_dataset()
    if pa_ds:
        partitioning = read_args.get("dataset_kwargs", {}).get("partitioning", None)
        if isinstance(partitioning, pa_ds.Partitioning):
            logger.info(
                "Forcing local metadata resolution since the provided partitioning "
                f"{partitioning} is not serializable."
            )
            force_local = True

    if force_local:
        (
            requested_parallelism,
            min_safe_parallelism,
            inmemory_size,
            read_tasks,
        ) = _get_read_tasks(datasource, ctx, cur_pg, parallelism, local_uri, read_args)
    else:
        # Prepare read in a remote task at same node.
        # NOTE: in Ray client mode, this is expected to be run on head node.
        # So we aren't attempting metadata resolution from the client machine.
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )
        get_read_tasks = cached_remote_fn(
            _get_read_tasks, retry_exceptions=False, num_cpus=0
        ).options(scheduling_strategy=scheduling_strategy)

        (
            requested_parallelism,
            min_safe_parallelism,
            inmemory_size,
            read_tasks,
        ) = ray.get(
            get_read_tasks.remote(
                datasource,
                ctx,
                cur_pg,
                parallelism,
                local_uri,
                _wrap_arrow_serialization_workaround(read_args),
            )
        )

    # Compute the number of blocks the read will return. If the number of blocks is
    # expected to be less than the requested parallelism, boost the number of blocks
    # by adding an additional split into `k` pieces to each read task.
    if read_tasks:
        if inmemory_size:
            expected_block_size = inmemory_size / len(read_tasks)
            logger.debug(f"Expected block size {expected_block_size}")
            size_based_splits = round(
                max(1, expected_block_size / ctx.target_max_block_size)
            )
        else:
            size_based_splits = 1
        logger.debug(f"Size based split factor {size_based_splits}")
        estimated_num_blocks = len(read_tasks) * size_based_splits
        logger.debug(f"Blocks after size splits {estimated_num_blocks}")

        # Add more output splitting for each read task if needed.
        if estimated_num_blocks < requested_parallelism:
            k = math.ceil(requested_parallelism / estimated_num_blocks)
            logger.info(
                f"To satisfy the requested parallelism of {requested_parallelism}, "
                f"each read task output will be split into {k} smaller blocks."
            )
            for r in read_tasks:
                r._set_additional_split_factor(k)
            estimated_num_blocks = estimated_num_blocks * k
        logger.debug("Estimated num output blocks {estimated_num_blocks}")
    else:
        estimated_num_blocks = 0

    read_stage_name = f"Read{datasource.get_name()}"
    available_cpu_slots = ray.available_resources().get("CPU", 1)
    if (
        requested_parallelism
        and len(read_tasks) > available_cpu_slots * 4
        and len(read_tasks) >= 5000
    ):
        logger.warn(
            f"{WARN_PREFIX} The requested parallelism of {requested_parallelism} "
            "is more than 4x the number of available CPU slots in the cluster of "
            f"{available_cpu_slots}. This can "
            "lead to slowdowns during the data reading phase due to excessive "
            "task creation. Reduce the parallelism to match with the available "
            "CPU slots in the cluster, or set parallelism to -1 for Ray Data "
            "to automatically determine the parallelism. "
            "You can ignore this message if the cluster is expected to autoscale."
        )

    block_list = LazyBlockList(
        read_tasks,
        read_stage_name=read_stage_name,
        ray_remote_args=ray_remote_args,
        owned_by_consumer=False,
    )
    block_list._estimated_num_blocks = estimated_num_blocks

    # TODO(hchen): move _get_read_tasks and related code to the Read physical operator,
    # after removing LazyBlockList code path.
    read_op = Read(datasource, read_tasks, estimated_num_blocks, ray_remote_args)
    logical_plan = LogicalPlan(read_op)

    return Dataset(
        plan=ExecutionPlan(block_list, block_list.stats(), run_by_consumer=False),
        epoch=0,
        lazy=True,
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
    **mongo_args,
) -> Dataset:
    """Create an Arrow dataset from MongoDB.

    The data to read from is specified via the ``uri``, ``database`` and ``collection``
    of the MongoDB. The dataset is created from the results of executing
    ``pipeline`` against the ``collection``. If ``pipeline`` is None, the entire
    ``collection`` will be read.

    You can check out more details here about these MongoDB concepts:
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
        ...     parallelism=10,
        ... )

    Args:
        uri: The URI of the source MongoDB where the dataset will be
            read from. For the URI format, see details in
            https://www.mongodb.com/docs/manual/reference/connection-string/.
        database: The name of the database hosted in the MongoDB. This database
            must exist otherwise ValueError will be raised.
        collection: The name of the collection in the database. This collection
            must exist otherwise ValueError will be raised.
        pipeline: A MongoDB pipeline, which will be executed on the given collection
            with results used to create Dataset. If None, the entire collection will
            be read.
        schema: The schema used to read the collection. If None, it'll be inferred from
            the results of pipeline.
        parallelism: The requested parallelism of the read. If -1, it will be
            automatically chosen based on the available cluster resources and estimated
            in-memory data size.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        mongo_args: kwargs passed to aggregate_arrow_all() in pymongoarrow in producing
            Arrow-formatted results.

    Returns:
        Dataset producing Arrow records from the results of executing the pipeline
        on the specified MongoDB collection.
    """
    return read_datasource(
        MongoDatasource(),
        parallelism=parallelism,
        uri=uri,
        database=database,
        collection=collection,
        pipeline=pipeline,
        schema=schema,
        ray_remote_args=ray_remote_args,
        **mongo_args,
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
    meta_provider: ParquetMetadataProvider = DefaultParquetMetadataProvider(),
    **arrow_parquet_args,
) -> Dataset:
    """Create an Arrow dataset from parquet files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_parquet("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_parquet(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        >>> # Specify a schema for the parquet file.
        >>> import pyarrow as pa
        >>> fields = [("sepal.length", pa.float64()),
        ...           ("sepal.width", pa.float64()),
        ...           ("petal.length", pa.float64()),
        ...           ("petal.width", pa.float64()),
        ...           ("variety", pa.string())]
        >>> ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet",
        ...     schema=pa.schema(fields))

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

        For further arguments you can pass to pyarrow as a keyword argument, see
        https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment

    Args:
        paths: A single file path or directory, or a list of file paths. Multiple
            directories are not supported.
        filesystem: The filesystem implementation to read from. These are specified in
            https://arrow.apache.org/docs/python/api/filesystems.html#filesystem-implementations.
        columns: A list of column names to read.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        tensor_column_schema: A dict of column name --> tensor dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to our tensor column extension
            type. This assumes that the tensors were serialized in the raw
            NumPy array format in C-contiguous order (e.g. via
            `arr.tobytes()`).
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        arrow_parquet_args: Other parquet read options to pass to pyarrow, see
            https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment

    Returns:
        Dataset producing Arrow records read from the specified paths.
    """
    arrow_parquet_args = _resolve_parquet_args(
        tensor_column_schema,
        **arrow_parquet_args,
    )
    return read_datasource(
        ParquetDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        columns=columns,
        ray_remote_args=ray_remote_args,
        meta_provider=meta_provider,
        **arrow_parquet_args,
    )


@PublicAPI(stability="beta")
def read_images(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    meta_provider: BaseFileMetadataProvider = _ImageFileMetadataProvider(),
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[
        PathPartitionFilter
    ] = ImageDatasource.file_extension_filter(),
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
) -> Dataset:
    """Read images from the specified paths.

    Examples:
        >>> import ray
        >>> path = "s3://anonymous@air-example-data-2/movie-image-small-filesize-1GB"
        >>> ds = ray.data.read_images(path)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=41979, schema={image: numpy.ndarray(ndim=3, dtype=uint8)})

        If you need image file paths, set ``include_paths=True``.

        >>> ds = ray.data.read_images(path, include_paths=True)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=41979, schema={image: numpy.ndarray(ndim=3, dtype=uint8), path: string})
        >>> ds.take(1)[0]["path"]  # doctest: +SKIP
        'air-example-data-2/movie-image-small-filesize-1GB/0.jpg'

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
        >>> ds = ray.data.read_images(root, size=(224, 224), partitioning=partitioning)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=94946, schema={image: TensorDtype(shape=(224, 224, 3), dtype=uint8), class: object})

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_file_args: kwargs passed to
            ``pyarrow.fs.FileSystem.open_input_file``.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match ``*.png``, ``*.jpg``, ``*.jpeg``, ``*.tiff``, ``*.bmp``, or ``*.gif``.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        size: The desired height and width of loaded images. If unspecified, images
            retain their original shape.
        mode: A `Pillow mode <https://pillow.readthedocs.io/en/stable/handbook/concepts.html#modes>`_
            describing the desired type and depth of pixels. If unspecified, image
            modes are inferred by
            `Pillow <https://pillow.readthedocs.io/en/stable/index.html>`_.
        include_paths: If ``True``, include the path to each image. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file/directory paths in ``paths``
            that are not found. Defaults to False.

    Returns:
        A :class:`~ray.data.Dataset` producing tensors that represent the images at
        the specified paths. For information on working with tensors, read the
        :ref:`tensor data guide <working_with_tensors>`.

    Raises:
        ValueError: if ``size`` contains non-positive numbers.
        ValueError: if ``mode`` is unsupported.
    """  # noqa: E501
    return read_datasource(
        ImageDatasource(),
        paths=paths,
        filesystem=filesystem,
        parallelism=parallelism,
        meta_provider=meta_provider,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_file_args,
        partition_filter=partition_filter,
        partitioning=partitioning,
        size=size,
        mode=mode,
        include_paths=include_paths,
        ignore_missing_paths=ignore_missing_paths,
    )


@PublicAPI
def read_parquet_bulk(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    meta_provider: BaseFileMetadataProvider = FastFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = (
        ParquetBaseDatasource.file_extension_filter()
    ),
    **arrow_parquet_args,
) -> Dataset:
    """Create an Arrow dataset from a large number (such as >1K) of parquet files
    quickly.

    By default, ONLY file paths should be provided as input (i.e. no directory paths),
    and an OSError will be raised if one or more paths point to directories. If your
    use-case requires directory paths, then the metadata provider should be changed to
    one that supports directory expansion (e.g. ``DefaultFileMetadataProvider``).

    Offers improved performance vs. :func:`read_parquet` due to not using PyArrow's
    ``ParquetDataset`` abstraction, whose latency scales linearly with the number of
    input files due to collecting all file metadata on a single node.

    Also supports a wider variety of input Parquet file types than :func:`read_parquet`
    due to not trying to merge and resolve a unified schema for all files.

    However, unlike :func:`read_parquet`, this does not offer file metadata resolution
    by default, so a custom metadata provider should be provided if your use-case
    requires a unified schema, block sizes, row counts, etc.

    Examples:
        >>> # Read multiple local files. You should always provide only input file
        >>> # paths (i.e. no directory paths) when known to minimize read latency.
        >>> ray.data.read_parquet_bulk( # doctest: +SKIP
        ...     ["/path/to/file1", "/path/to/file2"])

        >>> # Read a directory of files in remote storage. Caution should be taken
        >>> # when providing directory paths, since the time to both check each path
        >>> # type and expand its contents may result in greatly increased latency
        >>> # and/or request rate throttling from cloud storage service providers.
        >>> ray.data.read_parquet_bulk( # doctest: +SKIP
        ...     "s3://bucket/path",
        ...     meta_provider=DefaultFileMetadataProvider())

    Args:
        paths: A single file path or a list of file paths. If one or more directories
            are provided, then ``meta_provider`` should also be set to an implementation
            that supports directory expansion (e.g. ``DefaultFileMetadataProvider``).
        filesystem: The filesystem implementation to read from.
        columns: A list of column names to read.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_file_args: kwargs passed to
            ``pyarrow.fs.FileSystem.open_input_file``.
        tensor_column_schema: A dict of column name --> tensor dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to our tensor column extension
            type. This assumes that the tensors were serialized in the raw
            NumPy array format in C-contiguous order (e.g. via
            ``arr.tobytes()``).
        meta_provider: File metadata provider. Defaults to a fast file metadata
            provider that skips file size collection and requires all input paths to be
            files. Change to ``DefaultFileMetadataProvider`` or a custom metadata
            provider if directory expansion and/or file metadata resolution is required.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.parquet*".
        arrow_parquet_args: Other parquet read options to pass to pyarrow.

    Returns:
        Dataset producing Arrow records read from the specified paths.
    """
    arrow_parquet_args = _resolve_parquet_args(
        tensor_column_schema,
        **arrow_parquet_args,
    )
    return read_datasource(
        ParquetBaseDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        columns=columns,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_file_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        **arrow_parquet_args,
    )


@PublicAPI
def read_json(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[
        PathPartitionFilter
    ] = JSONDatasource.file_extension_filter(),
    partitioning: Partitioning = Partitioning("hive"),
    ignore_missing_paths: bool = False,
    **arrow_json_args,
) -> Dataset:
    """Create an Arrow dataset from json files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_json("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_json(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        >>> # Read multiple directories.
        >>> ray.data.read_json( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

        By default, ``read_json`` parses
        `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning/>`_
        from file paths. If your data adheres to a different partitioning scheme, set
        the ``partitioning`` parameter.

        >>> ds = ray.data.read_json("s3://anonymous@ray-example-data/year=2022/month=09/sales.json")
        >>> ds.take(1)
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}]

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.json*".
        arrow_json_args: Other json read options to pass to pyarrow.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. By default, this function parses
            `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning/>`_.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.

    Returns:
        :class:`~ray.data.Dataset` producing records read from the specified paths.
    """  # noqa: E501
    return read_datasource(
        JSONDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        **arrow_json_args,
    )


@PublicAPI
def read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    ignore_missing_paths: bool = False,
    **arrow_csv_args,
) -> Dataset:
    r"""Create an Arrow dataset from csv files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_csv("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_csv(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        >>> # Read multiple directories.
        >>> ray.data.read_csv( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

        >>> # Read files that use a different delimiter. For more uses of ParseOptions see
        >>> # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html  # noqa: #501
        >>> from pyarrow import csv
        >>> parse_options = csv.ParseOptions(delimiter="\\t")  # doctest: +SKIP
        >>> ds = ray.data.read_csv(  # doctest: +SKIP
        ...     "s3://anonymous@ray-example-data/iris.tsv",
        ...     parse_options=parse_options)

        >>> # Convert a date column with a custom format from a CSV file.
        >>> # For more uses of ConvertOptions see
        >>> # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html  # noqa: #501
        >>> from pyarrow import csv
        >>> convert_options = csv.ConvertOptions(
        ...     timestamp_parsers=["%m/%d/%Y"])
        >>> ds = ray.data.read_csv(
        ...     "s3://anonymous@ray-example-data/dow_jones.csv",
        ...     convert_options=convert_options)

        By default, ``read_csv`` parses
        `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning/>`_
        from file paths. If your data adheres to a different partitioning scheme, set
        the ``partitioning`` parameter.

        >>> ds = ray.data.read_csv("s3://anonymous@ray-example-data/year=2022/month=09/sales.csv")
        >>> ds.take(1)
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}]

        By default, ``read_csv`` reads all files from file paths. If you want to filter
        files by file extensions, set the ``partition_filter`` parameter.

        >>> # Read only *.csv files from multiple directories.
        >>> from ray.data.datasource import FileExtensionFilter
        >>> ray.data.read_csv("s3://anonymous@ray-example-data/different-extensions/",  # doctest: +SKIP
        ...     partition_filter=FileExtensionFilter("csv"))

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this does not filter out any files.
            If wishing to filter out all file paths except those whose file extension
            matches e.g. "*.csv*", a ``FileExtensionFilter("csv")`` can be provided.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. By default, this function parses
            `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning/>`_.
        arrow_csv_args: Other csv read options to pass to pyarrow.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.

    Returns:
        Dataset producing Arrow records read from the specified paths.
    """  # noqa: E501
    return read_datasource(
        CSVDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        **arrow_csv_args,
    )


@PublicAPI
def read_text(
    paths: Union[str, List[str]],
    *,
    encoding: str = "utf-8",
    errors: str = "ignore",
    drop_empty_lines: bool = True,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
) -> Dataset:
    """Create a dataset from lines stored in text files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_text("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_text(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

    Args:
        paths: A single file path or a list of file paths (or directories).
        encoding: The encoding of the files (e.g., "utf-8" or "ascii").
        errors: What to do with errors on decoding. Specify either "strict",
            "ignore", or "replace". Defaults to "ignore".
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the stream.
        ray_remote_args: Kwargs passed to ray.remote in the read tasks and
            in the subsequent text decoding map task.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a stream.
            By default, this does not filter out any files.
            If wishing to filter out all file paths except those whose file extension
            matches e.g. "*.txt*", a ``FileXtensionFilter("txt")`` can be provided.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.

    Returns:
        Dataset producing lines of text read from the specified paths.
    """
    return read_datasource(
        TextDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        drop_empty_lines=drop_empty_lines,
        encoding=encoding,
        ignore_missing_paths=ignore_missing_paths,
    )


@PublicAPI
def read_numpy(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[
        PathPartitionFilter
    ] = NumpyDatasource.file_extension_filter(),
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
    **numpy_load_args,
) -> Dataset:
    """Create an Arrow dataset from numpy files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_numpy("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_numpy(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        >>> # Read multiple directories.
        >>> ray.data.read_numpy( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        numpy_load_args: Other options to pass to np.load.
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match "*.npy*".
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.

    Returns:
        Dataset holding Tensor records read from the specified paths.
    """
    return read_datasource(
        NumpyDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        **numpy_load_args,
    )


@PublicAPI(stability="alpha")
def read_tfrecords(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = None,
    ignore_missing_paths: bool = False,
    tf_schema: Optional["schema_pb2.Schema"] = None,
) -> Dataset:
    """Create a dataset from TFRecord files that contain
    `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_
    messages.

    .. warning::
        This function exclusively supports ``tf.train.Example`` messages. If a file
        contains a message that isn't of type ``tf.train.Example``, then this function
        errors.

    Examples:
        >>> import os
        >>> import tempfile
        >>> import tensorflow as tf
        >>> features = tf.train.Features(
        ...     feature={
        ...         "length": tf.train.Feature(float_list=tf.train.FloatList(value=[5.1])),
        ...         "width": tf.train.Feature(float_list=tf.train.FloatList(value=[3.5])),
        ...         "species": tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"setosa"])),
        ...     }
        ... )
        >>> example = tf.train.Example(features=features)
        >>> path = os.path.join(tempfile.gettempdir(), "data.tfrecords")
        >>> with tf.io.TFRecordWriter(path=path) as writer:
        ...     writer.write(example.SerializeToString())

        This function reads ``tf.train.Example`` messages into a tabular
        :class:`~ray.data.Dataset`.

        >>> import ray
        >>> ray.data.read_tfrecords("s3://anonymous@ray-example-data/iris.tfrecords")
        Dataset(
           num_blocks=...,
           num_rows=150,
           schema={...}
        )

        We can also read compressed TFRecord files which uses one of the
        `compression type supported by Arrow <https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html>`_:

        >>> ds = ray.data.read_tfrecords(
        ...     "s3://anonymous@ray-example-data/iris.tfrecords.gz",
        ...     arrow_open_stream_args={"compression": "gzip"},
        ... )
        >>> ds.to_pandas()  # doctest: +SKIP
           length  width    species
        0     5.1    3.5  b'setosa'

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files in the dataset.
        arrow_open_stream_args: Key-word arguments passed to
            ``pyarrow.fs.FileSystem.open_input_stream``. To read a compressed TFRecord file,
            pass the corresponding compression type (e.g. for ``GZIP`` or ``ZLIB``, use
            ``arrow_open_stream_args={'compression_type': 'gzip'}``).
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match ``"*.tfrecords*"``.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        tf_schema: Optional TensorFlow Schema which is used to explicitly set the schema
            of the underlying Dataset.

    Returns:
        A :class:`~ray.data.Dataset` that contains the example features.

    Raises:
        ValueError: If a file contains a message that isn't a ``tf.train.Example``.
    """  # noqa: E501
    return read_datasource(
        TFRecordDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        tf_schema=tf_schema,
    )


@PublicAPI(stability="alpha")
def read_webdataset(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = None,
    decoder: Optional[Union[bool, str, callable, list]] = True,
    fileselect: Optional[Union[list, callable]] = None,
    filerename: Optional[Union[list, callable]] = None,
    suffixes: Optional[Union[list, callable]] = None,
    verbose_open: bool = False,
) -> Dataset:
    """Create a dataset from WebDataset files.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files in the dataset.
        arrow_open_stream_args: Key-word arguments passed to
            ``pyarrow.fs.FileSystem.open_input_stream``. To read a compressed TFRecord file,
            pass the corresponding compression type (e.g. for ``GZIP`` or ``ZLIB``, use
            ``arrow_open_stream_args={'compression_type': 'gzip'}``).
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
        decoder: A function or list of functions to decode the data.
        fileselect: A callable or list of glob patterns to select files.
        filerename: A function or list of tuples to rename files prior to grouping.
        suffixes: A function or list of suffixes to select for creating samples.
        verbose_open: Whether to print the file names as they are opened.

    Returns:
        A :class:`~ray.data.Dataset` that contains the example features.

    Raises:
        ValueError: If a file contains a message that isn't a ``tf.train.Example``.
    """  # noqa: E501
    return read_datasource(
        WebDatasetDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        decoder=decoder,
        fileselect=fileselect,
        filerename=filerename,
        suffixes=suffixes,
        verbose_open=verbose_open,
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
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
    output_arrow_format: bool = False,
) -> Dataset:
    """Create a dataset from binary files of arbitrary contents.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_binary_files("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_binary_files( # doctest: +SKIP
        ...     ["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        include_paths: Whether to include the full path of the file in the
            dataset records. When specified, the stream records will be a
            tuple of the file path and the file contents.
        filesystem: The filesystem implementation to read from.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the stream.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this does not filter out any files.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        ignore_missing_paths: If True, ignores any file paths in ``paths`` that are not
            found. Defaults to False.
        output_arrow_format: If True, returns data in Arrow format, instead of Python
            list format. Defaults to False.

    Returns:
        Dataset producing records read from the specified paths.
    """
    output_arrow_format = True

    return read_datasource(
        BinaryDatasource(),
        parallelism=parallelism,
        paths=paths,
        include_paths=include_paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        output_arrow_format=output_arrow_format,
    )


@PublicAPI(stability="alpha")
def read_sql(
    sql: str,
    connection_factory: Callable[[], Connection],
    *,
    parallelism: int = -1,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dataset:
    """Read from a database that provides a
    `Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

    .. note::

        By default, ``read_sql`` launches multiple read tasks, and each task executes a
        ``LIMIT`` and ``OFFSET`` to fetch a subset of the rows. However, for many
        databases, ``OFFSET`` is slow.

        As a workaround, set ``parallelism=1`` to directly fetch all rows in a single
        task. Note that this approach requires all result rows to fit in the memory of
        single task. If the rows don't fit, your program may raise an out of memory
        error.

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

    Args:
        sql: The SQL query to execute.
        connection_factory: A function that takes no arguments and returns a
            Python DB API2
            `Connection object <https://peps.python.org/pep-0249/#connection-objects>`_.
        parallelism: The requested parallelism of the read.
        ray_remote_args: Keyword arguments passed to :func:`ray.remote` in read tasks.

    Returns:
        A :class:`Dataset` containing the queried data.
    """
    datasource = SQLDatasource(connection_factory)
    return read_datasource(
        datasource,
        sql=sql,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
    )


@PublicAPI
def from_dask(df: "dask.DataFrame") -> MaterializedDataset:
    """Create a dataset from a Dask DataFrame.

    Args:
        df: A Dask DataFrame.

    Returns:
        MaterializedDataset holding Arrow records read from the DataFrame.
    """
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
def from_mars(df: "mars.DataFrame") -> MaterializedDataset:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        MaterializedDataset holding Arrow records read from the dataframe.
    """
    import mars.dataframe as md

    ds: Dataset = md.to_ray_dataset(df)
    return ds


@PublicAPI
def from_modin(df: "modin.DataFrame") -> MaterializedDataset:
    """Create a dataset from a Modin dataframe.

    Args:
        df: A Modin dataframe, which must be using the Ray backend.

    Returns:
        MaterializedDataset holding Arrow records read from the dataframe.
    """
    from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

    parts = unwrap_partitions(df, axis=0)
    ds = from_pandas_refs(parts)
    return ds


@PublicAPI
def from_pandas(
    dfs: Union["pandas.DataFrame", List["pandas.DataFrame"]]
) -> MaterializedDataset:
    """Create a dataset from a list of Pandas dataframes.

    Args:
        dfs: A Pandas dataframe or a list of Pandas dataframes.

    Returns:
        MaterializedDataset holding Arrow records read from the dataframes.
    """
    import pandas as pd

    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

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
    """Create a dataset from a list of Ray object references to Pandas
    dataframes.

    Args:
        dfs: A Ray object references to pandas dataframe, or a list of
             Ray object references to pandas dataframes.

    Returns:
        MaterializedDataset holding Arrow records read from the dataframes.
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
        logical_plan = LogicalPlan(FromPandas(dfs, metadata))
        return MaterializedDataset(
            ExecutionPlan(
                BlockList(dfs, metadata, owned_by_consumer=False),
                DatasetStats(stages={"FromPandas": metadata}, parent=None),
                run_by_consumer=False,
            ),
            0,
            True,
            logical_plan,
        )

    df_to_block = cached_remote_fn(pandas_df_to_arrow_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = map(list, zip(*res))
    metadata = ray.get(metadata)
    logical_plan = LogicalPlan(FromPandas(blocks, metadata))
    return MaterializedDataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"FromPandas": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        True,
        logical_plan,
    )


@PublicAPI
def from_numpy(ndarrays: Union[np.ndarray, List[np.ndarray]]) -> MaterializedDataset:
    """Create a dataset from a list of NumPy ndarrays.

    Args:
        ndarrays: A NumPy ndarray or a list of NumPy ndarrays.

    Returns:
        MaterializedDataset holding the given ndarrays.
    """
    if isinstance(ndarrays, np.ndarray):
        ndarrays = [ndarrays]

    return from_numpy_refs([ray.put(ndarray) for ndarray in ndarrays])


@DeveloperAPI
def from_numpy_refs(
    ndarrays: Union[ObjectRef[np.ndarray], List[ObjectRef[np.ndarray]]],
) -> MaterializedDataset:
    """Create a dataset from a list of NumPy ndarray futures.

    Args:
        ndarrays: A Ray object reference to a NumPy ndarray or a list of Ray object
            references to NumPy ndarrays.

    Returns:
        MaterializedDataset holding the given ndarrays.
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

    logical_plan = LogicalPlan(FromNumpy(blocks, metadata))

    return MaterializedDataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"FromNumpy": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        True,
        logical_plan,
    )


@PublicAPI
def from_arrow(
    tables: Union["pyarrow.Table", bytes, List[Union["pyarrow.Table", bytes]]],
) -> MaterializedDataset:
    """Create a dataset from a list of Arrow tables.

    Args:
        tables: An Arrow table, or a list of Arrow tables,
                or its streaming format in bytes.

    Returns:
        MaterializedDataset holding Arrow records from the tables.
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
    """Create a dataset from a set of Arrow tables.

    Args:
        tables: A Ray object reference to Arrow table, or list of Ray object
                references to Arrow tables, or its streaming format in bytes.

    Returns:
        MaterializedDataset holding Arrow records from the tables.
    """
    if isinstance(tables, ray.ObjectRef):
        tables = [tables]

    get_metadata = cached_remote_fn(get_table_block_metadata)
    metadata = ray.get([get_metadata.remote(t) for t in tables])
    logical_plan = LogicalPlan(FromArrow(tables, metadata))

    return MaterializedDataset(
        ExecutionPlan(
            BlockList(tables, metadata, owned_by_consumer=False),
            DatasetStats(stages={"FromArrow": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        True,
        logical_plan,
    )


@PublicAPI
def from_spark(
    df: "pyspark.sql.DataFrame", *, parallelism: Optional[int] = None
) -> MaterializedDataset:
    """Create a dataset from a Spark dataframe.

    Args:
        spark: A SparkSession, which must be created by RayDP (Spark-on-Ray).
        df: A Spark dataframe, which must be created by RayDP (Spark-on-Ray).
            parallelism: The amount of parallelism to use for the dataset.
            If not provided, it will be equal to the number of partitions of
            the original Spark dataframe.

    Returns:
        MaterializedDataset holding Arrow records read from the dataframe.
    """
    import raydp

    return raydp.spark.spark_dataframe_to_ray_dataset(df, parallelism)


@PublicAPI
def from_huggingface(
    dataset: Union["datasets.Dataset", "datasets.DatasetDict"],
) -> Union[MaterializedDataset, Dict[str, MaterializedDataset]]:
    """Create a dataset from a Hugging Face Datasets Dataset.

    This function is not parallelized, and is intended to be used
    with Hugging Face Datasets that are loaded into memory (as opposed
    to memory-mapped).

    Example:

    .. doctest::

        >>> import ray
        >>> import datasets
        >>> hf_dataset = datasets.load_dataset("tweet_eval", "emotion")
        Downloading ...
        >>> ray_ds = ray.data.from_huggingface(hf_dataset)
        >>> ray_ds
        {'train': MaterializedDataset(
           num_blocks=...,
           num_rows=3257,
           schema={text: string, label: int64}
        ), 'test': MaterializedDataset(
           num_blocks=...,
           num_rows=1421,
           schema={text: string, label: int64}
        ), 'validation': MaterializedDataset(
           num_blocks=...,
           num_rows=374,
           schema={text: string, label: int64}
        )}
        >>> ray_ds = ray.data.from_huggingface(hf_dataset["train"])
        >>> ray_ds
        MaterializedDataset(
           num_blocks=...,
           num_rows=3257,
           schema={text: string, label: int64}
        )

    Args:
        dataset: A Hugging Face Dataset, or DatasetDict. IterableDataset is not
            supported. ``IterableDataset`` is not supported.

    Returns:
        Dataset holding Arrow records from the Hugging Face Dataset, or a dict of
            datasets in case dataset is a DatasetDict.
    """
    import datasets

    def convert(ds: "datasets.Dataset") -> Dataset:
        # To get the resulting Arrow table from a Hugging Face Dataset after
        # applying transformations (e.g. train_test_split(), shard(), select()),
        # we create a copy of the Arrow table, which applies the indices
        # mapping from the transformations.
        hf_ds_arrow = ds.with_format("arrow")
        ray_ds = from_arrow(hf_ds_arrow[:])
        return ray_ds

    if isinstance(dataset, datasets.DatasetDict):
        available_keys = list(dataset.keys())
        logger.warning(
            "You provided a Huggingface DatasetDict which contains multiple "
            "datasets. The output of `from_huggingface` is a dictionary of Ray "
            "Datasets. To convert just a single Huggingface Dataset to a "
            "Ray Dataset, specify a split. For example, "
            "`ray.data.from_huggingface(my_dataset_dictionary"
            f"['{available_keys[0]}'])`. "
            f"Available splits are {available_keys}."
        )
        return {k: convert(ds) for k, ds in dataset.items()}
    elif isinstance(dataset, datasets.Dataset):
        return convert(dataset)
    else:
        raise TypeError(
            "`dataset` must be a `datasets.Dataset` or `datasets.DatasetDict`."
            f"got {type(dataset)}"
        )


@PublicAPI
def from_tf(
    dataset: "tf.data.Dataset",
) -> MaterializedDataset:
    """Create a dataset from a TensorFlow dataset.

    This function is inefficient. Use it to read small datasets or prototype.

    .. warning::
        If your dataset is large, this function may execute slowly or raise an
        out-of-memory error. To avoid issues, read the underyling data with a function
        like :meth:`~ray.data.read_images`.

    .. note::
        This function isn't paralellized. It loads the entire dataset into the local
        node's memory before moving the data to the distributed object store.

    Examples:
        >>> import ray
        >>> import tensorflow_datasets as tfds
        >>> dataset, _ = tfds.load('cifar10', split=["train", "test"])  # doctest: +SKIP
        >>> ds = ray.data.from_tf(dataset)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=50000, schema={id: binary, image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8), label: int64})
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
        dataset: A TensorFlow dataset.

    Returns:
        A :class:`MaterializedDataset` that contains the samples stored in the
        TensorFlow dataset.
    """  # noqa: E501
    # FIXME: `as_numpy_iterator` errors if `dataset` contains ragged tensors.
    return from_items(list(dataset.as_numpy_iterator()))


@PublicAPI
def from_torch(
    dataset: "torch.utils.data.Dataset",
) -> MaterializedDataset:
    """Create a dataset from a Torch dataset.

    This function is inefficient. Use it to read small datasets or prototype.

    .. warning::
        If your dataset is large, this function may execute slowly or raise an
        out-of-memory error. To avoid issues, read the underyling data with a function
        like :meth:`~ray.data.read_images`.

    .. note::
        This function isn't paralellized. It loads the entire dataset into the head
        node's memory before moving the data to the distributed object store.

    Examples:
        >>> import ray
        >>> from torchvision import datasets
        >>> dataset = datasets.MNIST("data", download=True)  # doctest: +SKIP
        >>> ds = ray.data.from_torch(dataset)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=..., num_rows=60000, schema={item: object})
        >>> ds.take(1)  # doctest: +SKIP
        {"item": (<PIL.Image.Image image mode=L size=28x28 at 0x...>, 5)}

    Args:
        dataset: A Torch dataset.

    Returns:
        A :class:`MaterializedDataset` containing the Torch dataset samples.
    """
    return from_items(list(dataset))


def _get_read_tasks(
    ds: Datasource,
    ctx: DataContext,
    cur_pg: Optional[PlacementGroup],
    parallelism: int,
    local_uri: bool,
    kwargs: dict,
) -> Tuple[int, int, Optional[int], List[ReadTask]]:
    """Generates read tasks.

    Args:
        ds: Datasource to read from.
        ctx: Dataset config to use.
        cur_pg: The current placement group, if any.
        parallelism: The user-requested parallelism, or -1 for autodetection.
        kwargs: Additional kwargs to pass to the reader.

    Returns:
        Request parallelism from the datasource, the min safe parallelism to avoid
        OOM, the estimated inmemory data size, and list of read tasks generated.
    """
    kwargs = _unwrap_arrow_serialization_workaround(kwargs)
    if local_uri:
        kwargs["local_uri"] = local_uri
    DataContext._set_current(ctx)
    reader = ds.create_reader(**kwargs)
    requested_parallelism, min_safe_parallelism, mem_size = _autodetect_parallelism(
        parallelism, cur_pg, DataContext.get_current(), reader
    )
    return (
        requested_parallelism,
        min_safe_parallelism,
        mem_size,
        reader.get_read_tasks(requested_parallelism),
    )


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
                    ArrowTensorArray.from_numpy(np_col),
                )
            if existing_block_udf is not None:
                # Apply UDF after casting the tensor columns.
                block = existing_block_udf(block)
            return block

        arrow_parquet_args["_block_udf"] = _block_udf
    return arrow_parquet_args
