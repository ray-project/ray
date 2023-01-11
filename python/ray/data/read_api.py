import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypeVar, Union

import numpy as np

import ray
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.pandas_block import PandasRow
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import (
    _lazy_import_pyarrow_dataset,
    _autodetect_parallelism,
    _is_local_scheme,
)
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import DEFAULT_SCHEDULING_STRATEGY, WARN_PREFIX, DatasetContext
from ray.data.dataset import Dataset
from ray.data.datasource import (
    BaseFileMetadataProvider,
    BinaryDatasource,
    CSVDatasource,
    Datasource,
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
    ImageDatasource,
    JSONDatasource,
    NumpyDatasource,
    ParquetBaseDatasource,
    ParquetDatasource,
    ParquetMetadataProvider,
    PathPartitionFilter,
    RangeDatasource,
    MongoDatasource,
    ReadTask,
    TextDatasource,
    TFRecordDatasource,
)
from ray.data.datasource.file_based_datasource import (
    _unwrap_arrow_serialization_workaround,
    _wrap_arrow_serialization_workaround,
)
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
    import pyspark
    import pymongoarrow.api
    import tensorflow as tf
    import torch


T = TypeVar("T")

logger = logging.getLogger(__name__)


@PublicAPI
def from_items(items: List[Any], *, parallelism: int = -1) -> Dataset[Any]:
    """Create a dataset from a list of local Python objects.

    Examples:
        >>> import ray
        >>> ds = ray.data.from_items([1, 2, 3, 4, 5]) # doctest: +SKIP
        >>> ds # doctest: +SKIP
        Dataset(num_blocks=5, num_rows=5, schema=<class 'int'>)
        >>> ds.take(2) # doctest: +SKIP
        [1, 2]

    Args:
        items: List of local Python objects.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset holding the items.
    """

    detected_parallelism, _ = _autodetect_parallelism(
        parallelism,
        ray.util.get_current_placement_group(),
        DatasetContext.get_current(),
    )
    block_size = max(
        1,
        len(items) // detected_parallelism,
    )

    blocks: List[ObjectRef[Block]] = []
    metadata: List[BlockMetadata] = []
    i = 0
    while i < len(items):
        stats = BlockExecStats.builder()
        builder = DelegatingBlockBuilder()
        for item in items[i : i + block_size]:
            builder.add(item)
        block = builder.build()
        blocks.append(ray.put(block))
        metadata.append(
            BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
        )
        i += block_size

    return Dataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"from_items": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        False,
    )


@PublicAPI
def range(n: int, *, parallelism: int = -1) -> Dataset[int]:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range(10000) # doctest: +SKIP
        >>> ds # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)
        >>> ds.map(lambda x: x * 2).take(4) # doctest: +SKIP
        [0, 2, 4, 6]

    Args:
        n: The upper bound of the range of integers.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset holding the integers.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, block_format="list"
    )


@PublicAPI
def range_table(n: int, *, parallelism: int = -1) -> Dataset[ArrowRow]:
    """Create a tabular dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range_table(1000) # doctest: +SKIP
        >>> ds # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=1000, schema={value: int64})
        >>> ds.map(lambda r: {"v2": r["value"] * 2}).take(2) # doctest: +SKIP
        [ArrowRow({'v2': 0}), ArrowRow({'v2': 2})]

    This is similar to range(), but uses Arrow tables to hold the integers
    in Arrow records. The dataset elements take the form {"value": N}.

    Args:
        n: The upper bound of the range of integer records.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset holding the integers as Arrow records.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, block_format="arrow"
    )


@Deprecated
def range_arrow(*args, **kwargs):
    raise DeprecationWarning("range_arrow() is deprecated, use range_table() instead.")


@PublicAPI
def range_tensor(
    n: int, *, shape: Tuple = (1,), parallelism: int = -1
) -> Dataset[ArrowRow]:
    """Create a Tensor dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range_tensor(1000, shape=(2, 2)) # doctest: +SKIP
        >>> ds # doctest: +SKIP
        Dataset(
            num_blocks=200,
            num_rows=1000,
            schema={__value__: <ArrowTensorType: shape=(2, 2), dtype=int64>},
        )
        >>> ds.map_batches(lambda arr: arr * 2).take(2) # doctest: +SKIP
        [array([[0, 0],
                [0, 0]]),
        array([[2, 2],
                [2, 2]])]

    This is similar to range_table(), but uses the ArrowTensorArray extension
    type. The dataset elements take the form {VALUE_COL_NAME: array(N, shape=shape)}.

    Args:
        n: The upper bound of the range of integer records.
        shape: The shape of each record.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset holding the integers as Arrow tensor records.
    """
    return read_datasource(
        RangeDatasource(),
        parallelism=parallelism,
        n=n,
        block_format="tensor",
        tensor_shape=tuple(shape),
    )


@PublicAPI
def read_datasource(
    datasource: Datasource[T],
    *,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    **read_args,
) -> Dataset[T]:
    """Read a dataset from a custom data source.

    Args:
        datasource: The datasource to read data from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the available partitioning of the datasource. If set to -1,
            parallelism will be automatically chosen based on the available cluster
            resources and estimated in-memory data size.
        read_args: Additional kwargs to pass to the datasource impl.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.

    Returns:
        Dataset holding the data read from the datasource.
    """
    ctx = DatasetContext.get_current()

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

    if (
        "scheduling_strategy" not in ray_remote_args
        and ctx.scheduling_strategy == DEFAULT_SCHEDULING_STRATEGY
    ):
        ray_remote_args["scheduling_strategy"] = "SPREAD"

    # TODO(ekl) remove this feature flag.
    force_local = "RAY_DATASET_FORCE_LOCAL_METADATA" in os.environ
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
        requested_parallelism, min_safe_parallelism, read_tasks = _get_read_tasks(
            datasource, ctx, cur_pg, parallelism, local_uri, read_args
        )
    else:
        # Prepare read in a remote task so that in Ray client mode, we aren't
        # attempting metadata resolution from the client machine.
        get_read_tasks = cached_remote_fn(
            _get_read_tasks, retry_exceptions=False, num_cpus=0
        )

        requested_parallelism, min_safe_parallelism, read_tasks = ray.get(
            get_read_tasks.remote(
                datasource,
                ctx,
                cur_pg,
                parallelism,
                local_uri,
                _wrap_arrow_serialization_workaround(read_args),
            )
        )

    if read_tasks and len(read_tasks) < min_safe_parallelism * 0.7:
        perc = 1 + round((min_safe_parallelism - len(read_tasks)) / len(read_tasks), 1)
        logger.warning(
            f"{WARN_PREFIX} The blocks of this dataset are estimated to be {perc}x "
            "larger than the target block size "
            f"of {int(ctx.target_max_block_size / 1024 / 1024)} MiB. This may lead to "
            "out-of-memory errors during processing. Consider reducing the size of "
            "input files or using `.repartition(n)` to increase the number of "
            "dataset blocks."
        )
    elif len(read_tasks) < requested_parallelism and (
        len(read_tasks) < ray.available_resources().get("CPU", 1) // 2
    ):
        logger.warning(
            f"{WARN_PREFIX} The number of blocks in this dataset ({len(read_tasks)}) "
            f"limits its parallelism to {len(read_tasks)} concurrent tasks. "
            "This is much less than the number "
            "of available CPU slots in the cluster. Use `.repartition(n)` to "
            "increase the number of "
            "dataset blocks."
        )

    block_list = LazyBlockList(
        read_tasks, ray_remote_args=ray_remote_args, owned_by_consumer=False
    )

    return Dataset(
        plan=ExecutionPlan(block_list, block_list.stats(), run_by_consumer=False),
        epoch=0,
        lazy=True,
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
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from MongoDB.

    The data to read from is specified via the ``uri``, ``database`` and ``collection``
    of the MongoDB. The dataset is created from the results of executing ``pipeline``
    against the ``collection``. If ``pipeline`` is None, the entire ``collection`` will
    be read.

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
        mong_args: kwargs passed to aggregate_arrow_all() in pymongoarrow in producing
            Arrow-formatted results.

    Returns:
        Dataset holding Arrow records from the results of executing the pipeline on the
        specified MongoDB collection.
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
) -> Dataset[ArrowRow]:
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
        >>> ray.data.read_parquet("example://iris.parquet",
        ...     schema=pa.schema(fields))
        Dataset(num_blocks=..., num_rows=150, schema={sepal.length: double, ...})

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
        Dataset holding Arrow records read from the specified paths.
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
    partition_filter: Optional[
        PathPartitionFilter
    ] = ImageDatasource.file_extension_filter(),
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
):
    """Read images from the specified paths.

    Examples:
        >>> import ray
        >>> path = "s3://air-example-data-2/movie-image-small-filesize-1GB"
        >>> ds = ray.data.read_images(path)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=41979, schema={image: ArrowVariableShapedTensorType(dtype=uint8, ndim=3)})

        If you need image file paths, set ``include_paths=True``.

        >>> ds = ray.data.read_images(path, include_paths=True)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=41979, schema={image: ArrowVariableShapedTensorType(dtype=uint8, ndim=3), path: string})
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
        >>> root = "example://tiny-imagenet-200/train"
        >>> partitioning = Partitioning("dir", field_names=["class"], base_dir=root)
        >>> ds = ray.data.read_images(root, size=(224, 224), partitioning=partitioning)  # doctest: +SKIP
        >>> ds  # doctest: +SKIP
        Dataset(num_blocks=176, num_rows=94946, schema={image: TensorDtype(shape=(224, 224, 3), dtype=uint8), class: object})

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
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

    Returns:
        A :class:`~ray.data.Dataset` containing tensors that represent the images at
        the specified paths. For information on working with tensors, read the
        :ref:`tensor data guide <datasets_tensor_support>`.

    Raises:
        ValueError: if ``size`` contains non-positive numbers.
        ValueError: if ``mode`` is unsupported.
    """  # noqa: E501
    return read_datasource(
        ImageDatasource(),
        paths=paths,
        filesystem=filesystem,
        parallelism=parallelism,
        partition_filter=partition_filter,
        partitioning=partitioning,
        size=size,
        mode=mode,
        include_paths=include_paths,
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
) -> Dataset[ArrowRow]:
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
    requires a unified dataset schema, block sizes, row counts, etc.

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
        Dataset holding Arrow records read from the specified paths.
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
    **arrow_json_args,
) -> Dataset[ArrowRow]:
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

        >>> ds = ray.data.read_json("example://year=2022/month=09/sales.json")  # doctest: + SKIP
        >>> ds.take(1)  # doctest: + SKIP
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}

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

    Returns:
        Dataset holding Arrow records read from the specified paths.
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
    **arrow_csv_args,
) -> Dataset[ArrowRow]:
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
        >>> parse_options = csv.ParseOptions(delimiter="\t")
        >>> ray.data.read_csv( # doctest: +SKIP
        ...     "example://iris.tsv",
        ...     parse_options=parse_options)

        >>> # Convert a date column with a custom format from a CSV file.
        >>> # For more uses of ConvertOptions see
        >>> # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html  # noqa: #501
        >>> from pyarrow import csv
        >>> convert_options = csv.ConvertOptions(
        ...     timestamp_parsers=["%m/%d/%Y"])
        >>> ray.data.read_csv( # doctest: +SKIP
        ...     "example://dow_jones_index.csv",
        ...     convert_options=convert_options)

        By default, ``read_csv`` parses
        `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning/>`_
        from file paths. If your data adheres to a different partitioning scheme, set
        the ``partitioning`` parameter.

        >>> ds = ray.data.read_csv("example://year=2022/month=09/sales.csv")  # doctest: + SKIP
        >>> ds.take(1)  # doctest: + SKIP
        [{'order_number': 10107, 'quantity': 30, 'year': '2022', 'month': '09'}

        By default, ``read_csv`` reads all files from file paths. If you want to filter
        files by file extensions, set the ``partition_filter`` parameter.

        >>> # Read only *.csv files from multiple directories.
        >>> from ray.data.datasource import FileExtensionFilter
        >>> ray.data.read_csv( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"],
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

    Returns:
        Dataset holding Arrow records read from the specified paths.
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
) -> Dataset[str]:
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
            limited by the number of files of the dataset.
        ray_remote_args: Kwargs passed to ray.remote in the read tasks and
            in the subsequent text decoding map task.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this does not filter out any files.
            If wishing to filter out all file paths except those whose file extension
            matches e.g. "*.txt*", a ``FileXtensionFilter("txt")`` can be provided.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.

    Returns:
        Dataset holding lines of text read from the specified paths.
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
    **numpy_load_args,
) -> Dataset[ArrowRow]:
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
) -> Dataset[PandasRow]:
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
        >>> ds = ray.data.read_tfrecords(path)
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
            ``pyarrow.fs.FileSystem.open_input_stream``.
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this filters out any file paths whose file extension does not
            match ``"*.tfrecords*"``.

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
) -> Dataset[Union[Tuple[str, bytes], bytes]]:
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
            dataset records. When specified, the dataset records will be a
            tuple of the file path and the file contents.
        filesystem: The filesystem implementation to read from.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
            By default, this does not filter out any files.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
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
    )


@PublicAPI
def from_dask(df: "dask.DataFrame") -> Dataset[ArrowRow]:
    """Create a dataset from a Dask DataFrame.

    Args:
        df: A Dask DataFrame.

    Returns:
        Dataset holding Arrow records read from the DataFrame.
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

    return from_pandas_refs(
        [to_ref(next(iter(part.dask.values()))) for part in persisted_partitions]
    )


@PublicAPI
def from_mars(df: "mars.DataFrame") -> Dataset[ArrowRow]:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    import mars.dataframe as md

    return md.to_ray_dataset(df)


@PublicAPI
def from_modin(df: "modin.DataFrame") -> Dataset[ArrowRow]:
    """Create a dataset from a Modin dataframe.

    Args:
        df: A Modin dataframe, which must be using the Ray backend.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

    parts = unwrap_partitions(df, axis=0)
    return from_pandas_refs(parts)


@PublicAPI
def from_pandas(
    dfs: Union["pandas.DataFrame", List["pandas.DataFrame"]]
) -> Dataset[ArrowRow]:
    """Create a dataset from a list of Pandas dataframes.

    Args:
        dfs: A Pandas dataframe or a list of Pandas dataframes.

    Returns:
        Dataset holding Arrow records read from the dataframes.
    """
    import pandas as pd

    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]
    return from_pandas_refs([ray.put(df) for df in dfs])


@DeveloperAPI
def from_pandas_refs(
    dfs: Union[ObjectRef["pandas.DataFrame"], List[ObjectRef["pandas.DataFrame"]]]
) -> Dataset[ArrowRow]:
    """Create a dataset from a list of Ray object references to Pandas
    dataframes.

    Args:
        dfs: A Ray object references to pandas dataframe, or a list of
             Ray object references to pandas dataframes.

    Returns:
        Dataset holding Arrow records read from the dataframes.
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

    context = DatasetContext.get_current()
    if context.enable_pandas_block:
        get_metadata = cached_remote_fn(_get_metadata)
        metadata = ray.get([get_metadata.remote(df) for df in dfs])
        return Dataset(
            ExecutionPlan(
                BlockList(dfs, metadata, owned_by_consumer=False),
                DatasetStats(stages={"from_pandas_refs": metadata}, parent=None),
                run_by_consumer=False,
            ),
            0,
            False,
        )

    df_to_block = cached_remote_fn(_df_to_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = map(list, zip(*res))
    metadata = ray.get(metadata)
    return Dataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"from_pandas_refs": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        False,
    )


@PublicAPI
def from_numpy(ndarrays: Union[np.ndarray, List[np.ndarray]]) -> Dataset[ArrowRow]:
    """Create a dataset from a list of NumPy ndarrays.

    Args:
        ndarrays: A NumPy ndarray or a list of NumPy ndarrays.

    Returns:
        Dataset holding the given ndarrays.
    """
    if isinstance(ndarrays, np.ndarray):
        ndarrays = [ndarrays]

    return from_numpy_refs([ray.put(ndarray) for ndarray in ndarrays])


@DeveloperAPI
def from_numpy_refs(
    ndarrays: Union[ObjectRef[np.ndarray], List[ObjectRef[np.ndarray]]],
) -> Dataset[ArrowRow]:
    """Create a dataset from a list of NumPy ndarray futures.

    Args:
        ndarrays: A Ray object reference to a NumPy ndarray or a list of Ray object
            references to NumPy ndarrays.

    Returns:
        Dataset holding the given ndarrays.
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

    ndarray_to_block = cached_remote_fn(_ndarray_to_block, num_returns=2)

    res = [ndarray_to_block.remote(ndarray) for ndarray in ndarrays]
    blocks, metadata = map(list, zip(*res))
    metadata = ray.get(metadata)
    return Dataset(
        ExecutionPlan(
            BlockList(blocks, metadata, owned_by_consumer=False),
            DatasetStats(stages={"from_numpy_refs": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        False,
    )


@PublicAPI
def from_arrow(
    tables: Union["pyarrow.Table", bytes, List[Union["pyarrow.Table", bytes]]]
) -> Dataset[ArrowRow]:
    """Create a dataset from a list of Arrow tables.

    Args:
        tables: An Arrow table, or a list of Arrow tables,
                or its streaming format in bytes.

    Returns:
        Dataset holding Arrow records from the tables.
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
    ]
) -> Dataset[ArrowRow]:
    """Create a dataset from a set of Arrow tables.

    Args:
        tables: A Ray object reference to Arrow table, or list of Ray object
                references to Arrow tables, or its streaming format in bytes.

    Returns:
        Dataset holding Arrow records from the tables.
    """
    if isinstance(tables, ray.ObjectRef):
        tables = [tables]

    get_metadata = cached_remote_fn(_get_metadata)
    metadata = ray.get([get_metadata.remote(t) for t in tables])
    return Dataset(
        ExecutionPlan(
            BlockList(tables, metadata, owned_by_consumer=False),
            DatasetStats(stages={"from_arrow_refs": metadata}, parent=None),
            run_by_consumer=False,
        ),
        0,
        False,
    )


@PublicAPI
def from_spark(
    df: "pyspark.sql.DataFrame", *, parallelism: Optional[int] = None
) -> Dataset[ArrowRow]:
    """Create a dataset from a Spark dataframe.

    Args:
        spark: A SparkSession, which must be created by RayDP (Spark-on-Ray).
        df: A Spark dataframe, which must be created by RayDP (Spark-on-Ray).
            parallelism: The amount of parallelism to use for the dataset.
            If not provided, it will be equal to the number of partitions of
            the original Spark dataframe.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    import raydp

    return raydp.spark.spark_dataframe_to_ray_dataset(df, parallelism)


@PublicAPI
def from_huggingface(
    dataset: Union["datasets.Dataset", "datasets.DatasetDict"],
) -> Union[Dataset[ArrowRow], Dict[str, Dataset[ArrowRow]]]:
    """Create a dataset from a Hugging Face Datasets Dataset.

    This function is not parallelized, and is intended to be used
    with Hugging Face Datasets that are loaded into memory (as opposed
    to memory-mapped).

    Args:
        dataset: A Hugging Face ``Dataset``, or ``DatasetDict``.
            ``IterableDataset`` is not supported.

    Returns:
        Dataset holding Arrow records from the Hugging Face Dataset, or a
        dict of datasets in case ``dataset`` is a ``DatasetDict``.
    """
    import datasets

    def convert(ds: "datasets.Dataset") -> Dataset[ArrowRow]:
        return from_arrow(ds.data.table)

    if isinstance(dataset, datasets.DatasetDict):
        return {k: convert(ds) for k, ds in dataset.items()}
    elif isinstance(dataset, datasets.Dataset):
        return convert(dataset)
    else:
        raise TypeError(
            "`dataset` must be a `datasets.Dataset` or `datasets.DatasetDict`, "
            f"got {type(dataset)}"
        )


@PublicAPI
def from_tf(
    dataset: "tf.data.Dataset",
) -> Dataset:
    """Create a dataset from a TensorFlow dataset.

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
        >>> import tensorflow_datasets as tfds
        >>> dataset, _ = tfds.load('cifar10', split=["train", "test"])  # doctest: +SKIP
        >>> dataset = ray.data.from_tf(dataset)  # doctest: +SKIP
        >>> dataset  # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=50000, schema={id: binary, image: ArrowTensorType(shape=(32, 32, 3), dtype=uint8), label: int64})
        >>> dataset.take(1)  # doctest: +SKIP
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
        A :class:`Dataset` that contains the samples stored in the TensorFlow dataset.
    """  # noqa: E501
    # FIXME: `as_numpy_iterator` errors if `dataset` contains ragged tensors.
    return from_items(list(dataset.as_numpy_iterator()))


@PublicAPI
def from_torch(
    dataset: "torch.utils.data.Dataset",
) -> Dataset:
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
        >>> dataset = ray.data.from_torch(dataset)  # doctest: +SKIP
        >>> dataset  # doctest: +SKIP
        Dataset(num_blocks=200, num_rows=60000, schema=<class 'tuple'>)
        >>> dataset.take(1)  # doctest: +SKIP
        [(<PIL.Image.Image image mode=L size=28x28 at 0x...>, 5)]

    Args:
        dataset: A Torch dataset.

    Returns:
        A :class:`Dataset` that contains the samples stored in the Torch dataset.
    """
    return from_items(list(dataset))


def _df_to_block(df: "pandas.DataFrame") -> Block[ArrowRow]:
    stats = BlockExecStats.builder()
    import pyarrow as pa

    block = pa.table(df)
    return (
        block,
        BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        ),
    )


def _ndarray_to_block(ndarray: np.ndarray) -> Block[np.ndarray]:
    stats = BlockExecStats.builder()
    block = BlockAccessor.batch_to_block(ndarray)
    metadata = BlockAccessor.for_block(block).get_metadata(
        input_files=None, exec_stats=stats.build()
    )
    return block, metadata


def _get_metadata(table: Union["pyarrow.Table", "pandas.DataFrame"]) -> BlockMetadata:
    stats = BlockExecStats.builder()
    return BlockAccessor.for_block(table).get_metadata(
        input_files=None, exec_stats=stats.build()
    )


def _get_read_tasks(
    ds: Datasource,
    ctx: DatasetContext,
    cur_pg: Optional[PlacementGroup],
    parallelism: int,
    local_uri: bool,
    kwargs: dict,
) -> Tuple[int, int, List[ReadTask]]:
    """Generates read tasks.

    Args:
        ds: Datasource to read from.
        ctx: Dataset config to use.
        cur_pg: The current placement group, if any.
        parallelism: The user-requested parallelism, or -1 for autodetection.
        kwargs: Additional kwargs to pass to the reader.

    Returns:
        Request parallelism from the datasource, the min safe parallelism to avoid
        OOM, and the list of read tasks generated.
    """
    kwargs = _unwrap_arrow_serialization_workaround(kwargs)
    if local_uri:
        kwargs["local_uri"] = local_uri
    DatasetContext._set_current(ctx)
    reader = ds.create_reader(**kwargs)
    requested_parallelism, min_safe_parallelism = _autodetect_parallelism(
        parallelism, cur_pg, DatasetContext.get_current(), reader
    )
    return (
        requested_parallelism,
        min_safe_parallelism,
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
