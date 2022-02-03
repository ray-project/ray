import itertools
import os
import logging
from typing import (
    List,
    Any,
    Dict,
    Union,
    Optional,
    Tuple,
    Callable,
    TypeVar,
    TYPE_CHECKING,
)
import uuid

import numpy as np

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import dask
    import mars
    import modin
    import pyspark

import ray
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    MaybeBlockPartition,
    BlockExecStats,
    BlockPartitionMetadata,
)
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset
from ray.data.datasource import (
    Datasource,
    RangeDatasource,
    JSONDatasource,
    CSVDatasource,
    ParquetDatasource,
    BinaryDatasource,
    NumpyDatasource,
    ReadTask,
)
from ray.data.datasource.file_based_datasource import (
    _wrap_s3_filesystem_workaround,
    _unwrap_s3_filesystem_workaround,
)
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder
from ray.data.impl.arrow_block import ArrowRow
from ray.data.impl.block_list import BlockList
from ray.data.impl.lazy_block_list import LazyBlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.stats import DatasetStats, get_or_create_stats_actor
from ray.data.impl.util import _get_spread_resources_iter

T = TypeVar("T")

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
def from_items(items: List[Any], *, parallelism: int = 200) -> Dataset[Any]:
    """Create a dataset from a list of local Python objects.

    Examples:
        >>> ray.data.from_items([1, 2, 3, 4, 5])

    Args:
        items: List of local Python objects.
        parallelism: The amount of parallelism to use for the dataset.
            Parallelism may be limited by the number of items.

    Returns:
        Dataset holding the items.
    """
    block_size = max(1, len(items) // parallelism)

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
        BlockList(blocks, metadata),
        0,
        DatasetStats(stages={"from_items": metadata}, parent=None),
    )


@PublicAPI(stability="beta")
def range(n: int, *, parallelism: int = 200) -> Dataset[int]:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> ray.data.range(10000).map(lambda x: x * 2).show()

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


@PublicAPI(stability="beta")
def range_arrow(n: int, *, parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from a range of integers [0..n).

    Examples:
        >>> ds = ray.data.range_arrow(1000)
        >>> ds.map(lambda r: {"v2": r["value"] * 2}).show()

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


@PublicAPI(stability="beta")
def range_tensor(
    n: int, *, shape: Tuple = (1,), parallelism: int = 200
) -> Dataset[ArrowRow]:
    """Create a Tensor dataset from a range of integers [0..n).

    Examples:
        >>> ds = ray.data.range_tensor(1000, shape=(3, 10))
        >>> ds.map_batches(lambda arr: arr * 2, batch_format="pandas").show()

    This is similar to range_arrow(), but uses the ArrowTensorArray extension
    type. The dataset elements take the form {"value": array(N, shape=shape)}.

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


@PublicAPI(stability="beta")
def read_datasource(
    datasource: Datasource[T],
    *,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    _spread_resource_prefix: Optional[str] = None,
    **read_args,
) -> Dataset[T]:
    """Read a dataset from a custom data source.

    Args:
        datasource: The datasource to read data from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the available partitioning of the datasource.
        read_args: Additional kwargs to pass to the datasource impl.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.

    Returns:
        Dataset holding the data read from the datasource.
    """

    # TODO(ekl) remove this feature flag.
    if "RAY_DATASET_FORCE_LOCAL_METADATA" in os.environ:
        read_tasks = datasource.prepare_read(parallelism, **read_args)
    else:
        # Prepare read in a remote task so that in Ray client mode, we aren't
        # attempting metadata resolution from the client machine.
        ctx = DatasetContext.get_current()
        prepare_read = cached_remote_fn(
            _prepare_read, retry_exceptions=False, num_cpus=0
        )
        read_tasks = ray.get(
            prepare_read.remote(
                datasource, ctx, parallelism, _wrap_s3_filesystem_workaround(read_args)
            )
        )

    context = DatasetContext.get_current()
    stats_actor = get_or_create_stats_actor()
    stats_uuid = uuid.uuid4()
    stats_actor.record_start.remote(stats_uuid)

    def remote_read(i: int, task: ReadTask) -> MaybeBlockPartition:
        DatasetContext._set_current(context)
        stats = BlockExecStats.builder()

        # Execute the read task.
        block = task()

        if context.block_splitting_enabled:
            metadata = task.get_metadata()
            metadata.exec_stats = stats.build()
        else:
            metadata = BlockAccessor.for_block(block).get_metadata(
                input_files=task.get_metadata().input_files, exec_stats=stats.build()
            )
        stats_actor.record_task.remote(stats_uuid, i, metadata)
        return block

    if ray_remote_args is None:
        ray_remote_args = {}
    # Increase the read parallelism by default to maximize IO throughput. This
    # is particularly important when reading from e.g., remote storage.
    if "num_cpus" not in ray_remote_args:
        # Note that the too many workers warning triggers at 4x subscription,
        # so we go at 0.5 to avoid the warning message.
        ray_remote_args["num_cpus"] = 0.5
    remote_read = cached_remote_fn(remote_read)

    if _spread_resource_prefix is not None:
        # Use given spread resource prefix for round-robin resource-based
        # scheduling.
        nodes = ray.nodes()
        resource_iter = _get_spread_resources_iter(
            nodes, _spread_resource_prefix, ray_remote_args
        )
    else:
        # If no spread resource prefix given, yield an empty dictionary.
        resource_iter = itertools.repeat({})

    calls: List[Callable[[], ObjectRef[MaybeBlockPartition]]] = []
    metadata: List[BlockPartitionMetadata] = []

    for i, task in enumerate(read_tasks):
        calls.append(
            lambda i=i, task=task, resources=next(resource_iter): remote_read.options(
                **ray_remote_args, resources=resources
            ).remote(i, task)
        )
        metadata.append(task.get_metadata())

    block_list = LazyBlockList(calls, metadata)

    # Get the schema from the first block synchronously.
    if metadata and metadata[0].schema is None:
        block_list.ensure_schema_for_first_block()

    return Dataset(
        block_list,
        0,
        DatasetStats(
            stages={"read": metadata},
            parent=None,
            stats_actor=stats_actor,
            stats_uuid=stats_uuid,
        ),
    )


@PublicAPI(stability="beta")
def read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    _tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    **arrow_parquet_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from parquet files.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_parquet("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_parquet(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        filesystem: The filesystem implementation to read from.
        columns: A list of column names to read.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        _tensor_column_schema: A dict of column name --> tensor dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to our tensor column extension
            type. This assumes that the tensors were serialized in the raw
            NumPy array format in C-contiguous order (e.g. via
            `arr.tobytes()`).
        arrow_parquet_args: Other parquet read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    if _tensor_column_schema is not None:
        existing_block_udf = arrow_parquet_args.pop("_block_udf", None)

        def _block_udf(block: "pyarrow.Table") -> "pyarrow.Table":
            from ray.data.extensions import ArrowTensorArray

            for tensor_col_name, (dtype, shape) in _tensor_column_schema.items():
                # NOTE(Clark): We use NumPy to consolidate these potentially
                # non-contiguous buffers, and to do buffer bookkeeping in
                # general.
                np_col = np.array(
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

    return read_datasource(
        ParquetDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        columns=columns,
        ray_remote_args=ray_remote_args,
        **arrow_parquet_args,
    )


@PublicAPI(stability="beta")
def read_json(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    **arrow_json_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from json files.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_json("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_json(["/path/to/file1", "/path/to/file2"])

        >>> # Read multiple directories.
        >>> ray.data.read_json(["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        arrow_json_args: Other json read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    return read_datasource(
        JSONDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        **arrow_json_args,
    )


@PublicAPI(stability="beta")
def read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    **arrow_csv_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from csv files.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_csv("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_csv(["/path/to/file1", "/path/to/file2"])

        >>> # Read multiple directories.
        >>> ray.data.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        arrow_csv_args: Other csv read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    return read_datasource(
        CSVDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        **arrow_csv_args,
    )


@PublicAPI(stability="beta")
def read_text(
    paths: Union[str, List[str]],
    *,
    encoding: str = "utf-8",
    errors: str = "ignore",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
) -> Dataset[str]:
    """Create a dataset from lines stored in text files.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_text("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_text(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        encoding: The encoding of the files (e.g., "utf-8" or "ascii").
        errors: What to do with errors on decoding. Specify either "strict",
            "ignore", or "replace". Defaults to "ignore".
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream

    Returns:
        Dataset holding lines of text read from the specified paths.
    """

    return read_binary_files(
        paths,
        filesystem=filesystem,
        parallelism=parallelism,
        arrow_open_stream_args=arrow_open_stream_args,
    ).flat_map(lambda x: x.decode(encoding, errors=errors).split("\n"))


@PublicAPI(stability="beta")
def read_numpy(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    **numpy_load_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from csv files.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_numpy("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_numpy(["/path/to/file1", "/path/to/file2"])

        >>> # Read multiple directories.
        >>> ray.data.read_numpy(["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        numpy_load_args: Other options to pass to np.load.

    Returns:
        Dataset holding Tensor records read from the specified paths.
    """
    return read_datasource(
        NumpyDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        **numpy_load_args,
    )


@PublicAPI(stability="beta")
def read_binary_files(
    paths: Union[str, List[str]],
    *,
    include_paths: bool = False,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
) -> Dataset[Union[Tuple[str, bytes], bytes]]:
    """Create a dataset from binary files of arbitrary contents.

    Examples:
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_binary_files("s3://bucket/path")

        >>> # Read multiple local files.
        >>> ray.data.read_binary_files(["/path/to/file1", "/path/to/file2"])

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
        schema=bytes,
    )


@PublicAPI(stability="beta")
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


@PublicAPI(stability="beta")
def from_mars(df: "mars.DataFrame") -> Dataset[ArrowRow]:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@PublicAPI(stability="beta")
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


@PublicAPI(stability="beta")
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
        metadata = [get_metadata.remote(df) for df in dfs]
        return Dataset(BlockList(dfs, ray.get(metadata)), 0, DatasetStats.TODO())

    df_to_block = cached_remote_fn(_df_to_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = zip(*res)
    return Dataset(
        BlockList(blocks, ray.get(list(metadata))),
        0,
        DatasetStats(stages={"from_pandas_refs": metadata}, parent=None),
    )


def from_numpy(ndarrays: List[ObjectRef[np.ndarray]]) -> Dataset[ArrowRow]:
    """Create a dataset from a set of NumPy ndarrays.

    Args:
        ndarrays: A list of Ray object references to NumPy ndarrays.

    Returns:
        Dataset holding the given ndarrays.
    """
    ndarray_to_block = cached_remote_fn(_ndarray_to_block, num_returns=2)

    res = [ndarray_to_block.remote(ndarray) for ndarray in ndarrays]
    blocks, metadata = zip(*res)
    return Dataset(
        BlockList(blocks, ray.get(list(metadata))),
        0,
        DatasetStats(stages={"from_numpy": metadata}, parent=None),
    )


@PublicAPI(stability="beta")
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
    metadata = [get_metadata.remote(t) for t in tables]
    return Dataset(
        BlockList(tables, ray.get(metadata)),
        0,
        DatasetStats(stages={"from_arrow_refs": metadata}, parent=None),
    )


@PublicAPI(stability="beta")
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
    import pyarrow as pa
    from ray.data.extensions import TensorArray

    table = pa.Table.from_pydict({"value": TensorArray(ndarray)})
    return (
        table,
        BlockAccessor.for_block(table).get_metadata(
            input_files=None, exec_stats=stats.build()
        ),
    )


def _get_metadata(table: Union["pyarrow.Table", "pandas.DataFrame"]) -> BlockMetadata:
    stats = BlockExecStats.builder()
    return BlockAccessor.for_block(table).get_metadata(
        input_files=None, exec_stats=stats.build()
    )


def _prepare_read(
    ds: Datasource, ctx: DatasetContext, parallelism: int, kwargs: dict
) -> List[ReadTask]:
    kwargs = _unwrap_s3_filesystem_workaround(kwargs)
    DatasetContext._set_current(ctx)
    return ds.prepare_read(parallelism, **kwargs)
