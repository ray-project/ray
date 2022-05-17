import os
import logging
from typing import (
    List,
    Any,
    Dict,
    Union,
    Optional,
    Tuple,
    TypeVar,
    TYPE_CHECKING,
)

import numpy as np

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import dask
    import mars
    import modin
    import pyspark
    import datasets

import ray
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockExecStats,
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
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
    ParquetMetadataProvider,
    DefaultParquetMetadataProvider,
    PathPartitionFilter,
    ParquetBaseDatasource,
)
from ray.data.datasource.file_based_datasource import (
    _wrap_arrow_serialization_workaround,
    _unwrap_arrow_serialization_workaround,
)
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder
from ray.data.impl.arrow_block import ArrowRow
from ray.data.impl.block_list import BlockList
from ray.data.impl.lazy_block_list import LazyBlockList
from ray.data.impl.plan import ExecutionPlan
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.stats import DatasetStats
from ray.data.impl.util import _lazy_import_pyarrow_dataset

T = TypeVar("T")

logger = logging.getLogger(__name__)


@PublicAPI
def from_items(items: List[Any], *, parallelism: int = 200) -> Dataset[Any]:
    """Create a dataset from a list of local Python objects.

    Examples:
        >>> import ray
        >>> ray.data.from_items([1, 2, 3, 4, 5]) # doctest: +SKIP

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
        ExecutionPlan(
            BlockList(blocks, metadata),
            DatasetStats(stages={"from_items": metadata}, parent=None),
        ),
        0,
        False,
    )


@PublicAPI
def range(n: int, *, parallelism: int = 200) -> Dataset[int]:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ray.data.range(10000).map(lambda x: x * 2).show() # doctest: +SKIP

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
def range_table(n: int, *, parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a tabular dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range_table(1000) # doctest: +SKIP
        >>> ds.map(lambda r: {"v2": r["value"] * 2}).show() # doctest: +SKIP

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


def range_arrow(*args, **kwargs):
    raise DeprecationWarning("range_arrow() is deprecated, use range_table() instead.")


@PublicAPI
def range_tensor(
    n: int, *, shape: Tuple = (1,), parallelism: int = 200
) -> Dataset[ArrowRow]:
    """Create a Tensor dataset from a range of integers [0..n).

    Examples:
        >>> import ray
        >>> ds = ray.data.range_tensor(1000, shape=(3, 10)) # doctest: +SKIP
        >>> ds.map_batches( # doctest: +SKIP
        ...     lambda arr: arr * 2, batch_format="pandas").show()

    This is similar to range_table(), but uses the ArrowTensorArray extension
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


@PublicAPI
def read_datasource(
    datasource: Datasource[T],
    *,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
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
    force_local = "RAY_DATASET_FORCE_LOCAL_METADATA" in os.environ
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
                datasource,
                ctx,
                parallelism,
                _wrap_arrow_serialization_workaround(read_args),
            )
        )

    if len(read_tasks) < parallelism and (
        len(read_tasks) < ray.available_resources().get("CPU", parallelism) // 2
    ):
        logger.warning(
            "The number of blocks in this dataset ({}) limits its parallelism to {} "
            "concurrent tasks. This is much less than the number of available "
            "CPU slots in the cluster. Use `.repartition(n)` to increase the number of "
            "dataset blocks.".format(len(read_tasks), len(read_tasks))
        )

    if ray_remote_args is None:
        ray_remote_args = {}
    if "scheduling_strategy" not in ray_remote_args:
        ray_remote_args["scheduling_strategy"] = "SPREAD"

    block_list = LazyBlockList(read_tasks, ray_remote_args=ray_remote_args)
    block_list.compute_first_block()
    block_list.ensure_metadata_for_first_block()

    return Dataset(
        ExecutionPlan(block_list, block_list.stats()),
        0,
        False,
    )


@PublicAPI
def read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = 200,
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

    Args:
        paths: A single file path or a list of file paths (or directories).
        filesystem: The filesystem implementation to read from.
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
        arrow_parquet_args: Other parquet read options to pass to pyarrow.

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


@PublicAPI
def read_parquet_bulk(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    meta_provider: BaseFileMetadataProvider = FastFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
    **arrow_parquet_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from a large number (e.g. >1K) of parquet files quickly.

    By default, ONLY file paths should be provided as input (i.e. no directory paths),
    and an OSError will be raised if one or more paths point to directories. If your
    use-case requires directory paths, then the metadata provider should be changed to
    one that supports directory expansion (e.g. DefaultFileMetadataProvider).

    Offers improved performance vs. `read_parquet` due to not using PyArrow's
    `ParquetDataset` abstraction, whose latency scales linearly with the number of
    input files due to collecting all file metadata on a single node.

    Also supports a wider variety of input Parquet file types than `read_parquet` due
    to not trying to merge and resolve a unified schema for all files.

    However, unlike `read_parquet`, this does not offer file metadata resolution by
    default, so a custom metadata provider should be provided if your use-case requires
    a unified dataset schema, block sizes, row counts, etc.

    Examples:
        >>> # Read multiple local files. You should always provide only input file
        >>> # paths (i.e. no directory paths) when known to minimize read latency.
        >>> ray.data.read_parquet_bulk(["/path/to/file1", "/path/to/file2"])

        >>> # Read a directory of files in remote storage. Caution should be taken
        >>> # when providing directory paths, since the time to both check each path
        >>> # type and expand its contents may result in greatly increased latency
        >>> # and/or request rate throttling from cloud storage service providers.
        >>> ray.data.read_parquet_bulk(
        >>>     "s3://bucket/path",
        >>>     meta_provider=DefaultFileMetadataProvider(),
        >>> )

    Args:
        paths: A single file path or a list of file paths. If one or more directories
            are provided, then `meta_provider` should also be set to an implementation
            that supports directory expansion (e.g. DefaultFileMetadataProvider).
        filesystem: The filesystem implementation to read from.
        columns: A list of column names to read.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_open_file_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_file
        tensor_column_schema: A dict of column name --> tensor dtype and shape
            mappings for converting a Parquet column containing serialized
            tensors (ndarrays) as their elements to our tensor column extension
            type. This assumes that the tensors were serialized in the raw
            NumPy array format in C-contiguous order (e.g. via
            `arr.tobytes()`).
        meta_provider: File metadata provider. Defaults to a fast file metadata
            provider that skips file size collection and requires all input paths to be
            files. Change to DefaultFileMetadataProvider or a custom metadata provider
            if directory expansion and/or file metadata resolution is required.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.
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
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
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
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        **arrow_json_args,
    )


@PublicAPI
def read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
    **arrow_csv_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from csv files.

    Examples:
        >>> import ray
        >>> # Read a directory of files in remote storage.
        >>> ray.data.read_csv("s3://bucket/path") # doctest: +SKIP

        >>> # Read multiple local files.
        >>> ray.data.read_csv(["/path/to/file1", "/path/to/file2"]) # doctest: +SKIP

        >>> # Read multiple directories.
        >>> ray.data.read_csv( # doctest: +SKIP
        ...     ["s3://bucket/path1", "s3://bucket/path2"])

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
        meta_provider=meta_provider,
        partition_filter=partition_filter,
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
    parallelism: int = 200,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
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
        arrow_open_stream_args: kwargs passed to
            pyarrow.fs.FileSystem.open_input_stream
        meta_provider: File metadata provider. Custom metadata providers may
            be able to resolve file metadata more quickly and/or accurately.
        partition_filter: Path-based partition filter, if any. Can be used
            with a custom callback to read only selected partitions of a dataset.

    Returns:
        Dataset holding lines of text read from the specified paths.
    """

    def to_text(s):
        lines = s.decode(encoding).split("\n")
        if drop_empty_lines:
            lines = [line for line in lines if line.strip() != ""]
        return lines

    return read_binary_files(
        paths,
        filesystem=filesystem,
        parallelism=parallelism,
        arrow_open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
    ).flat_map(to_text)


@PublicAPI
def read_numpy(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
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
        **numpy_load_args,
    )


@PublicAPI
def read_binary_files(
    paths: Union[str, List[str]],
    *,
    include_paths: bool = False,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
    partition_filter: PathPartitionFilter = None,
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
        meta_provider=meta_provider,
        partition_filter=partition_filter,
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
        metadata = [get_metadata.remote(df) for df in dfs]
        return Dataset(
            ExecutionPlan(BlockList(dfs, ray.get(metadata)), DatasetStats.TODO()),
            0,
            False,
        )

    df_to_block = cached_remote_fn(_df_to_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = zip(*res)
    return Dataset(
        ExecutionPlan(
            BlockList(blocks, ray.get(list(metadata))),
            DatasetStats(stages={"from_pandas_refs": metadata}, parent=None),
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
    blocks, metadata = zip(*res)
    return Dataset(
        ExecutionPlan(
            BlockList(blocks, ray.get(list(metadata))),
            DatasetStats(stages={"from_numpy": metadata}, parent=None),
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
    metadata = [get_metadata.remote(t) for t in tables]
    return Dataset(
        ExecutionPlan(
            BlockList(tables, ray.get(metadata)),
            DatasetStats(stages={"from_arrow_refs": metadata}, parent=None),
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
    kwargs = _unwrap_arrow_serialization_workaround(kwargs)
    DatasetContext._set_current(ctx)
    return ds.prepare_read(parallelism, **kwargs)


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
    return arrow_parquet_args
