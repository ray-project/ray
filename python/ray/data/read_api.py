import logging
from typing import List, Any, Dict, Union, Optional, Tuple, Callable, \
    TypeVar, TYPE_CHECKING

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
from ray.util.annotations import PublicAPI
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.dataset import Dataset
from ray.data.datasource import Datasource, RangeDatasource, \
    JSONDatasource, CSVDatasource, ParquetDatasource, BinaryDatasource, \
    NumpyDatasource, ReadTask
from ray.data.impl.arrow_block import ArrowRow, \
    DelegatingArrowBlockBuilder
from ray.data.impl.block_list import BlockList
from ray.data.impl.lazy_block_list import LazyBlockList
from ray.data.impl.remote_fn import cached_remote_fn

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

    Returns:
        Dataset holding the items.
    """
    block_size = max(1, len(items) // parallelism)

    blocks: List[ObjectRef[Block]] = []
    metadata: List[BlockMetadata] = []
    i = 0
    while i < len(items):
        builder = DelegatingArrowBlockBuilder()
        for item in items[i:i + block_size]:
            builder.add(item)
        block = builder.build()
        blocks.append(ray.put(block))
        metadata.append(
            BlockAccessor.for_block(block).get_metadata(input_files=None))
        i += block_size

    return Dataset(BlockList(blocks, metadata))


@PublicAPI(stability="beta")
def range(n: int, *, parallelism: int = 200) -> Dataset[int]:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> ray.data.range(10000).map(lambda x: x * 2).show()

    Args:
        n: The upper bound of the range of integers.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding the integers.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, block_format="list")


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

    Returns:
        Dataset holding the integers as Arrow records.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, block_format="arrow")


@PublicAPI(stability="beta")
def range_tensor(n: int, *, shape: Tuple = (1, ),
                 parallelism: int = 200) -> Dataset[np.ndarray]:
    """Create a Tensor dataset from a range of integers [0..n).

    Examples:
        >>> ds = ray.data.range_tensor(1000, shape=(3, 10))
        >>> ds.map_batches(lambda arr: arr ** 2).show()

    This is similar to range(), but uses np.ndarrays to hold the integers
    in tensor form. The dataset has overall the shape ``(n,) + shape``.

    Args:
        n: The upper bound of the range of integer records.
        shape: The shape of each record.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding the integers as tensors.
    """
    return read_datasource(
        RangeDatasource(),
        parallelism=parallelism,
        n=n,
        block_format="tensor",
        tensor_shape=tuple(shape))


@PublicAPI(stability="beta")
def read_datasource(datasource: Datasource[T],
                    *,
                    parallelism: int = 200,
                    ray_remote_args: Dict[str, Any] = None,
                    **read_args) -> Dataset[T]:
    """Read a dataset from a custom data source.

    Args:
        datasource: The datasource to read data from.
        parallelism: The requested parallelism of the read.
        read_args: Additional kwargs to pass to the datasource impl.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.

    Returns:
        Dataset holding the data read from the datasource.
    """

    read_tasks = datasource.prepare_read(parallelism, **read_args)

    def remote_read(task: ReadTask) -> Block:
        return task()

    if ray_remote_args:
        remote_read = ray.remote(**ray_remote_args)(remote_read)
    else:
        remote_read = ray.remote(remote_read)

    calls: List[Callable[[], ObjectRef[Block]]] = []
    metadata: List[BlockMetadata] = []

    for task in read_tasks:
        calls.append(lambda task=task: remote_read.remote(task))
        metadata.append(task.get_metadata())

    block_list = LazyBlockList(calls, metadata)

    # Get the schema from the first block synchronously.
    if metadata and metadata[0].schema is None:
        get_schema = cached_remote_fn(_get_schema)
        schema0 = ray.get(get_schema.remote(next(iter(block_list))))
        block_list.set_metadata(
            0,
            BlockMetadata(
                num_rows=metadata[0].num_rows,
                size_bytes=metadata[0].size_bytes,
                schema=schema0,
                input_files=metadata[0].input_files,
            ))

    return Dataset(block_list)


@PublicAPI(stability="beta")
def read_parquet(paths: Union[str, List[str]],
                 *,
                 filesystem: Optional["pyarrow.fs.FileSystem"] = None,
                 columns: Optional[List[str]] = None,
                 parallelism: int = 200,
                 ray_remote_args: Dict[str, Any] = None,
                 **arrow_parquet_args) -> Dataset[ArrowRow]:
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
        parallelism: The amount of parallelism to use for the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
        arrow_parquet_args: Other parquet read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    return read_datasource(
        ParquetDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        columns=columns,
        ray_remote_args=ray_remote_args,
        **arrow_parquet_args)


@PublicAPI(stability="beta")
def read_json(paths: Union[str, List[str]],
              *,
              filesystem: Optional["pyarrow.fs.FileSystem"] = None,
              parallelism: int = 200,
              ray_remote_args: Dict[str, Any] = None,
              **arrow_json_args) -> Dataset[ArrowRow]:
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
        parallelism: The amount of parallelism to use for the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
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
        **arrow_json_args)


@PublicAPI(stability="beta")
def read_csv(paths: Union[str, List[str]],
             *,
             filesystem: Optional["pyarrow.fs.FileSystem"] = None,
             parallelism: int = 200,
             ray_remote_args: Dict[str, Any] = None,
             **arrow_csv_args) -> Dataset[ArrowRow]:
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
        parallelism: The amount of parallelism to use for the dataset.
        ray_remote_args: kwargs passed to ray.remote in the read tasks.
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
        **arrow_csv_args)


@PublicAPI(stability="beta")
def read_text(
        paths: Union[str, List[str]],
        *,
        encoding: str = "utf-8",
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        parallelism: int = 200,
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
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding lines of text read from the specified paths.
    """

    return read_binary_files(
        paths, filesystem=filesystem, parallelism=parallelism).flat_map(
            lambda x: x.decode(encoding).split("\n"))


@PublicAPI(stability="beta")
def read_numpy(paths: Union[str, List[str]],
               *,
               filesystem: Optional["pyarrow.fs.FileSystem"] = None,
               parallelism: int = 200,
               **numpy_load_args) -> Dataset[np.ndarray]:
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
        parallelism: The amount of parallelism to use for the dataset.
        numpy_load_args: Other options to pass to np.load.

    Returns:
        Dataset holding Tensor records read from the specified paths.
    """
    return read_datasource(
        NumpyDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        **numpy_load_args)


@PublicAPI(stability="beta")
def read_binary_files(
        paths: Union[str, List[str]],
        *,
        include_paths: bool = False,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        parallelism: int = 200,
        ray_remote_args: Dict[str, Any] = None,
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
        parallelism: The amount of parallelism to use for the dataset.

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
        schema=bytes)


@PublicAPI(stability="beta")
def from_dask(df: "dask.DataFrame", *,
              parallelism: int = 200) -> Dataset[ArrowRow]:
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
    return from_pandas(
        [next(iter(part.dask.values())) for part in persisted_partitions])


@PublicAPI(stability="beta")
def from_mars(df: "mars.DataFrame", *,
              parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@PublicAPI(stability="beta")
def from_modin(df: "modin.DataFrame", *,
               parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Modin dataframe.

    Args:
        df: A Modin dataframe, which must be using the Ray backend.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@PublicAPI(stability="beta")
def from_pandas(dfs: List[ObjectRef["pandas.DataFrame"]],
                *,
                parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a set of Pandas dataframes.

    Args:
        dfs: A list of Ray object references to pandas dataframes.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframes.
    """
    df_to_block = cached_remote_fn(_df_to_block, num_returns=2)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = zip(*res)
    return Dataset(BlockList(blocks, ray.get(list(metadata))))


@PublicAPI(stability="beta")
def from_arrow(tables: List[ObjectRef["pyarrow.Table"]],
               *,
               parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a set of Arrow tables.

    Args:
        dfs: A list of Ray object references to Arrow tables.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records from the tables.
    """

    get_metadata = cached_remote_fn(_get_metadata)
    metadata = [get_metadata.remote(t) for t in tables]
    return Dataset(BlockList(tables, ray.get(metadata)))


@PublicAPI(stability="beta")
def from_spark(df: "pyspark.sql.DataFrame", *,
               parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Spark dataframe.

    Args:
        df: A Spark dataframe, which must be created by RayDP (Spark-on-Ray).
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P2


def _df_to_block(df: "pandas.DataFrame") -> Block[ArrowRow]:
    import pyarrow as pa
    block = pa.table(df)
    return (block,
            BlockAccessor.for_block(block).get_metadata(input_files=None))


def _get_schema(block: Block) -> Any:
    return BlockAccessor.for_block(block).schema()


def _get_metadata(table: "pyarrow.Table") -> BlockMetadata:
    return BlockAccessor.for_block(table).get_metadata(input_files=None)
