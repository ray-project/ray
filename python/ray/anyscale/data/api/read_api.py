import functools
import inspect
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union

import numpy as np

import ray
import ray.data.read_api as oss_read_api
from ray._private.auto_init_hook import wrap_auto_init
from ray.anyscale.data._internal.logical.operators.list_files_operator import ListFiles
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.readers import (
    AudioReader,
    AvroReader,
    BinaryReader,
    CSVReader,
    FileReader,
    ImageReader,
    JSONReader,
    NumpyReader,
    ParquetReader,
    TextReader,
    VideoReader,
    WebDatasetReader,
)
from ray.anyscale.data.datasource.snowflake_datasource import SnowflakeDatasource
from ray.data._internal.datasource.image_datasource import ImageDatasource
from ray.data._internal.datasource.json_datasource import JSONDatasource
from ray.data._internal.datasource.numpy_datasource import NumpyDatasource
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import RandomShuffle
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import _is_local_scheme
from ray.data.dataset import Dataset
from ray.data.datasource import Partitioning, PathPartitionFilter
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.data.read_api import _resolve_parquet_args, _validate_shuffle_arg
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pyarrow
    import pyarrow.fs


def _try_fallback_to_oss(runtime_func):
    oss_func = getattr(oss_read_api, runtime_func.__name__, None)
    if oss_func is None:
        return runtime_func

    @functools.wraps(oss_func)
    def wrapped(*args, **kwargs):
        runtime_parameters = set(inspect.signature(runtime_func).parameters)
        oss_parameters = set(inspect.signature(oss_func).parameters)
        # Runtime APIs shouldn't introduce new parameters.
        assert runtime_parameters <= oss_parameters, runtime_parameters - oss_parameters
        # If any user-specified parameter isn't supported on Ray runtime, fall back to
        # the OSS implementation.
        for kwarg in kwargs:
            if kwarg in oss_parameters and kwarg not in runtime_parameters:
                warnings.warn(
                    f"Parameter '{kwarg}' isn't supported on Ray runtime. Falling back "
                    "to OSS implementation."
                )
                return oss_func(*args, **kwargs)

        return runtime_func(*args, **kwargs)

    return wrapped


# TODO(@bveeramani): Add `read_tfrecords`.


@_try_fallback_to_oss
def read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    include_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    **arrow_parquet_args,
) -> Dataset:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from packaging.version import parse as parse_version

    if ray_remote_args is None:
        ray_remote_args = {}

    if _is_local_scheme(paths):
        if "scheduling_strategy" in ray_remote_args:
            warnings.warn(
                "You specified the 'scheduling_strategy' remote argument and "
                "a 'local://' path. To read local files, Ray Data will override the "
                "your specified 'scheduling_strategy'."
            )

        if ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the Ray "
                "cluster can't access your local files. To fix this issue, store "
                "files in cloud storage or a distributed filesystem like NFS."
            )

        ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )

    ctx = ray.data.DataContext.get_current()
    if "scheduling_strategy" not in ray_remote_args:
        ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy

    # The OSS `read_parquet` interface implicitly supports all of the arguments below.
    # It isn't great that we expose implementation details in the interface, but we need
    # to support them on runtime for backward compatibility.
    arrow_parquet_args = _resolve_parquet_args(
        tensor_column_schema,
        **arrow_parquet_args,
    )
    block_udf = arrow_parquet_args.pop("_block_udf", None)
    dataset_kwargs = arrow_parquet_args.pop("dataset_kwargs", {})
    schema = arrow_parquet_args.pop("schema", None)
    batch_size = arrow_parquet_args.pop("batch_size", None)
    use_threads = arrow_parquet_args.pop("use_threads", False)

    filters = arrow_parquet_args.pop("filter", None)
    filter_expr = None
    if filters is not None:
        if parse_version(pa.__version__) < parse_version("10.0.0"):
            # pyarrow < 10 uses a different API for converting filters to expressions
            # TODO: Remove after we drop support for pyarrow < 10.0.0
            filter_expr = pq._filters_to_expression(filters)
        else:
            filter_expr = pq.filters_to_expression(filters)

    to_batches_kwargs = arrow_parquet_args

    if "partitioning" in dataset_kwargs:
        raise ValueError(
            "The 'partitioning' parameter isn't supported in 'dataset_kwargs'. "
            "Use the top-level 'partitioning' parameter instead."
        )

    reader = ParquetReader(
        schema=schema,
        dataset_kwargs=dataset_kwargs,
        batch_size=batch_size,
        use_threads=use_threads,
        to_batches_kwargs=to_batches_kwargs,
        block_udf=block_udf,
        include_paths=include_paths,
        partitioning=partitioning,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=columns,
        partition_filter=partition_filter,
        filter_expr=filter_expr,
        ignore_missing_paths=False,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_audio(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Optional[Partitioning] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
):
    """Creates a :class:`~ray.data.Dataset` from audio files.

    Examples:
        >>> import ray
        >>> path = "s3://anonymous@air-example-data-2/6G-audio-data-LibriSpeech-train-clean-100-flac/train-clean-100/5022/29411/5022-29411-0000.flac"
        >>> ds = ray.data.read_audio(path)
        >>> ds.schema()
        Column       Type
        ------       ----
        amplitude    numpy.ndarray(shape=(1, 191760), dtype=float)
        sample_rate  int64

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
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_file>`_.
            when opening input files to read.
        partition_filter:  A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`. Use
            with a custom callback to read only selected partitions of a dataset.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        include_paths: If ``True``, include the path to each image. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file/directory paths in ``paths``
            that are not found. Defaults to False.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.

    Returns:
        A :class:`~ray.data.Dataset` containing audio amplitudes and associated
        metadata.
    """  # noqa: E501
    reader = AudioReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_videos(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Optional[Partitioning] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
):
    """Creates a :class:`~ray.data.Dataset` from video files.

    Each row in the resulting dataset represents a video frame.

    Examples:
        >>> import ray
        >>> path = "s3://anonymous@ray-example-data/basketball.mp4"
        >>> ds = ray.data.read_videos(path)
        >>> ds.schema()
        Column       Type
        ------       ----
        frame        numpy.ndarray(shape=(720, 1280, 3), dtype=uint8)
        frame_index  int64

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
        arrow_open_stream_args: kwargs passed to
            `pyarrow.fs.FileSystem.open_input_file <https://arrow.apache.org/docs/\
                python/generated/pyarrow.fs.FileSystem.html\
                    #pyarrow.fs.FileSystem.open_input_file>`_.
            when opening input files to read.
        partition_filter:  A
            :class:`~ray.data.datasource.partitioning.PathPartitionFilter`. Use
            with a custom callback to read only selected partitions of a dataset.
        partitioning: A :class:`~ray.data.datasource.partitioning.Partitioning` object
            that describes how paths are organized. Defaults to ``None``.
        include_paths: If ``True``, include the path to each image. File paths are
            stored in the ``'path'`` column.
        ignore_missing_paths: If True, ignores any file/directory paths in ``paths``
            that are not found. Defaults to False.
        file_extensions: A list of file extensions to filter files by.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.

    Returns:
        A :class:`~ray.data.Dataset` containing video frames from the video files.
    """
    reader = VideoReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_snowflake(
    sql: str,
    connection_parameters: Dict[str, Any],
    *,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Read data from a Snowflake data set.

    Example:

        .. testcode::
            :skipif: True

            import ray

            connection_parameters = dict(
                user=...,
                account="ABCDEFG-ABC12345",
                password=...,
                database="SNOWFLAKE_SAMPLE_DATA",
                schema="TPCDS_SF100TCL"
            )
            ds = ray.data.read_snowflake("SELECT * FROM CUSTOMERS", connection_parameters)

    Args:
        sql: The SQL query to execute.
        connection_parameters: Keyword arguments to pass to
            ``snowflake.connector.connect``. To view supported parameters, read
            https://docs.snowflake.com/developer-guide/python-connector/python-connector-api#functions.

    Returns:
        A ``Dataset`` containing the data from the Snowflake data set.
    """  # noqa: E501
    return ray.data.read_datasource(
        SnowflakeDatasource(sql, connection_parameters),
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


@_try_fallback_to_oss
def read_webdataset(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    decoder: Optional[Union[bool, str, callable, list]] = True,
    fileselect: Optional[Union[list, callable]] = None,
    filerename: Optional[Union[list, callable]] = None,
    suffixes: Optional[Union[list, callable]] = None,
    verbose_open: bool = False,
    include_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
) -> Dataset:
    reader = WebDatasetReader(
        decoder=decoder,
        fileselect=fileselect,
        filerename=filerename,
        suffixes=suffixes,
        verbose_open=verbose_open,
        include_paths=include_paths,
        # TODO: `read_webdataset` doesn't support `partitioning` yet.
        partitioning=None,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        # TODO: `read_webdataset` doesn't support `ignore_missing_paths` yet.
        ignore_missing_paths=False,
        file_extensions=file_extensions,
        concurrency=concurrency,
        shuffle=shuffle,
        # TODO: `read_webdataset` doesn't support `ray_remote_args` yet.
        ray_remote_args=None,
    )


@_try_fallback_to_oss
def read_avro(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
) -> Dataset:
    reader = AvroReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_json(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = JSONDatasource._FILE_EXTENSIONS,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    **arrow_json_args,
) -> Dataset:
    reader = JSONReader(
        arrow_json_args,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_numpy(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = NumpyDatasource._FILE_EXTENSIONS,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    **numpy_load_args,
) -> Dataset:
    reader = NumpyReader(
        numpy_load_args=numpy_load_args,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args={},
    )


@_try_fallback_to_oss
def read_binary_files(
    paths: Union[str, List[str]],
    *,
    include_paths: bool = False,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
) -> Dataset:
    reader = BinaryReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_images(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = ImageDatasource._FILE_EXTENSIONS,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
) -> Dataset:
    reader = ImageReader(
        size=size,
        mode=mode,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_file_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_text(
    paths: Union[str, List[str]],
    *,
    encoding: str = "utf-8",
    drop_empty_lines: bool = True,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
) -> Dataset:
    reader = TextReader(
        drop_empty_lines=drop_empty_lines,
        encoding=encoding,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@_try_fallback_to_oss
def read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = Partitioning("hive"),
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    file_extensions: Optional[List[str]] = None,
    shuffle: Union[Literal["files"], None] = None,
    concurrency: Optional[int] = None,
    **arrow_csv_args,
) -> Dataset:
    reader = CSVReader(
        arrow_csv_args=arrow_csv_args,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        columns=None,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        shuffle=shuffle,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@wrap_auto_init
def read_files(
    paths: Union[str, List[str]],
    reader: FileReader,
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"],
    columns: Optional[List[str]],
    partition_filter: Optional[PathPartitionFilter],
    filter_expr: Optional["pyarrow.dataset.Expression"] = None,
    ignore_missing_paths: bool,
    file_extensions: Optional[List[str]],
    shuffle: Union[Literal["files"], None],
    concurrency: Optional[int],
    ray_remote_args: Dict[str, Any],
) -> Dataset:
    _validate_shuffle_arg(shuffle)

    paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    list_files_op = ListFiles(
        paths=paths,
        filesystem=filesystem,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        partition_filter=partition_filter,
    )
    if shuffle == "files":
        list_files_op = RandomShuffle(list_files_op)
    partition_files_op = PartitionFiles(
        list_files_op,
        reader=reader,
        filesystem=filesystem,
    )
    read_files_op = ReadFiles(
        partition_files_op,
        reader=reader,
        filesystem=filesystem,
        filter_expr=filter_expr,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        columns=columns,
    )

    execution_plan = ExecutionPlan(
        DatasetStats(metadata={"ReadFiles": []}, parent=None)
    )
    logical_plan = LogicalPlan(read_files_op, execution_plan._context)
    return Dataset(plan=execution_plan, logical_plan=logical_plan)
