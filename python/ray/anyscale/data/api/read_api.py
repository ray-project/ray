import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union

import ray
from ray._private.auto_init_hook import wrap_auto_init
from ray.anyscale.data._internal.logical.operators.expand_paths_operator import (
    ExpandPaths,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data.datasource.file_reader import FileReader
from ray.anyscale.data.datasource.snowflake_datasource import SnowflakeDatasource
from ray.data._internal.datasource.image_datasource import ImageDatasource
from ray.data._internal.datasource.json_datasource import JSONDatasource
from ray.data._internal.datasource.numpy_datasource import NumpyDatasource
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.dataset import Dataset
from ray.data.datasource import Partitioning, PathPartitionFilter
from ray.data.datasource.file_meta_provider import BaseFileMetadataProvider
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    import pyarrow.fs


META_PROVIDER_WARNING = (
    "You don't need to use `meta_provider` if you're using Ray on Anyscale."
)


# TODO(@bveeramani): Add `read_tfrecords`.


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
    from ray.anyscale.data.datasource.audio_reader import AudioReader

    reader = AudioReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.video_reader import VideoReader

    reader = VideoReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.webdataset_reader import WebDatasetReader
    from ray.data.read_api import read_webdataset as read_webdataset_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_webdataset_fallback(
            paths=paths,
            filesystem=filesystem,
            parallelism=parallelism,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            decoder=decoder,
            fileselect=fileselect,
            filerename=filerename,
            suffixes=suffixes,
            verbose_open=verbose_open,
            include_paths=include_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

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
        partition_filter=partition_filter,
        # TODO: `read_webdataset` doesn't support `ignore_missing_paths` yet.
        ignore_missing_paths=False,
        file_extensions=file_extensions,
        concurrency=concurrency,
        # TODO: `read_webdataset` doesn't support `ray_remote_args` yet.
        ray_remote_args=None,
    )


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
    from ray.anyscale.data.datasource.avro_reader import AvroReader
    from ray.data.read_api import read_avro as read_avro_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_avro_fallback(
            paths=paths,
            filesystem=filesystem,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

    reader = AvroReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.json_reader import JSONReader
    from ray.data.read_api import read_json as read_json_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_json_fallback(
            paths=paths,
            filesystem=filesystem,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **arrow_json_args,
        )

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
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.numpy_reader import NumpyReader
    from ray.data.read_api import read_numpy as read_numpy_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_numpy_fallback(
            paths=paths,
            filesystem=filesystem,
            parallelism=parallelism,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **numpy_load_args,
        )

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
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args={},
    )


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
    from ray.anyscale.data.datasource.binary_reader import BinaryReader
    from ray.data.read_api import read_binary_files as read_binary_files_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_binary_files_fallback(
            paths=paths,
            include_paths=include_paths,
            filesystem=filesystem,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

    reader = BinaryReader(
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_stream_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.image_reader import ImageReader
    from ray.data.read_api import read_images as read_images_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_images_fallback(
            paths,
            filesystem=filesystem,
            parallelism=parallelism,
            meta_provider=meta_provider,
            ray_remote_args=ray_remote_args,
            arrow_open_file_args=arrow_open_file_args,
            partition_filter=partition_filter,
            partitioning=partitioning,
            size=size,
            mode=mode,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

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
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.text_reader import TextReader
    from ray.data.read_api import read_text as read_text_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_text_fallback(
            paths,
            encoding=encoding,
            drop_empty_lines=drop_empty_lines,
            filesystem=filesystem,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

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
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


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
    from ray.anyscale.data.datasource.csv_reader import CSVReader
    from ray.data.read_api import read_csv as read_csv_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            warnings.warn(META_PROVIDER_WARNING)

        return read_csv_fallback(
            paths,
            filesystem=filesystem,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            arrow_open_stream_args=arrow_open_stream_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **arrow_csv_args,
        )

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
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


@wrap_auto_init
def read_files(
    paths: Union[str, List[str]],
    reader: FileReader,
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"],
    partition_filter: Optional[PathPartitionFilter],
    ignore_missing_paths: bool,
    file_extensions: Optional[List[str]],
    concurrency: Optional[int],
    ray_remote_args: Dict[str, Any],
) -> Dataset:
    paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
    expand_paths_op = ExpandPaths(
        paths=paths,
        reader=reader,
        filesystem=filesystem,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        partition_filter=partition_filter,
    )
    read_files_op = ReadFiles(
        expand_paths_op,
        paths=paths,
        reader=reader,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
    logical_plan = LogicalPlan(read_files_op)
    return Dataset(
        plan=ExecutionPlan(
            DatasetStats(metadata={"ReadFiles": []}, parent=None),
        ),
        logical_plan=logical_plan,
    )
