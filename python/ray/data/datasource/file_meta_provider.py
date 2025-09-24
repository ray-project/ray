import itertools
import logging
import os
import pathlib
import re
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.data.datasource.path_util import _has_file_extension
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


@DeveloperAPI
class FileMetadataProvider:
    """Abstract callable that provides metadata for the files of a single dataset block.

    Current subclasses:
        - :class:`BaseFileMetadataProvider`
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        **kwargs,
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files in the given paths.

        All file paths provided should belong to a single dataset block.

        Args:
            paths: The file paths for a single dataset block.
            **kwargs: Additional kwargs used to determine block metadata.

        Returns:
            BlockMetadata aggregated across the given paths.
        """
        raise NotImplementedError

    def __call__(
        self,
        paths: List[str],
        **kwargs,
    ) -> BlockMetadata:
        return self._get_block_metadata(paths, **kwargs)


@DeveloperAPI
class BaseFileMetadataProvider(FileMetadataProvider):
    """Abstract callable that provides metadata for
    :class:`~ray.data.datasource.file_based_datasource.FileBasedDatasource`
    implementations that reuse the base :meth:`~ray.data.Datasource.prepare_read`
    method.

    Also supports file and file size discovery in input directory paths.

    Current subclasses:
        - :class:`DefaultFileMetadataProvider`
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files of a single dataset block.

        Args:
            paths: The file paths for a single dataset block. These
                paths will always be a subset of those previously returned from
                :meth:`.expand_paths`.
            rows_per_file: The fixed number of rows per input file, or None.
            file_sizes: Optional file size per input file previously returned
                from :meth:`.expand_paths`, where `file_sizes[i]` holds the size of
                the file at `paths[i]`.

        Returns:
            BlockMetadata aggregated across the given file paths.
        """
        raise NotImplementedError

    def expand_paths(
        self,
        paths: List[str],
        filesystem: Optional["RetryingPyFileSystem"],
        partitioning: Optional[Partitioning] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        """Expands all paths into concrete file paths by walking directories.

        Also returns a sidecar of file sizes.

        The input paths must be normalized for compatibility with the input
        filesystem prior to invocation.

        Args:
            paths: A list of file and/or directory paths compatible with the
                given filesystem.
            filesystem: The filesystem implementation that should be used for
                expanding all paths and reading their files.
            ignore_missing_paths: If True, ignores any file paths in ``paths`` that
                are not found. Defaults to False.

        Returns:
            An iterator of `(file_path, file_size)` pairs. None may be returned for the
            file size if it is either unknown or will be fetched later by
            `_get_block_metadata()`, but the length of
            both lists must be equal.
        """
        raise NotImplementedError


@DeveloperAPI
class DefaultFileMetadataProvider(BaseFileMetadataProvider):
    """Default metadata provider for
    :class:`~ray.data.datasource.file_based_datasource.FileBasedDatasource`
    implementations that reuse the base `prepare_read` method.

    Calculates block size in bytes as the sum of its constituent file sizes,
    and assumes a fixed number of rows per file.
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        if rows_per_file is None:
            num_rows = None
        else:
            num_rows = len(paths) * rows_per_file
        return BlockMetadata(
            num_rows=num_rows,
            size_bytes=None if None in file_sizes else int(sum(file_sizes)),
            input_files=paths,
            exec_stats=None,
        )  # Exec stats filled in later.

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "RetryingPyFileSystem",
        partitioning: Optional[Partitioning] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        yield from _expand_paths(paths, filesystem, partitioning, ignore_missing_paths)


@DeveloperAPI
class FastFileMetadataProvider(DefaultFileMetadataProvider):
    """Fast Metadata provider for
    :class:`~ray.data.datasource.file_based_datasource.FileBasedDatasource`
    implementations.

    Offers improved performance vs.
    :class:`DefaultFileMetadataProvider`
    by skipping directory path expansion and file size collection.
    While this performance improvement may be negligible for local filesystems,
    it can be substantial for cloud storage service providers.

    This should only be used when all input paths exist and are known to be files.
    """

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "RetryingPyFileSystem",
        partitioning: Optional[Partitioning] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        if ignore_missing_paths:
            raise ValueError(
                "`ignore_missing_paths` cannot be set when used with "
                "`FastFileMetadataProvider`. All paths must exist when "
                "using `FastFileMetadataProvider`."
            )

        logger.warning(
            f"Skipping expansion of {len(paths)} path(s). If your paths contain "
            f"directories or if file size collection is required, try rerunning this "
            f"read with `meta_provider=DefaultFileMetadataProvider()`."
        )

        yield from zip(paths, itertools.repeat(None, len(paths)))


def _handle_read_os_error(error: OSError, paths: Union[str, List[str]]) -> str:
    # NOTE: this is not comprehensive yet, and should be extended as more errors arise.
    # NOTE: The latter patterns are raised in Arrow 10+, while the former is raised in
    # Arrow < 10.
    aws_error_pattern = (
        r"^(?:(.*)AWS Error \[code \d+\]: No response body\.(.*))|"
        r"(?:(.*)AWS Error UNKNOWN \(HTTP status 400\) during HeadObject operation: "
        r"No response body\.(.*))|"
        r"(?:(.*)AWS Error ACCESS_DENIED during HeadObject operation: No response "
        r"body\.(.*))$"
    )
    if re.match(aws_error_pattern, str(error)):
        # Specially handle AWS error when reading files, to give a clearer error
        # message to avoid confusing users. The real issue is most likely that the AWS
        # S3 file credentials have not been properly configured yet.
        if isinstance(paths, str):
            # Quote to highlight single file path in error message for better
            # readability. List of file paths will be shown up as ['foo', 'boo'],
            # so only quote single file path here.
            paths = f'"{paths}"'
        raise OSError(
            (
                f"Failing to read AWS S3 file(s): {paths}. "
                "Please check that file exists and has properly configured access. "
                "You can also run AWS CLI command to get more detailed error message "
                "(e.g., aws s3 ls <file-name>). "
                "See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html "  # noqa
                "and https://docs.ray.io/en/latest/data/creating-datasets.html#reading-from-remote-storage "  # noqa
                "for more information."
            )
        )
    else:
        raise error


def _list_files(
    paths: List[str],
    filesystem: "RetryingPyFileSystem",
    *,
    partition_filter: Optional[PathPartitionFilter],
    file_extensions: Optional[List[str]],
) -> List[Tuple[str, int]]:
    return list(
        _list_files_internal(
            paths,
            filesystem,
            partition_filter=partition_filter,
            file_extensions=file_extensions,
        )
    )


def _list_files_internal(
    paths: List[str],
    filesystem: "RetryingPyFileSystem",
    *,
    partition_filter: Optional[PathPartitionFilter],
    file_extensions: Optional[List[str]],
) -> Iterator[Tuple[str, int]]:
    default_meta_provider = DefaultFileMetadataProvider()

    for path, file_size in default_meta_provider.expand_paths(paths, filesystem):
        # HACK: PyArrow's `ParquetDataset` errors if input paths contain non-parquet
        # files. To avoid this, we expand the input paths with the default metadata
        # provider and then apply the partition filter or file extensions.
        if (
            partition_filter
            and not partition_filter.apply(path)
            or not _has_file_extension(path, file_extensions)
        ):
            continue

        yield path, file_size


def _expand_paths(
    paths: List[str],
    filesystem: "RetryingPyFileSystem",
    partitioning: Optional[Partitioning],
    ignore_missing_paths: bool = False,
) -> Iterator[Tuple[str, int]]:
    """Get the file sizes for all provided file paths."""
    from pyarrow.fs import LocalFileSystem

    from ray.data.datasource.file_based_datasource import (
        FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD,
    )
    from ray.data.datasource.path_util import _is_http_url, _unwrap_protocol

    # We break down our processing paths into a few key cases:
    # 1. If len(paths) < threshold, fetch the file info for the individual files/paths
    #    serially.
    # 2. If all paths are contained under the same parent directory (or base directory,
    #    if using partitioning), fetch all file infos at this prefix and filter to the
    #    provided paths on the client; this should be a single file info request.
    # 3. If more than threshold requests required, parallelize them via Ray tasks.
    # 1. Small # of paths case.
    is_local = isinstance(filesystem, LocalFileSystem)
    if isinstance(filesystem, RetryingPyFileSystem):
        is_local = isinstance(filesystem.unwrap(), LocalFileSystem)

    if (
        len(paths) < FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
        # Local file systems are very fast to hit.
        or is_local
    ):
        yield from _get_file_infos_serial(paths, filesystem, ignore_missing_paths)
    else:
        # 2. Common path prefix case.
        # Get longest common path of all paths.
        common_path = os.path.commonpath(paths)
        # If parent directory (or base directory, if using partitioning) is common to
        # all paths, fetch all file infos at that prefix and filter the response to the
        # provided paths.
        if not _is_http_url(common_path) and (
            (
                partitioning is not None
                and common_path == _unwrap_protocol(partitioning.base_dir)
            )
            or all(str(pathlib.Path(path).parent) == common_path for path in paths)
        ):
            yield from _get_file_infos_common_path_prefix(
                paths, common_path, filesystem, ignore_missing_paths
            )
        # 3. Parallelization case.
        else:
            # Parallelize requests via Ray tasks.
            yield from _get_file_infos_parallel(paths, filesystem, ignore_missing_paths)


def _get_file_infos_serial(
    paths: List[str],
    filesystem: "RetryingPyFileSystem",
    ignore_missing_paths: bool = False,
) -> Iterator[Tuple[str, int]]:
    for path in paths:
        yield from _get_file_infos(path, filesystem, ignore_missing_paths)


def _get_file_infos_common_path_prefix(
    paths: List[str],
    common_path: str,
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_paths: bool = False,
) -> Iterator[Tuple[str, int]]:
    path_to_size = {path: None for path in paths}
    for path, file_size in _get_file_infos(
        common_path, filesystem, ignore_missing_paths
    ):
        if path in path_to_size:
            path_to_size[path] = file_size

    # Check if all `paths` have file size metadata.
    # If any of paths has no file size, fall back to get files metadata in parallel.
    # This can happen when path is a directory, but not a file.
    have_missing_path = False
    for path in paths:
        if path_to_size[path] is None:
            logger.debug(
                f"Finding path {path} not have file size metadata. "
                "Fall back to get files metadata in parallel for all paths."
            )
            have_missing_path = True
            break

    if have_missing_path:
        # Parallelize requests via Ray tasks.
        yield from _get_file_infos_parallel(paths, filesystem, ignore_missing_paths)
    else:
        # Iterate over `paths` to yield each path in original order.
        # NOTE: do not iterate over `path_to_size` because the dictionary skips
        # duplicated path, while `paths` might contain duplicated path if one wants
        # to read same file multiple times.
        for path in paths:
            yield path, path_to_size[path]


def _get_file_infos_parallel(
    paths: List[str],
    filesystem: "RetryingPyFileSystem",
    ignore_missing_paths: bool = False,
) -> Iterator[Tuple[str, int]]:
    from ray.data.datasource.file_based_datasource import (
        PATHS_PER_FILE_SIZE_FETCH_TASK,
        _unwrap_s3_serialization_workaround,
        _wrap_s3_serialization_workaround,
    )

    logger.warning(
        f"Expanding {len(paths)} path(s). This may be a HIGH LATENCY "
        f"operation on some cloud storage services. Moving all the "
        "paths to a common parent directory will lead to faster "
        "metadata fetching."
    )

    # Capture the filesystem in the fetcher func closure, but wrap it in our
    # serialization workaround to make sure that the pickle roundtrip works as expected.
    filesystem = _wrap_s3_serialization_workaround(filesystem)

    def _file_infos_fetcher(paths: List[str]) -> List[Tuple[str, int]]:
        fs = _unwrap_s3_serialization_workaround(filesystem)
        return list(
            itertools.chain.from_iterable(
                _get_file_infos(path, fs, ignore_missing_paths) for path in paths
            )
        )

    yield from _fetch_metadata_parallel(
        paths, _file_infos_fetcher, PATHS_PER_FILE_SIZE_FETCH_TASK
    )


Uri = TypeVar("Uri")
Meta = TypeVar("Meta")


def _fetch_metadata_parallel(
    uris: List[Uri],
    fetch_func: Callable[[List[Uri]], List[Meta]],
    desired_uris_per_task: int,
    **ray_remote_args,
) -> Iterator[Meta]:
    """Fetch file metadata in parallel using Ray tasks."""
    remote_fetch_func = cached_remote_fn(fetch_func)
    if ray_remote_args:
        remote_fetch_func = remote_fetch_func.options(**ray_remote_args)
    # Choose a parallelism that results in a # of metadata fetches per task that
    # dominates the Ray task overhead while ensuring good parallelism.
    # Always launch at least 2 parallel fetch tasks.
    parallelism = max(len(uris) // desired_uris_per_task, 2)
    metadata_fetch_bar = ProgressBar(
        "Metadata Fetch Progress", total=parallelism, unit="task"
    )
    fetch_tasks = []
    for uri_chunk in np.array_split(uris, parallelism):
        if len(uri_chunk) == 0:
            continue
        fetch_tasks.append(remote_fetch_func.remote(uri_chunk))
    results = metadata_fetch_bar.fetch_until_complete(fetch_tasks)
    yield from itertools.chain.from_iterable(results)


def _get_file_infos(
    path: str, filesystem: "RetryingPyFileSystem", ignore_missing_path: bool = False
) -> List[Tuple[str, int]]:
    """Get the file info for all files at or under the provided path."""
    from pyarrow.fs import FileType

    file_infos = []
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)
    if file_info.type == FileType.Directory:
        for file_path, file_size in _expand_directory(path, filesystem):
            file_infos.append((file_path, file_size))
    elif file_info.type == FileType.File:
        file_infos.append((path, file_info.size))
    elif file_info.type == FileType.NotFound and ignore_missing_path:
        pass
    else:
        raise FileNotFoundError(path)

    return file_infos


def _expand_directory(
    path: str,
    filesystem: "RetryingPyFileSystem",
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
) -> List[Tuple[str, int]]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".

    Returns:
        An iterator of (file_path, file_size) tuples.
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    from pyarrow.fs import FileSelector

    selector = FileSelector(path, recursive=True, allow_not_found=ignore_missing_path)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    out = []
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        out.append((file_path, file_.size))
    # We sort the paths to guarantee a stable order.
    return sorted(out)
