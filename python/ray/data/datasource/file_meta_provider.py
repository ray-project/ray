import itertools
import logging
import pathlib
import os
import re
from typing import (
    List,
    Optional,
    Union,
    Iterator,
    Tuple,
    Any,
    TYPE_CHECKING,
)

import numpy as np

if TYPE_CHECKING:
    import pyarrow
    from ray.data.datasource.file_based_datasource import _S3FileSystemWrapper

from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import Partitioning
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _estimate_avail_cpus
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


# We should parallelize file size fetch operations beyond this threshold.
FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD = 16

# 16 file size fetches from S3 takes ~1.5 seconds with Arrow's S3FileSystem.
PATHS_PER_FILE_SIZE_FETCH_TASK = 16


@DeveloperAPI
class FileMetadataProvider:
    """Abstract callable that provides metadata for the files of a single dataset block.

    Current subclasses:
        BaseFileMetadataProvider
        ParquetMetadataProvider
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        **kwargs,
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files in the given paths.

        All file paths provided should belong to a single dataset block.

        Args:
            paths: The file paths for a single dataset block.
            schema: The user-provided or inferred schema for the given paths,
                if any.

        Returns:
            BlockMetadata aggregated across the given paths.
        """
        raise NotImplementedError

    def __call__(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        **kwargs,
    ) -> BlockMetadata:
        return self._get_block_metadata(paths, schema, **kwargs)


@DeveloperAPI
class BaseFileMetadataProvider(FileMetadataProvider):
    """Abstract callable that provides metadata for FileBasedDatasource
     implementations that reuse the base `prepare_read` method.

    Also supports file and file size discovery in input directory paths.

     Current subclasses:
         DefaultFileMetadataProvider
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files of a single dataset block.

        Args:
            paths: The file paths for a single dataset block. These
                paths will always be a subset of those previously returned from
                `expand_paths()`.
            schema: The user-provided or inferred schema for the given file
                paths, if any.
            rows_per_file: The fixed number of rows per input file, or None.
            file_sizes: Optional file size per input file previously returned
                from `expand_paths()`, where `file_sizes[i]` holds the size of
                the file at `paths[i]`.

        Returns:
            BlockMetadata aggregated across the given file paths.
        """
        raise NotImplementedError

    def expand_paths(
        self,
        paths: List[str],
        filesystem: Optional["pyarrow.fs.FileSystem"],
        partitioning: Optional[Partitioning] = None,
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

         Returns:
             An iterator of (file_path, file_size) pairs. None may be returned for the
             file size if it is either unknown or will be fetched later by
             `_get_block_metadata()`, but the length of both lists must be equal.
        """
        raise NotImplementedError


@DeveloperAPI
class DefaultFileMetadataProvider(BaseFileMetadataProvider):
    """Default metadata provider for FileBasedDatasource implementations that
    reuse the base `prepare_read` method.

    Calculates block size in bytes as the sum of its constituent file sizes,
    and assumes a fixed number of rows per file.
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
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
            size_bytes=None if None in file_sizes else sum(file_sizes),
            schema=schema,
            input_files=paths,
            exec_stats=None,
        )  # Exec stats filled in later.

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "pyarrow.fs.FileSystem",
        partitioning: Optional[Partitioning] = None,
    ) -> Iterator[Tuple[str, int]]:
        if len(paths) > 1:
            logger.warning(
                f"Expanding {len(paths)} path(s). This may be a HIGH LATENCY "
                f"operation on some cloud storage services. If the specified paths "
                f"all point to files and never directories, try rerunning this read "
                f"with `meta_provider=FastFileMetadataProvider()`."
            )

        yield from _expand_paths(paths, filesystem, partitioning)


@DeveloperAPI
class FastFileMetadataProvider(DefaultFileMetadataProvider):
    """Fast Metadata provider for FileBasedDatasource implementations.

    Offers improved performance vs. DefaultFileMetadataProvider by skipping directory
    path expansion and file size collection. While this performance improvement may be
    negligible for local filesystems, it can be substantial for cloud storage service
    providers.

    This should only be used when all input paths are known to be files.
    """

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "pyarrow.fs.FileSystem",
        partitioning: Optional[Partitioning] = None,
    ) -> Iterator[Tuple[str, int]]:
        logger.warning(
            f"Skipping expansion of {len(paths)} path(s). If your paths contain "
            f"directories or if file size collection is required, try rerunning this "
            f"read with `meta_provider=DefaultFileMetadataProvider()`."
        )

        yield from zip(paths, itertools.repeat(None, len(paths)))


@DeveloperAPI
class ParquetMetadataProvider(FileMetadataProvider):
    """Abstract callable that provides block metadata for Arrow Parquet file fragments.

    All file fragments should belong to a single dataset block.

    Supports optional pre-fetching of ordered metadata for all file fragments in
    a single batch to help optimize metadata resolution.

    Current subclasses:
        DefaultParquetMetadataProvider
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        *,
        pieces: List["pyarrow.dataset.ParquetFileFragment"],
        prefetched_metadata: Optional[List[Any]],
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files of a single dataset block.

        Args:
            paths: The file paths for a single dataset block.
            schema: The user-provided or inferred schema for the given file
                paths, if any.
            pieces: The Parquet file fragments derived from the input file paths.
            prefetched_metadata: Metadata previously returned from
                `prefetch_file_metadata()` for each file fragment, where
                `prefetched_metadata[i]` contains the metadata for `pieces[i]`.

        Returns:
            BlockMetadata aggregated across the given file paths.
        """
        raise NotImplementedError

    def prefetch_file_metadata(
        self,
        pieces: List["pyarrow.dataset.ParquetFileFragment"],
        **ray_remote_args,
    ) -> Optional[List[Any]]:
        """Pre-fetches file metadata for all Parquet file fragments in a single batch.

        Subsets of the metadata returned will be provided as input to
        subsequent calls to _get_block_metadata() together with their
        corresponding Parquet file fragments.

        Implementations that don't support pre-fetching file metadata shouldn't
        override this method.

        Args:
            pieces: The Parquet file fragments to fetch metadata for.

        Returns:
            Metadata resolved for each input file fragment, or `None`. Metadata
            must be returned in the same order as all input file fragments, such
            that `metadata[i]` always contains the metadata for `pieces[i]`.
        """
        return None


@DeveloperAPI
class DefaultParquetMetadataProvider(ParquetMetadataProvider):
    """The default file metadata provider for ParquetDatasource.

    Aggregates total block bytes and number of rows using the Parquet file metadata
    associated with a list of Arrow Parquet dataset file fragments.
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        *,
        pieces: List["pyarrow.dataset.ParquetFileFragment"],
        prefetched_metadata: Optional[List["pyarrow.parquet.FileMetaData"]],
    ) -> BlockMetadata:
        if prefetched_metadata is not None and len(prefetched_metadata) == len(pieces):
            # Piece metadata was available, construct a normal
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=sum(m.num_rows for m in prefetched_metadata),
                size_bytes=sum(
                    sum(m.row_group(i).total_byte_size for i in range(m.num_row_groups))
                    for m in prefetched_metadata
                ),
                schema=schema,
                input_files=paths,
                exec_stats=None,
            )  # Exec stats filled in later.
        else:
            # Piece metadata was not available, construct an empty
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=schema,
                input_files=paths,
                exec_stats=None,
            )
        return block_metadata

    def prefetch_file_metadata(
        self,
        pieces: List["pyarrow.dataset.ParquetFileFragment"],
        **ray_remote_args,
    ) -> Optional[List["pyarrow.parquet.FileMetaData"]]:
        from ray.data.datasource.parquet_datasource import (
            PARALLELIZE_META_FETCH_THRESHOLD,
            _fetch_metadata_remotely,
            _fetch_metadata,
        )

        if len(pieces) > PARALLELIZE_META_FETCH_THRESHOLD:
            return _fetch_metadata_remotely(pieces, **ray_remote_args)
        else:
            return _fetch_metadata(pieces)


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


def _expand_paths(
    paths: List[str],
    filesystem: "pyarrow.fs.FileSystem",
    partitioning: Optional[Partitioning],
) -> Iterator[Tuple[str, int]]:
    """Get the file sizes for all provided file paths."""
    from pyarrow.fs import LocalFileSystem
    from ray.data.datasource.file_based_datasource import (
        _wrap_s3_serialization_workaround,
        _unwrap_protocol,
    )

    # We break down our processing paths into a few key cases:
    # 1. If len(paths) < threshold, fetch the file info for the individual files/paths
    #    serially.
    # 2. If all paths are contained under the same parent directory (or base directory,
    #    if using partitioning), fetch all file infos at this prefix and filter to the
    #    provided paths on the client; this should be a single file info request.
    # 3. If more than threshold requests required, parallelize them via Ray tasks.

    # 1. Small # of paths case.
    if (
        len(paths) < FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
        # Local file systems are very fast to hit.
        or isinstance(filesystem, LocalFileSystem)
    ):
        for path in paths:
            yield from _get_file_infos(path, filesystem)
        return

    # 2. Common path prefix case.
    # Get longest common path of all paths.
    common_path = os.path.commonpath(paths)
    common_path = os.path.normpath(common_path)
    # If parent directory (or base directory, if using partitioning) is common to all
    # paths, fetch all file infos at that prefix and filter the response to the provided
    # paths.
    normalized_base_dir = _unwrap_protocol(partitioning.base_dir)
    if (partitioning is not None and common_path == normalized_base_dir) or all(
        str(pathlib.Path(path).parent) == common_path for path in paths
    ):
        path_to_size = {path: 0 for path in paths}
        for path, file_size in _get_file_infos(common_path, filesystem):
            if path in path_to_size:
                path_to_size[path] = file_size
        # Dictionaries are insertion-ordered, so this path + size pairs should be
        # yielded in the order of the original paths arg.
        yield from path_to_size.items()
        return

    # 3. Parallelization case.
    # Parallelize requests via Ray tasks.
    # TODO(Clark): Group file paths by parent directory paths and do a prefix
    # fetch + client-side filter if the number of groups is << the total number of
    # paths?
    # Only request 0.25 CPU since these tasks are I/O-bound.
    num_cpus = 0.25
    get_file_infos_remotely = cached_remote_fn(
        _get_file_infos_remotely,
        num_cpus=num_cpus,
        scheduling_strategy="SPREAD",
    )
    available_cpus = _estimate_avail_cpus()
    parallelism = min(
        # Choose a parallelism that results in a # of file size fetches per task that
        # dominates the Ray task overhead while ensuring good parallelism.
        # Always launch at least 2 parallel fetch tasks.
        max(len(paths) // PATHS_PER_FILE_SIZE_FETCH_TASK, 2),
        # Oversubscribe cluster CPU by 4x since these tasks are I/O-bound.
        round(available_cpus / num_cpus),
    )
    size_fetch_bar = ProgressBar("Metadata Fetch Progress", total=parallelism)
    results = []
    filesystem = _wrap_s3_serialization_workaround(filesystem)
    for path_chunks in np.array_split(paths, parallelism):
        if len(path_chunks) == 0:
            continue
        results.append(get_file_infos_remotely.remote(path_chunks, filesystem))
    for result in size_fetch_bar.fetch_until_complete(results):
        yield from result


def _get_file_infos_remotely(
    paths: List[str],
    filesystem: Union["pyarrow.fs.FileSystem", "_S3FileSystemWrapper"],
) -> Iterator[Tuple[str, int]]:
    """Get the file infos for all files at or under the provided paths."""
    from ray.data.datasource.file_based_datasource import (
        _unwrap_s3_serialization_workaround,
    )

    filesystem = _unwrap_s3_serialization_workaround(filesystem)
    out = []
    for path in paths:
        out.extend(_get_file_infos(path, filesystem))
    return out


def _get_file_infos(
    path: str,
    filesystem: "pyarrow.fs.FileSystem",
) -> Iterator[Tuple[str, int]]:
    """Get the file info for all files at or under the provided path."""
    from pyarrow.fs import FileType

    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)
    if file_info.type == FileType.Directory:
        yield from _expand_directory(path, filesystem)
    elif file_info.type == FileType.File:
        yield path, file_info.size
    else:
        raise FileNotFoundError(path)


def _expand_directory(
    path: str,
    filesystem: "pyarrow.fs.FileSystem",
    exclude_prefixes: Optional[List[str]] = None,
) -> Iterator[Tuple[str, int]]:
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

    selector = FileSelector(path, recursive=True)
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
    yield from sorted(out)
