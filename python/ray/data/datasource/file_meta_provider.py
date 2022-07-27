import logging
import re
from typing import (
    List,
    Optional,
    Union,
    TYPE_CHECKING,
    Tuple,
    Any,
)

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


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
    ) -> Tuple[List[str], List[Optional[int]]]:
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
             A tuple whose first item contains the list of file paths discovered,
             and whose second item contains the size of each file. `None` may be
             returned if a file size is either unknown or will be fetched later
             by `_get_block_metadata()`, but the length of both lists must be
             equal.
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
    ) -> Tuple[List[str], List[Optional[int]]]:
        from pyarrow.fs import FileType
        from ray.data.datasource.file_based_datasource import _expand_directory

        if len(paths) > 1:
            logger.warning(
                f"Expanding {len(paths)} path(s). This may be a HIGH LATENCY "
                f"operation on some cloud storage services. If the specified paths "
                f"all point to files and never directories, try rerunning this read "
                f"with `meta_provider=FastFileMetadataProvider()`."
            )
        expanded_paths = []
        file_infos = []
        for path in paths:
            try:
                file_info = filesystem.get_file_info(path)
            except OSError as e:
                _handle_read_os_error(e, path)
            if file_info.type == FileType.Directory:
                paths, file_infos_ = _expand_directory(path, filesystem)
                expanded_paths.extend(paths)
                file_infos.extend(file_infos_)
            elif file_info.type == FileType.File:
                expanded_paths.append(path)
                file_infos.append(file_info)
            else:
                raise FileNotFoundError(path)
        file_sizes = [file_info.size for file_info in file_infos]
        return expanded_paths, file_sizes


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
    ) -> Tuple[List[str], List[Optional[int]]]:
        logger.warning(
            f"Skipping expansion of {len(paths)} path(s). If your paths contain "
            f"directories or if file size collection is required, try rerunning this "
            f"read with `meta_provider=DefaultFileMetadataProvider()`."
        )
        import numpy as np

        return paths, np.empty(len(paths), dtype=object)


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
    ) -> Optional[List["pyarrow.parquet.FileMetaData"]]:
        from ray.data.datasource.parquet_datasource import (
            PARALLELIZE_META_FETCH_THRESHOLD,
            _fetch_metadata_remotely,
            _fetch_metadata,
        )

        if len(pieces) > PARALLELIZE_META_FETCH_THRESHOLD:
            return _fetch_metadata_remotely(pieces)
        else:
            return _fetch_metadata(pieces)


def _handle_read_os_error(error: OSError, paths: Union[str, List[str]]) -> str:
    # NOTE: this is not comprehensive yet, and should be extended as more errors arise.
    aws_error_pattern = r"^(.*)AWS Error \[code \d+\]: No response body\.(.*)$"
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
