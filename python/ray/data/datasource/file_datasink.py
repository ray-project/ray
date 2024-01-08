import posixpath
import warnings
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray._private.utils import _add_creatable_buckets_param_if_s3_uri
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _is_local_scheme, call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.file_based_datasource import _open_file_with_retry
from ray.data.datasource.filename_provider import (
    FilenameProvider,
    _DefaultFilenameProvider,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

logger = DatasetLogger(__name__)


WRITE_FILE_MAX_ATTEMPTS = 10
WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS = 32


class _FileDatasink(Datasink):
    def __init__(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        file_format: Optional[str] = None,
    ):
        """Initialize this datasink.

        Args:
            path: The folder to write files to.
            filesystem: The filesystem to write files to. If not provided, the
                filesystem is inferred from the path.
            try_create_dir: Whether to create the directory to write files to.
            open_stream_args: Arguments to pass to ``filesystem.open_output_stream``.
            filename_provider: A :class:`ray.data.datasource.FilenameProvider` that
                generates filenames for each row or block.
            dataset_uuid: The UUID of the dataset being written. If specified, it's
                included in the filename.
            file_format: The file extension. If specified, files are written with this
                extension.
        """
        if open_stream_args is None:
            open_stream_args = {}

        if block_path_provider is not None:
            warnings.warn(
                "`block_path_provider` has been deprecated in favor of "
                "`filename_provider`. For more information, see "
                "https://docs.ray.io/en/master/data/api/doc/ray.data.datasource.FilenameProvider.html",  # noqa: E501
                DeprecationWarning,
            )

        if filename_provider is None and block_path_provider is None:
            filename_provider = _DefaultFilenameProvider(
                dataset_uuid=dataset_uuid, file_format=file_format
            )

        self.unresolved_path = path
        paths, self.filesystem = _resolve_paths_and_filesystem(path, filesystem)
        assert len(paths) == 1, len(paths)
        self.path = paths[0]

        self.try_create_dir = try_create_dir
        self.open_stream_args = open_stream_args
        self.filename_provider = filename_provider
        self.block_path_provider = block_path_provider
        self.dataset_uuid = dataset_uuid
        self.file_format = file_format

        self.has_created_dir = False

    def on_write_start(self) -> None:
        """Create a directory to write files to.

        If ``try_create_dir`` is ``False``, this method is a no-op.
        """
        from pyarrow.fs import FileType

        if self.try_create_dir:
            if self.filesystem.get_file_info(self.path).type is FileType.NotFound:
                # Arrow's S3FileSystem doesn't allow creating buckets by default, so we
                # add a query arg enabling bucket creation if an S3 URI is provided.
                tmp = _add_creatable_buckets_param_if_s3_uri(self.path)
                self.filesystem.create_dir(tmp, recursive=True)
                self.has_created_dir = True

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        num_rows_written = 0

        block_index = 0
        for block in blocks:
            block = BlockAccessor.for_block(block)
            if block.num_rows() == 0:
                continue

            self.write_block(block, block_index, ctx)

            num_rows_written += block.num_rows()
            block_index += 1

        if num_rows_written == 0:
            logger.get_logger().warning(
                f"Skipped writing empty dataset with UUID {self.dataset_uuid} at "
                f"{self.path}.",
            )
            return "skip"

        # TODO: decide if we want to return richer object when the task
        # succeeds.
        return "ok"

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        raise NotImplementedError

    def on_write_complete(self, write_results: List[Any]) -> None:
        if not self.has_created_dir:
            return

        if all(write_results == "skip" for write_results in write_results):
            self.filesystem.delete_dir(self.path)

    @property
    def supports_distributed_writes(self) -> bool:
        return not _is_local_scheme(self.unresolved_path)


@DeveloperAPI
class RowBasedFileDatasink(_FileDatasink):
    """A datasink that writes one row to each file.

    Subclasses must implement ``write_row_to_file`` and call the superclass constructor.

    Examples:
        .. testcode::

            import io
            import numpy as np
            from PIL import Image
            from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
            from ray.data.datasource import FileBasedDatasource

            class ImageDatasource(FileBasedDatasource):
                def __init__(self, paths):
                    super().__init__(
                        paths,
                        file_extensions=["png", "jpg", "jpeg", "bmp", "gif", "tiff"],
                    )

                def _read_stream(self, f, path):
                    data = f.readall()
                    image = Image.open(io.BytesIO(data))

                    builder = DelegatingBlockBuilder()
                    array = np.array(image)
                    item = {"image": array}
                    builder.add(item)
                    yield builder.build()
    """  # noqa: E501

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        """Write a row to a file.

        Args:
            row: The row to write.
            file: The file to write the row to.
        """
        raise NotImplementedError

    def _write_row_to_file_with_retry(
        self, row: Dict[str, Any], file: "pyarrow.NativeFile", path: str
    ):
        call_with_retry(
            lambda: self.write_row_to_file(row, file),
            match=DataContext.get_current().write_file_retry_on_errors,
            description=f"write '{path}'",
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        for row_index, row in enumerate(block.iter_rows(public_row_format=False)):
            if self.filename_provider is not None:
                filename = self.filename_provider.get_filename_for_row(
                    row, ctx.task_idx, block_index, row_index
                )
            else:
                # TODO: Remove this code path once we remove `BlockWritePathProvider`.
                filename = (
                    f"{self.dataset_uuid}_{ctx.task_idx:06}_{block_index:06}_"
                    f"{row_index:06}.{self.file_format}"
                )
            write_path = posixpath.join(self.path, filename)

            logger.get_logger().debug(f"Writing {write_path} file.")
            with _open_file_with_retry(
                write_path,
                lambda: self.filesystem.open_output_stream(
                    write_path, **self.open_stream_args
                ),
            ) as file:
                self._write_row_to_file_with_retry(row, file, write_path)


@DeveloperAPI
class BlockBasedFileDatasink(_FileDatasink):
    """A datasink that writes multiple rows to each file.

    Subclasses must implement ``write_block_to_file`` and call the superclass
    constructor.

    Examples:
        .. testcode::

            class CSVDatasink(BlockBasedFileDatasink):
                def __init__(self, path: str):
                    super().__init__(path, file_format="csv")

                def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
                    from pyarrow import csv
                    csv.write_csv(block.to_arrow(), file)
    """  # noqa: E501

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        """Write a block of data to a file.

        Args:
            block: The block to write.
            file: The file to write the block to.
        """
        raise NotImplementedError

    def _write_block_to_file_with_retry(
        self, block: BlockAccessor, file: "pyarrow.NativeFile", path: str
    ):
        call_with_retry(
            lambda: self.write_block_to_file(block, file),
            match=DataContext.get_current().write_file_retry_on_errors,
            description=f"write '{path}'",
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        if self.filename_provider is not None:
            filename = self.filename_provider.get_filename_for_block(
                block, ctx.task_idx, block_index
            )
            write_path = posixpath.join(self.path, filename)
        else:
            # TODO: Remove this code path once we remove `BlockWritePathProvider`.
            write_path = self.block_path_provider(
                self.path,
                filesystem=self.filesystem,
                dataset_uuid=self.dataset_uuid,
                task_index=ctx.task_idx,
                block_index=block_index,
                file_format=self.file_format,
            )

        logger.get_logger().debug(f"Writing {write_path} file.")
        with _open_file_with_retry(
            write_path,
            lambda: self.filesystem.open_output_stream(
                write_path, **self.open_stream_args
            ),
        ) as file:
            self._write_block_to_file_with_retry(block, file, write_path)
