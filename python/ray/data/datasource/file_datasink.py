import logging
import posixpath
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional
from urllib.parse import urlparse

from ray._common.retry import call_with_retry
from ray._private.arrow_utils import add_creatable_buckets_param_if_s3_uri
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _is_local_scheme,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.filename_provider import (
    FilenameProvider,
    _DefaultFilenameProvider,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


class _FileDatasink(Datasink[None]):
    def __init__(
        self,
        path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        dataset_uuid: Optional[str] = None,
        file_format: Optional[str] = None,
        mode: SaveMode = SaveMode.APPEND,
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

        if filename_provider is None:
            filename_provider = _DefaultFilenameProvider(
                dataset_uuid=dataset_uuid, file_format=file_format
            )

        self._data_context = DataContext.get_current()
        self.unresolved_path = path
        paths, self.filesystem = _resolve_paths_and_filesystem(path, filesystem)
        self.filesystem = RetryingPyFileSystem.wrap(
            self.filesystem, retryable_errors=self._data_context.retried_io_errors
        )
        assert len(paths) == 1, len(paths)
        self.path = paths[0]

        self.try_create_dir = try_create_dir
        self.open_stream_args = open_stream_args
        self.filename_provider = filename_provider
        self.dataset_uuid = dataset_uuid
        self.file_format = file_format
        self.mode = mode
        self.has_created_dir = False
        self._skip_write = False
        self._write_started = False

    def open_output_stream(self, path: str) -> "pyarrow.NativeFile":
        return self.filesystem.open_output_stream(path, **self.open_stream_args)

    def on_write_start(self, schema: Optional["pyarrow.Schema"] = None) -> None:
        # Make idempotent - if already called, return early.
        if self._write_started:
            return
        self._write_started = True

        from pyarrow.fs import FileType

        dir_exists = (
            self.filesystem.get_file_info(self.path).type is not FileType.NotFound
        )
        if dir_exists:
            if self.mode == SaveMode.ERROR:
                raise ValueError(
                    f"Path {self.path} already exists. "
                    "If this is unexpected, use mode='ignore' to ignore those files"
                )
            if self.mode == SaveMode.IGNORE:
                logger.warning(f"[SaveMode={self.mode}] Skipping {self.path}")
                self._skip_write = True
                return
            if self.mode == SaveMode.OVERWRITE:
                logger.warning(f"[SaveMode={self.mode}] Replacing contents {self.path}")
                self.filesystem.delete_dir_contents(self.path)
        self.has_created_dir = self._create_dir(self.path)

    def _create_dir(self, dest) -> bool:
        """Create a directory to write files to.

        If ``try_create_dir`` is ``False``, this method is a no-op.
        """
        from pyarrow.fs import FileType

        # We should skip creating directories in s3 unless the user specifically
        # overrides this behavior. PyArrow's s3fs implementation for create_dir
        # will attempt to check if the parent directory exists before trying to
        # create the directory (with recursive=True it will try to do this to
        # all of the directories until the root of the bucket). An IAM Policy that
        # restricts access to a subset of prefixes within the bucket might cause
        # the creation of the directory to fail even if the permissions should
        # allow the data can be written to the specified path. For example if a
        # a policy only allows users to write blobs prefixed with s3://bucket/foo
        # a call to create_dir for s3://bucket/foo/bar will fail even though it
        # should not.
        parsed_uri = urlparse(dest)
        is_s3_uri = parsed_uri.scheme == "s3"
        skip_create_dir_for_s3 = is_s3_uri and not self._data_context.s3_try_create_dir

        if self.try_create_dir and not skip_create_dir_for_s3:
            if self.filesystem.get_file_info(dest).type is FileType.NotFound:
                # Arrow's S3FileSystem doesn't allow creating buckets by default, so we
                # add a query arg enabling bucket creation if an S3 URI is provided.
                tmp = add_creatable_buckets_param_if_s3_uri(dest)
                self.filesystem.create_dir(tmp, recursive=True)
                return True

        return False

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()
        block_accessor = BlockAccessor.for_block(block)

        if block_accessor.num_rows() == 0:
            logger.warning(f"Skipped writing empty block to {self.path}")
            return

        self.write_block(block_accessor, 0, ctx)

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        raise NotImplementedError

    def on_write_complete(self, write_result: WriteResult[None]):
        # If no rows were written, we can delete the directory.
        if self.has_created_dir and write_result.num_rows == 0:
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
            from typing import Any, Dict

            import pyarrow
            from PIL import Image

            from ray.data.datasource import RowBasedFileDatasink

            class ImageDatasink(RowBasedFileDatasink):
                def __init__(self, path: str, *, column: str, file_format: str = "png"):
                    super().__init__(path, file_format=file_format)
                    self._file_format = file_format
                    self._column = column

                def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
                    image = Image.fromarray(row[self._column])
                    buffer = io.BytesIO()
                    image.save(buffer, format=self._file_format)
                    file.write(buffer.getvalue())
    """  # noqa: E501

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        """Write a row to a file.

        Args:
            row: The row to write.
            file: The file to write the row to.
        """
        raise NotImplementedError

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        for row_index, row in enumerate(block.iter_rows(public_row_format=False)):
            filename = self.filename_provider.get_filename_for_row(
                row,
                ctx.kwargs[WRITE_UUID_KWARG_NAME],
                ctx.task_idx,
                block_index,
                row_index,
            )
            write_path = posixpath.join(self.path, filename)
            logger.debug(f"Writing {write_path} file.")

            def write_row_to_path():
                with self.open_output_stream(write_path) as file:
                    self.write_row_to_file(row, file)

            call_with_retry(
                write_row_to_path,
                description=f"write '{write_path}'",
                match=self._data_context.retried_io_errors,
            )


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

    def __init__(
        self, path, *, min_rows_per_file: Optional[int] = None, **file_datasink_kwargs
    ):
        super().__init__(path, **file_datasink_kwargs)

        self._min_rows_per_file = min_rows_per_file

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        """Write a block of data to a file.

        Args:
            block: The block to write.
            file: The file to write the block to.
        """
        raise NotImplementedError

    def write_block(self, block: BlockAccessor, block_index: int, ctx: TaskContext):
        filename = self.filename_provider.get_filename_for_block(
            block, ctx.kwargs[WRITE_UUID_KWARG_NAME], ctx.task_idx, block_index
        )
        write_path = posixpath.join(self.path, filename)

        def write_block_to_path():
            with self.open_output_stream(write_path) as file:
                self.write_block_to_file(block, file)

        logger.debug(f"Writing {write_path} file.")
        call_with_retry(
            write_block_to_path,
            description=f"write '{write_path}'",
            match=self._data_context.retried_io_errors,
        )

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._min_rows_per_file
