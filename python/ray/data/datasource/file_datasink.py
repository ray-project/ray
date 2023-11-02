import posixpath
import warnings
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray._private.utils import _add_creatable_buckets_param_if_s3_uri
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _is_local_scheme
from ray.data.block import Block, BlockAccessor
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
    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        raise NotImplementedError

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
                self.write_row_to_file(row, file)


@DeveloperAPI
class BlockBasedFileDatasink(_FileDatasink):
    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        raise NotImplementedError

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
            self.write_block_to_file(block, file)
