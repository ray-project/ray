import logging
import posixpath
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.file_datasink import _FileDatasink
from ray.data.datasource.filename_provider import FilenameProvider

if TYPE_CHECKING:
    import pyarrow

WRITE_FILE_MAX_ATTEMPTS = 10
WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS = 32

logger = logging.getLogger(__name__)


class ParquetDatasink(_FileDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
        num_rows_per_file: Optional[int] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.arrow_parquet_args_fn = arrow_parquet_args_fn
        self.arrow_parquet_args = arrow_parquet_args
        self.num_rows_per_file = num_rows_per_file

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            file_format="parquet",
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        import pyarrow.parquet as pq

        blocks = list(blocks)

        if all(BlockAccessor.for_block(block).num_rows() == 0 for block in blocks):
            return "skip"

        filename = self.filename_provider.get_filename_for_block(
            blocks[0], ctx.task_idx, 0
        )
        write_path = posixpath.join(self.path, filename)

        def write_blocks_to_path():
            with self.open_output_stream(write_path) as file:
                schema = BlockAccessor.for_block(blocks[0]).to_arrow().schema
                with pq.ParquetWriter(file, schema) as writer:
                    for block in blocks:
                        table = BlockAccessor.for_block(block).to_arrow()
                        writer.write_table(table)

        logger.debug(f"Writing {write_path} file.")
        call_with_retry(
            write_blocks_to_path,
            description=f"write '{write_path}'",
            match=DataContext.get_current().write_file_retry_on_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

        return "ok"

    @property
    def num_rows_per_write(self) -> Optional[int]:
        return self.num_rows_per_file
