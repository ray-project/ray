import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


class _ParquetDatasink(BlockBasedFileDatasink):
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
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.arrow_parquet_args_fn = arrow_parquet_args_fn
        self.arrow_parquet_args = arrow_parquet_args

        super().__init__(
            path,
            num_rows_per_file=num_rows_per_file,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="parquet",
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        import pyarrow.parquet as pq

        writer_args = _resolve_kwargs(
            self.arrow_parquet_args_fn, **self.arrow_parquet_args
        )
        pq.write_table(block.to_arrow(), file, **writer_args)
