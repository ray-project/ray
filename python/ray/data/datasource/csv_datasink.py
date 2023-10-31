from typing import Any, Callable, Dict, Optional

import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class _CSVDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_csv_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if arrow_csv_args is None:
            arrow_csv_args = {}

        self.arrow_csv_args_fn = arrow_csv_args_fn
        self.arrow_csv_args = arrow_csv_args

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="csv",
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        from pyarrow import csv

        writer_args = _resolve_kwargs(self.arrow_csv_args_fn, **self.arrow_csv_args)
        write_options = writer_args.pop("write_options", None)
        csv.write_csv(block.to_arrow(), file, write_options, **writer_args)
