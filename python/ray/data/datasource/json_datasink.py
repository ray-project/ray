from typing import Any, Callable, Dict, Optional

import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class _JSONDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        pandas_json_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        pandas_json_args: Optional[Dict[str, Any]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if pandas_json_args is None:
            pandas_json_args = {}

        self.pandas_json_args_fn = pandas_json_args_fn
        self.pandas_json_args = pandas_json_args

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="json",
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        writer_args = _resolve_kwargs(self.pandas_json_args_fn, **self.pandas_json_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        block.to_pandas().to_json(file, orient=orient, lines=lines, **writer_args)
