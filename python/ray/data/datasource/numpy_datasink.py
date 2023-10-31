from typing import Any, Dict, Optional

import numpy as np
import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class _NumpyDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        column: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        self.column = column

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="npy",
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        value = block.to_numpy(self.column)
        np.save(file, value)
