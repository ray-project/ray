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
        file_format: str = "npy",
        **file_datasink_kwargs,
    ):
        self.column = column

        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        value = block.to_numpy(self.column)
        np.save(file, value)
