import numpy as np
import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.file_datasink import BlockBasedFileDatasink


class NumpyDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        column: str,
        *,
        file_format: str = "npy",
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        self.column = column

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        value = block.to_numpy(self.column)
        np.save(file, value)
