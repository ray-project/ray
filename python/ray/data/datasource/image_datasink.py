import io
from typing import Any, Dict, Optional

import pyarrow

from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_datasink import RowBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class _ImageDatasink(RowBasedFileDatasink):
    def __init__(
        self, path: str, column: str, file_format: str, **file_datasink_kwargs
    ):
        self.column = column
        self.file_format = file_format

        super().__init__(path, **file_datasink_kwargs)

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        from PIL import Image

        image = Image.fromarray(row[self.column])
        buffer = io.BytesIO()
        image.save(buffer, format=self.file_format)
        file.write(buffer.getvalue())
