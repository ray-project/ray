import io
from typing import Any, Dict

import pyarrow

from ray.data.datasource.file_datasink import RowBasedFileDatasink


class ImageDatasink(RowBasedFileDatasink):
    def __init__(
        self, path: str, column: str, file_format: str, **file_datasink_kwargs
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        self.column = column
        self.file_format = file_format

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        from PIL import Image

        image = Image.fromarray(row[self.column])
        buffer = io.BytesIO()
        image.save(buffer, format=self.file_format)
        file.write(buffer.getvalue())
