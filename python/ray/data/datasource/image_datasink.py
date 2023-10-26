import io
from typing import Any, Dict, Optional

import pyarrow

from ray.data.datasource.file_datasink import RowBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class ImageDatasink(RowBasedFileDatasink):
    def __init__(
        self,
        path: str,
        dataset_uuid: str,
        filesystem: Any | None = None,
        try_create_dir: bool = True,
        open_stream_args: Dict[str, Any] | None = None,
        filename_provider: FilenameProvider | None = None,
        file_format: str | None = None,
    ):
        super().__init__(
            path,
            dataset_uuid,
            filesystem,
            try_create_dir,
            open_stream_args,
            filename_provider,
            file_format,
        )

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):

        from PIL import Image

        image = Image.fromarray(row[column])
        buffer = io.BytesIO()
        image.save(buffer, format=file_format)
        f.write(buffer.getvalue())
