import io
from typing import Any, Dict, Optional

import pyarrow

from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_datasink import RowBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider


class _ImageDatasink(RowBasedFileDatasink):
    def __init__(
        self,
        path: str,
        column: str,
        file_format: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        self.column = column
        self.file_format = file_format

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format=file_format,
        )

    def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
        from PIL import Image

        image = Image.fromarray(row[self.column])
        buffer = io.BytesIO()
        image.save(buffer, format=self.file_format)
        file.write(buffer.getvalue())
