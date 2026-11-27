from typing import TYPE_CHECKING

from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data.datasource.file_based_datasource import FileBasedDatasource

BYTES_COLUMN_NAME = "bytes"

if TYPE_CHECKING:
    import pyarrow


class BinaryDatasource(FileBasedDatasource):
    """Binary datasource, for reading and writing binary files."""

    _COLUMN_NAME = BYTES_COLUMN_NAME

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        data = f.readall()

        builder = ArrowBlockBuilder()
        item = {self._COLUMN_NAME: data}
        builder.add(item)
        yield builder.build()

    def _rows_per_file(self):
        return 1
