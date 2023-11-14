from typing import TYPE_CHECKING

from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class BinaryDatasource(FileBasedDatasource):
    """Binary datasource, for reading and writing binary files."""

    _COLUMN_NAME = "bytes"

    def _read_file(self, f: "pyarrow.NativeFile", path: str):
        data = f.readall()

        builder = ArrowBlockBuilder()
        item = {self._COLUMN_NAME: data, "path": path}
        builder.add(item)
        return builder.build()

    def _rows_per_file(self):
        return 1
