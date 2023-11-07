from typing import TYPE_CHECKING, List, Union

from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class BinaryDatasource(FileBasedDatasource):
    """Binary datasource, for reading and writing binary files."""

    _COLUMN_NAME = "bytes"

    def __init__(
        self,
        paths: Union[str, List[str]],
        include_paths: bool = False,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self.include_paths = include_paths

    def _read_file(self, f: "pyarrow.NativeFile", path: str):
        data = f.readall()

        builder = ArrowBlockBuilder()
        if self.include_paths:
            item = {self._COLUMN_NAME: data, "path": path}
        else:
            item = {self._COLUMN_NAME: data}
        builder.add(item)
        return builder.build()

    def _rows_per_file(self):
        return 1
