from typing import TYPE_CHECKING, List

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.util.annotations import PublicAPI


if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class TextDatasource(BinaryDatasource):
    """Text datasource, for reading and writing text files."""

    _COLUMN_NAME = "text"

    def _read_file(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> List[str]:
        _, data = super()._read_file_as_binary(f, path, **reader_args)

        builder = DelegatingBlockBuilder()

        drop_empty_lines = reader_args["drop_empty_lines"]
        lines = data.decode(reader_args["encoding"]).split("\n")
        for line in lines:
            if drop_empty_lines and line.strip() == "":
                continue
            item = {self._COLUMN_NAME: line}
            builder.add(item)

        block = builder.build()
        return block

    def _rows_per_file(self):
        return None
