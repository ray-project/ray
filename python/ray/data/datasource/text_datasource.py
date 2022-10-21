from typing import TYPE_CHECKING, List, Optional

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
        block = super()._read_file(f, path, **reader_args)
        assert len(block) == 1
        data = block[0]

        lines = data.decode(reader_args["encoding"]).split("\n")
        if reader_args["drop_empty_lines"]:
            lines = [line for line in lines if line.strip() != ""]
        return lines

    def _convert_block_to_tabular_block(
        self,
        block: List[str],
        column_name: Optional[str] = None,
    ) -> "pyarrow.Table":
        import pyarrow as pa

        if column_name is None:
            column_name = self._COLUMN_NAME

        return pa.table({column_name: block})

    def _rows_per_file(self):
        return None
