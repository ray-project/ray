from typing import TYPE_CHECKING, List, Optional

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
        block = super()._read_file(f, path, **reader_args)
        assert len(block) == 1
        data = block[0]

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

    def _convert_block_to_tabular_block(
        self,
        block: List[str],
        column_name: Optional[str] = None,
    ) -> "pyarrow.Table":
        import pyarrow as pa

        assert isinstance(block, pa.Table)
        return block

    def _rows_per_file(self):
        return None
