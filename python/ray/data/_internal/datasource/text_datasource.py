from typing import TYPE_CHECKING, Iterator, List

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class TextDatasource(FileBasedDatasource):
    """Text datasource, for reading and writing text files."""

    _COLUMN_NAME = "text"

    def __init__(
        self,
        paths: List[str],
        *,
        drop_empty_lines: bool = False,
        encoding: str = "utf-8",
        **file_based_datasource_kwargs
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self.drop_empty_lines = drop_empty_lines
        self.encoding = encoding

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        data = f.readall()

        builder = DelegatingBlockBuilder()

        lines = data.decode(self.encoding).split("\n")
        for line in lines:
            if self.drop_empty_lines and line.strip() == "":
                continue
            item = {self._COLUMN_NAME: line}
            builder.add(item)

        block = builder.build()
        yield block
