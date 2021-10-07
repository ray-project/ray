from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import (FileBasedDatasource)


class JSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON files.

    Examples:
        >>> source = JSONDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [ArrowRow({"a": 1, "b": "foo"}), ...]
    """

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        from pyarrow import json

        read_options = reader_args.pop(
            "read_options", json.ReadOptions(use_threads=False))
        return json.read_json(f, read_options=read_options, **reader_args)

    def _write_block(self, f: "pyarrow.NativeFile", block: BlockAccessor,
                     **writer_args):
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)
        block.to_pandas().to_json(f, orient=orient, lines=lines, **writer_args)

    def _file_format(self):
        return "json"
