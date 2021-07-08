from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.experimental.data.datasource.file_based_datasource import (
    FileBasedDatasource)


class JSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON files.

    Examples:
        >>> source = JSONDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... {"a": 1, "b": "foo"}
    """

    def _read_file(self, f: "pyarrow.NativeFile", **arrow_reader_args):
        from pyarrow import json

        read_options = arrow_reader_args.pop(
            "read_options", json.ReadOptions(use_threads=False))
        return json.read_json(
            f, read_options=read_options, **arrow_reader_args)
