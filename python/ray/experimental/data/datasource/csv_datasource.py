from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.experimental.data.datasource.file_based_datasource import (
    FileBasedDatasource)


class CSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files.

    Examples:
        >>> source = CSVDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... {"a": 1, "b": "foo"}
    """

    def _read_file(self, f: "pyarrow.NativeFile", **arrow_reader_args):
        from pyarrow import csv

        read_options = arrow_reader_args.pop(
            "read_options", csv.ReadOptions(use_threads=False))
        return csv.read_csv(f, read_options=read_options, **arrow_reader_args)
