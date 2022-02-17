import io
from typing import TYPE_CHECKING

import pyarrow
import snappy

if TYPE_CHECKING:
    import pyarrow

from ray.data.datasource.file_based_datasource import FileBasedDatasource
from pyarrow.fs import FileSystem


class SnappyTextDatasource(FileBasedDatasource):
    """Snappy datasource, for reading snappy compressed text files.
    Examples:
        >>> source = SnappyTextDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [b"file_data", ...]
    """

    def _read_file(
        self,
        f: "pyarrow.NativeFile",
        path: str,
        filesystem: "pyarrow.fs.FileSystem",
        **reader_args
    ):
        include_paths = reader_args.pop("include_paths", False)
        rawbytes = io.BytesIO()

        if isinstance(filesystem, pyarrow.fs.HadoopFileSystem):
            snappy.hadoop_snappy.stream_decompress(src=f, dst=rawbytes)
        else:
            snappy.stream_decompress(src=f, dst=rawbytes)

        uncompressed_bytes = rawbytes.getvalue()

        if include_paths:
            return [(path, uncompressed_bytes)]
        else:
            return [uncompressed_bytes]

    def _rows_per_file(self):
        return 1
