from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.data.datasource.file_based_datasource import FileBasedDatasource


class BinaryDatasource(FileBasedDatasource):
    """Binary datasource, for reading and writing binary files.

    Examples:
        >>> source = BinaryDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [b"file_data", ...]
    """

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        from pyarrow.fs import HadoopFileSystem

        include_paths = reader_args.pop("include_paths", False)
        if reader_args.get("compression") == "snappy":
            import snappy

            filesystem = reader_args.get("filesystem", None)
            rawbytes = BytesIO()

            if isinstance(filesystem, HadoopFileSystem):
                snappy.hadoop_snappy.stream_decompress(src=f, dst=rawbytes)
            else:
                snappy.stream_decompress(src=f, dst=rawbytes)

            data = rawbytes.getvalue()
        else:
            data = f.readall()
        if include_paths:
            return [(path, data)]
        else:
            return [data]

    def _rows_per_file(self):
        return 1
