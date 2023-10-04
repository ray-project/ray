from io import BytesIO
from typing import TYPE_CHECKING

from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class BinaryDatasource(FileBasedDatasource):
    """Binary datasource, for reading and writing binary files.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import BinaryDatasource
        >>> source = BinaryDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [b"file_data", ...]
    """

    _COLUMN_NAME = "bytes"

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

        builder = ArrowBlockBuilder()
        if include_paths:
            item = {self._COLUMN_NAME: data, "path": path}
        else:
            item = {self._COLUMN_NAME: data}
        builder.add(item)
        return builder.build()

    def _rows_per_file(self):
        return 1
