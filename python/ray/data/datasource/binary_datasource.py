from io import BytesIO
from typing import TYPE_CHECKING

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import BYTES_COLUMN_NAME
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

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        path, data = self._read_file_as_binary(f, path, **reader_args)
        include_paths = reader_args.pop("include_paths", False)
        builder = DelegatingBlockBuilder()
        if include_paths:
            item = {BYTES_COLUMN_NAME: data, "path": path}
        else:
            item = {BYTES_COLUMN_NAME: data}
        builder.add(item)
        block = builder.build()
        return block

    def _read_file_as_binary(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        """Read the file as binary data blob."""
        from pyarrow.fs import HadoopFileSystem

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
        return (path, data)

    def _rows_per_file(self):
        return 1
