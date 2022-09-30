from io import BytesIO
from typing import TYPE_CHECKING, List, Union, Tuple, Optional

if TYPE_CHECKING:
    import pyarrow

from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI


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
        if include_paths:
            return [(path, data)]
        else:
            return [data]

    def _convert_block_to_tabular_block(
        self,
        block: List[Union[bytes, Tuple[bytes, str]]],
        column_name: Optional[str] = None,
    ) -> "pyarrow.Table":
        import pyarrow as pa

        if column_name is None:
            column_name = self._COLUMN_NAME

        assert len(block) == 1
        record = block[0]

        if isinstance(record, tuple):
            path, data = record
            return pa.table({column_name: [data], "path": [path]})

        return pa.table({column_name: [record]})

    def _rows_per_file(self):
        return 1
