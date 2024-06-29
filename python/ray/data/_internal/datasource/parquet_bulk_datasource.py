import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


class ParquetBulkDatasource(FileBasedDatasource):
    """Minimal Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSIONS = ["parquet"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        read_table_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if read_table_args is None:
            read_table_args = {}

        self.read_table_args = read_table_args

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `FileBasedDatasource` method.
        """
        return "ParquetBulk"

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        import pyarrow.parquet as pq

        use_threads = self.read_table_args.pop("use_threads", False)
        yield pq.read_table(f, use_threads=use_threads, **self.read_table_args)

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # Parquet requires `open_input_file` due to random access reads
        return filesystem.open_input_file(path, **open_args)
