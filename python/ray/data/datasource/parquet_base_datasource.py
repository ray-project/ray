import logging
from typing import (
    Dict,
    Any,
    Callable,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)

logger = logging.getLogger(__name__)


class ParquetBaseDatasource(FileBasedDatasource):
    """Minimal Parquet datasource, for reading and writing Parquet files."""

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        import pyarrow.parquet as pq

        use_threads = reader_args.pop("use_threads", False)
        return pq.read_table(f, use_threads=use_threads, **reader_args)

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # Parquet requires `open_input_file` due to random access reads
        return filesystem.open_input_file(path, **open_args)

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        import pyarrow.parquet as pq

        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        pq.write_table(block.to_arrow(), f, **writer_args)

    def _file_format(self) -> str:
        return "parquet"
