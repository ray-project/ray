import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator

from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


@PublicAPI
class ParquetBaseDatasource(FileBasedDatasource):
    """Minimal Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSION = "parquet"

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `FileBasedDatasource` method.
        """
        return "ParquetBulk"

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

    def _write_blocks(
        self,
        f: "pyarrow.NativeFile",
        blocks: Iterator[Block],
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        row_group_size=None,
        **writer_args,
    ):
        import pyarrow.parquet as pq

        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)

        writer = None
        for block in blocks:
            block = BlockAccessor.for_block(block).to_arrow()
            assert block.num_rows > 0, "Cannot write an empty block."
            if writer is None:
                writer = pq.ParquetWriter(f, block.schema, **writer_args)
            writer.write_table(block, row_group_size=row_group_size)
        writer.close()
