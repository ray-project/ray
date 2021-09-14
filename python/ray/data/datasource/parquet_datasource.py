import logging
from typing import Callable, Optional, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource, _resolve_paths_and_filesystem)
from ray.data.impl.block_list import BlockMetadata
from ray.data.impl.util import _check_pyarrow_version

logger = logging.getLogger(__name__)


class ParquetDatasource(FileBasedDatasource):
    """Parquet datasource, for reading and writing Parquet files.

    Examples:
        >>> source = ParquetDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [ArrowRow({"a": 1, "b": "foo"}), ...]
    """

    def prepare_read(
            self,
            parallelism: int,
            paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None,
            columns: Optional[List[str]] = None,
            schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
            _block_udf: Optional[Callable[[Block], Block]] = None,
            **reader_args) -> List[ReadTask]:
        """Creates and returns read tasks for a Parquet file-based datasource.
        """
        # NOTE: We override the base class FileBasedDatasource.prepare_read
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side (do_write), however.
        _check_pyarrow_version()
        from ray import cloudpickle
        import pyarrow as pa
        import pyarrow.parquet as pq
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        pq_ds = pq.ParquetDataset(
            paths,
            **dataset_kwargs,
            filesystem=filesystem,
            use_legacy_dataset=False)
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema([schema.field(column) for column in columns],
                               schema.metadata)

        def read_pieces(serialized_pieces: List[str]):
            # Implicitly trigger S3 subsystem initialization by importing
            # pyarrow.fs.
            import pyarrow.fs  # noqa: F401

            # Deserialize after loading the filesystem class.
            pieces: List["pyarrow._dataset.ParquetFileFragment"] = [
                cloudpickle.loads(p) for p in serialized_pieces
            ]

            # Ensure that we're reading at least one dataset fragment.
            assert len(pieces) > 0

            from pyarrow.dataset import _get_partition_keys

            logger.debug(f"Reading {len(pieces)} parquet pieces")
            use_threads = reader_args.pop("use_threads", False)
            tables = []
            for piece in pieces:
                table = piece.to_table(
                    use_threads=use_threads,
                    columns=columns,
                    schema=schema,
                    **reader_args)
                part = _get_partition_keys(piece.partition_expression)
                if part:
                    for col, value in part.items():
                        table = table.set_column(
                            table.schema.get_field_index(col), col,
                            pa.array([value] * len(table)))
                # If the table is empty, drop it.
                if table.num_rows > 0:
                    tables.append(table)
            if len(tables) > 1:
                table = pa.concat_tables(tables, promote=True)
            elif len(tables) == 1:
                table = tables[0]
            if _block_udf is not None:
                table = _block_udf(table)
            # If len(tables) == 0, all fragments were empty, and we return the
            # empty table from the last fragment.
            return table

        if _block_udf is not None:
            # Try to infer dataset schema by passing dummy table through UDF.
            dummy_table = schema.empty_table()
            try:
                inferred_schema = _block_udf(dummy_table).schema
                inferred_schema = inferred_schema.with_metadata(
                    schema.metadata)
            except Exception:
                logger.debug(
                    "Failed to infer schema of dataset by passing dummy table "
                    "through UDF due to the following exception:",
                    exc_info=True)
                inferred_schema = schema
        else:
            inferred_schema = schema
        read_tasks = []
        for pieces in np.array_split(pq_ds.pieces, parallelism):
            if len(pieces) == 0:
                continue
            metadata = _get_metadata(pieces, inferred_schema)
            pieces = [cloudpickle.dumps(p) for p in pieces]
            read_tasks.append(
                ReadTask(
                    lambda pieces_=pieces: read_pieces(pieces_), metadata))

        return read_tasks

    def _write_block(self, f: "pyarrow.NativeFile", block: BlockAccessor,
                     **writer_args):
        import pyarrow.parquet as pq

        pq.write_table(block.to_arrow(), f, **writer_args)

    def _file_format(self):
        return "parquet"


def _get_metadata(pieces: List["pyarrow._dataset.ParquetFileFragment"],
                  schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None):
    piece_metadata = []
    for p in pieces:
        try:
            piece_metadata.append(p.metadata)
        except AttributeError:
            break
    input_files = [p.path for p in pieces]
    if len(piece_metadata) == len(pieces):
        # Piece metadata was available, construct a normal
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=sum(m.num_rows for m in piece_metadata),
            size_bytes=sum(
                sum(
                    m.row_group(i).total_byte_size
                    for i in range(m.num_row_groups)) for m in piece_metadata),
            schema=schema,
            input_files=input_files)
    else:
        # Piece metadata was not available, construct an empty
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=schema,
            input_files=input_files)
    return block_metadata
