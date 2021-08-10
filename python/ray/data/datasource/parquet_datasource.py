import logging
from typing import Optional, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.data.impl.arrow_block import ArrowRow
from ray.data.impl.block_list import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.file_based_datasource import (
    _resolve_paths_and_filesystem)

logger = logging.getLogger(__name__)


class ParquetDatasource(Datasource[ArrowRow]):
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
            **reader_args) -> List[ReadTask]:
        """Creates and returns read tasks for a file-based datasource.
        """
        from ray import cloudpickle
        import pyarrow.parquet as pq
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        pq_ds = pq.ParquetDataset(
            paths, **dataset_kwargs, filesystem=filesystem,
            use_legacy_dataset=False)
        if schema is None:
            schema = pq_ds.schema
        pieces = pq_ds.pieces

        def read_pieces(serialized_pieces: List[str]):
            # Implicitly trigger S3 subsystem initialization by importing
            # pyarrow.fs.
            import pyarrow.fs  # noqa: F401

            # Deserialize after loading the filesystem class.
            pieces: List["pyarrow._dataset.ParquetFileFragment"] = [
                cloudpickle.loads(p) for p in serialized_pieces
            ]

            import pyarrow as pa
            from pyarrow.dataset import _get_partition_keys

            logger.debug(f"Reading {len(pieces)} parquet pieces")
            use_threads = reader_args.pop("use_threads", False)
            tables = []
            for piece in pieces:
                table = piece.to_table(
                    use_threads=use_threads, columns=columns, schema=schema,
                    **reader_args)
                part = _get_partition_keys(piece.partition_expression)
                if part:
                    for col, value in part.items():
                        table = table.set_column(
                            table.schema.get_field_index(col), col,
                            pa.array([value] * len(table)))
                tables.append(table)
            if len(tables) > 1:
                table = pa.concat_tables(tables)
            else:
                table = tables[0]
            return table

        read_tasks = []
        for pieces_ in np.array_split(pieces, parallelism):
            if len(pieces_) == 0:
                continue
            metadata = _get_metadata(pieces_, schema)
            pieces_ = [cloudpickle.dumps(p) for p in pieces_]
            read_tasks.append(
                ReadTask(lambda pieces=pieces_: read_pieces(pieces), metadata))

        return read_tasks


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
