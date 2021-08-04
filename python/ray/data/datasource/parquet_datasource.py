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

        paths, file_infos, filesystem = _resolve_paths_and_filesystem(
            paths, filesystem)
        file_sizes = [file_info.size for file_info in file_infos]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        pq_ds = pq.ParquetDataset(
            paths, **dataset_kwargs, filesystem=filesystem)
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
            logger.debug(f"Reading {len(pieces)} parquet pieces")
            use_threads = reader_args.pop("use_threads", False)
            tables = [
                piece.to_table(
                    use_threads=use_threads, columns=columns, **reader_args)
                for piece in pieces
            ]
            if len(tables) > 1:
                table = pa.concat_tables(tables)
            else:
                table = tables[0]
            return table

        read_tasks = []
        for pieces, file_sizes in zip(
                np.array_split(pieces, parallelism),
                np.array_split(file_sizes, parallelism)):
            if len(pieces) == 0:
                continue
            metadata = _get_metadata(pieces, file_sizes, schema)
            pieces = [cloudpickle.dumps(p) for p in pieces]
            read_tasks.append(
                ReadTask(lambda pieces=pieces: read_pieces(pieces), metadata))

        return read_tasks


def _get_metadata(pieces: List["pyarrow._dataset.ParquetFileFragment"],
                  file_sizes: List[int],
                  schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None):
    piece_metadata = []
    for p in pieces:
        try:
            piece_metadata.append(p.metadata)
        except AttributeError:
            break
    input_files = [p.path for p in pieces]
    if len(piece_metadata) == len(pieces):
        # Piece metadata was available, constructo a normal
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=sum(m.num_rows for m in piece_metadata),
            size_bytes=sum(
                sum(
                    m.row_group(i).total_byte_size
                    for i in range(m.num_row_groups)) for m in piece_metadata),
            schema=piece_metadata[0].schema.to_arrow_schema(),
            input_files=input_files)
    else:
        # Piece metadata was not available, construct an empty
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=None,
            size_bytes=sum(file_sizes),
            schema=schema,
            input_files=input_files)
    return block_metadata
