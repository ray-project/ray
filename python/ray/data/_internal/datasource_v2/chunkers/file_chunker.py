"""File chunkers for DataSourceV2.

A ``FileChunker`` decides how a single listed file is split into one or
more parallel-read units. The indexer drives the chunker once per file
and emits one manifest row per chunk; downstream the partitioner /
reader carry the per-chunk metadata through to the read task.
"""

import abc
import logging
import math
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    cast,
    get_type_hints,
)

from ray.data._internal.util import MiB, infer_compression
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)


class ChunkMetadata(TypedDict):
    """Base interface for chunk metadata types."""

    pass


_ChunkMetadataT = TypeVar("_ChunkMetadataT", bound=ChunkMetadata)


def create_chunk_metadata(cls: Type[_ChunkMetadataT], **kwargs) -> _ChunkMetadataT:
    """Create a metadata instance with validation, ensure the keys are correct."""
    required_keys = list(get_type_hints(cls).keys())

    missing_keys = [key for key in required_keys if key not in kwargs]
    if missing_keys:
        raise ValueError(f"Missing required keys: {missing_keys}")

    extra_keys = [key for key in kwargs if key not in required_keys]
    if extra_keys:
        raise ValueError(f"Unexpected keys: {extra_keys}")

    return cast(_ChunkMetadataT, kwargs)


class LineDelimitedFileChunkMetadata(ChunkMetadata):
    """Metadata for line-delimited file chunks."""

    chunk_byte_start_idx: int
    chunk_byte_end_idx: int


class ParquetFileChunkMetadata(ChunkMetadata):
    """Metadata for Parquet file chunks.

    A chunk is an explicit, half-open range of consecutive row groups
    ``[row_group_start, row_group_end)`` within a single file, computed at
    listing time from the file's footer. The reader slices the fragment to
    exactly this range — no estimation or read-time reconciliation.
    """

    row_group_start: int  # inclusive
    row_group_end: int  # exclusive


@DeveloperAPI
class FileChunker(abc.ABC):
    """Abstract base class for chunking files into smaller pieces for parallel processing.

    File chunkers determine how large files should be split into chunks that can be
    processed in parallel. Different file formats may require different chunking strategies.

    For example:
    - Line-delimited files (JSONL, CSV) can be chunked by byte ranges
    - Parquet files can be chunked by row groups
    """

    # Whether ``generate_chunk_metadatas`` performs file I/O (e.g. reading a
    # Parquet footer). When True, the indexer fans chunking across its thread
    # pool so the per-file reads parallelize even for a single input
    # directory. When False, the indexer chunks inline (no thread hand-off).
    reads_file_metadata: bool = False

    @abc.abstractmethod
    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        """Generate metadata for file chunks.

        Args:
            path: The file path being chunked.
            file_size: The total size in bytes of the file to be chunked.
            filesystem: PyArrow filesystem used to read per-file metadata
                (e.g. the Parquet footer). Ignored by chunkers that do not
                read file metadata.

        Returns:
            An iterable of tuples containing (metadata, chunk_size) where metadata
            describes the chunk and chunk_size is the size of the chunk in bytes.
            Metadata can be None for chunks that don't require metadata
            (e.g., whole file processing).
        """
        ...


@DeveloperAPI
class WholeFileChunker(FileChunker):
    """File chunker that treats the whole file as a single chunk.

    This chunker is used when files should be processed as a single unit,
    typically for smaller files or when the file format doesn't support
    efficient chunking (e.g., compressed files).

    Yields a single chunk with no metadata, indicating the entire file
    should be processed as one unit.
    """

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        yield None, file_size


@DeveloperAPI
class LineDelimitedFileChunker(FileChunker):
    """File chunker for line-delimited files (JSONL, CSV, TSV, etc.).

    This chunker splits files into fixed-size byte chunks (default: 256 MiB)
    and provides metadata about the byte ranges for each chunk. The actual
    line boundaries are handled by the reader to ensure complete records.
    """

    _CHUNK_BYTE_SIZE = 256 * MiB  # 256 MiB

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        compression = infer_compression(path)
        if compression is not None:
            yield None, file_size
        else:
            num_chunks = math.ceil(file_size / self._CHUNK_BYTE_SIZE)
            for chunk_idx in range(num_chunks):
                chunk_start = self._CHUNK_BYTE_SIZE * chunk_idx
                chunk_end = min(self._CHUNK_BYTE_SIZE * (chunk_idx + 1), file_size)
                chunk_size = chunk_end - chunk_start
                yield (
                    create_chunk_metadata(
                        LineDelimitedFileChunkMetadata,
                        chunk_byte_start_idx=chunk_start,
                        chunk_byte_end_idx=chunk_end,
                    ),
                    chunk_size,
                )


@DeveloperAPI
class ParquetFileChunker(FileChunker):
    """File chunker for Parquet files.

    Reads each file's footer at listing time and chunks on **true row-group
    boundaries**: consecutive row groups are bundled into a chunk until the
    bundle's on-disk size reaches ``target_chunk_size`` (always at least one
    row group per chunk). Each chunk carries an explicit half-open row-group
    range, so the reader slices to exactly those row groups with no
    estimation or read-time reconciliation, and the listing stage never
    produces empty read tasks.

    The row group is Parquet's atomic read unit, so a chunk can never be
    smaller than a single row group. With the default target (which falls
    back to ``DataContext.target_min_block_size``), a file's row groups map
    1:1 to chunks unless they are smaller than the target, in which case
    consecutive small row groups are bundled to avoid an excessive number of
    tiny chunks.
    """

    # Hard fallback used only when neither an explicit target nor the
    # DataContext size knobs are set.
    _FALLBACK_TARGET_CHUNK_SIZE = 1 * MiB

    # Footer reads are file I/O — let the indexer parallelize them.
    reads_file_metadata: bool = True

    def __init__(self, target_chunk_size: Optional[int] = None):
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        self._target_chunk_size = (
            target_chunk_size
            or ctx.parquet_chunker_target_chunk_size
            or ctx.target_min_block_size
            or self._FALLBACK_TARGET_CHUNK_SIZE
        )

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        import pyarrow.parquet as pq

        try:
            # Reads only the Parquet footer (file metadata), not data.
            metadata = pq.read_metadata(path, filesystem=filesystem)
        except Exception as e:
            # Corrupt / unreadable footer (or a non-Parquet file that slipped
            # through). Fall back to a single whole-file chunk so the file is
            # still read rather than dropped.
            logger.debug(
                "Could not read Parquet footer for chunking (%s): %s; "
                "falling back to a whole-file chunk.",
                path,
                e,
            )
            yield None, file_size
            return

        num_row_groups = metadata.num_row_groups
        if num_row_groups == 0:
            yield None, file_size
            return

        # Greedily bundle consecutive row groups until the running on-disk
        # size reaches the target. Always emit at least one row group per
        # chunk (the atomic read unit).
        start = 0
        running_size = 0
        for rg_idx in range(num_row_groups):
            rg_size = metadata.row_group(rg_idx).total_byte_size
            if running_size > 0 and running_size + rg_size > self._target_chunk_size:
                yield (
                    create_chunk_metadata(
                        ParquetFileChunkMetadata,
                        row_group_start=start,
                        row_group_end=rg_idx,
                    ),
                    running_size,
                )
                start = rg_idx
                running_size = 0
            running_size += rg_size

        # Flush the final bundle.
        yield (
            create_chunk_metadata(
                ParquetFileChunkMetadata,
                row_group_start=start,
                row_group_end=num_row_groups,
            ),
            running_size,
        )
