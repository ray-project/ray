"""File chunkers for DataSourceV2.

A ``FileChunker`` decides how a single listed file is split into one or
more parallel-read units. The indexer drives the chunker once per file
and emits one manifest row per chunk; downstream the partitioner /
reader carry the per-chunk metadata through to the read task.
"""

import abc
import math
from typing import (
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


class ParquetRowGroupChunkMetadata(ChunkMetadata):
    """Metadata for a Parquet chunk described by explicit row-group indices.

    Produced by the footer-based chunking path (``ListFiles`` reads each file's
    footer, prunes/bin-packs its row groups, and emits one manifest row per file
    per bin), so it carries the exact physical row groups the reader should scan
    for the file in that bin -- no size-based reconciliation needed.

    ``row_group_ids`` are physical row-group indices into the file; any
    coalescing/splitting the bin packer applied is already expanded away here.
    ``num_rows`` is the summed footer row count of those groups (for sizing /
    limit accounting). ``uncompressed_size`` is their summed, projection-scoped
    uncompressed byte size, carried so the reader can size batches without
    re-reading the footer ``ListFiles`` already read.
    """

    row_group_ids: Tuple[int, ...]
    num_rows: int
    uncompressed_size: int
    # Whether every row in these groups survives the pushed predicate. Only
    # exact-survivor counts may drive limit push-down.
    fully_matched: bool
    # Per-physical-row-group breakdown, in ``row_group_ids`` order. Populated
    # for coalesced runs so a partitioner can split at exact boundaries.
    rg_sizes: Tuple[int, ...]
    rg_rows: Tuple[int, ...]


@DeveloperAPI
class FileChunker(abc.ABC):
    """Abstract base class for chunking files into smaller pieces for parallel processing.

    File chunkers determine how large files should be split into chunks that can be
    processed in parallel. Different file formats may require different chunking strategies.

    For example:
    - Line-delimited files (JSONL, CSV) can be chunked by byte ranges
    - Parquet files can be chunked by row groups
    """

    @property
    def requires_file_io(self) -> bool:
        """Whether chunk planning opens and reads file contents.

        Most chunkers derive ranges from the listed file size alone. Formats
        that inspect headers or footers override this so ``ListFiles`` can use
        normal backpressure instead of its metadata-only fast path.
        """
        return False

    @abc.abstractmethod
    def generate_chunk_metadatas(
        self, path: str, file_size: int
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        """Generate metadata for file chunks.

        Args:
            path: The file path being chunked.
            file_size: The total size in bytes of the file to be chunked.

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
        self, path: str, file_size: int
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
        self, path: str, file_size: int
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
