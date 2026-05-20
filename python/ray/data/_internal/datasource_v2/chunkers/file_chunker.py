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

from ray.data._internal.util import GiB, infer_compression
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


class ParquetFileChunkMetadata(ChunkMetadata):
    """Metadata for Parquet file chunks.

    For a parquet file, the chunks are based on the total size of the file, not on the
    underlying row groups. We will split a file into potentially many chunks of the
    target chunk size. This may correspond to 0, 1, or more row groups per chunk.
    """

    chunk_idx: int
    total_num_chunks: int


@DeveloperAPI
class FileChunker(abc.ABC):
    """Abstract base class for chunking files into smaller pieces for parallel processing.

    File chunkers determine how large files should be split into chunks that can be
    processed in parallel. Different file formats may require different chunking strategies.

    For example:
    - Line-delimited files (JSONL, CSV) can be chunked by byte ranges
    - Parquet files can be chunked by row groups
    """

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

    This chunker splits files into fixed-size byte chunks (default: 256MB)
    and provides metadata about the byte ranges for each chunk. The actual
    line boundaries are handled by the reader to ensure complete records.
    """

    _CHUNK_BYTE_SIZE = 256 * 1024 * 1024  # 256MB

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
                yield create_chunk_metadata(
                    LineDelimitedFileChunkMetadata,
                    chunk_byte_start_idx=chunk_start,
                    chunk_byte_end_idx=chunk_end,
                ), chunk_size


@DeveloperAPI
class ParquetFileChunker(FileChunker):
    """File chunker for Parquet files.

    This chunker splits Parquet files into an estimated number of chunks. We do not
    fetch the metadata for the file, so we may overestimate the number of chunks
    compared to the actual number of underlying row groups. The partitioner creates
    groupings based on these estimates, and the reader fetches the metadata and
    ensures that all row groups are read / any overestimated row groups are ignored.
    """

    # Chosen so that we can effectively chunk files but will not result in OOMs if
    # the compression ratio is high.
    #
    # If the compression ratio is high and this chunk size is large, we end up with
    # larger chunks than we need and reading can OOM. Reducing the chunk size gives
    # better memory performance by reading a smaller fraction of row groups at a time.
    #
    # We also want to keep this large enough such that we do not end up reading too
    # much data if we underestimate the number of chunks. If row groups are larger
    # than the chunk size and we place many of them in the same read task, the total
    # amount of data read might be larger than expected. By increasing the chunk
    # size we are less likely to put many such row groups in the same task.
    _DEFAULT_TARGET_CHUNK_SIZE = 1 * GiB

    def __init__(self, target_chunk_size: Optional[int] = None):
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        if target_chunk_size is not None:
            self._target_chunk_size = target_chunk_size
        elif ctx.parquet_chunker_target_chunk_size is not None:
            self._target_chunk_size = ctx.parquet_chunker_target_chunk_size
        else:
            self._target_chunk_size = self._DEFAULT_TARGET_CHUNK_SIZE

    def generate_chunk_metadatas(
        self, path: str, file_size: int
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        if file_size <= self._target_chunk_size:
            # Do not chunk if the file is smaller than the target chunk size; when
            # we read the file, this prevents additional metadata fetching since we
            # want to read the entire file.
            yield None, file_size
            return

        num_chunks = math.ceil(file_size / self._target_chunk_size)
        for i in range(num_chunks):
            chunk_start = self._target_chunk_size * i
            chunk_end = min(self._target_chunk_size * (i + 1), file_size)
            chunk_size = chunk_end - chunk_start
            yield create_chunk_metadata(
                ParquetFileChunkMetadata,
                chunk_idx=i,
                total_num_chunks=num_chunks,
            ), chunk_size
