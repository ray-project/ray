"""Per-row chunk metadata carried by a ``FileManifest``.

A manifest row is a whole file (``chunk_metadata`` is ``None``) or a part of
one that an indexer chose to read separately, such as a run of Parquet row
groups emitted by ``FooterFileIndexer``. The metadata names that part so the
partitioner and reader never re-derive it.
"""

from typing import Tuple, Type, TypedDict, TypeVar, cast, get_type_hints


class ChunkMetadata(TypedDict):
    """Base interface for chunk metadata types."""

    pass


_ChunkMetadataT = TypeVar("_ChunkMetadataT", bound=ChunkMetadata)


class LineDelimitedFileChunkMetadata(ChunkMetadata):
    """A byte range of a line-delimited file (CSV, JSONL).

    Produced by indexers that split large uncompressed files for parallel
    reads. The range is aligned to record boundaries by the indexer; the reader
    aligns it again before parsing, so a stale or unaligned range is never
    read twice or skipped.
    """

    chunk_byte_start_idx: int
    chunk_byte_end_idx: int


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
