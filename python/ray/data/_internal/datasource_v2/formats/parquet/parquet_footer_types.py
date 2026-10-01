from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from ray.data._internal.datasource_v2.interfaces.file_manifest import ChunkMetadata


@dataclass(frozen=True)
class RowGroupInfo:
    """A chunk of one file: a contiguous run of ``rg_count`` physical row groups.

    ``uncompressed_size`` / ``num_rows`` are summed over the run. For a single
    physical row group (``rg_count == 1``) the run is its own atom and needs no
    breakdown, so ``rg_sizes`` / ``rg_rows`` stay empty; they're populated only
    for coalesced runs (``rg_count > 1``) so the bin packer can split them back
    at exact byte/row boundaries.
    """

    # Start row-group index (== the row group's index when rg_count == 1).
    rg_idx: int
    uncompressed_size: int  # summed over the run
    num_rows: int  # summed over the run
    # True when every row in the run is guaranteed to satisfy the filter (or there
    # is no filter), so ``num_rows`` is an exact survivor count and the limit can
    # be pushed down on it. False for partially-matching groups, whose ``num_rows``
    # overestimates survivors. Coalescing never merges across this flag.
    fully_matched: bool = True
    # Number of consecutive physical row groups this chunk covers.
    rg_count: int = 1
    # Per-physical-row-group uncompressed sizes / row counts, in ``rg_idx`` order.
    # Populated only for coalesced runs (``rg_count > 1``).
    rg_sizes: Tuple[int, ...] = ()
    rg_rows: Tuple[int, ...] = ()


@dataclass(frozen=True)
class FileChunks:
    """The footer-derived chunks for a single file."""

    path: str
    size: int  # on-disk file size, from the file listing
    row_groups: Tuple[RowGroupInfo, ...]


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
