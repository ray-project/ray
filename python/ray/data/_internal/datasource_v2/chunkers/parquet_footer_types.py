from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


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


@dataclass(frozen=True)
class BinItem:
    """One file's row-group chunk placed into a bin.

    Same contiguous-run shape as :class:`RowGroupInfo`; ``path`` is the packer's
    "colour". A (possibly-split) item covers physical row groups
    ``range(rg_idx, rg_idx + rg_count)``.
    """

    path: str  # colour
    rg_idx: int  # start row-group index (see RowGroupInfo.rg_idx)
    uncompressed_size: int
    num_rows: int
    fully_matched: bool = True  # see RowGroupInfo.fully_matched
    rg_count: int = 1  # number of consecutive physical row groups this item covers
    # Per-physical-row-group breakdown (rg_idx order), populated only for
    # coalesced runs (rg_count > 1); lets the packer split at row-group boundaries.
    rg_sizes: Tuple[int, ...] = ()
    rg_rows: Tuple[int, ...] = ()


@dataclass(frozen=True)
class Bin:
    """A sealed bin: a set of row-group chunks (across one or more files) whose
    combined uncompressed size targets one bin budget. Becomes one
    ``FileManifest`` block == one downstream read task."""

    items: Tuple[BinItem, ...]
    total_uncompressed_size: int
