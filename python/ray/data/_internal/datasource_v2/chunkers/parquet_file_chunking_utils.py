"""Parquet chunk helpers for DataSourceV2.

Maps ``ParquetRowGroupChunkMetadata`` (the explicit surviving row groups a bin
assigns to a file) to PyArrow ``ParquetFileFragment`` subsets for reading.
"""
from typing import Iterable, List, Tuple

import pyarrow.dataset as pds


def _fragments_from_row_group_ids(
    fragment: pds.ParquetFileFragment,
    row_group_ids: Iterable[int],
    *,
    per_row_group_offsets: bool,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Slice ``fragment`` to the explicit physical ``row_group_ids`` of one bin.

    Used by the footer-based chunking path, where ``ParquetRowGroupChunkMetadata``
    names the exact surviving row groups for a file (predicate pruning + bin
    packing already happened upstream), so no size-based reconciliation is needed.

    When ``per_row_group_offsets`` is False (the common case) the file's groups
    are scanned together as a single sub-fragment with a row offset of 0 -- this
    lets PyArrow coalesce reads across the groups. When True (``include_row_hash``
    is on), one sub-fragment per row group is returned, each paired with its
    cumulative pre-filter row offset within the file, so row hashes stay unique
    and match the physical row positions even when pruned groups make the
    surviving set non-contiguous.
    """
    ids = sorted(row_group_ids)
    if not ids:
        return []
    if not per_row_group_offsets:
        return [(fragment.subset(row_group_ids=ids), 0)]

    metadata = fragment.metadata
    # Cumulative pre-filter row offset at the start of each physical row group.
    prefix = [0] * (metadata.num_row_groups + 1)
    for i in range(metadata.num_row_groups):
        prefix[i + 1] = prefix[i] + metadata.row_group(i).num_rows
    return [(fragment.subset(row_group_ids=[rg_id]), prefix[rg_id]) for rg_id in ids]
