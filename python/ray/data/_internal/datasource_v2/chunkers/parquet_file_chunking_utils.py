"""Parquet chunk helpers for DataSourceV2.

Maps ``ParquetRowGroupChunkMetadata`` (the explicit surviving row groups a bin
assigns to a file) to PyArrow ``ParquetFileFragment`` subsets for reading.
"""
from typing import Callable, Iterable, List, Tuple, TypeVar

import pyarrow.dataset as pds

from ray._common.retry import call_with_retry

R = TypeVar("R")


def _with_io_retry(f: Callable[[], R], description: str) -> R:
    """Run ``f``, retrying the transient IO errors configured on the context.

    ``ParquetFileFragment.subset`` and ``.metadata`` both open the file to read
    its footer, so on remote storage they fail with the same transient errors
    (S3 timeouts, throttling) the rest of the read path already retries.
    """
    from ray.data.context import DataContext

    return call_with_retry(
        f,
        description=description,
        match=DataContext.get_current().retried_io_errors,
    )


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

    def _subset(rg_ids: List[int]) -> pds.ParquetFileFragment:
        return _with_io_retry(
            lambda: fragment.subset(row_group_ids=rg_ids),
            f"subset row groups {rg_ids} of {fragment.path}",
        )

    if not per_row_group_offsets:
        return [(_subset(ids), 0)]

    metadata = _with_io_retry(
        lambda: fragment.metadata, f"read Parquet footer for {fragment.path}"
    )
    # Cumulative pre-filter row offset at the start of each physical row group.
    prefix = [0] * (metadata.num_row_groups + 1)
    for i in range(metadata.num_row_groups):
        prefix[i + 1] = prefix[i] + metadata.row_group(i).num_rows
    return [(_subset([rg_id]), prefix[rg_id]) for rg_id in ids]
