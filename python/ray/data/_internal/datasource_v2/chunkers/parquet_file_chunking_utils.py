"""Parquet chunk helpers for DataSourceV2.

Two chunking strategies live here. The size-based one maps planner chunk
metadata (``ParquetFileChunkMetadata``) to row-group ranges. The footer-based
one maps ``ParquetRowGroupChunkMetadata`` -- the explicit surviving row groups a
bin assigns to a file -- straight to PyArrow ``ParquetFileFragment`` subsets.
Both produce ``(fragment, file_row_offset)`` pairs for the reader.
"""
from typing import Callable, Iterable, List, Optional, Tuple, TypeVar

import pyarrow.dataset as pds

from ray._common.retry import call_with_retry
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunkMetadata,
)

R = TypeVar("R")


def _calculate_row_group_range(
    chunk_idx: int, total_num_chunks: int, total_row_groups: int
) -> Optional[Tuple[int, int]]:
    """Compute the half-open row-group range for a given chunk.

    Distributes row groups as evenly as possible across chunks. If row groups
    don't divide evenly, earlier chunks get the extra row groups.

    Example:
        - 10 row groups, 3 chunks -> [0:4), [4:7), [7:10)
        - 11 row groups, 3 chunks -> [0:4), [4:8), [8:11)

    Args:
        chunk_idx: Index of the current chunk (0-based).
        total_num_chunks: Total number of chunks.
        total_row_groups: Total number of row groups to distribute.

    Returns:
        Tuple ``(start_row_group, end_row_group)`` where ``end`` is exclusive,
        or ``None`` if ``chunk_idx`` falls beyond the actual number of row
        groups (i.e. the planner over-estimated the chunk count).
    """
    assert (
        total_row_groups >= 0
    ), f"total_row_groups must be non-negative, got {total_row_groups}"
    assert (
        total_num_chunks > 0
    ), f"total_num_chunks must be positive, got {total_num_chunks}"
    assert (
        chunk_idx < total_num_chunks
    ), f"chunk_idx must be less than total_num_chunks, got {chunk_idx} and {total_num_chunks}"
    assert chunk_idx >= 0, f"chunk_idx must be non-negative, got {chunk_idx}"

    # Handle the case where ``chunk_idx`` exceeds the actual number of chunks
    # needed. This happens when the planner overestimated the number of chunks
    # (the chunker doesn't fetch metadata).
    if chunk_idx >= total_row_groups:
        return None

    base_row_groups_per_chunk = total_row_groups // total_num_chunks
    remainder = total_row_groups % total_num_chunks

    # Chunks 0 through (remainder-1) get one extra row group.
    if chunk_idx < remainder:
        row_groups_in_this_chunk = base_row_groups_per_chunk + 1
        start = chunk_idx * row_groups_in_this_chunk
    else:
        row_groups_in_this_chunk = base_row_groups_per_chunk
        start = (
            remainder * (base_row_groups_per_chunk + 1)
            + (chunk_idx - remainder) * base_row_groups_per_chunk
        )

    end = start + row_groups_in_this_chunk

    assert (
        0 <= start <= end <= total_row_groups
    ), f"Invalid range [{start}, {end}) for {total_row_groups} row groups"

    return start, end


def _fragments_from_chunk_metadata(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: ParquetFileChunkMetadata,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Slice ``fragment`` into per-row-group sub-fragments per chunk metadata.

    Returns one ``(ParquetFileFragment, file_row_offset)`` pair per row group
    covered by the chunk, where ``file_row_offset`` is the sum of ``num_rows``
    across all row groups that precede the sub-fragment in the underlying
    file. Callers seed per-fragment hashing offsets with this value so
    sub-fragments of the same file don't collide on ``(path, 0, n)``.

    Returns an empty list when the chunk index falls beyond the file's actual
    row-group count (the planner over-estimated; we silently drop the slice).
    """
    chunk_idx = chunk_metadata["chunk_idx"]
    total_num_chunks = chunk_metadata["total_num_chunks"]
    metadata = fragment.metadata
    total_row_groups = metadata.num_row_groups

    row_group_range = _calculate_row_group_range(
        chunk_idx, total_num_chunks, total_row_groups
    )

    if row_group_range is None:
        return []

    start, end = row_group_range

    file_row_offset = sum(metadata.row_group(i).num_rows for i in range(start))
    sub_fragments: List[Tuple[pds.ParquetFileFragment, int]] = []
    for row_group_index in range(start, end):
        sub_fragments.append(
            (fragment.subset(row_group_ids=[row_group_index]), file_row_offset)
        )
        file_row_offset += metadata.row_group(row_group_index).num_rows
    return sub_fragments


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
