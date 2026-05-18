"""Parquet file-level chunking helpers for DataSourceV2.

Maps planner chunk metadata (``ParquetFileChunkMetadata``) to row-group
ranges and PyArrow ``ParquetFileFragment`` subsets for parallel reads.
"""
from typing import List, Optional, Tuple

import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunkMetadata,
)


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
) -> List[pds.ParquetFileFragment]:
    """Slice ``fragment`` into per-row-group sub-fragments per chunk metadata.

    Returns one ``ParquetFileFragment`` per row group covered by the chunk.
    Returns an empty list when the chunk index falls beyond the file's actual
    row-group count (the planner over-estimated; we silently drop the slice).
    """
    chunk_idx = chunk_metadata["chunk_idx"]
    total_num_chunks = chunk_metadata["total_num_chunks"]
    total_row_groups = fragment.metadata.num_row_groups

    row_group_range = _calculate_row_group_range(
        chunk_idx, total_num_chunks, total_row_groups
    )

    if row_group_range is None:
        return []

    start, end = row_group_range

    return [
        fragment.subset(row_group_ids=[row_group_index])
        for row_group_index in range(start, end)
    ]
