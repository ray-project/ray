"""Parquet file-level chunking helpers for DataSourceV2.

Maps planner chunk metadata to PyArrow ``ParquetFileFragment`` subsets for
parallel reads. Two metadata schemas are supported so the chunker can be
toggled at runtime (``DataContext.parquet_chunker_row_group_aware``):

* :class:`ParquetFileChunkMetadata` carries an explicit half-open row-group
  range computed at listing time from the footer — no reconciliation needed.
* :class:`ByteEstimateParquetFileChunkMetadata` carries a ``chunk_idx`` /
  ``total_num_chunks`` byte estimate that is reconciled to a real row-group
  range here, at read time.

``_fragments_from_chunk_metadata`` dispatches on which schema is present.
"""
from typing import List, Optional, Tuple, Union

import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ByteEstimateParquetFileChunkMetadata,
    ParquetFileChunkMetadata,
)


def _calculate_row_group_range(
    chunk_idx: int, total_num_chunks: int, total_row_groups: int
) -> Optional[Tuple[int, int]]:
    """Compute the half-open row-group range for a byte-estimate chunk.

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
        groups (i.e. the byte-estimate chunker over-estimated the chunk count).
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
    # needed. This happens when the chunker overestimated the number of chunks
    # (it doesn't fetch metadata).
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


def _sub_fragments_for_range(
    fragment: pds.ParquetFileFragment,
    start: int,
    end: int,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Slice ``fragment`` into one sub-fragment per row group in ``[start, end)``.

    Returns ``(ParquetFileFragment, file_row_offset)`` pairs, where
    ``file_row_offset`` is the sum of ``num_rows`` across all row groups that
    precede the sub-fragment in the underlying file. Callers seed per-fragment
    hashing offsets with this value so sub-fragments of the same file don't
    collide on ``(path, 0, n)``.
    """
    metadata = fragment.metadata
    file_row_offset = sum(metadata.row_group(i).num_rows for i in range(start))
    sub_fragments: List[Tuple[pds.ParquetFileFragment, int]] = []
    for row_group_index in range(start, end):
        sub_fragments.append(
            (fragment.subset(row_group_ids=[row_group_index]), file_row_offset)
        )
        file_row_offset += metadata.row_group(row_group_index).num_rows
    return sub_fragments


def _fragments_from_chunk_metadata(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: Union[
        ParquetFileChunkMetadata, ByteEstimateParquetFileChunkMetadata
    ],
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Slice ``fragment`` into per-row-group sub-fragments for a chunk.

    Dispatches on the chunk-metadata schema:

    * Row-group-aware (``row_group_start`` / ``row_group_end``): use the
      explicit range directly, defensively clamped to the file's actual
      row-group count (a no-op in practice, since the range came from the same
      footer the reader sees).
    * Byte-estimate (``chunk_idx`` / ``total_num_chunks``): reconcile the chunk
      index to a row-group range; returns an empty list when the index falls
      beyond the file's actual row groups (the chunker over-estimated).
    """
    metadata = fragment.metadata
    total_row_groups = metadata.num_row_groups

    if "row_group_start" in chunk_metadata:
        start = min(chunk_metadata["row_group_start"], total_row_groups)
        end = min(chunk_metadata["row_group_end"], total_row_groups)
        return _sub_fragments_for_range(fragment, start, end)

    # Legacy byte-estimate metadata: reconcile chunk index -> row-group range.
    row_group_range = _calculate_row_group_range(
        chunk_metadata["chunk_idx"],
        chunk_metadata["total_num_chunks"],
        total_row_groups,
    )
    if row_group_range is None:
        return []
    start, end = row_group_range
    return _sub_fragments_for_range(fragment, start, end)
