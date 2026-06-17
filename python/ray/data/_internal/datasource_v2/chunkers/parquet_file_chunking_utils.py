"""Parquet file-level chunking helpers for DataSourceV2.

Maps planner chunk metadata to PyArrow ``ParquetFileFragment`` subsets for
parallel reads. Two metadata schemas are supported so the chunker can be
toggled at runtime (``DataContext.parquet_chunker_row_group_aware``):

* :class:`ParquetFileChunkMetadata` carries an explicit half-open row-group
  range computed at listing time from the footer — no reconciliation needed.
* :class:`ByteEstimateParquetFileChunkMetadata` carries a ``chunk_idx`` /
  ``total_num_chunks`` byte estimate that is reconciled to a real row-group
  range here, at read time.

``fragments_to_read_for_manifest`` coalesces a partition's chunks **per file
into contiguous row-group runs**, so sister chunks of the same file (e.g. from
``FileAffinityPartitioner``) are read in a single scan — one file open, one
(already-cached) footer, sequential I/O — instead of one scan per row group.
"""
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple, Union

import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ByteEstimateParquetFileChunkMetadata,
    ParquetFileChunkMetadata,
)

_ChunkMetadata = Union[ParquetFileChunkMetadata, ByteEstimateParquetFileChunkMetadata]


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
        ``(start, end)`` (``end`` exclusive), or ``None`` if ``chunk_idx``
        falls beyond the actual number of row groups (the byte-estimate
        chunker over-estimated the chunk count).
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

    if chunk_idx >= total_row_groups:
        return None

    base_row_groups_per_chunk = total_row_groups // total_num_chunks
    remainder = total_row_groups % total_num_chunks

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


def _row_group_range_for_chunk(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: _ChunkMetadata,
) -> Optional[Tuple[int, int]]:
    """Resolve a chunk's half-open row-group range, or ``None`` if empty.

    Dispatches on the metadata schema: explicit range (row-group-aware chunker,
    defensively clamped to the file's actual row-group count) or reconciled
    chunk index (legacy byte-estimate chunker).
    """
    total_row_groups = fragment.metadata.num_row_groups
    if "row_group_start" in chunk_metadata:
        start = min(chunk_metadata["row_group_start"], total_row_groups)
        end = min(chunk_metadata["row_group_end"], total_row_groups)
        return (start, end) if start < end else None
    return _calculate_row_group_range(
        chunk_metadata["chunk_idx"],
        chunk_metadata["total_num_chunks"],
        total_row_groups,
    )


def _row_offset_before(metadata, start: int) -> int:
    """Sum of ``num_rows`` for all row groups preceding ``start`` in the file.

    Seeds the per-fragment row-hash offset so sub-fragments of the same file
    don't collide on ``(path, 0, n)``.
    """
    return sum(metadata.row_group(i).num_rows for i in range(start))


def _contiguous_runs(sorted_ids: List[int]) -> List[List[int]]:
    """Split a sorted list of row-group ids into maximal contiguous runs.

    e.g. ``[0, 1, 2, 5, 6] -> [[0, 1, 2], [5, 6]]``. Each run becomes one scan.
    """
    runs: List[List[int]] = []
    for rg in sorted_ids:
        if runs and rg == runs[-1][-1] + 1:
            runs[-1].append(rg)
        else:
            runs.append([rg])
    return runs


def _fragments_from_chunk_metadata(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: _ChunkMetadata,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """One coalesced sub-fragment for a single chunk's row-group range.

    Returns ``[(sub_fragment, file_row_offset)]`` covering the chunk's whole
    range in one scan (empty list if the range is empty). Cross-chunk
    coalescing across sister chunks is done by
    :func:`fragments_to_read_for_manifest`.
    """
    rng = _row_group_range_for_chunk(fragment, chunk_metadata)
    if rng is None:
        return []
    start, end = rng
    offset = _row_offset_before(fragment.metadata, start)
    return [(fragment.subset(row_group_ids=list(range(start, end))), offset)]


def fragments_to_read_for_manifest(
    path_to_fragment: Dict[str, pds.ParquetFileFragment],
    paths,
    chunk_metadatas,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Map a partition's chunks to ``(sub_fragment, file_row_offset)`` scans,
    coalescing each file's row groups into **contiguous runs**.

    Sister chunks of the same file (consecutive row-group ranges, e.g. from
    ``FileAffinityPartitioner``) collapse into a single sub-fragment per run, so
    the reader opens the file once and streams those row groups sequentially
    (the fragment — and thus the footer — is shared per path by the caller).
    Whole-file chunks (``None`` metadata) pass through as the full fragment.
    """
    whole_file_paths: List[str] = []
    path_to_row_groups: Dict[str, Set[int]] = defaultdict(set)
    for path, chunk_metadata in zip(paths, chunk_metadatas):
        if chunk_metadata is None:
            whole_file_paths.append(path)
            continue
        rng = _row_group_range_for_chunk(path_to_fragment[path], chunk_metadata)
        if rng is not None:
            path_to_row_groups[path].update(range(rng[0], rng[1]))

    fragments: List[Tuple[pds.ParquetFileFragment, int]] = []
    for path in whole_file_paths:
        fragments.append((path_to_fragment[path], 0))
    for path, row_groups in path_to_row_groups.items():
        fragment = path_to_fragment[path]
        metadata = fragment.metadata
        for run in _contiguous_runs(sorted(row_groups)):
            offset = _row_offset_before(metadata, run[0])
            fragments.append((fragment.subset(row_group_ids=run), offset))
    return fragments
