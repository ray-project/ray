"""Parquet file-level chunking helpers for DataSourceV2.

Maps planner chunk metadata to PyArrow ``ParquetFileFragment`` subsets for
parallel reads. :class:`ParquetFileChunkMetadata` carries an explicit half-open
row-group range computed at listing time from the footer — the reader slices to
exactly that range, with no read-time reconciliation.

``fragments_to_read_for_manifest`` coalesces a partition's chunks **per file
into contiguous row-group runs**, so sister chunks of the same file (e.g. from
``FileAffinityPartitioner``) are read in a single scan — one file open, one
(already-cached) footer, sequential I/O — instead of one scan per row group.
"""
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunkMetadata,
)


def _row_group_range_for_chunk(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: ParquetFileChunkMetadata,
) -> Optional[Tuple[int, int]]:
    """Resolve a chunk's half-open row-group range, or ``None`` if empty.

    The explicit ``[row_group_start, row_group_end)`` range is defensively
    clamped to the file's actual row-group count.
    """
    total_row_groups = fragment.metadata.num_row_groups
    start = min(chunk_metadata["row_group_start"], total_row_groups)
    end = min(chunk_metadata["row_group_end"], total_row_groups)
    return (start, end) if start < end else None


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
        # Prefix sum of per-row-group row counts computed once per file:
        # ``row_offsets[i]`` is the number of rows in row groups ``[0, i)``. Each
        # run's file-row offset is then an O(1) lookup (it seeds the per-fragment
        # row-hash so sub-fragments of the same file don't collide on
        # ``(path, 0, n)``).
        row_offsets = [0] * (metadata.num_row_groups + 1)
        for i in range(metadata.num_row_groups):
            row_offsets[i + 1] = row_offsets[i] + metadata.row_group(i).num_rows
        for run in _contiguous_runs(sorted(row_groups)):
            fragments.append((fragment.subset(row_group_ids=run), row_offsets[run[0]]))
    return fragments
