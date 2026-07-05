"""Parquet file-level chunking helpers for DataSourceV2.

Maps planner chunk metadata (``ParquetFileChunkMetadata``) to PyArrow
``ParquetFileFragment`` subsets for parallel reads. Chunk metadata carries
an explicit half-open row-group range computed at listing time from the
file's footer, so no estimation or reconciliation is needed here.
"""
from typing import List, Tuple

import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunkMetadata,
)


def _fragments_from_chunk_metadata(
    fragment: pds.ParquetFileFragment,
    chunk_metadata: ParquetFileChunkMetadata,
) -> List[Tuple[pds.ParquetFileFragment, int]]:
    """Slice ``fragment`` into per-row-group sub-fragments for the chunk's range.

    The chunk carries an explicit ``[row_group_start, row_group_end)`` range.
    Returns one ``(ParquetFileFragment, file_row_offset)`` pair per row group
    in that range, where ``file_row_offset`` is the sum of ``num_rows`` across
    all row groups that precede the sub-fragment in the underlying file.
    Callers seed per-fragment hashing offsets with this value so sub-fragments
    of the same file don't collide on ``(path, 0, n)``.

    The range is defensively clamped to the file's actual row-group count;
    since ranges are computed from the same footer the reader sees, the clamp
    is a no-op in practice and never drops real row groups.
    """
    start = chunk_metadata["row_group_start"]
    end = chunk_metadata["row_group_end"]
    metadata = fragment.metadata
    total_row_groups = metadata.num_row_groups

    start = min(start, total_row_groups)
    end = min(end, total_row_groups)

    file_row_offset = sum(metadata.row_group(i).num_rows for i in range(start))
    sub_fragments: List[Tuple[pds.ParquetFileFragment, int]] = []
    for row_group_index in range(start, end):
        sub_fragments.append(
            (fragment.subset(row_group_ids=[row_group_index]), file_row_offset)
        )
        file_row_offset += metadata.row_group(row_group_index).num_rows
    return sub_fragments
