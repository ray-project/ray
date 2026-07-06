from typing import Optional, Tuple

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    OrcFileChunkMetadata,
)


def _calculate_stripe_range(
    chunk_idx: int,
    total_num_chunks: int,
    total_stripes: int,
) -> Optional[Tuple[int, int]]:
    """Return the half-open stripe range assigned to an estimated chunk."""
    if total_num_chunks <= 0 or total_stripes <= 0:
        return None
    if chunk_idx < 0 or chunk_idx >= total_num_chunks:
        return None

    base = total_stripes // total_num_chunks
    remainder = total_stripes % total_num_chunks
    stripe_count = base + (1 if chunk_idx < remainder else 0)
    if stripe_count == 0:
        return None

    start = chunk_idx * base + min(chunk_idx, remainder)
    return start, start + stripe_count


def stripe_range_from_chunk_metadata(
    chunk_metadata: OrcFileChunkMetadata,
    total_stripes: int,
) -> Optional[Tuple[int, int]]:
    return _calculate_stripe_range(
        chunk_metadata["chunk_idx"],
        chunk_metadata["total_num_chunks"],
        total_stripes,
    )
