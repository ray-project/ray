"""
Object size calculation for Ray Data caching.

This module estimates the size of cached objects to determine cache placement.

Key Insight: For Ray objects (ObjectRef, MaterializedDataset), we only measure
the pointer/metadata size, NOT the actual data size. This is correct because:
1. Ray data lives in the object store (separate memory space)
2. We only cache the reference structure, not the data itself
3. This keeps cache size calculations fast and accurate

Examples:
    - int (count result): ~28 bytes → Local cache
    - List[str] (columns): ~1KB → Local cache
    - Schema object: ~5KB → Local cache
    - MaterializedDataset: ~100KB (just ObjectRefs) → Ray cache
    - Large DataFrame: >10MB → Don't cache
"""

import sys
from typing import Any

from .constants import (
    RAY_BLOCK_OVERHEAD_BYTES,
    RAY_DATASET_METADATA_SIZE_BYTES,
    RAY_OBJECTREF_SIZE_BYTES,
)


def get_object_size(obj: Any) -> int:
    """Get approximate size of an object in bytes.

    This function estimates object size for cache placement decisions.
    For Ray objects, it returns only the pointer/metadata size, NOT the
    actual data size, which is correct for caching purposes.

    Size Estimation Rules:
        1. Ray ObjectRef → 64 bytes (just the pointer)
        2. MaterializedDataset → num_blocks * 64 + 1KB (ObjectRef structure)
        3. Regular Python objects → sys.getsizeof() (actual memory usage)

    Args:
        obj: The object to measure

    Returns:
        Estimated size in bytes

    Examples:
        >>> get_object_size(100)  # int
        28
        >>> get_object_size(["id", "value"])  # small list
        ~1000
        >>> get_object_size(ray.put(large_df))  # ObjectRef
        64  # Just the pointer, not the DataFrame!

    Note:
        This is an approximation. For complex objects, it may underestimate
        the true memory footprint, but it's sufficient for cache decisions.
    """
    # -------------------------------------------------------------------------
    # Ray ObjectRef - just measure the pointer, not the data
    # -------------------------------------------------------------------------
    try:
        import ray

        if isinstance(obj, ray.ObjectRef):
            # ObjectRef is just a pointer to data in the object store
            # The data itself is NOT in our cache, so we only count the pointer
            return RAY_OBJECTREF_SIZE_BYTES
    except ImportError:
        # Ray not available, continue with other checks
        pass

    # -------------------------------------------------------------------------
    # Ray MaterializedDataset - measure the ObjectRef structure
    # -------------------------------------------------------------------------
    # MaterializedDataset contains a list of Block ObjectRefs + metadata.
    # We cache this structure (not the data), so measure structure size.
    if hasattr(obj, "num_blocks"):
        try:
            num_blocks = obj.num_blocks()
            # Size = (number of block ObjectRefs * overhead) + metadata overhead
            # Example: 10 blocks = 10 * 64 + 1024 = 1664 bytes
            return (
                num_blocks * RAY_BLOCK_OVERHEAD_BYTES  # Each block ObjectRef
                + RAY_DATASET_METADATA_SIZE_BYTES  # Dataset metadata
            )
        except (AttributeError, TypeError):
            # num_blocks() call failed, fall through to default
            pass

    # -------------------------------------------------------------------------
    # Regular Python objects - use sys.getsizeof
    # -------------------------------------------------------------------------
    # For normal objects (int, str, list, dict, etc.), measure actual size
    # This is fast and accurate for small-to-medium objects
    return sys.getsizeof(obj)
