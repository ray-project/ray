"""
Simple object size calculation for Ray Data caching.

For Ray objects, measures only the pointer size, not the data.
"""

import sys
from typing import Any

from .constants import (
    RAY_BLOCK_OVERHEAD_BYTES,
    RAY_DATASET_METADATA_SIZE_BYTES,
    RAY_OBJECTREF_SIZE_BYTES,
)


def get_object_size(obj: Any) -> int:
    """Get approximate size of object in bytes.

    For Ray objects, returns only pointer size, not data size.
    """
    try:
        # Ray ObjectRefs - just the pointer
        import ray

        if isinstance(obj, ray.ObjectRef):
            return RAY_OBJECTREF_SIZE_BYTES
    except ImportError:
        pass

    # Ray MaterializedDataset - just the ObjectRef structure
    if hasattr(obj, "num_blocks"):
        try:
            return (
                obj.num_blocks() * RAY_BLOCK_OVERHEAD_BYTES
                + RAY_DATASET_METADATA_SIZE_BYTES
            )
        except (AttributeError, TypeError):
            pass

    # For everything else, use sys.getsizeof
    return sys.getsizeof(obj)
