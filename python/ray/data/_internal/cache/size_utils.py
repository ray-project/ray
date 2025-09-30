"""
Simple object size calculation for Ray Data caching.

For Ray objects, measures only the pointer size, not the data.
"""

import sys
from typing import Any

# Configuration constants

# Estimated size of a Ray ObjectRef pointer.
# ObjectRefs are lightweight references that don't contain the actual data.
RAY_OBJECTREF_SIZE_BYTES = 64

# Estimated overhead per block in a MaterializedDataset.
# This accounts for the ObjectRef pointer plus some metadata overhead.
RAY_BLOCK_OVERHEAD_BYTES = 64

# Base metadata size for MaterializedDataset objects.
# This covers the dataset structure itself, excluding the block ObjectRefs.
RAY_DATASET_METADATA_SIZE_BYTES = 1024


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
