"""Object size calculation for Ray Data caching.

For Ray objects (ObjectRef, MaterializedDataset), only measures pointer/metadata
size, not actual data size, since data lives in the object store.
"""

import sys
from typing import Any

from .constants import (
    RAY_BLOCK_OVERHEAD_BYTES,
    RAY_DATASET_METADATA_SIZE_BYTES,
    RAY_OBJECTREF_SIZE_BYTES,
)


def get_object_size(obj: Any) -> int:
    """Get approximate size of an object in bytes."""
    try:
        import ray

        if isinstance(obj, ray.ObjectRef):
            return RAY_OBJECTREF_SIZE_BYTES
    except ImportError:
        pass

    if hasattr(obj, "num_blocks"):
        try:
            num_blocks = obj.num_blocks()
            return (
                num_blocks * RAY_BLOCK_OVERHEAD_BYTES + RAY_DATASET_METADATA_SIZE_BYTES
            )
        except (AttributeError, TypeError):
            pass

    return sys.getsizeof(obj)
