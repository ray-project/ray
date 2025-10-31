"""Object size calculation for Ray Data caching."""

import sys
from typing import Any


def get_object_size(obj: Any) -> int:
    """Get approximate size of an object in bytes."""
    return sys.getsizeof(obj)
