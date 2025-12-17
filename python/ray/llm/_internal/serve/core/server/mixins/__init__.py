"""Server capability mixins.

Provides passthrough mixins that delegate to engine capabilities.
"""

from ray.llm._internal.serve.core.server.mixins.cache_manager import (
    CacheManagerServerMixin,
)
from ray.llm._internal.serve.core.server.mixins.sleepable import SleepableServerMixin

__all__ = [
    "CacheManagerServerMixin",
    "SleepableServerMixin",
]
