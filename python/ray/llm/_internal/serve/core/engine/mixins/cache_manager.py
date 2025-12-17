"""Cache manager engine mixin protocol.

Defines the interface for engines that support cache management operations.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class CacheManagerEngineMixin(Protocol):
    """Protocol for engines that support cache management.

    Provides operations for managing the KV prefix cache used
    during inference.
    """

    async def reset_prefix_cache(self) -> None:
        """Reset the KV prefix cache.

        Clears any cached key-value pairs from previous requests.
        Useful for benchmarking or when cache invalidation is needed.
        """
