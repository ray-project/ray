"""
Simple, elegant decorators for Ray Data operations.

Follows Ray Data patterns: lightweight decorators that handle cache invalidation
automatically while maintaining clean, readable code.
"""

from typing import Callable, List, Optional

from ray.data._internal.cache.dataset_cache import (
    cache_result,
    invalidate_cache_on_transform,
)
from ray.data._internal.util import AllToAllAPI, ConsumptionAPI

# =============================================================================
# SIMPLE, ELEGANT DECORATORS
# =============================================================================


def transform(transform_name: Optional[str] = None) -> Callable:
    """Mark a method as a basic transformation with cache invalidation."""

    def decorator(func: Callable) -> Callable:
        name = transform_name or func.__name__
        return invalidate_cache_on_transform(name)(func)

    return decorator


def shuffle_transform(transform_name: Optional[str] = None) -> Callable:
    """Mark a method as a shuffling transformation (all-to-all + cache invalidation)."""

    def decorator(func: Callable) -> Callable:
        name = transform_name or func.__name__
        func = AllToAllAPI(func)
        func = invalidate_cache_on_transform(name)(func)
        return func

    return decorator


def combine_transform(transform_name: Optional[str] = None) -> Callable:
    """Mark a method as a combining transformation (all-to-all + cache invalidation)."""

    def decorator(func: Callable) -> Callable:
        name = transform_name or func.__name__
        func = AllToAllAPI(func)
        func = invalidate_cache_on_transform(name)(func)
        return func

    return decorator


def consume_with_cache(cache_params: Optional[List[str]] = None) -> Callable:
    """Mark a method as a consumption operation with result caching."""

    def decorator(func: Callable) -> Callable:
        func = ConsumptionAPI(func)
        func = cache_result(func.__name__, include_params=cache_params)(func)
        return func

    return decorator


def inspect_with_cache(cache_params: Optional[List[str]] = None) -> Callable:
    """Mark a method as a metadata inspection operation with result caching."""

    def decorator(func: Callable) -> Callable:
        func = cache_result(func.__name__, include_params=cache_params)(func)
        return func

    return decorator


# =============================================================================
# CONVENIENT ALIASES
# =============================================================================

# Short, intuitive names that map to specific transformation types
transform_op = transform  # ROW_PRESERVING_SCHEMA_CHANGE
shuffle = shuffle_transform  # REORDERING_ONLY
combine = combine_transform  # COMBINING
aggregate = combine_transform  # GROUPING (uses same decorator as combine)
consume = consume_with_cache
inspect = inspect_with_cache


# Specific transformation type aliases
def filter_op():
    """ROW_CHANGING_NO_SCHEMA_CHANGE decorator."""
    return transform("filter")


def limit_op(name=None):
    """SCHEMA_PRESERVING_COUNT_CHANGING decorator."""
    return transform(name or "limit")


def row_and_schema_change():
    """ROW_CHANGING_SCHEMA_CHANGE decorator."""
    return transform()


def expression():
    """ROW_PRESERVING_SCHEMA_CHANGE decorator for expressions."""
    return transform("add_column")
