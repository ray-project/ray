import functools
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Tuple, TypeVar

from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")

_IS_TIMED_CACHE_ENABLED = True


def enable_timed_cache():
    global _IS_TIMED_CACHE_ENABLED
    _IS_TIMED_CACHE_ENABLED = True


def disable_timed_cache():
    global _IS_TIMED_CACHE_ENABLED
    _IS_TIMED_CACHE_ENABLED = False


@contextmanager
def _disable_timed_cache_for_tests():
    """Bypass all TTL caches, for tests that assert on up-to-date values."""
    disable_timed_cache()
    try:
        yield
    finally:
        enable_timed_cache()


def timed_cache(
    ttl: float, get_time_fn: Callable[[], float] = time.time
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator that caches function results for a given TTL (in seconds).

    Args:
        ttl: Time-to-live in seconds for each cache entry.
        get_time_fn: Callable returning the current time in seconds, used to
            measure cache entry age. Defaults to ``time.time``.

    Returns:
        A decorator that wraps a function with TTL-based result caching.
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        cache: Dict[Tuple, Tuple[float, Any]] = {}

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not _IS_TIMED_CACHE_ENABLED:
                return fn(*args, **kwargs)

            key = args + tuple(sorted(kwargs.items()))
            try:
                hash(key)
            except TypeError as e:
                raise ValueError(
                    f"`timed_cache` only supports arguments that are hashable, "
                    f"but '{key}' isn't hashable"
                ) from e

            now = get_time_fn()
            if key in cache:
                cached_time, value = cache[key]
                if now - cached_time < ttl:
                    return value

            result = fn(*args, **kwargs)
            cache[key] = (now, result)
            return result

        return wrapper

    return decorator
