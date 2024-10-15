"""Expose backports in a single place."""
import sys

if sys.version_info >= (3,):  # pragma: no cover (PY3+)
    from functools import lru_cache
else:  # pragma: no cover (<PY3)
    from functools32 import lru_cache

if sys.version_info >= (3, 8):  # pragma: no cover (PY38+)
    import importlib.metadata as importlib_metadata
else:  # pragma: no cover (<PY38)
    import importlib_metadata

__all__ = ("lru_cache", "importlib_metadata")
