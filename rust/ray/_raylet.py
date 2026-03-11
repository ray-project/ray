"""Compatibility shim for ray._raylet.

The Python (C++) backend exposes ObjectRef via ray._raylet.
The Rust backend defines ObjectRef in ray.__init__.
This module re-exports it so that code using `from ray._raylet import ObjectRef`
works with the Rust backend.
"""

from ray import ObjectRef

__all__ = ["ObjectRef"]
