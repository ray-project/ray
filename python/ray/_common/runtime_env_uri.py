"""Runtime env URI parsing shared across Ray libraries.

Re-exports from _private so that libraries (e.g. Serve) can depend on
ray._common instead of ray._private for URI validation.
"""

from ray._private.runtime_env.packaging import parse_uri
from ray._private.runtime_env.protocol import Protocol

__all__ = ["parse_uri", "Protocol"]
