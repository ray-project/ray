"""This module provides the Python API for emitting internal Ray events
via the ONE-Event framework. Events are buffered and exported through
the C++ RayEventRecorder.
"""

from ray._common.observability.internal_event import InternalEventBuilder

__all__ = [
    "InternalEventBuilder",
]
