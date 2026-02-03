"""Ray internal event emission API.

This module provides functions for emitting internal Ray events to the
event aggregator via Cython bindings.

Example usage:

    from ray._private.event import emit_event
    from ray._private.event.submission_job_events import (
        SubmissionJobDefinitionEventBuilder,
    )

    event = SubmissionJobDefinitionEventBuilder(
        submission_id="raysubmit_123",
        entrypoint="python script.py",
        ...
    ).build()
    emit_event(event)
"""

from typing import List, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from ray._raylet import RayEvent

logger = logging.getLogger(__name__)


def emit_event(event: "RayEvent") -> bool:
    """Emit a single event to the event aggregator.

    This function uses the global event recorder singleton that is initialized
    during Ray worker startup. If the recorder is not initialized, the event
    will be silently dropped.

    Args:
        event: A RayEvent object (created via InternalEventBuilder.build()).

    Returns:
        True if the event was successfully queued, False otherwise.
    """
    return emit_events([event])


def emit_events(events: List["RayEvent"]) -> bool:
    """Emit multiple events to the event aggregator.

    This function uses the global event recorder singleton that is initialized
    during Ray worker startup. If the recorder is not initialized, events
    will be silently dropped.

    Args:
        events: List of RayEvent objects to emit.

    Returns:
        True if events were successfully queued, False otherwise.
    """
    if not events:
        return True

    try:
        from ray._raylet import _get_global_event_recorder

        recorder = _get_global_event_recorder()
        if recorder is None or not recorder.is_initialized():
            logger.debug(
                "Event recorder not initialized, dropping %d events", len(events)
            )
            return False

        return recorder.add_events(events)
    except Exception as e:
        # Events are best-effort, don't fail on errors
        logger.debug("Failed to emit events: %s", e)
        return False


# Re-export for convenience
from ray._private.event.internal_event import InternalEventBuilder

__all__ = [
    "emit_event",
    "emit_events",
    "InternalEventBuilder",
]
