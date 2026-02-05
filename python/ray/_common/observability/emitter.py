"""Functions for emitting events to dashboard-agents aggregator agent service."""

import logging
from typing import TYPE_CHECKING, List

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
    will be dropped.

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
        logger.error("Failed to emit one or more events: %s", e)
        return False
