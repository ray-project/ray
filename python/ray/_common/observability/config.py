from ray._private import ray_constants


def is_python_ray_event_enabled(event_type: str) -> bool:
    """Whether events of the given type should be emitted through the Python
    EventRecorder (Ray event export framework).

    Args:
        event_type: Name of a RayEvent.EventType, e.g. "PLATFORM_EVENT".

    Returns:
        True if RAY_enable_ray_event is on and event_type is listed in
        RAY_ENABLE_PYTHON_RAY_EVENT_TYPES.
    """
    # RAY_enable_ray_event must also be on, otherwise the C++ event recorder
    # silently drops the events.
    return (
        ray_constants.RAY_ENABLE_RAY_EVENT
        and event_type in ray_constants.RAY_ENABLE_PYTHON_RAY_EVENT_TYPES
    )
