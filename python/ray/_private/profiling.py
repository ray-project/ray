import ray
import os


class _NullLogSpan:
    """A log span context manager that does nothing"""

    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        pass


PROFILING_ENABLED = "RAY_PROFILING" in os.environ
NULL_LOG_SPAN = _NullLogSpan()


def profile(event_type, extra_data=None):
    """Profile a span of time so that it appears in the timeline visualization.

    Note that this only works in the raylet code path.

    This function can be used as follows (both on the driver or within a task).

    .. code-block:: python
        import ray._private.profiling as profiling

        with profiling.profile("custom event", extra_data={'key': 'val'}):
            # Do some computation here.

    Optionally, a dictionary can be passed as the "extra_data" argument, and
    it can have keys "name" and "cname" if you want to override the default
    timeline display text and box color. Other values will appear at the bottom
    of the chrome tracing GUI when you click on the box corresponding to this
    profile span.

    Args:
        event_type: A string describing the type of the event.
        extra_data: This must be a dictionary mapping strings to strings. This
            data will be added to the json objects that are used to populate
            the timeline, so if you want to set a particular color, you can
            simply set the "cname" attribute to an appropriate color.
            Similarly, if you set the "name" attribute, then that will set the
            text displayed on the box in the timeline.

    Returns:
        An object that can profile a span of time via a "with" statement.
    """
    if not PROFILING_ENABLED:
        return NULL_LOG_SPAN
    worker = ray.worker.global_worker
    if worker.mode == ray.worker.LOCAL_MODE:
        return NULL_LOG_SPAN
    return worker.core_worker.profile_event(event_type.encode("ascii"), extra_data)
