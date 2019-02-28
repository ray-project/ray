from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import time
import threading
import traceback

import ray

LOG_POINT = 0
LOG_SPAN_START = 1
LOG_SPAN_END = 2


class _NullLogSpan(object):
    """A log span context manager that does nothing"""

    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        pass


NULL_LOG_SPAN = _NullLogSpan()


def profile(event_type, extra_data=None):
    """Profile a span of time so that it appears in the timeline visualization.

    Note that this only works in the raylet code path.

    This function can be used as follows (both on the driver or within a task).

    .. code-block:: python

        with ray.profile("custom event", extra_data={'key': 'value'}):
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
    worker = ray.worker.global_worker
    return RayLogSpanRaylet(worker.profiler, event_type, extra_data=extra_data)


class Profiler(object):
    """A class that holds the profiling states.

    Attributes:
        worker: the worker to profile.
        events: the buffer of events.
        lock: the lock to protect access of events.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
    """

    def __init__(self, worker, threads_stopped):
        self.worker = worker
        self.events = []
        self.lock = threading.Lock()
        self.threads_stopped = threads_stopped

    def start_flush_thread(self):
        self.t = threading.Thread(
            target=self._periodically_flush_profile_events,
            name="ray_push_profiling_information")
        # Making the thread a daemon causes it to exit when the main thread
        # exits.
        self.t.daemon = True
        self.t.start()

    def join_flush_thread(self):
        """Wait for the flush thread to exit."""
        self.t.join()

    def _periodically_flush_profile_events(self):
        """Drivers run this as a thread to flush profile data in the
        background."""
        # Note(rkn): This is run on a background thread in the driver. It uses
        # the local scheduler client. This should be ok because it doesn't read
        # from the local scheduler client and we have the GIL here. However,
        # if either of those things changes, then we could run into issues.
        while True:
            # Sleep for 1 second. This will be interrupted if
            # self.threads_stopped is set.
            self.threads_stopped.wait(timeout=1)

            # Exit if we received a signal that we should stop.
            if self.threads_stopped.is_set():
                return

            self.flush_profile_data()

    def flush_profile_data(self):
        """Push the logged profiling data to the global control store."""
        with self.lock:
            events = self.events
            self.events = []

        if self.worker.mode == ray.WORKER_MODE:
            component_type = "worker"
        else:
            component_type = "driver"

        self.worker.raylet_client.push_profile_events(
            component_type, ray.UniqueID(self.worker.worker_id),
            self.worker.node_ip_address, events)

    def add_event(self, event):
        with self.lock:
            self.events.append(event)


class RayLogSpanRaylet(object):
    """An object used to enable logging a span of events with a with statement.

    Attributes:
        event_type (str): The type of the event being logged.
        extra_data: Additional information to log.
    """

    def __init__(self, profiler, event_type, extra_data=None):
        """Initialize a RayLogSpanRaylet object."""
        self.profiler = profiler
        self.event_type = event_type
        self.extra_data = extra_data if extra_data is not None else {}

    def set_attribute(self, key, value):
        """Add a key-value pair to the extra_data dict.

        This can be used to add attributes that are not available when
        ray.profile was called.

        Args:
            key: The attribute name.
            value: The attribute value.
        """
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("The arguments 'key' and 'value' must both be "
                             "strings. Instead they are {} and {}.".format(
                                 key, value))
        self.extra_data[key] = value

    def __enter__(self):
        """Log the beginning of a span event.

        Returns:
            The object itself is returned so that if the block is opened using
                "with ray.profile(...) as prof:", we can call
                "prof.set_attribute" inside the block.
        """
        self.start_time = time.time()
        return self

    def __exit__(self, type, value, tb):
        """Log the end of a span event. Log any exception that occurred."""
        for key, value in self.extra_data.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("The extra_data argument must be a "
                                 "dictionary mapping strings to strings. "
                                 "Instead it is {}.".format(self.extra_data))

        if type is not None:
            extra_data = json.dumps({
                "type": str(type),
                "value": str(value),
                "traceback": str(traceback.format_exc()),
            })
        else:
            extra_data = json.dumps(self.extra_data)

        event = {
            "event_type": self.event_type,
            "start_time": self.start_time,
            "end_time": time.time(),
            "extra_data": extra_data,
        }

        self.profiler.add_event(event)
