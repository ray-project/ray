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


def profile(event_type, extra_data=None, worker=None):
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
    if worker is None:
        worker = ray.worker.global_worker
    if not worker.use_raylet:
        # Log the event if this is a worker and not a driver, since the
        # driver's event log never gets flushed.
        if worker.mode == ray.WORKER_MODE:
            return RayLogSpanNonRaylet(
                worker.profiler, event_type, contents=extra_data)
        else:
            return NULL_LOG_SPAN
    else:
        return RayLogSpanRaylet(
            worker.profiler, event_type, extra_data=extra_data)


class Profiler(object):
    """A class that holds the profiling states.

    Attributes:
        worker: the worker to profile.
        events: the buffer of events.
        lock: the lock to protect access of events.
    """

    def __init__(self, worker):
        self.worker = worker
        self.events = []
        self.lock = threading.Lock()

    def start_flush_thread(self):
        t = threading.Thread(target=self._periodically_flush_profile_events)
        # Making the thread a daemon causes it to exit when the main thread
        # exits.
        t.daemon = True
        t.start()

    def _periodically_flush_profile_events(self):
        """Drivers run this as a thread to flush profile data in the
        background."""
        # Note(rkn): This is run on a background thread in the driver. It uses
        # the local scheduler client. This should be ok because it doesn't read
        # from the local scheduler client and we have the GIL here. However,
        # if either of those things changes, then we could run into issues.
        try:
            while True:
                time.sleep(1)
                self.flush_profile_data()
        except AttributeError:
            # This is to suppress errors that occur at shutdown.
            pass

    def flush_profile_data(self):
        """Push the logged profiling data to the global control store.

        By default, profiling information for a given task won't appear in the
        timeline until after the task has completed. For very long-running
        tasks, we may want profiling information to appear more quickly.
        In such cases, this function can be called. Note that as an
        aalternative, we could start thread in the background on workers that
        calls this automatically.
        """
        with self.lock:
            events = self.events
            self.events = []

        if not self.worker.use_raylet:
            event_log_key = b"event_log:" + self.worker.worker_id
            event_log_value = json.dumps(events)
            self.worker.local_scheduler_client.log_event(
                event_log_key, event_log_value, time.time())
        else:
            if self.worker.mode == ray.WORKER_MODE:
                component_type = "worker"
            else:
                component_type = "driver"

            self.worker.local_scheduler_client.push_profile_events(
                component_type, ray.ObjectID(self.worker.worker_id),
                self.worker.node_ip_address, events)

    def add_event(self, event):
        with self.lock:
            self.events.append(event)


class RayLogSpanNonRaylet(object):
    """An object used to enable logging a span of events with a with statement.

    Attributes:
        event_type (str): The type of the event being logged.
        contents: Additional information to log.
    """

    def __init__(self, profiler, event_type, contents=None):
        """Initialize a RayLogSpanNonRaylet object."""
        self.profiler = profiler
        self.event_type = event_type
        self.contents = contents

    def _log(self, event_type, kind, contents=None):
        """Log an event to the global state store.

        This adds the event to a buffer of events locally. The buffer can be
        flushed and written to the global state store by calling
        flush_profile_data().

        Args:
            event_type (str): The type of the event.
            contents: More general data to store with the event.
            kind (int): Either LOG_POINT, LOG_SPAN_START, or LOG_SPAN_END. This
                is LOG_POINT if the event being logged happens at a single
                point in time. It is LOG_SPAN_START if we are starting to log a
                span of time, and it is LOG_SPAN_END if we are finishing
                logging a span of time.
        """
        # TODO(rkn): This code currently takes around half a microsecond. Since
        # we call it tens of times per task, this adds up. We will need to redo
        # the logging code, perhaps in C.
        contents = {} if contents is None else contents
        assert isinstance(contents, dict)
        # Make sure all of the keys and values in the dictionary are strings.
        contents = {str(k): str(v) for k, v in contents.items()}
        self.profiler.add_event((time.time(), event_type, kind, contents))

    def __enter__(self):
        """Log the beginning of a span event."""
        self._log(
            event_type=self.event_type,
            contents=self.contents,
            kind=LOG_SPAN_START)

    def __exit__(self, type, value, tb):
        """Log the end of a span event. Log any exception that occurred."""
        if type is None:
            self._log(event_type=self.event_type, kind=LOG_SPAN_END)
        else:
            self._log(
                event_type=self.event_type,
                contents={
                    "type": str(type),
                    "value": value,
                    "traceback": traceback.format_exc()
                },
                kind=LOG_SPAN_END)


class RayLogSpanRaylet(object):
    """An object used to enable logging a span of events with a with statement.

    Attributes:
        event_type (str): The type of the event being logged.
        contents: Additional information to log.
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
            raise ValueError("The extra_data argument must be a "
                             "dictionary mapping strings to strings.")
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
                                 "dictionary mapping strings to strings.")

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
