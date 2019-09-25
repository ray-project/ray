from libcpp.string cimport string as c_string

from ray.includes.libcoreworker cimport CProfiler, CProfilingEvent

import json
import traceback

cdef class ProfilingEvent:
    """Cython wrapper class of C++ `ray::worker::ProfilingEvent`."""
    cdef:
        unique_ptr[CProfilingEvent] inner
        dict extra_data

    @staticmethod
    cdef make(CProfiler &profiler, c_string &event_type, dict extra_data):
        cdef ProfilingEvent self = ProfilingEvent.__new__(ProfilingEvent)
        self.inner.reset(new CProfilingEvent(profiler, event_type))
        self.extra_data = extra_data
        return self

    def set_extra_data(self, c_string extra_data):
        self.inner.get().SetExtraData(extra_data)

    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        extra_data = {}
        if type is not None:
            extra_data = {
                "type": str(type),
                "value": str(value),
                "traceback": str(traceback.format_exc()),
            }
        elif self.extra_data is not None:
            extra_data = self.extra_data

        self.inner.get().SetExtraData(json.dumps(extra_data).encode("ascii"))

        # Deleting the CProfilingEvent will add it to a queue to be pushed to
        # the driver.
        self.inner.reset()
