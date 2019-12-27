from libcpp.string cimport string as c_string

from ray.includes.libcoreworker cimport CProfileEvent

import json
import traceback

cdef class ProfileEvent:
    """Cython wrapper class of C++ `ray::worker::ProfileEvent`."""
    cdef:
        unique_ptr[CProfileEvent] inner
        object extra_data

    @staticmethod
    cdef make(unique_ptr[CProfileEvent] event, object extra_data):
        cdef ProfileEvent self = ProfileEvent.__new__(ProfileEvent)
        self.inner = move(event)
        self.extra_data = extra_data
        return self

    def set_extra_data(self, c_string extra_data):
        self.inner.get().SetExtraData(extra_data)

    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        extra_data = None
        if type is not None:
            extra_data = {
                "type": str(type),
                "value": str(value),
                "traceback": str(traceback.format_exc()),
            }
        elif self.extra_data is not None:
            extra_data = self.extra_data

        if not extra_data:
            self.inner.get().SetExtraData(b"{}")
        elif isinstance(extra_data, dict):
            self.inner.get().SetExtraData(
                json.dumps(extra_data).encode("ascii"))
        else:
            self.inner.get().SetExtraData(extra_data)

        # Deleting the CProfileEvent will add it to a queue to be pushed to
        # the driver.
        self.inner.reset()
