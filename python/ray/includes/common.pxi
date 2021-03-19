from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CGcsClientOptions,
)


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] inner

    def __init__(self, redis_ip, int redis_port,
                 redis_password):
        if not redis_password:
            redis_password = ""
        self.inner.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  redis_port,
                                  redis_password.encode("ascii")))

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())
