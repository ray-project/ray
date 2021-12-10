from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CObjectLocation,
    CGcsClientOptions,
)


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] inner

    @classmethod
    def from_redis_address(cls, redis_ip, int redis_port,
                           redis_password):
        if not redis_password:
            redis_password = ""
        self = cls()
        self.inner.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  redis_port,
                                  redis_password.encode("ascii")))
        return self

    @classmethod
    def from_gcs_address(cls, gcs_ip, gcs_port):
        self = cls()
        self.inner = make_unique[CGcsClientOptions](gcs_ip, gcs_port)
        return self

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())
