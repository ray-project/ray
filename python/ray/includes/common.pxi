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

    cdef from_redis_address(redis_ip, int redis_port,
                            redis_password,
                            c_bool enable_sync_conn = True,
                            c_bool enable_async_conn = True,
                            c_bool enable_subscribe_conn = True):
        if not redis_password:
            redis_password = ""
        self = GcsClientOptions()
        self.inner.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  redis_port,
                                  redis_password.encode("ascii"),
                                  enable_sync_conn,
                                  enable_async_conn,
                                  enable_subscribe_conn))
        return self

    cdef from_gcs_address(gcs_ip, int gcs_port):
        self = GcsClientOptions()
        self.inner.reset(new CGcsClientOptions(gcs_ip.encode("ascii"), gcs_port))
        return self

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())
