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
    def from_redis_address(
            cls, redis_address,
            redis_password,
            c_bool enable_sync_conn=True,
            c_bool enable_async_conn=True,
            c_bool enable_subscribe_conn=True):
        if not redis_password:
            redis_password = ""
        redis_ip, redis_port = redis_address.split(":")
        self = GcsClientOptions()
        self.inner.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  int(redis_port),
                                  redis_password.encode("ascii"),
                                  enable_sync_conn,
                                  enable_async_conn,
                                  enable_subscribe_conn))
        return self

    @classmethod
    def from_gcs_address(cls, gcs_address):
        self = GcsClientOptions()
        self.inner.reset(
            new CGcsClientOptions(gcs_address.encode("ascii")))
        return self

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())
