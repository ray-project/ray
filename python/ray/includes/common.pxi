from libcpp.string cimport string as c_string

from ray.includes.common cimport CGcsClientOptions


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] gcs_client_options

    def __init__(self, redis_ip, int redis_port, redis_password):
        if not redis_password:
            redis_password = ""
        self.gcs_client_options.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  redis_port,
                                  redis_password.encode("ascii")))

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.gcs_client_options.get())
