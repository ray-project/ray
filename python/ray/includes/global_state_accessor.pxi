from ray.includes.global_state_accessor cimport (
    CGlobalStateAccessor,
)

cdef class GlobalStateAccessor:
    """Cython wrapper class of C++ `ray::gcs::GlobalStateAccessor`."""
    cdef:
        unique_ptr[CGlobalStateAccessor] inner

    def __init__(self, redis_address, redis_password, c_bool is_test_client=False):
        if not redis_password:
            redis_password = ""
        self.inner.reset(
            new CGlobalStateAccessor(redis_address.encode("ascii"),
                redis_password.encode("ascii"), is_test_client))

    def connect(self):
        return self.inner.get().Connect()

    def disconnect(self):
        self.inner.get().Disconnect()

    def get_job_table(self):
        return self.inner.get().GetAllJobInfo()

    def get_profile_table(self):
        return self.inner.get().GetAllProfileInfo()
