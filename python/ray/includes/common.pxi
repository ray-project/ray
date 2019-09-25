from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CActorHandle,
    CGcsClientOptions,
)


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] inner

    def __init__(self, redis_ip, int redis_port,
                 redis_password, c_bool is_test_client=False):
        if not redis_password:
            redis_password = ""
        self.inner.reset(
            new CGcsClientOptions(redis_ip.encode("ascii"),
                                  redis_port,
                                  redis_password.encode("ascii"),
                                  is_test_client))

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())

cdef class ActorHandle:
    """Cython wrapper class of C++ `ray::ActorHandle`."""
    cdef:
        unique_ptr[CActorHandle] inner

    def __init__(self, ActorID actor_id, ActorHandleID actor_handle_id,
                 JobID job_id, list creation_function_descriptor):
        cdef:
            c_vector[c_string] c_descriptor
            ObjectID cursor = ObjectID.from_random()

        c_descriptor = string_vector_from_list(creation_function_descriptor)
        self.inner.reset(new CActorHandle(
            actor_id.native(), actor_handle_id.native(), job_id.native(),
            cursor.native(), LANGUAGE_PYTHON, False, c_descriptor))

    def fork(self, c_bool ray_forking):
        cdef:
            ActorHandle other = ActorHandle.__new__(ActorHandle)
        if ray_forking:
            other.inner = self.inner.get().Fork()
        else:
            other.inner = self.inner.get().ForkForSerialization()
        return other

    @staticmethod
    def from_bytes(c_string bytes, TaskID current_task_id):
        cdef:
            ActorHandle self = ActorHandle.__new__(ActorHandle)
        self.inner.reset(new CActorHandle(bytes, current_task_id.native()))
        return self

    def to_bytes(self):
        cdef:
            c_string output

        self.inner.get().Serialize(&output)
        return output

    def actor_id(self):
        return ActorID(self.inner.get().GetActorID().Binary())

    def actor_handle_id(self):
        return ActorHandleID(self.inner.get().GetActorHandleID().Binary())
