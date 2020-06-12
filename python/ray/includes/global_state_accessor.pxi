from ray.includes.unique_ids cimport (
    CActorID,
    CClientID,
    CObjectID,
)

from ray.includes.global_state_accessor cimport (
    CGlobalStateAccessor,
)

from libcpp.string cimport string as c_string

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

    def get_node_table(self):
        return self.inner.get().GetAllNodeInfo()

    def get_profile_table(self):
        return self.inner.get().GetAllProfileInfo()

    def get_object_table(self):
        return self.inner.get().GetAllObjectInfo()

    def get_object_info(self, object_id):
        object_info = self.inner.get().GetObjectInfo(CObjectID.FromBinary(object_id.binary()))
        if object_info:
            return c_string(object_info.get().data(), object_info.get().size())
        return None

    def get_actor_table(self):
        return self.inner.get().GetAllActorInfo()

    def get_actor_info(self, actor_id):
        actor_info = self.inner.get().GetActorInfo(CActorID.FromBinary(actor_id.binary()))
        if actor_info:
            return c_string(actor_info.get().data(), actor_info.get().size())
        return None

    def get_node_resource_info(self, node_id):
        return self.inner.get().GetNodeResourceInfo(CClientID.FromBinary(node_id.binary()))
