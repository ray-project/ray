from ray.includes.unique_ids cimport (
    CActorID,
    CNodeID,
    CObjectID,
    CWorkerID,
    CPlacementGroupID
)

from ray.includes.global_state_accessor cimport (
    CGlobalStateAccessor,
)

from libcpp.string cimport string as c_string

cdef class GlobalStateAccessor:
    """Cython wrapper class of C++ `ray::gcs::GlobalStateAccessor`."""
    cdef:
        unique_ptr[CGlobalStateAccessor] inner

    def __init__(self, redis_address, redis_password):
        if not redis_password:
            redis_password = ""
        self.inner.reset(
            new CGlobalStateAccessor(
                redis_address.encode("ascii"),
                redis_password.encode("ascii"),
            ),
        )

    def connect(self):
        cdef c_bool result
        with nogil:
            result = self.inner.get().Connect()
        return result

    def disconnect(self):
        with nogil:
            self.inner.get().Disconnect()

    def get_job_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllJobInfo()
        return result

    def get_node_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllNodeInfo()
        return result

    def get_all_available_resources(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllAvailableResources()
        return result

    def get_profile_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllProfileInfo()
        return result

    def get_object_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllObjectInfo()
        return result

    def get_object_info(self, object_id):
        cdef unique_ptr[c_string] object_info
        cdef CObjectID cobject_id = CObjectID.FromBinary(object_id.binary())
        with nogil:
            object_info = self.inner.get().GetObjectInfo(cobject_id)
        if object_info:
            return c_string(object_info.get().data(), object_info.get().size())
        return None

    def get_all_resource_usage(self):
        """Get newest resource usage of all nodes from GCS service."""
        cdef unique_ptr[c_string] result
        with nogil:
            result = self.inner.get().GetAllResourceUsage()
        if result:
            return c_string(result.get().data(), result.get().size())
        return None

    def get_actor_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllActorInfo()
        return result

    def get_actor_info(self, actor_id):
        cdef unique_ptr[c_string] actor_info
        cdef CActorID cactor_id = CActorID.FromBinary(actor_id.binary())
        with nogil:
            actor_info = self.inner.get().GetActorInfo(cactor_id)
        if actor_info:
            return c_string(actor_info.get().data(), actor_info.get().size())
        return None

    def get_node_resource_info(self, node_id):
        cdef c_string result
        cdef CNodeID cnode_id = CNodeID.FromBinary(node_id.binary())
        with nogil:
            result = self.inner.get().GetNodeResourceInfo(cnode_id)
        return result

    def get_worker_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllWorkerInfo()
        return result

    def get_worker_info(self, worker_id):
        cdef unique_ptr[c_string] worker_info
        cdef CWorkerID cworker_id = CWorkerID.FromBinary(worker_id.binary())
        with nogil:
            worker_info = self.inner.get().GetWorkerInfo(cworker_id)
        if worker_info:
            return c_string(worker_info.get().data(), worker_info.get().size())
        return None

    def add_worker_info(self, serialized_string):
        cdef c_bool result
        cdef c_string cserialized_string = serialized_string
        with nogil:
            result = self.inner.get().AddWorkerInfo(cserialized_string)
        return result

    def get_placement_group_table(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllPlacementGroupInfo()
        return result

    def get_placement_group_info(self, placement_group_id):
        cdef unique_ptr[c_string] result
        cdef CPlacementGroupID cplacement_group_id = (
            CPlacementGroupID.FromBinary(placement_group_id.binary()))
        with nogil:
            result = self.inner.get().GetPlacementGroupInfo(
                cplacement_group_id)
        if result:
            return c_string(result.get().data(), result.get().size())
        return None

    def get_placement_group_by_name(self, placement_group_name):
        cdef unique_ptr[c_string] result
        cdef c_string cplacement_group_name = placement_group_name
        with nogil:
            result = self.inner.get().GetPlacementGroupByName(
                cplacement_group_name)
        if result:
            return c_string(result.get().data(), result.get().size())
        return None
