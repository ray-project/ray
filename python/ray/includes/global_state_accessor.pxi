from ray.includes.common cimport (
    CGcsClientOptions
)

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
from libcpp.memory cimport make_unique as c_make_unique

cdef class GlobalStateAccessor:
    """Cython wrapper class of C++ `ray::gcs::GlobalStateAccessor`."""
    cdef:
        unique_ptr[CGlobalStateAccessor] inner

    def __cinit__(self, GcsClientOptions gcs_options):
        cdef CGcsClientOptions *opts
        opts = gcs_options.native()
        self.inner = c_make_unique[CGlobalStateAccessor](opts[0])

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

    def get_next_job_id(self):
        cdef CJobID cjob_id
        with nogil:
            cjob_id = self.inner.get().GetNextJobID()
        return cjob_id.ToInt()

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

    def get_task_events(self):
        cdef c_vector[c_string] result
        with nogil:
            result = self.inner.get().GetAllTaskEvents()
        return result

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

    def get_placement_group_by_name(self, placement_group_name, ray_namespace):
        cdef unique_ptr[c_string] result
        cdef c_string cplacement_group_name = placement_group_name
        cdef c_string cray_namespace = ray_namespace
        with nogil:
            result = self.inner.get().GetPlacementGroupByName(
                cplacement_group_name, cray_namespace)
        if result:
            return c_string(result.get().data(), result.get().size())
        return None

    def get_system_config(self):
        return self.inner.get().GetSystemConfig()

    def get_node_to_connect_for_driver(self, node_ip_address):
        cdef CRayStatus status
        cdef c_string cnode_ip_address = node_ip_address
        cdef c_string cnode_to_connect
        with nogil:
            status = self.inner.get().GetNodeToConnectForDriver(
                cnode_ip_address, &cnode_to_connect)
        if not status.ok():
            raise RuntimeError(status.message())
        return cnode_to_connect
