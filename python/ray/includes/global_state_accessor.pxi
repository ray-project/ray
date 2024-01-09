from ray.includes.common cimport (
    CGcsClientOptions,
    CGcsNodeState,
    PythonGetResourcesTotal,
    PythonGetNodeLabels
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
    RedisDelKeySync,
)

from ray.includes.optional cimport (
    optional,
    nullopt,
    make_optional
)

from libc.stdint cimport uint32_t as c_uint32_t, int32_t as c_int32_t
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
        cdef:
            c_vector[c_string] items
            c_string item
            CGcsNodeInfo c_node_info
            unordered_map[c_string, double] c_resources
        with nogil:
            items = self.inner.get().GetAllNodeInfo()
        results = []
        for item in items:
            c_node_info.ParseFromString(item)
            node_info = {
                "NodeID": ray._private.utils.binary_to_hex(c_node_info.node_id()),
                "Alive": c_node_info.state() == CGcsNodeState.ALIVE,
                "NodeManagerAddress": c_node_info.node_manager_address().decode(),
                "NodeManagerHostname": c_node_info.node_manager_hostname().decode(),
                "NodeManagerPort": c_node_info.node_manager_port(),
                "ObjectManagerPort": c_node_info.object_manager_port(),
                "ObjectStoreSocketName":
                    c_node_info.object_store_socket_name().decode(),
                "RayletSocketName": c_node_info.raylet_socket_name().decode(),
                "MetricsExportPort": c_node_info.metrics_export_port(),
                "NodeName": c_node_info.node_name().decode(),
                "RuntimeEnvAgentPort": c_node_info.runtime_env_agent_port(),
            }
            node_info["alive"] = node_info["Alive"]
            c_resources = PythonGetResourcesTotal(c_node_info)
            node_info["Resources"] = (
                {key.decode(): value for key, value in c_resources}
                if node_info["Alive"]
                else {}
            )
            c_labels = PythonGetNodeLabels(c_node_info)
            node_info["Labels"] = \
                {key.decode(): value.decode() for key, value in c_labels}
            results.append(node_info)
        return results

    def get_draining_nodes(self):
        cdef c_vector[CNodeID] draining_nodes
        with nogil:
            draining_nodes = self.inner.get().GetDrainingNodes()
        results = set()
        for draining_node in draining_nodes:
            results.add(ray._private.utils.binary_to_hex(draining_node.Binary()))
        return results

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

    def get_actor_table(self, job_id, actor_state_name):
        cdef c_vector[c_string] result
        cdef optional[CActorID] cactor_id = nullopt
        cdef optional[CJobID] cjob_id
        cdef optional[c_string] cactor_state_name
        cdef c_string c_name
        if job_id is not None:
            cjob_id = make_optional[CJobID](CJobID.FromBinary(job_id.binary()))
        if actor_state_name is not None:
            c_name = actor_state_name
            cactor_state_name = make_optional[c_string](c_name)
        with nogil:
            result = self.inner.get().GetAllActorInfo(
                cactor_id, cjob_id, cactor_state_name)
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

    def get_worker_debugger_port(self, worker_id):
        cdef c_uint32_t result
        cdef CWorkerID cworker_id = CWorkerID.FromBinary(worker_id.binary())
        with nogil:
            result = self.inner.get().GetWorkerDebuggerPort(cworker_id)
        return result

    def update_worker_debugger_port(self, worker_id, debugger_port):
        cdef c_bool result
        cdef CWorkerID cworker_id = CWorkerID.FromBinary(worker_id.binary())
        cdef c_uint32_t cdebugger_port = debugger_port
        with nogil:
            result = self.inner.get().UpdateWorkerDebuggerPort(
                cworker_id,
                cdebugger_port)
        return result

    def update_worker_num_paused_threads(self, worker_id, num_paused_threads_delta):
        cdef c_bool result
        cdef CWorkerID cworker_id = CWorkerID.FromBinary(worker_id.binary())
        cdef c_int32_t cnum_paused_threads_delta = num_paused_threads_delta

        with nogil:
            result = self.inner.get().UpdateWorkerNumPausedThreads(
                cworker_id, cnum_paused_threads_delta)
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
        cdef CGcsNodeInfo c_node_info
        with nogil:
            status = self.inner.get().GetNodeToConnectForDriver(
                cnode_ip_address, &cnode_to_connect)
        if not status.ok():
            raise RuntimeError(status.message())
        c_node_info.ParseFromString(cnode_to_connect)
        return {
            "object_store_socket_name": c_node_info.object_store_socket_name().decode(),
            "raylet_socket_name": c_node_info.raylet_socket_name().decode(),
            "node_manager_port": c_node_info.node_manager_port(),
        }
