from ray.includes.common cimport *
from ray.includes.libraylet_client cimport *
include "includes/unique_ids.pxi"

from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr


cdef class RayletClient:
    cdef unique_ptr[LibRayletClient] client
    def __cinit__(self, const c_string & raylet_socket,
                  ClientID client_id,
                  c_bool is_worker,
                  JobID driver_id,
                  Language language):
        self.client.reset(new LibRayletClient(raylet_socket, client_id.data,
                          is_worker, driver_id.data, PYTHON))

    def disconnect(self):
        cdef:
            RayStatus status = self.client.get().Disconnect()
        if not status.ok():
            raise ConnectionError("[RayletClient] Failed to disconnect.")

    def submit_task(self, const vector[ObjectID] & execution_dependencies,
                    const RayletTaskSpecification & task_spec):
        raise NotImplementedError

    def get_task(self):
        return NotImplementedError

    def fetch_or_reconstruct(self, vector[ObjectID] & object_ids,
                             c_bool fetch_only,
                             const TaskID & current_task_id):
        raise NotImplementedError

    def notify_blocked(self, const TaskID & current_task_id):
        raise NotImplementedError

    def wait(self, const vector[ObjectID] & object_ids, int num_returns,
             int64_t timeout_milliseconds, c_bool wait_local,
             TaskID current_task_id):
        cdef:
            WaitResultPair *result

        raise NotImplementedError

    def resource_ids(self):
        raise NotImplementedError

    def compute_put_id(self):
        raise NotImplementedError

    def push_error(self, JobID job_id, const c_string & type,
                   const c_string & error_message, double timestamp):
        raise NotImplementedError

    def push_profile_events(self, const ProfileTableDataT & profile_events):
        raise NotImplementedError

    def free_objects(self, const vector[ObjectID] & object_ids,
                     c_bool local_only):
        raise NotImplementedError

    @property
    def language(self):
        return self.client.get().GetLanguage()

    @property
    def client_id(self):
        return ClientID.from_native(self.client.get().GetClientID())

    @property
    def driver_id(self):
        return JobID.from_native(self.client.get().GetDriverID())

    @property
    def is_worker(self):
        return self.client.get().IsWorker()
