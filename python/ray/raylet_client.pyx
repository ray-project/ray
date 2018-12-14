from ray.includes.common cimport *
from ray.includes.libraylet_client cimport *

cdef class RayletClient:
    def __cinit__(self, raylet_socket, client_id, driver_id, language):
        self.client = LibRayletClient(raylet_socket, client_id, driver_id,
                                      Language.PYTHON)

    def disconnect(self):
        cdef:
            RayStatus status = self.client.Disconnect()
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

    def notify_blocked(const TaskID & current_task_id):
        raise NotImplementedError

    def wait(const vector[ObjectID] & object_ids, int num_returns,
             int64_t timeout_milliseconds, c_bool wait_local,
             const TaskID & current_task_id):
        cdef:
            WaitResultPair *result

        raise NotImplementedError

    def resource_ids(self):
        raise NotImplementedError

    def compute_put_id(self):
        raise NotImplementedError

    def push_error(self, const JobID & job_id, const c_string & type,
                   const c_string & error_message, double timestamp):
        raise NotImplementedError

    def push_profile_events(self, const ProfileTableDataT & profile_events):
        raise NotImplementedError

    def free_objects(self, const vector[ObjectID] & object_ids,
                     c_bool local_only):
        raise NotImplementedError

    @property
    def language(self):
        return self.client.GetLanguage()

    @property
    def client_id(self):
        return self.client.GetClientID()

    @property
    def driver_id(self):
        return self.client.GetDriverID()

    @property
    def is_worker(self):
        return self.client.IsWorker()
