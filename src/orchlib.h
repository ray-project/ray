// A minimal C API that is used for implementing Orchestra workers in C based
// languages (Python at the moment, in the future potentially Julia, R, MATLAB)

extern "C" {

struct slice {
  char* data;
  size_t len;
};

struct Worker;
struct RemoteCallRequest;
struct Value;

// connect to the scheduler and the object store
Worker* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr);
// start the worker service for this worker
void orch_start_worker_service(Worker* worker);
// Submit a function call to the scheduler
size_t orch_remote_call(Worker* worker, RemoteCallRequest* request);
size_t orch_push(Worker* worker, Obj* value);
Call* orch_wait_for_next_task(Worker* worker);
slice orch_get_serialized_obj(Worker* worker, size_t objref);
void orch_register_function(Worker* worker, const char* name, size_t num_return_vals);

}
