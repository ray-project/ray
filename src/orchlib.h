extern "C" {

struct slice {
  char* data;
  size_t len;
};

struct Worker;
struct RemoteCallRequest;
struct Value;

Worker* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr);
size_t orch_remote_call(Worker* context, RemoteCallRequest* request);
size_t orch_push(Worker* context, Obj* value);
Call* orch_main_loop(Worker* worker);
slice orch_get_serialized_obj(Worker* worker, size_t objref);
void orch_register_function(Worker* worker, const char* name, size_t num_return_vals);

}
