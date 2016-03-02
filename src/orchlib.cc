#include "worker.h"

Worker* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr) {
  auto server_channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
  auto objstore_channel = grpc::CreateChannel(objstore_addr, grpc::InsecureChannelCredentials());
  Worker* worker = new Worker(std::string(worker_addr), server_channel, objstore_channel);
  worker->register_worker(std::string(worker_addr), std::string(objstore_addr));
  return worker;
}

size_t orch_remote_call(Worker* worker, RemoteCallRequest* request) {
  return worker->remote_call(request);
}

void orch_start_worker_service(Worker* worker) {
  worker->start_worker_service();
}

Call* orch_wait_for_next_task(Worker* worker) {
  return worker->receive_next_task();
}

size_t orch_push(Worker* worker, Obj* obj) {
  return worker->push_obj(obj);
}

slice orch_get_serialized_obj(Worker* worker, ObjRef objref) {
  return worker->get_serialized_obj(objref);
}

void orch_register_function(Worker* worker, const char* name, size_t num_return_vals) {
  worker->register_function(std::string(name), num_return_vals);
}
