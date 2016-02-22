#include "worker.h"

Worker* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr) {
  auto server_channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
  auto objstore_channel = grpc::CreateChannel(objstore_addr, grpc::InsecureChannelCredentials());
  Worker* worker = new Worker(server_channel, objstore_channel);
  worker->register_worker(std::string(worker_addr), std::string(objstore_addr));
  return worker;
}

size_t orch_remote_call(Worker* worker, RemoteCallRequest* request) {
  return worker->RemoteCall(request);
}

void orch_main_loop(Worker* worker) {
  worker->MainLoop();
}

size_t orch_push(Worker* worker, Obj* obj) {
  return worker->PushObj(obj);
}

slice orch_get_serialized_obj(Worker* worker, ObjRef objref) {
  return worker->GetSerializedObj(objref);
}

void orch_register_function(Worker* worker, const char* name, size_t num_return_vals) {
  // worker->register_function(std::string(name), num_return_vals);
}
