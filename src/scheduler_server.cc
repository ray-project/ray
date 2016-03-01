#include "scheduler_server.h"

Status SchedulerServerServiceImpl::RemoteCall(ServerContext* context, const RemoteCallRequest* request, RemoteCallReply* reply) {
  size_t num_return_vals = scheduler_->add_task(request->call());
  for (size_t i = 0; i < num_return_vals; ++i) {
    ObjRef result = scheduler_->register_new_object();
    reply->add_result(result);
  }
  return Status::OK;
}

Status SchedulerServerServiceImpl::PushObj(ServerContext* context, const PushObjRequest* request, PushObjReply* reply) {
  ObjRef objref = scheduler_->register_new_object();
  ObjStoreId objstoreid = scheduler_->get_store(request->workerid());
  scheduler_->add_location(objref, objstoreid);
  reply->set_objref(objref);
  return Status::OK;
}

  /*
  Status PushObj(ServerContext* context, ServerReader<ObjChunk> *reader, AckReply* reply) override {
    ObjChunk chunk;
    while (reader->Read(&chunk)) {

    }
    std::cout << "got chunks" << std::endl;
    return Status::OK;
  }
  */

void start_scheduler_server(const char* server_address) {
  SchedulerServerServiceImpl service;
  ServerBuilder builder;

  builder.AddListeningPort(std::string(server_address), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    return 1;
  }

  start_scheduler_server(argv[1]);

  return 0;
}
