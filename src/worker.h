#ifndef ORCHESTRA_WORKER_H
#define ORCHESTRA_WORKER_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <boost/interprocess/managed_shared_memory.hpp>
using namespace boost::interprocess;

#include <grpc++/grpc++.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#include "orchestra.grpc.pb.h"
#include "orchlib.h"

#include "orchestra/orchestra.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;

class WorkerServiceImpl final : public WorkerServer::Service {
  Status InvokeCall(ServerContext* context, const InvokeCallRequest* request,
    InvokeCallReply* reply) override {
      std::cout << "invoke call request" << std::endl;
      return Status::OK;
  }
};

void start_server() {
  std::string server_address("0.0.0.0:50053");
  WorkerServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

class Worker {
  managed_shared_memory segment;
 public:
  Worker(std::shared_ptr<Channel> scheduler_channel, std::shared_ptr<Channel> objstore_channel)
      : scheduler_stub_(SchedulerServer::NewStub(scheduler_channel)),
        objstore_stub_(ObjStore::NewStub(objstore_channel))
    {}

  size_t RemoteCall(RemoteCallRequest* request) {
    // RemoteCallReply reply;
    // ClientContext context;

    // Status status = stub_->RemoteCall(&context, *request, &reply);

    // return reply.result();
    return 42;
  }

  void register_worker(const std::string& worker_address, const std::string& objstore_address) {
    RegisterWorkerRequest request;
    request.set_worker_address(worker_address);
    request.set_objstore_address(objstore_address);
    RegisterWorkerReply reply;
    ClientContext context;
    Status status = scheduler_stub_->RegisterWorker(&context, request, &reply);
    return;
  }

  const size_t CHUNK_SIZE = 8 * 1024;

  ObjRef PushObj(Obj* obj) {
    // first get objref for the new object
    PushObjRequest push_request;
    PushObjReply push_reply;
    ClientContext push_context;
    Status push_status = scheduler_stub_->PushObj(&push_context, push_request, &push_reply);
    ObjRef objref = push_reply.objref();
    ObjChunk chunk;
    std::string data;
    obj->SerializeToString(&data);
    size_t totalsize = data.size();
    ClientContext context;
    AckReply reply;
    std::unique_ptr<ClientWriter<ObjChunk> > writer(
      objstore_stub_->StreamObj(&context, &reply));
    const char* head = data.c_str();
    for (size_t i = 0; i < data.length(); i += CHUNK_SIZE) {
      chunk.set_objref(objref);
      std::cout << "chunk totalsize" << std::endl;
      chunk.set_totalsize(totalsize);
      chunk.set_data(head + i, std::min(CHUNK_SIZE, data.length() - i));
      if (!writer->Write(chunk)) {
        std::cout << "write failed" << std::endl;
        // throw std::runtime_error("write failed");
      }
    }
    writer->WritesDone();
    Status status = writer->Finish();
    return objref;
  }

  slice GetSerializedObj(ObjRef objref) {
    ClientContext context;
    GetObjRequest request;
    request.set_objref(objref);
    GetObjReply reply;
    objstore_stub_->GetObj(&context, request, &reply);
    segment = managed_shared_memory(open_only, reply.bucket().c_str());
    slice slice;
    slice.data = static_cast<char*>(segment.get_address_from_handle(reply.handle()));
    slice.len = reply.size();
    return slice;
  }

  void register_function(const std::string& name, size_t num_return_vals) {
    ClientContext context;
    RegisterFunctionRequest request;
    request.set_fnname(name);
    request.set_num_return_vals(num_return_vals);
    AckReply reply;
    scheduler_stub_->RegisterFunction(&context, request, &reply);
  }

  void MainLoop() {
    scheduler_thread = std::thread(start_server);

  }



 private:
  std::unique_ptr<SchedulerServer::Stub> scheduler_stub_;
  std::unique_ptr<ObjStore::Stub> objstore_stub_;
  std::thread scheduler_thread;
};

#endif
