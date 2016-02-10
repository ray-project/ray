#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#include "orchestra.grpc.pb.h"
#include "orchlib.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class Client {
 public:
  Client(std::shared_ptr<Channel> channel)
      : stub_(Orchestra::NewStub(channel)) {}

  size_t RemoteCall(const std::string& name) {
    RemoteCallRequest request;
    request.set_name(name);

    RemoteCallReply reply;
    ClientContext context;

    Status status = stub_->RemoteCall(&context, request, &reply);

    return reply.result();
  }

  void RegisterWorker() {
    RegisterWorkerRequest request;
    RegisterWorkerReply reply;
    ClientContext context;
    Status status = stub_->RegisterWorker(&context, request, &reply);
    return;
  }

 private:
  std::unique_ptr<Orchestra::Stub> stub_;
};

class WorkerServiceImpl final : public Worker::Service {
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

void* orch_create_context(const char* server_addr) {
  Client* client = new Client(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));
  client->RegisterWorker();
  return client;
}

size_t orch_remote_call(void* context, const char* name, void* args) {
  Client* client = (Client*)context;
  return client->RemoteCall(std::string(name));
}

int main(int argc, char** argv) {
  Client greeter(
      grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()));
  std::string user("world");
  greeter.RemoteCall(user);

  return 0;
}
