#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "orchestra.grpc.pb.h"
#include "orchlib.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class Client {
 public:
  Client(std::shared_ptr<Channel> channel)
      : stub_(Orchestra::NewStub(channel)) {}

  void RemoteCall(const std::string& name) {
    RemoteCallRequest request;
    request.set_name(name);

    RemoteCallReply reply;
    ClientContext context;

    Status status = stub_->RemoteCall(&context, request, &reply);

    return;
  }

 private:
  std::unique_ptr<Orchestra::Stub> stub_;
};

void* orch_create_context(const char* server_addr) {
  Client* client = new Client(grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()));
  return client;
}

size_t orch_remote_call(void* context, const char* name, void* args) {
  Client* client = (Client*)context;
  client->RemoteCall(std::string(name));
}

int main(int argc, char** argv) {
  Client greeter(
      grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()));
  std::string user("world");
  greeter.RemoteCall(user);

  return 0;
}
