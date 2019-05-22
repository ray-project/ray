#include <ctime>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <grpc++/grpc++.h>

#include "example/proto/greeter.grpc.pb.h"
#include "example/proto/greeter.pb.h"

using greeter::Greeter;
using greeter::HelloReply;
using greeter::HelloRequest;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class TimeGreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext *context, const HelloRequest *request,
                  HelloReply *reply) override {
    std::time_t result = std::time(nullptr);
    std::ostringstream ostream;
    ostream << std::asctime(std::localtime(&result));
    std::string time_stamp = ostream.str();
    // Remove '\n' at end.
    time_stamp.resize(time_stamp.length() - 1);
    reply->set_message(time_stamp + ": Hello " + request->name());
    std::cerr << time_stamp << ": Received greeting from: " << request->name()
              << std::endl;
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("localhost:10086");
  TimeGreeterServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char **argv) {
  RunServer();
  return 0;
}
