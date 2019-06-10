#include "ray/rpc/grpc_server.h"

namespace ray {

void GrpcServer::Run() {
  std::string server_address("0.0.0.0:" + std::to_string(port_));

  ::grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, ::grpc::InsecureServerCredentials());
  RegisterServices(builder);
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Finally assemble the server.
  server_ = builder.BuildAndStart();

  EnqueueRequests();
  StartPolling();
}

void GrpcServer::StartPolling() {
  auto polling_func = [this]() {
    void *tag;
    bool ok;
    while (true) {
      RAY_CHECK(cq_->Next(&tag, &ok));
      RAY_CHECK(ok);
      // Handle requests;
      GrpcRequestTag* request_tag = static_cast<GrpcRequestTag *>(tag);
      request_tag->OnCompleted(ok);
    }
  };

  polling_thread_.reset(new std::thread(std::move(polling_func)));
}

}  // namespace ray
