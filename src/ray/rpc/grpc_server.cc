#include "ray/rpc/grpc_server.h"
#include <grpcpp/impl/service_type.h>

namespace ray {
namespace rpc {

void GrpcServer::Run() {
  std::string server_address("0.0.0.0:" + std::to_string(port_));

  grpc::ServerBuilder builder;
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Build and start server.
  server_ = builder.BuildAndStart();
  RAY_LOG(DEBUG) << name_ << " server started, listening on port " << port_ << ".";

  // Create calls for all the server call factories.
  for (auto &entry : server_call_factories_and_concurrencies_) {
    for (int i = 0; i < entry.second; i++) {
      // Create and request calls from the factory.
      entry.first->CreateCall();
    }
  }
  // Start a thread that polls incoming requests.
  std::thread polling_thread(&GrpcServer::PollEventsFromCompletionQueue, this);
  polling_thread.detach();
}

void GrpcServer::RegisterService(GrpcService &service) {
  services_.emplace_back(service.GetGrpcService());
  service.InitServerCallFactories(cq_, &server_call_factories_and_concurrencies_);
}

void GrpcServer::PollEventsFromCompletionQueue() {
  void *tag;
  bool ok;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (cq_->Next(&tag, &ok)) {
    ServerCall *server_call = static_cast<ServerCall *>(tag);
    // `ok == false` indicates that the server has been shut down.
    // We should delete the call object in this case.
    bool delete_call = !ok;
    if (ok) {
      switch (server_call->GetState()) {
      case ServerCallState::PENDING:
        // We've received a new incoming request. Now this call object is used to
        // track this request. So we need to create another call to handle next
        // incoming request.
        server_call->GetFactory().CreateCall();
        server_call->SetState(ServerCallState::PROCESSING);
        server_call->HandleRequest();
        break;
      case ServerCallState::SENDING_REPLY:
        // The reply has been sent, this call can be deleted now.
        // This event is triggered by `ServerCallImpl::SendReply`.
        delete_call = true;
        break;
      default:
        RAY_LOG(FATAL) << "Shouldn't reach here.";
        break;
      }
    }
    if (delete_call) {
      delete server_call;
    }
  }
}

}  // namespace rpc
}  // namespace ray
