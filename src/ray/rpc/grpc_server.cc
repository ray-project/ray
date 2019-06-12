#include "ray/rpc/grpc_server.h"

namespace ray {

void GrpcServer::Run() {
  std::string server_address("0.0.0.0:" + std::to_string(port_));

  ::grpc::ServerBuilder builder;
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, ::grpc::InsecureServerCredentials(), &port_);
  // Allow subclasses to register concrete services.
  RegisterServices(builder);
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Build and start server.
  server_ = builder.BuildAndStart();
  RAY_LOG(DEBUG) << name_ << " server started, listening on port " << port_ << ".";

  // Allow subclasses to initialize the server call factories.
  InitServerCallFactories(&server_call_factories_);
  for (auto &factory : server_call_factories_) {
    // Create and request calls from the factories.
    factory->CreateCall();
  }
  // Start polling incoming requests.
  StartPolling();
}

void GrpcServer::StartPolling() {
  auto polling_func = [this]() {
    void *tag;
    bool ok;
    while (cq_->Next(&tag, &ok)) {
      ServerCall *server_call = static_cast<ServerCall *>(tag);
      // `ok == False` indicates that the server has been shut down.
      // We should delete the call object in this case.
      bool delete_call = !ok;
      if (ok) {
        switch (server_call->GetState()) {
        case ServerCallState::PENDING:
          // We've received a new incoming request. Now we use this call object
          // to handle this request. So we need to create a new call to handle next
          // incoming request.
          server_call->GetFactory().CreateCall();
          server_call->OnRequestReceived();
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
  };

  polling_thread_.reset(new std::thread(std::move(polling_func)));
}

}  // namespace ray
