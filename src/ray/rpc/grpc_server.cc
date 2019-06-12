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

  InitServerCallFactories(&server_call_factories_);
  for (auto &factory : server_call_factories_) {
    factory->CreateCall();
  }
  StartPolling();
  RAY_LOG(DEBUG) << "Grpc server started " << server_address;
}

void GrpcServer::StartPolling() {
  auto polling_func = [this]() {
    void *tag;
    bool ok;
    while (cq_->Next(&tag, &ok)) {
      UntypedServerCall *server_call = static_cast<UntypedServerCall *>(tag);
      // `ok == False` indicates that the server has been shut down.
      // We should delete the call object in this case.
      bool delete_call = !ok;
      if (ok) {
        switch (server_call->GetState()) {
        case ServerCallState::PENDING:
          server_call->GetFactory().CreateCall();
          break;
        case ServerCallState::PROCECCSSING:
          delete_call = true;
          break;
        case ServerCallState::REPLY_SENT:
          RAY_LOG(FATAL) << "Shouldn't reach here.";
          break;
        }
        server_call->Proceed();
      }
      if (delete_call) {
        delete server_call;
      }
    }
  };

  polling_thread_.reset(new std::thread(std::move(polling_func)));
}

}  // namespace ray
