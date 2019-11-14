
#include "src/ray/rpc/grpc_server.h"
#include <grpcpp/impl/service_type.h>

namespace {

bool PortNotInUse(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    return false;
  }
  struct sockaddr_in server_addr = {0};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(port);
  int err = bind(fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
  close(fd);
  return err == 0;
}

}  // namespace

namespace ray {
namespace rpc {

void GrpcServer::Run() {
  std::string server_address("0.0.0.0:" + std::to_string(port_));
  // Unfortunately, grpc will not return an error if the specified port is in
  // use. There is a race condition here where two servers could check the same
  // port, but only one would succeed in binding.
  if (port_ > 0) {
    RAY_CHECK(PortNotInUse(port_))
        << "Port " << port_
        << " specified by caller already in use. Try passing node_manager_port=... into "
           "ray.init() to pick a specific port";
  }

  grpc::ServerBuilder builder;
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service is found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Build and start server.
  server_ = builder.BuildAndStart();
  RAY_LOG(INFO) << name_ << " server started, listening on port " << port_ << ".";

  // Create calls for all the server call factories.
  for (auto &entry : server_call_factories_and_concurrencies_) {
    for (int i = 0; i < entry.second; i++) {
      // Create and request calls from the factory.
      entry.first->CreateCall();
    }
  }
  // Start a thread that polls incoming requests.
  polling_thread_ = std::thread(&GrpcServer::PollEventsFromCompletionQueue, this);
  // Set the server as running.
  is_closed_ = false;
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
    auto *server_call = static_cast<ServerCall *>(tag);
    bool delete_call = false;
    if (ok) {
      switch (server_call->GetState()) {
      case ServerCallState::PENDING:
        // We've received a new incoming request. Now this call object is used to
        // track this request.
        server_call->SetState(ServerCallState::PROCESSING);
        server_call->HandleRequest();
        break;
      case ServerCallState::SENDING_REPLY:
        // GRPC has sent reply successfully, invoking the callback.
        server_call->OnReplySent();
        // The rpc call has finished and can be deleted now.
        delete_call = true;
        break;
      default:
        RAY_LOG(FATAL) << "Shouldn't reach here.";
        break;
      }
    } else {
      // `ok == false` will occur in two situations:
      // First, the server has been shut down, the server call's status is PENDING
      // Second, server has sent reply to client and failed, the server call's status is
      // SENDING_REPLY
      if (server_call->GetState() == ServerCallState::SENDING_REPLY) {
        server_call->OnReplyFailed();
      }
      delete_call = true;
    }
    if (delete_call) {
      delete server_call;
    }
  }
}

}  // namespace rpc
}  // namespace ray
