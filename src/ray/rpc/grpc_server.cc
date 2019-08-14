
#include "src/ray/rpc/grpc_server.h"
#include <grpcpp/impl/service_type.h>

namespace ray {
namespace rpc {

void GrpcServer::Run() {
  std::string server_address;
  // Set unix domain socket or tcp address.
  if (!unix_socket_path_.empty()) {
    server_address = "unix://" + unix_socket_path_;
  } else {
    server_address = "0.0.0.0:" + std::to_string(port_);
  }

  grpc::ServerBuilder builder;
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Build and start server.
  server_ = builder.BuildAndStart();
  if (unix_socket_path_.empty()) {
    // For a TCP-based server, the actual port is decided after `AddListeningPort`.
    server_address = "0.0.0.0:" + std::to_string(port_);
  }
  RAY_LOG(INFO) << name_ << " server started, listening on " << server_address;

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
        HandleReceivedRequest(server_call);
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

void GrpcServer::HandleReceivedRequest(ServerCall *server_call) {
  auto request_index_str = server_call->GetClientMeta("REQUEST_INDEX");
  if (request_index_str.empty()) {
    // If this request doesn't have `request_index`, it means that we don't need to guarantee
    // request order. So we can immediately handle this request.
    server_call->HandleRequest();
  } else {
    auto client_id = server_call->GetClientMeta("CLIENT_ID");
    RAY_CHECK(!client_id.empty());
    auto request_index = std::stoull(request_index_str);
    auto &pending_requests = pending_requests_by_client_id_[client_id];
    if (request_index == pending_requests.next_request_index_to_handle) {
      // If this request is the next expected request, keep handling all available requests in the buffer.
      while (true) {
        auto it =
            pending_requests.buffer.find(pending_requests.next_request_index_to_handle);
        if (it == pending_requests.buffer.end()) {
          break;
        }
        it->second->HandleRequest();
        pending_requests.buffer.erase(it);
        pending_requests.next_request_index_to_handle++;
      }
    } else {
      // Buffer this request, if it's received earlier than the next expected request.
      pending_requests.buffer.emplace(request_index, server_call);
    }
  }
}

}  // namespace rpc
}  // namespace ray
