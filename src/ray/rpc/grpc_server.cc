
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

void GrpcServer::ProcessDefaultCall(std::shared_ptr<ServerCall> server_call,
                                    ServerCallTag *tag, bool ok) {
  bool delete_tag = false;
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
      // GRPC has sent reply successfully, invoking the callback.
      server_call->OnReplySent();
      // The rpc call has finished and can be deleted now.
      delete_tag = true;
      break;
    default:
      RAY_LOG(FATAL) << "Shouldn't reach here.";
      break;
    }
  } else {
    if (server_call->GetState() == ServerCallState::SENDING_REPLY) {
      server_call->OnReplyFailed();
    }
    delete_tag = true;
  }
  if (delete_tag) {
    delete tag;
  }
}

void GrpcServer::ProcessStreamCall(std::shared_ptr<ServerCall> server_call,
                                   ServerCallTag *tag, bool ok) {
  if (ok) {
    if (tag->IsReplyWriterTag()) {
      server_call->AsyncWriteNextReply();
    } else {
      switch (server_call->GetState()) {
      case ServerCallState::CONNECT:
        server_call->OnConnectingFinished();
        server_call->AsyncReadNextRequest();
        break;
      case ServerCallState::PENDING:
        server_call->HandleRequest();
        break;
      case ServerCallState::FINISH:
        server_call->DeleteServerCallTag();
        RAY_LOG(INFO)
            << "Stream server received received `FINISH` from completion queue.";
        break;
      default:
        RAY_LOG(FATAL) << "Shouldn't reach here.";
        break;
      }
    }
  } else {
    RAY_LOG(DEBUG) << "Receive `ok == false`, tag type: "
                   << static_cast<int>(tag->GetType())
                   << ", call type: " << static_cast<int>(tag->GetCall()->GetType());

    if (tag->IsReplyWriterTag()) {
      server_call->DeleteReplyWriterTag();
    } else {
      server_call->Finish();
    }
  }
}

void GrpcServer::PollEventsFromCompletionQueue() {
  void *got_tag;
  bool ok;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (cq_->Next(&got_tag, &ok)) {
    auto tag = reinterpret_cast<ServerCallTag *>(got_tag);
    auto call = tag->GetCall();
    auto call_type = call->GetType();
    switch (call_type) {
    case ServerCallType::UNARY_ASYNC_CALL:
      ProcessDefaultCall(call, tag, ok);
      break;
    case ServerCallType::STREAM_ASYNC_CALL:
      ProcessStreamCall(call, tag, ok);
      break;
    default:
      RAY_LOG(FATAL) << "Shouldn't reach here, unrecognized type: "
                     << static_cast<int>(call_type);
      break;
    }
  }
}

}  // namespace rpc
}  // namespace ray
