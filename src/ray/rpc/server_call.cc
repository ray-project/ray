#include "ray/rpc/server_call.h"

void ServerCallImplBase::HandleRequest() {
  if (!io_service_.stopped()) {
    io_service_.post([this] { HandleRequestImpl(); });
  } else {
    // Handle service for rpc call has stopped, we must handle the call here
    // to send reply and remove it from cq
    RAY_LOG(DEBUG) << "Handle service has been closed.";
    SendReply(Status::Invalid("HandleServiceClosed"));
  }
}

void ServerCallImplBase::HandleRequestImpl() {
  state_ = ServerCallState::PROCESSING;
  // NOTE(hchen): This `factory` local variable is needed. Because `SendReply` runs in
  // a different thread, and will cause `this` to be deleted.
  const auto &factory = factory_;
  (service_handler_.*handle_request_function_)(
      request_, &reply_,
      [this](Status status, std::function<void()> success,
             std::function<void()> failure) {
        // These two callbacks must be set before `SendReply`, because `SendReply`
        // is async and this `ServerCall` might be deleted right after `SendReply`.
        send_reply_success_callback_ = std::move(success);
        send_reply_failure_callback_ = std::move(failure);

        // When the handler is done with the request, tell gRPC to finish this request.
        // Must send reply at the bottom of this callback, once we invoke this funciton,
        // this server call might be deleted
        SendReply(status);
      });
  // We've finished handling this request,
  // create a new `ServerCall` to accept the next incoming request.
  factory.CreateCall();
}

void ServerCallImplBase::OnReplySent() {
  if (send_reply_success_callback_ && !io_service_.stopped()) {
    auto callback = std::move(send_reply_success_callback_);
    io_service_.post([callback]() { callback(); });
  }
}

void ServerCallImplBase::OnReplyFailed() {
  if (send_reply_failure_callback_ && !io_service_.stopped()) {
    auto callback = std::move(send_reply_failure_callback_);
    io_service_.post([callback]() { callback(); });
  }
}

void ServerCallImplBase::SendReply(const Status &status) {
  state_ = ServerCallState::SENDING_REPLY;
  response_writer_.Finish(reply_, RayStatusToGrpcStatus(status), this);
}
