#ifndef RAY_RPC_SERVER_CALL_H
#define RAY_RPC_SERVER_CALL_H

#include <grpcpp/grpcpp.h>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

/// Represents the callback function to be called when a `ServiceHandler` finishes
/// handling a request.
/// \param status The status would be returned to client.
/// \param success Success callback which will be invoked when the reply is successfully
/// sent to the client.
/// \param failure Failure callback which will be invoked when the reply fails to be
/// sent to the client.
using SendReplyCallback = std::function<void(Status status, std::function<void()> success,
                                             std::function<void()> failure)>;

/// At present, this gRPC server supports two types of request.
/// The first is default asynchronous request call that each call contains exactly a request and a reply.
/// Second, asynchronous stream call that the client can send a sequence of request messages
///         and receive a sequence of reply messages, and the number of reply messages is determined by user.
enum class ServerCallType {
  DEFAULT_ASYNC_CALL = 1,
  STREAM_ASYNC_CALL = 2,
};

/// Represents state of a `ServerCall`.
enum class ServerCallState {
  /// The call is created and waiting for an incoming request.
  PENDING,
  /// Request is received and being processed.
  PROCESSING,
  /// Request processing is done, and reply is being sent to client.
  SENDING_REPLY
};

class ServerCallFactory;

/// Represents an incoming request of a gRPC server.
///
/// The lifecycle and state transition of a `ServerCall` is as follows:
///
/// --(1)--> PENDING --(2)--> PROCESSING --(3)--> SENDING_REPLY --(4)--> [FINISHED]
///
/// (1) The `GrpcServer` creates a `ServerCall` and use it as the tag to accept requests
///     gRPC `CompletionQueue`. Now the state is `PENDING`.
/// (2) When a request is received, an event will be gotten from the `CompletionQueue`.
///     `GrpcServer` then should change `ServerCall`'s state to PROCESSING and call
///     `ServerCall::HandleRequest`.
/// (3) When the `ServiceHandler` finishes handling the request, `ServerCallImpl::Finish`
///     will be called, and the state becomes `SENDING_REPLY`.
/// (4) When the reply is sent, an event will be gotten from the `CompletionQueue`.
///     `GrpcServer` will then delete this call.
///
/// NOTE(hchen): Compared to `ServerCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `GrpcServer`) not having to use
/// template as well.
class ServerCall {
 public:
  /// Get the state of this `ServerCall`.
  virtual ServerCallState GetState() const = 0;

  /// Set state of this `ServerCall`.
  virtual void SetState(const ServerCallState &new_state) = 0;

  /// Handle the requst. This is the callback function to be called by
  /// `GrpcServer` when the request is received.
  virtual void HandleRequest() = 0;

  /// Get the factory that created this `ServerCall`.
  virtual const ServerCallFactory &GetFactory() const = 0;

  /// Invoked when sending reply successes.
  virtual void OnReplySent() = 0;

  // Invoked when sending reply fails.
  virtual void OnReplyFailed() = 0;

  /// Virtual destruct function to make sure subclass would destruct properly.
  virtual ~ServerCall() = default;

  ServerCall(ServerCallType type) type_(type) {}

  const ServerCallType &GetCallType() { return type_; }
 protected:
  /// Context for the request, allowing to tweak aspects of it such as the use
  /// of compression, authentication, as well as to send metadata back to the client.
  grpc::ServerContext context_;
  /// The type of this server call.
  const ServerCallType type_;
};

/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                       SendReplyCallback);

/// Implementation of `ServerCall`. It represents `ServerCall` for a particular
/// RPC method.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
class ServerCallImpl : public ServerCall {
 public:
  /// Constructor.
  ///
  /// \param[in] factory The factory which created this call.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] io_service The event loop.
  ServerCallImpl(
      const ServerCallFactory &factory, ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      boost::asio::io_service &io_service)
      : ServerCall(ServerCallType::DEFAULT_ASYNC_CALL),
        state_(ServerCallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        response_writer_(&context_),
        io_service_(io_service) {}

  ServerCallState GetState() const override { return state_; }

  void SetState(const ServerCallState &new_state) override { state_ = new_state; }

  void HandleRequest() override {
    if (!io_service_.stopped()) {
      io_service_.post([this] { HandleRequestImpl(); });
    } else {
      // Handle service for rpc call has stopped, we must handle the call here
      // to send reply and remove it from cq
      RAY_LOG(DEBUG) << "Handle service has been closed.";
      SendReply(Status::Invalid("HandleServiceClosed"));
    }
  }

  void HandleRequestImpl() {
    state_ = ServerCallState::PROCESSING;
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
  }

  const ServerCallFactory &GetFactory() const override { return factory_; }

  void OnReplySent() override {
    if (send_reply_success_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_success_callback_);
      io_service_.post([callback]() { callback(); });
    }
  }

  void OnReplyFailed() override {
    if (send_reply_failure_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_failure_callback_);
      io_service_.post([callback]() { callback(); });
    }
  }

 private:
  /// Tell gRPC to finish this request and send reply asynchronously.
  void SendReply(const Status &status) {
    state_ = ServerCallState::SENDING_REPLY;
    response_writer_.Finish(reply_, RayStatusToGrpcStatus(status), this);
  }

  /// State of this call.
  ServerCallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// The response writer.
  grpc::ServerAsyncResponseWriter<Reply> response_writer_;

  /// The asynchronous reader writer for stream call.
  std::shared_ptr<ServerAsyncReaderWriter<Request, Reply>> server_stream_;

  /// The event loop.
  boost::asio::io_service &io_service_;

  ServerCallType type_;

  /// The request message.
  Request request_;

  /// The reply message.
  Reply reply_;

  /// The callback when sending reply successes.
  std::function<void()> send_reply_success_callback_ = nullptr;

  /// The callback when sending reply fails.
  std::function<void()> send_reply_failure_callback_ = nullptr;

  template <class T1, class T2, class T3, class T4>
  friend class ServerCallFactoryImpl;
};

template <class ServiceHandler, class Request, class Reply>
using HandleStreamRequestFunction = void (ServiceHandler::*)(const Request &,
                              std::shared_ptr<ServerAsyncReaderWriter<Request, Reply>>);

/// Implementation of `ServerCall`. It represents `ServerCall` for a stream type rpc.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
class ServerStreamCallImpl : public ServerCall {
 public:
  /// Constructor.
  ///
  /// \param[in] factory The factory which created this call.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] io_service The event loop.
  ServerStreamCallImpl(
      const ServerCallFactory &factory, ServiceHandler &service_handler,
      HandleStreamRequestFunction<ServiceHandler, Request, Reply> handle_stream_request_function,
      boost::asio::io_service &io_service)
      : ServerCall(ServerCallType::STREAM_ASYNC_CALL),
        state_(ServerCallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_stream_request_function_(handle_stream_request_function),
        server_stream_(std::make_shared<Request, Reply>(&context_)),
        io_service_(io_service) {
    // This is important as the server should know when the client is done.
    context_.AsyncNotifyWhenDone(reinterpret_cast<void*>(this));
    AsyncReadNextRequest();
  }

  void AsyncReadNextRequest() {
    server_stream_->Read(&request_, reinterpret_cast<void*>(this));
  }

  ServerCallState GetState() const override { return state_; }

  void SetState(const ServerCallState &new_state) override { state_ = new_state; }

  void HandleRequest() override {
    if (!io_service_.stopped()) {
      io_service_.post([this] {
        HandleRequestImpl();
        AsyncReadNextRequest();
        // Wait for next stream request.
        state_ = ServerCallState::PENDING;
      });
    } else {
      // Handle service for rpc call has stopped, we must handle the call here
      // to send reply and remove it from cq
      RAY_LOG(DEBUG) << "Handle service has been closed.";
    }
  }

  void HandleRequestImpl() {
    // Actual handler for the request.
    (service_handler_.*handle_stream_request_function_)(
        request_, server_stream_);
  }

  const ServerCallFactory &GetFactory() const override { return factory_; }

 private:
  /// State of this call.
  ServerCallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleStreamRequestFunction<ServiceHandler, Request, Reply> handle_stream_request_function_;

  /// The asynchronous reader writer for stream call.
  std::shared_ptr<ServerAsyncReaderWriter<Request, Reply>> server_stream_;

  /// The event loop.
  boost::asio::io_service &io_service_;

  ServerCallType type_;

  /// The request message.
  Request request_;

  /// The reply message.
  Reply reply_;

  template <class T1, class T2, class T3, class T4>
  friend class ServerCallFactoryImpl;
};

}  // namespace rpc
}  // namespace ray

#endif
