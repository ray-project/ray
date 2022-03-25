// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <google/protobuf/arena.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_defs.h"

namespace ray {
namespace rpc {

/// Represents the callback function to be called when a `ServiceHandler` finishes
/// handling a request.
/// \param status The status would be returned to client.
/// \param success Success callback which will be invoked when the reply is successfully
/// sent to the client.
/// \param failure Failure callback which will be invoked when the reply fails to be
/// sent to the client.
using SendReplyCallback = std::function<void(
    Status status, std::function<void()> success, std::function<void()> failure)>;

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

  /// Invoked when sending reply successes.
  virtual void OnReplySent() = 0;

  // Invoked when sending reply fails.
  virtual void OnReplyFailed() = 0;

  virtual const ServerCallFactory &GetServerCallFactory() = 0;

  /// Virtual destruct function to make sure subclass would destruct properly.
  virtual ~ServerCall() = default;
};

/// The factory that creates a particular kind of `ServerCall` objects.
class ServerCallFactory {
 public:
  /// Create a new `ServerCall` and request gRPC runtime to start accepting the
  /// corresponding type of requests.
  virtual void CreateCall() const = 0;

  /// Get the maximum request number to handle at the same time. -1 means no limit.
  virtual int64_t GetMaxActiveRPCs() const = 0;

  virtual ~ServerCallFactory() = default;
};

/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using HandleRequestFunction = void (ServiceHandler::*)(const Request &,
                                                       Reply *,
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
      const ServerCallFactory &factory,
      ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      instrumented_io_context &io_service,
      std::string call_name)
      : state_(ServerCallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        response_writer_(&context_),
        io_service_(io_service),
        call_name_(std::move(call_name)),
        start_time_(0) {
    reply_ = google::protobuf::Arena::CreateMessage<Reply>(&arena_);
    // TODO call_name_ sometimes get corrunpted due to memory issues.
    RAY_CHECK(!call_name_.empty()) << "Call name is empty";
    ray::stats::STATS_grpc_server_req_new.Record(1.0, call_name_);
  }

  ~ServerCallImpl() override = default;

  ServerCallState GetState() const override { return state_; }

  void SetState(const ServerCallState &new_state) override { state_ = new_state; }

  void HandleRequest() override {
    start_time_ = absl::GetCurrentTimeNanos();
    ray::stats::STATS_grpc_server_req_handling.Record(1.0, call_name_);
    if (!io_service_.stopped()) {
      io_service_.post([this] { HandleRequestImpl(); }, call_name_);
    } else {
      // Handle service for rpc call has stopped, we must handle the call here
      // to send reply and remove it from cq
      RAY_LOG(DEBUG) << "Handle service has been closed.";
      SendReply(Status::Invalid("HandleServiceClosed"));
    }
  }

  void HandleRequestImpl() {
    state_ = ServerCallState::PROCESSING;
    // NOTE(hchen): This `factory` local variable is needed. Because `SendReply` runs in
    // a different thread, and will cause `this` to be deleted.
    const auto &factory = factory_;
    if (factory.GetMaxActiveRPCs() == -1) {
      // Create a new `ServerCall` to accept the next incoming request.
      // We create this before handling the request only when no back pressure limit is
      // set. So that the it can be populated by the completion queue in the background if
      // a new request comes in.
      factory.CreateCall();
    }
    (service_handler_.*handle_request_function_)(
        request_,
        reply_,
        [this](
            Status status, std::function<void()> success, std::function<void()> failure) {
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

  void OnReplySent() override {
    ray::stats::STATS_grpc_server_req_finished.Record(1.0, call_name_);
    if (send_reply_success_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_success_callback_);
      io_service_.post([callback]() { callback(); }, call_name_ + ".success_callback");
    }
    LogProcessTime();
  }

  void OnReplyFailed() override {
    ray::stats::STATS_grpc_server_req_finished.Record(1.0, call_name_);
    if (send_reply_failure_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_failure_callback_);
      io_service_.post([callback]() { callback(); }, call_name_ + ".failure_callback");
    }
    LogProcessTime();
  }

  const ServerCallFactory &GetServerCallFactory() override { return factory_; }

 private:
  /// Log the duration this query used
  void LogProcessTime() {
    auto end_time = absl::GetCurrentTimeNanos();
    ray::stats::STATS_grpc_server_req_process_time_ms.Record(
        (end_time - start_time_) / 1000000.0, call_name_);
  }
  /// Tell gRPC to finish this request and send reply asynchronously.
  void SendReply(const Status &status) {
    state_ = ServerCallState::SENDING_REPLY;
    response_writer_.Finish(*reply_, RayStatusToGrpcStatus(status), this);
  }

  /// The memory pool for this request. It's used for reply.
  /// With arena, we'll be able to setup the reply without copying some field.
  google::protobuf::Arena arena_;

  /// State of this call.
  ServerCallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// Context for the request, allowing to tweak aspects of it such as the use
  /// of compression, authentication, as well as to send metadata back to the client.
  grpc::ServerContext context_;

  /// The response writer.
  grpc::ServerAsyncResponseWriter<Reply> response_writer_;

  /// The event loop.
  instrumented_io_context &io_service_;

  /// The request message.
  Request request_;

  /// The reply message. This one is owned by arena. It's not valid beyond
  /// the life-cycle of this call.
  Reply *reply_;

  /// Human-readable name for this RPC call.
  std::string call_name_;

  /// The callback when sending reply successes.
  std::function<void()> send_reply_success_callback_ = nullptr;

  /// The callback when sending reply fails.
  std::function<void()> send_reply_failure_callback_ = nullptr;

  /// The ts when the request created
  int64_t start_time_;

  template <class T1, class T2, class T3, class T4>
  friend class ServerCallFactoryImpl;
};

/// Represents the generic signature of a `FooService::AsyncService::RequestBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using RequestCallFunction =
    void (GrpcService::AsyncService::*)(grpc::ServerContext *,
                                        Request *,
                                        grpc::ServerAsyncResponseWriter<Reply> *,
                                        grpc::CompletionQueue *,
                                        grpc::ServerCompletionQueue *,
                                        void *);

/// Implementation of `ServerCallFactory`
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class ServiceHandler, class Request, class Reply>
class ServerCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GrpcService::AsyncService;

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generated `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  function.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  /// \param[in] io_service The event loop.
  /// \param[in] max_active_rpcs Maximum request number to handle at the same time. -1
  /// means no limit.
  ServerCallFactoryImpl(
      AsyncService &service,
      RequestCallFunction<GrpcService, Request, Reply> request_call_function,
      ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      instrumented_io_context &io_service,
      std::string call_name,
      int64_t max_active_rpcs)
      : service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq),
        io_service_(io_service),
        call_name_(std::move(call_name)),
        max_active_rpcs_(max_active_rpcs) {}

  void CreateCall() const override {
    // Create a new `ServerCall`. This object will eventually be deleted by
    // `GrpcServer::PollEventsFromCompletionQueue`.
    auto call = new ServerCallImpl<ServiceHandler, Request, Reply>(
        *this, service_handler_, handle_request_function_, io_service_, call_name_);
    /// Request gRPC runtime to starting accepting this kind of request, using the call as
    /// the tag.
    (service_.*request_call_function_)(&call->context_,
                                       &call->request_,
                                       &call->response_writer_,
                                       cq_.get(),
                                       cq_.get(),
                                       call);
  }

  int64_t GetMaxActiveRPCs() const override { return max_active_rpcs_; }

 private:
  /// The gRPC-generated `AsyncService`.
  AsyncService &service_;

  /// Pointer to the `AsyncService::RequestMethod` function.
  RequestCallFunction<GrpcService, Request, Reply> request_call_function_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// The `CompletionQueue`.
  const std::unique_ptr<grpc::ServerCompletionQueue> &cq_;

  /// The event loop.
  instrumented_io_context &io_service_;

  /// Human-readable name for this RPC call.
  std::string call_name_;

  /// Maximum request number to handle at the same time.
  /// -1 means no limit.
  uint64_t max_active_rpcs_;
};

}  // namespace rpc
}  // namespace ray
