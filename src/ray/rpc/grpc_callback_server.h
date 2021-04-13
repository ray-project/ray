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

#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <thread>
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace rpc {

// Create an async callback-based gRPC service. SERVICE should be the gRPC generated
// service (e.g. NodeManagerService), and HANLDERS should be one or more RPC methods,
// each returning a reactor.
//
// If using the callback-based API for unary RPCs, use the
// UNARY_CALLBACK_RPC_SERVICE_HANDLER to generate those unary RPC handlers. Otherwise,
// the service will have to manually implement this class, including its
// reactor-returning methods.
#define CALLBACK_SERVICE(SERVICE, HANDLERS)                              \
  class SERVICE##WithCallbacks final : public SERVICE::CallbackService { \
   public:                                                               \
    SERVICE##WithCallbacks(instrumented_io_context &io_context,          \
                           SERVICE##Handler &service_handler)            \
        : io_context_(io_context), service_handler_(service_handler) {}  \
    HANDLERS                                                             \
   private:                                                              \
    instrumented_io_context &io_context_;                                \
    SERVICE##Handler &service_handler_;                                  \
  };

#define UNARY_CALLBACK_RPC_SERVICE_HANDLER(SERVICE, HANDLER)                          \
  grpc::ServerUnaryReactor *HANDLER(grpc::CallbackServerContext *context,             \
                                    const HANDLER##Request *request,                  \
                                    HANDLER##Reply *reply) {                          \
    auto reactor = context->DefaultReactor();                                         \
    if (!io_context_.stopped()) {                                                     \
      io_context_.post(                                                               \
          [this, request, reply, reactor]() {                                         \
            service_handler_.Handle##HANDLER(                                         \
                *request, reply,                                                      \
                [this, reactor](Status status, std::function<void()> success,         \
                                std::function<void()> failure) {                      \
                  reactor->Finish(RayStatusToGrpcStatus(status));                     \
                  if (success != nullptr && !io_context_.stopped()) {                 \
                    io_context_.post([success = std::move(success)]() { success(); }, \
                                     #SERVICE ".grpc_server." #HANDLER                \
                                              ".success_callback");                   \
                  }                                                                   \
                });                                                                   \
          },                                                                          \
          #SERVICE ".grpc_server." #HANDLER);                                         \
    } else {                                                                          \
      RAY_LOG(DEBUG) << "Service " #SERVICE " closed";                                \
      reactor->Finish(                                                                \
          RayStatusToGrpcStatus(Status::Invalid("Service " #SERVICE " closed")));     \
    }                                                                                 \
    return reactor;                                                                   \
  }

// Declare an RPC service handler method, used as the callback for the corresponding
// unary RPC.
#define DECLARE_UNARY_CALLBACK_RPC_SERVICE_HANDLER_METHOD(METHOD)  \
  virtual void Handle##METHOD(const rpc::METHOD##Request &request, \
                              rpc::METHOD##Reply *reply,           \
                              rpc::SendReplyCallback send_reply_callback) = 0;

/// Class that represents an gRPC server.
///
/// A `GrpcCallbackServer` listens on a specific port.
/// Subclasses can register one or multiple services to a `GrpcCallbackServer`, see
/// `RegisterServices`.
class GrpcCallbackServer {
 public:
  /// Construct a gRPC server that listens on a TCP port.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  /// \param[in] shutdown_deadline_ms The deadline for gracefully shutting down the gRPC
  ///  server, in milliseconds. After this deadline expires, forced RPC cancellation will
  ///  take place. By default, this is 0.
  GrpcCallbackServer(std::string name, const uint32_t port,
                     int64_t shutdown_deadline_ms = 0);

  /// Destruct this gRPC server.
  ~GrpcCallbackServer() { Shutdown(); }

  /// Initialize and run this server.
  void Run();

  // Shutdown this server
  void Shutdown() {
    if (!is_closed_) {
      // Shutdown the server, with forced cancellation of RPCs taking place after the
      // provided deadline.
      auto deadline =
          gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                       gpr_time_from_millis(shutdown_deadline_ms_, GPR_TIMESPAN));
      server_->Shutdown(deadline);
      is_closed_ = true;
      RAY_LOG(DEBUG) << "gRPC server of " << name_ << " shutdown.";
    }
  }

  /// Get the port of this gRPC server.
  int GetPort() const { return port_; }

  /// Register a grpc service. Multiple services can be registered to the same server.
  ///
  /// \param[in] service A `grpc::Service` to register to this server.
  void RegisterService(grpc::Service &service);

 protected:
  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// Indicates whether this server has been closed.
  bool is_closed_;
  /// The `grpc::Service` objects which should be registered to `ServerBuilder`.
  std::vector<std::reference_wrapper<grpc::Service>> services_;
  /// The `Server` object.
  std::unique_ptr<grpc::Server> server_;
  /// The deadline for gracefully shutting down the gRPC server.
  int64_t shutdown_deadline_ms_;
};

}  // namespace rpc
}  // namespace ray
