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
/// \param MAX_ACTIVE_RPCS Maximum number of RPCs to handle at the same time. -1 means no
/// limit.
#define RPC_SERVICE_HANDLER(SERVICE, HANDLER, MAX_ACTIVE_RPCS) \
  std::unique_ptr<ServerCallFactory> HANDLER##_call_factory(   \
      new ServerCallFactoryImpl<SERVICE,                       \
                                SERVICE##Handler,              \
                                HANDLER##Request,              \
                                HANDLER##Reply>(               \
          service_,                                            \
          &SERVICE::AsyncService::Request##HANDLER,            \
          service_handler_,                                    \
          &SERVICE##Handler::Handle##HANDLER,                  \
          cq,                                                  \
          main_service_,                                       \
          #SERVICE ".grpc_server." #HANDLER,                   \
          MAX_ACTIVE_RPCS));                                   \
  server_call_factories->emplace_back(std::move(HANDLER##_call_factory));

// Define a void RPC client method.
#define DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(METHOD)                   \
  virtual void Handle##METHOD(const ::ray::rpc::METHOD##Request &request, \
                              ::ray::rpc::METHOD##Reply *reply,           \
                              ::ray::rpc::SendReplyCallback send_reply_callback) = 0;

class GrpcService;

/// Class that represents an gRPC server.
///
/// A `GrpcServer` listens on a specific port. It owns
/// 1) a `ServerCompletionQueue` that is used for polling events from gRPC,
/// 2) and a thread that polls events from the `ServerCompletionQueue`.
///
/// Subclasses can register one or multiple services to a `GrpcServer`, see
/// `RegisterServices`. And they should also implement `InitServerCallFactories` to decide
/// which kinds of requests this server should accept.
class GrpcServer {
 public:
  /// Construct a gRPC server that listens on a TCP port.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  GrpcServer(std::string name,
             const uint32_t port,
             bool listen_to_localhost_only,
             int num_threads = 1,
             int64_t keepalive_time_ms = 7200000 /*2 hours, grpc default*/);

  /// Destruct this gRPC server.
  ~GrpcServer() { Shutdown(); }

  /// Initialize and run this server.
  void Run();

  // Shutdown this server
  void Shutdown() {
    if (!is_closed_) {
      // Shutdown the server with an immediate deadline.
      // TODO(edoakes): do we want to do this in all cases?
      server_->Shutdown(gpr_now(GPR_CLOCK_REALTIME));
      for (const auto &cq : cqs_) {
        cq->Shutdown();
      }
      for (auto &polling_thread : polling_threads_) {
        polling_thread.join();
      }
      is_closed_ = true;
      RAY_LOG(DEBUG) << "gRPC server of " << name_ << " shutdown.";
      server_.reset();
    }
  }

  /// Get the port of this gRPC server.
  int GetPort() const { return port_; }

  /// Register a grpc service. Multiple services can be registered to the same server.
  /// Note that the `service` registered must remain valid for the lifetime of the
  /// `GrpcServer`, as it holds the underlying `grpc::Service`.
  ///
  /// \param[in] service A `GrpcService` to register to this server.
  void RegisterService(GrpcService &service);

 protected:
  /// This function runs in a background thread. It keeps polling events from the
  /// `ServerCompletionQueue`, and dispaches the event to the `ServiceHandler` instances
  /// via the `ServerCall` objects.
  void PollEventsFromCompletionQueue(int index);

  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// Listen to localhost (127.0.0.1) only if it's true, otherwise listen to all network
  /// interfaces (0.0.0.0)
  const bool listen_to_localhost_only_;
  /// Indicates whether this server has been closed.
  bool is_closed_;
  /// The `grpc::Service` objects which should be registered to `ServerBuilder`.
  std::vector<std::reference_wrapper<grpc::Service>> services_;
  /// The `ServerCallFactory` objects.
  std::vector<std::unique_ptr<ServerCallFactory>> server_call_factories_;
  /// The number of completion queues the server is polling from.
  int num_threads_;
  /// The `ServerCompletionQueue` object used for polling events.
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  /// The `Server` object.
  std::unique_ptr<grpc::Server> server_;
  /// The polling threads used to check the completion queues.
  std::vector<std::thread> polling_threads_;
  /// The interval to send a new gRPC keepalive timeout from server -> client.
  /// gRPC server cannot get the ping response within the time, it triggers
  /// the watchdog timer fired error, which will close the connection.
  const int64_t keepalive_time_ms_;
};

/// Base class that represents an abstract gRPC service.
///
/// Subclass should implement `InitServerCallFactories` to decide
/// which kinds of requests this service should accept.
class GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which service handler functions
  /// will be posted.
  explicit GrpcService(instrumented_io_context &main_service)
      : main_service_(main_service) {}

  /// Destruct this gRPC service.
  virtual ~GrpcService() = default;

 protected:
  /// Return the underlying grpc::Service object for this class.
  /// This is passed to `GrpcServer` to be registered to grpc `ServerBuilder`.
  virtual grpc::Service &GetGrpcService() = 0;

  /// Subclasses should implement this method to initialize the `ServerCallFactory`
  /// instances, as well as specify maximum number of concurrent requests that gRPC
  /// server can handle.
  ///
  /// \param[in] cq The grpc completion queue.
  /// \param[out] server_call_factories The `ServerCallFactory` objects,
  /// and the maximum number of concurrent requests that this gRPC server can handle.
  virtual void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) = 0;

  /// The main event loop, to which the service handler functions will be posted.
  instrumented_io_context &main_service_;

  friend class GrpcServer;
};

}  // namespace rpc
}  // namespace ray
