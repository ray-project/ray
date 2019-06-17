#ifndef RAY_RPC_GRPC_SERVER_H
#define RAY_RPC_GRPC_SERVER_H

#include <thread>

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace rpc {

/// Base class that represents an abstract gRPC server.
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
  /// Constructor.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  /// \param[in] main_service The main event loop, to which service handler functions
  /// will be posted.
  GrpcServer(const std::string &name, const uint32_t port,
             boost::asio::io_service &main_service)
      : name_(name), port_(port), main_service_(main_service) {}

  /// Destruct this gRPC server.
  ~GrpcServer() {
    // shutdown the server forcelly after 200ms, even some requests haven't finished
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(200);
    server_->Shutdown(deadline);
    cq_->Shutdown();
    RAY_LOG(INFO) << "gRPC server of " << name_ << " shutdown.";
  }

  /// Initialize and run this server.
  void Run();

  /// Get the port of this gRPC server.
  int GetPort() const { return port_; }

 protected:
  /// Subclasses should implement this method and register one or multiple gRPC services
  /// to the given `ServerBuilder`.
  ///
  /// \param[in] builder The `ServerBuilder` instance to register services to.
  virtual void RegisterServices(::grpc::ServerBuilder &builder) = 0;

  /// Subclasses should implement this method and initialize the `ServerCallFactory`
  /// instances. The returned factories will be used to create `ServerCall` objects, each
  /// of which is used to handle an incoming request.
  ///
  /// \param[out] server_call_factories The returned `ServerCallFactory` objects.
  virtual void InitServerCallFactories(
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) = 0;

  /// This function runs in a background thread. It keeps polling events from the
  /// `ServerCompletionQueue`, and dispaches the event to the `ServiceHandler` instances
  /// via the `ServerCall` objects.
  void PollEventsFromCompletionQueue();

  /// The main event loop, to which the service handler functions will be posted.
  boost::asio::io_service &main_service_;
  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// The `ServerCallFactory` objects.
  std::vector<std::unique_ptr<ServerCallFactory>> server_call_factories_;
  /// The `ServerCompletionQueue` object used for polling events.
  std::unique_ptr<::grpc::ServerCompletionQueue> cq_;
  /// The `Server` object.
  std::unique_ptr<::grpc::Server> server_;
  std::thread polling_thread_;
};

}  // namespace rpc
}  // namespace ray

#endif
