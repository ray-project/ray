#ifndef RAY_RPC_GRPC_SERVER_H
#define RAY_RPC_GRPC_SERVER_H

#include <thread>

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace rpc {

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
  /// Constructor.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  GrpcServer(const std::string &name, const uint32_t port) : name_(name), port_(port) {}

  /// Destruct this gRPC server.
  ~GrpcServer() {
    server_->Shutdown();
    cq_->Shutdown();
  }

  /// Initialize and run this server.
  void Run();

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
  void PollEventsFromCompletionQueue();

  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// The `grpc::Service` objects which should be registered to `ServerBuilder`.
  std::vector<std::reference_wrapper<grpc::Service>> services_;
  /// The `ServerCallFactory` objects, and the maximum number of concurrent requests that
  /// gRPC server can accept.
  std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
      server_call_factories_and_concurrencies_;
  /// The `ServerCompletionQueue` object used for polling events.
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;
  /// The `Server` object.
  std::unique_ptr<grpc::Server> server_;
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
  GrpcService(boost::asio::io_service &main_service) : main_service_(main_service) {}

  /// Destruct this gRPC service.
  ~GrpcService() {}

 protected:
  /// Return the underlying grpc::Service object for this class.
  /// This is passed to `GrpcServer` to be registered to grpc `ServerBuilder`.
  virtual grpc::Service &GetGrpcService() = 0;

  /// Subclasses should implement this method to initialize the `ServerCallFactory`
  /// instances, as well as specify maximum number of concurrent requests that gRPC
  /// server can "accept" (not "handle"). Each factory will be used to create
  /// `accept_concurrency` `ServerCall` objects, each of which will be used to accept and
  /// handle an incoming request.
  ///
  /// \param[in] cq The grpc completion queue.
  /// \param[out] server_call_factories_and_concurrencies The `ServerCallFactory` objects,
  /// and the maximum number of concurrent requests that gRPC server can accept.
  virtual void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) = 0;

  /// The main event loop, to which the service handler functions will be posted.
  boost::asio::io_service &main_service_;

  friend class GrpcServer;
};

}  // namespace rpc
}  // namespace ray

#endif
