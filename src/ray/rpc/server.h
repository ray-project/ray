#ifndef RAY_RPC_SERVER_H
#define RAY_RPC_SERVER_H

#include "ray/protobuf/common.pb.h"

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

/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                       SendReplyCallback);

/// Class that represents a rpc server.
///
/// An `RpcServer` listens on a specific port.
///
/// Subclasses can register one or multiple services to a `RpcServer`, see
/// `RegisterServices`. And they should also implement `InitServerCallFactories` to decide
/// which kinds of requests this server should accept.
class RpcServer {
 public:
  /// Construct a rpc server that listens on a TCP port.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  RpcServer(std::string name, const uint32_t port)
      : name_(std::move(name)), port_(port), is_closed_(false) {}

  /// Construct a gRPC server that listens on unix domain socket.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] unix_socket_path Unix domain socket full path.
  RpcServer(std::string name, const std::string &unix_socket_path)
      : RpcServer(std::move(name), 0) {
    unix_socket_path_ = unix_socket_path;
  }

  /// Destruct this gRPC server.
  virtual ~RpcServer() {}

  /// Initialize and run this server.
  virtual void Run() = 0;

  /// Get the port of this RPC server.
  int GetPort() const { return port_; }

 protected:
  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// Indicates whether this server has been closed.
  bool is_closed_;
  /// Unix domain socket path.
  std::string unix_socket_path_;
};

/// Base class that represents an abstract RPC service.
///
class RpcService {
 protected:
  /// Destruct this RPC service.
  virtual ~RpcService() {}
};

}  // namespace rpc
}  // namespace ray

#endif