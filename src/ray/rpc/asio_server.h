#ifndef RAY_RPC_ASIO_SERVER_H
#define RAY_RPC_ASIO_SERVER_H

#include <thread>
#include <utility>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include "src/ray/common/client_connection.h"
#include "src/ray/rpc/common.h"
#include "src/ray/rpc/server.h"
#include "src/ray/protobuf/asio.pb.h"

namespace ray {
namespace rpc {

class ServerCallMethod;

/// Class that represents an asio based rpc server.
///
/// An `AsioRpcServer` listens on a specific port. 
///
/// Subclasses can register one or multiple services to a `AsioRpcServer`, see
/// `RegisterServices`. And they should also implement `InitServerCallFactories` to decide
/// which kinds of requests this server should accept.
class AsioRpcServer : public RpcServer {
 public:
  /// Construct a rpc server that listens on a TCP port.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  AsioRpcServer(std::string name, const uint32_t port, boost::asio::io_service &io_service)
      : RpcServer(name, port), io_service_(io_service) {}

  /// Destruct this gRPC server.
  ~AsioRpcServer() { Shutdown(); }

  /// Initialize and run this server.
  void Run() override;

  // Shutdown this server
  void Shutdown() {
    if (!is_closed_) {
      is_closed_ = true;
      RAY_LOG(DEBUG) << "Asio RPC server of " << name_ << " shutdown.";
    }
  }

  /// Register a grpc service. Multiple services can be registered to the same server.
  /// Note that the `service` registered must remain valid for the lifetime of the
  /// `AsioRpcServer`, as it holds the underlying `grpc::Service`.
  ///
  /// \param[in] service A `GrpcService` to register to this server.
  void RegisterService(RpcService &service);

 protected:
  
    void DoAcceptTcp();
    void HandleAcceptTcp(const boost::system::error_code &error);

    void ProcessClientMessage(
        const std::shared_ptr<TcpClientConnection> &client, int64_t message_type,
        const uint8_t *message_data);
    void ProcessConnectClientMessage(
        const std::shared_ptr<TcpClientConnection> &client, const uint8_t *message_data);
    void ProcessDisconnectClientMessage(
        const std::shared_ptr<TcpClientConnection> &client);

  /// IO service to handle the service calls.
  boost::asio::io_service &io_service_;
  /// An acceptor for new tcp clients.
  std::unique_ptr<boost::asio::ip::tcp::acceptor> tcp_acceptor_;
  /// The socket to listen on for new tcp clients.
  std::unique_ptr<boost::asio::ip::tcp::socket> tcp_socket_;
};

/// Base class that represents an abstract gRPC service.
///
/// Subclass should implement `InitServerCallFactories` to decide
/// which kinds of requests this service should accept.
class AsioRpcService : public RpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which service handler functions
  /// will be posted.
  explicit AsioRpcService(rpc::RpcServiceType service_type)
    : RpcService(rpc::RpcType::Asio), service_type_(service_type) {}

  /// Destruct this gRPC service.
  ~AsioRpcService() = default;

 protected:
  rpc::RpcServiceType GetServiceType() const { return service_type_; }

  /// Subclasses should implement this method to initialize the `ServerCallFactory`
  /// instances, as well as specify maximum number of concurrent requests that gRPC
  /// server can handle.
  ///
  /// \param[in] cq The grpc completion queue.
  /// \param[out] server_call_factories_and_concurrencies The `ServerCallFactory` objects,
  /// and the maximum number of concurrent requests that this gRPC server can handle.
  virtual void InitMethodHandlers(
      std::vector<std::unique_ptr<ServerCallMethod>> *server_call_methods) = 0;

  rpc::RpcServiceType service_type_;
};

class ServerCallMethod {
 public:
  virtual int GetRequestType() const = 0;

  virtual void HandleRequest(const std::shared_ptr<TcpClientConnection> &client,
                     int64_t length, const uint8_t *message_data) = 0;
};

// Implementation of `ServerCallFactory`
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply, class MessageType>
class ServerCallMethodImpl : public ServerCallMethod {

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
  ServerCallMethodImpl(MessageType request_type, MessageType reply_type,
      ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function)
      : request_type_(request_type),
        reply_type_(reply_type),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function) {}
 
  int GetRequestType() const override { return static_cast<int>(request_type_); }

  void HandleRequest(const std::shared_ptr<TcpClientConnection> &client,
                     int64_t length, const uint8_t *message_data) override {
    RpcRequestMessage request_message;
    request_message.ParseFromArray(message_data, length);

    const auto request_id = request_message.request_id();

    Request request;
    request.ParseFromString(request_message.request());

    Reply reply;

    (service_handler_.*handle_request_function_)(
        request, &reply,
        [this, &request_id, &reply, &client](Status status, std::function<void()> success,
               std::function<void()> failure) {
          
            RpcReplyMessage reply_message;
            reply_message.set_request_id(request_id);
            reply_message.set_error_code(static_cast<uint32_t>(status.code()));
            reply_message.set_error_message(status.message());            
            reply.SerializeToString(reply_message.mutable_reply());

            std::string serialized_message;
            reply_message.SerializeToString(&serialized_message);

            client->WriteMessageAsync(reply_type_,
                static_cast<int64_t>(serialized_message.size()),
                reinterpret_cast<const uint8_t *>(serialized_message.data()),
                [success, failure](const ray::Status &status) {
                    status.ok() ? success() : failure();
                });
        });
  }

 private:
  /// Enum type for request message.
  MessageType request_type_;
  /// Enum type for reply message.
  MessageType reply_type_;
  /// The service handler that handles the request.
  ServiceHandler &service_handler_;
  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;
};


using boost::asio::local::stream_protocol;
using boost::asio::ip::tcp;
/*
const std::vector<std::string> GenerateEnumNames(int start_index, int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  for (int i = start_index; i <= end_index; i++) {
    enum_names.push_back(RpcServiceType_Name(static_cast<RpcServiceType>(i)));
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}
*/
static const std::vector<std::string> asio_common_message_enum =
    GenerateProtobufEnumNames<RpcServiceType>();

void AsioRpcServer::Run() {
  std::string server_address = "0.0.0.0:" + std::to_string(port_);

  tcp_socket_ = std::unique_ptr<tcp::socket>(
      new tcp::socket(io_service_));
  tcp_acceptor_ = std::unique_ptr<tcp::acceptor>(
      new tcp::acceptor(io_service_, tcp::endpoint(tcp::v4(), port_)));  

  DoAcceptTcp();

  is_closed_ = false;
}

void AsioRpcServer::DoAcceptTcp() {
  if (tcp_acceptor_ != nullptr) {
    (*tcp_acceptor_).async_accept(*tcp_socket_,
                                        boost::bind(&AsioRpcServer::HandleAcceptTcp, this,
                                                    boost::asio::placeholders::error));
  }
}

void AsioRpcServer::HandleAcceptTcp(const boost::system::error_code &error) {
  if (!error) {
    ClientHandler<tcp> client_handler =
        [](TcpClientConnection &client) {
          // Begin listening for messages.
          client.ProcessMessages();
        };
    MessageHandler<tcp> message_handler =
        [this](std::shared_ptr<TcpClientConnection> client, int64_t message_type,
               const uint8_t *message) {
          ProcessClientMessage(client, message_type, message);
        };
    // Accept a new TCP client and dispatch it to the node manager.
    auto new_connection = TcpClientConnection::Create(
        client_handler, message_handler, std::move(*tcp_socket_), name_,
        asio_common_message_enum,
        static_cast<int64_t>(ServiceMessageType::DisconnectClient));
  }
  // We're ready to accept another client.
  DoAcceptTcp();
}


void AsioRpcServer::ProcessClientMessage(
    const std::shared_ptr<TcpClientConnection> &client, int64_t message_type,
    const uint8_t *message_data) {

  auto message_type_value = static_cast<ServiceMessageType>(message_type);
  switch (message_type_value) {
  case ServiceMessageType::ConnectClient: {
    ProcessConnectClientMessage(client, message_data);
  } break;
  case ServiceMessageType::DisconnectClient: {
    ProcessDisconnectClientMessage(client);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
}

void AsioRpcServer::ProcessConnectClientMessage(
    const std::shared_ptr<TcpClientConnection> &client, const uint8_t *message_data) {
      /*
  // Find the handler for the type of service, and overwrite.
  ConnectClientMessage message;
  auto service_type = message.service_type;
  auto handler = ...
  client->SetHandler(handler);
  */
}

/*
void AsioRpcServer::RegisterService(GrpcService &service) {
  services_.emplace_back(service.GetGrpcService());
  service.InitServerCallFactories(cq_, &server_call_factories_and_concurrencies_);
}
*/


}  // namespace rpc
}  // namespace ray

#endif