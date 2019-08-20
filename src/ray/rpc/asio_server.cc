
#include <thread>
#include <utility>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include "src/ray/common/client_connection.h"
#include "src/ray/rpc/common.h"
#include "src/ray/rpc/asio_server.h"
#include "src/ray/protobuf/asio.pb.h"

namespace ray {
namespace rpc {

using boost::asio::local::stream_protocol;
using boost::asio::ip::tcp;

static const std::vector<std::string> asio_common_message_enum =
    GenerateEnumNames(RpcServiceType);

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

void AsioRpcServer::ProcessDisconnectClientMessage(
    const std::shared_ptr<TcpClientConnection> &client) {
  // TODO: fill this in!
}


void AsioRpcServer::RegisterService(AsioRpcService &service) {
  std::vector<std::shared_ptr<ServiceMethod>> server_call_methods;
  service.InitMethodHandlers(&server_call_methods);

  auto service_handler = [server_call_methods] (
      const std::shared_ptr<TcpClientConnection> &client, int64_t message_type, const uint8_t *message_data) {

    for (const auto &method : server_call_methods) {
      if (method->GetRequestType() == message_type) {
        method->HandleRequest(client, length, message_data);
        return;
      }
    }

    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  };

  service_handlers_.emplace(service.GetServiceType(), service_handler);
}



}  // namespace rpc
}  // namespace ray
