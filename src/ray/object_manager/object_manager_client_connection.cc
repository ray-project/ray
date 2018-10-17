#include "ray/object_manager/object_manager_client_connection.h"

namespace ray {

uint64_t SenderConnection::id_counter_;

std::shared_ptr<SenderConnection> SenderConnection::Create(
    boost::asio::io_service &io_service, const ClientID &client_id, const std::string &ip,
    uint16_t port) {
  boost::asio::ip::tcp::socket socket(io_service);
  Status status = TcpConnect(socket, ip, port);
  if (status.ok()) {
    std::shared_ptr<TcpServerConnection> conn =
        std::make_shared<TcpServerConnection>(std::move(socket));
    return std::make_shared<SenderConnection>(std::move(conn), client_id);
  } else {
    return nullptr;
  }
};

SenderConnection::SenderConnection(std::shared_ptr<TcpServerConnection> conn,
                                   const ClientID &client_id)
    : conn_(conn) {
  client_id_ = client_id;
  connection_id_ = SenderConnection::id_counter_++;
};

}  // namespace ray
