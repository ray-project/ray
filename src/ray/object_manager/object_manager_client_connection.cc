#include "object_manager_client_connection.h"

namespace ray {

uint64_t SenderConnection::id_counter_ = 0;

SenderConnection::pointer SenderConnection::Create(boost::asio::io_service &io_service,
                                                   const ClientID &client_id,
                                                   const std::string &ip, uint16_t port) {
  boost::asio::ip::tcp::socket socket(io_service);
  RAY_CHECK_OK(TcpConnect(socket, ip, port));
  return pointer(new SenderConnection(std::move(socket), client_id));
};

SenderConnection::SenderConnection(
    boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket,
    const ClientID &client_id)
    : ServerConnection<boost::asio::ip::tcp>(std::move(socket)) {
  client_id_ = client_id;
  connection_id_ = SenderConnection::id_counter_++;
};

boost::asio::ip::tcp::socket &SenderConnection::GetSocket() { return socket_; };

}  // namespace ray
