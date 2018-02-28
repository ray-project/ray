#include "om_client_connection.h"

namespace ray {

TCPClientConnection::TCPClientConnection(boost::asio::io_service& io_service) : socket_(io_service) {}

TCPClientConnection::pointer TCPClientConnection::Create(boost::asio::io_service& io_service) {
  return TCPClientConnection::pointer(new TCPClientConnection(io_service));
}

boost::asio::ip::tcp::socket& TCPClientConnection::GetSocket() {
  return socket_;
}

}
