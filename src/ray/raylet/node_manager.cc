
#include <iostream>

#include <boost/bind.hpp>

#include "node_manager.h"

using namespace std;
using namespace boost::asio;

namespace ray {

NodeServer::NodeServer(boost::asio::io_service& io_service,
                       const std::string &socket_name,
                       const ResourceSet &resource_config)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service),
      tcp_acceptor_(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0)),
      tcp_socket_(io_service),
      object_manager_(),
      local_scheduler_(socket_name, resource_config, object_manager_) {
  RegisterGcs();
  // Start listening for clients.
  DoAccept();
  DoAcceptTcp();
}

void NodeServer::RegisterGcs(){
//  ip::tcp::endpoint endpoint = tcp_acceptor_.local_endpoint();
//  std::string ip = endpoint.address().to_string();
//  ushort port = endpoint.port();
}

void NodeServer::DoAcceptTcp() {
  TCPClientConnection::pointer new_connection = TCPClientConnection::Create(acceptor_.get_io_service());
  tcp_acceptor_.async_accept(
      new_connection->GetSocket(),
      boost::bind(&NodeServer::HandleAcceptTcp, this, new_connection, boost::asio::placeholders::error)
  );
}

void NodeServer::HandleAcceptTcp(TCPClientConnection::pointer new_connection,
                                 const boost::system::error_code& error) {
  if (!error) {
    // Pass it off to object manager for now.
  }
  DoAcceptTcp();
}

void NodeServer::DoAccept() {
  acceptor_.async_accept(socket_,
      boost::bind(&NodeServer::HandleAccept, this, boost::asio::placeholders::error)
      );
}

void NodeServer::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // Accept a new client.
    auto new_connection = ClientConnection::Create(local_scheduler_, std::move(socket_));
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  DoAccept();
}

} // end namespace ray
