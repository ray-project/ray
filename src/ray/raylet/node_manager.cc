#include "node_manager.h"

#include <iostream>

#include <boost/bind.hpp>

using namespace std;
namespace ray {

NodeServer::NodeServer(boost::asio::io_service& io_service,
                       const std::string &socket_name,
                       const ResourceSet &resource_config)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service),
      local_scheduler_(socket_name, resource_config) {
  // Start listening for clients.
  doAccept();
}

void NodeServer::doAccept() {
  acceptor_.async_accept(socket_,
      boost::bind(&NodeServer::handleAccept, this, boost::asio::placeholders::error)
      );
}

void NodeServer::handleAccept(const boost::system::error_code& error) {
  if (!error) {
    // Accept a new client.
    auto new_connection = ClientConnection::Create(local_scheduler_, std::move(socket_));
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  doAccept();
}

} // end namespace ray
