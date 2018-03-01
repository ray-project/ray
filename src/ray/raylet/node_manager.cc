
#include <iostream>

#include <boost/bind.hpp>

#include "node_manager.h"

using namespace std;
using namespace boost::asio;

namespace ray {

NodeServer::NodeServer(boost::asio::io_service& io_service,
                       const std::string &socket_name,
                       const ResourceSet &resource_config,
                       const OMConfig &om_config,
                       shared_ptr<ray::GcsClient> gcs_client)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service),
      tcp_acceptor_(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0)),
      tcp_socket_(io_service),
      object_manager_(io_service, om_config, gcs_client),
      local_scheduler_(socket_name, resource_config, object_manager_),
      gcs_client_(gcs_client) {
  ClientID client_id = RegisterGcs();
  object_manager_.SetClientID(client_id);
  // Start listening for clients.
  DoAccept();
  DoAcceptTcp();
}

ClientID NodeServer::RegisterGcs(){
  ip::tcp::endpoint endpoint = tcp_acceptor_.local_endpoint();
  std::string ip = endpoint.address().to_string();
  ushort port = endpoint.port();
  ClientID client_id = gcs_client_->Register(ip, port);
  return client_id;
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
    ray::Status status = object_manager_.AddSock(std::move(new_connection));
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
    auto new_connection = LocalClientConnection::Create(local_scheduler_, std::move(socket_));
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  DoAccept();
}

ObjectManager &NodeServer::GetObjectManager() {
  return object_manager_;
}

ray::Status NodeServer::Terminate(){
  return object_manager_.Terminate();
}

} // end namespace ray
