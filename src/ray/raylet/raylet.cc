#include "raylet.h"

#include <boost/bind.hpp>
#include <iostream>

#include "ray/status.h"

namespace ray {

namespace raylet {

Raylet::Raylet(boost::asio::io_service &io_service, const std::string &socket_name,
               const ResourceSet &resource_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service),
      tcp_acceptor_(io_service,
                    boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
      tcp_socket_(io_service),
      node_manager_acceptor_(
          io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
      node_manager_socket_(io_service),
      gcs_client_(gcs_client),
      // TODO(swang): This will break...we need to initialize the client and
      // task tables before the lineage cache.
      lineage_cache_(gcs_client->client_table().GetLocalClientId(),
                     gcs_client->task_table(), gcs_client->object_table()),
      object_manager_(io_service, object_manager_config, gcs_client),
      node_manager_(io_service, socket_name, resource_config, object_manager_,
                    lineage_cache_, gcs_client_) {
  ClientID client_id = RegisterGcs(io_service);
  object_manager_.SetClientID(client_id);
  // Start listening for clients.
  DoAccept();
  DoAcceptTcp();
  DoAcceptNodeManager();
}

Raylet::~Raylet() {
  (void)gcs_client_->client_table().Disconnect();
  RAY_CHECK_OK(object_manager_.Terminate());
}

ClientID Raylet::RegisterGcs(boost::asio::io_service &io_service) {
  boost::asio::ip::tcp::endpoint endpoint = tcp_acceptor_.local_endpoint();
  std::string ip = endpoint.address().to_string();
  unsigned short object_manager_port = endpoint.port();

  endpoint = node_manager_acceptor_.local_endpoint();
  unsigned short node_manager_port = endpoint.port();

  ClientID client_id = ClientID::from_random();

  ClientTableDataT client_info;
  client_info.client_id = client_id.binary();
  client_info.node_manager_address = ip;
  // TODO(hme): Update port when we add new acceptor for local scheduler connections.
  client_info.local_scheduler_port = node_manager_port;
  client_info.object_manager_port = object_manager_port;
  // TODO(hme): Clean up constants.
  RAY_CHECK_OK(gcs_client_->Connect("127.0.0.1", 6379, client_info));
  RAY_CHECK_OK(gcs_client_->Attach(io_service));
  RAY_CHECK_OK(gcs_client_->client_table().Connect());


  auto node_manager_client_added = [this](gcs::AsyncGcsClient *client,
                      const UniqueID &id,
                      std::shared_ptr<ClientTableDataT> data) {
    node_manager_.ClientAdded(client, id, data);
  };
  gcs_client_->client_table().RegisterClientAddedCallback(node_manager_client_added);
  RAY_LOG(INFO) << "Registering as " << client_id.hex();

  return client_id;
}

void Raylet::DoAcceptTcp() {
  TCPClientConnection::pointer new_connection =
      TCPClientConnection::Create(acceptor_.get_io_service());
  tcp_acceptor_.async_accept(new_connection->GetSocket(),
                             boost::bind(&Raylet::HandleAcceptTcp, this, new_connection,
                                         boost::asio::placeholders::error));
}

void Raylet::DoAcceptNodeManager() {
  node_manager_acceptor_.async_accept(node_manager_socket_,
                                      boost::bind(&Raylet::HandleAcceptNodeManager, this,
                                                  boost::asio::placeholders::error));
}

void Raylet::HandleAcceptNodeManager(const boost::system::error_code &error) {
  if (!error) {
    ClientHandler<boost::asio::ip::tcp> client_handler =
        [this](std::shared_ptr<TcpClientConnection> client) {
          node_manager_.ProcessNewNodeManager(client);
        };
    MessageHandler<boost::asio::ip::tcp> message_handler = [this](
        std::shared_ptr<TcpClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      node_manager_.ProcessNodeManagerMessage(client, message_type, message);
    };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = TcpClientConnection::Create(client_handler, message_handler,
                                                      std::move(node_manager_socket_));
  }
  // We're ready to accept another client.
  DoAcceptNodeManager();
}

void Raylet::HandleAcceptTcp(TCPClientConnection::pointer new_connection,
                             const boost::system::error_code &error) {
  if (!error) {
    // Pass it off to object manager for now.
    ray::Status status = object_manager_.AcceptConnection(std::move(new_connection));
  }
  DoAcceptTcp();
}

void Raylet::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&Raylet::HandleAccept, this,
                                              boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // TODO: typedef these handlers.
    ClientHandler<boost::asio::local::stream_protocol> client_handler =
        [this](std::shared_ptr<LocalClientConnection> client) {
          node_manager_.ProcessNewClient(client);
        };
    MessageHandler<boost::asio::local::stream_protocol> message_handler = [this](
        std::shared_ptr<LocalClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      node_manager_.ProcessClientMessage(client, message_type, message);
    };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = LocalClientConnection::Create(client_handler, message_handler,
                                                        std::move(socket_));
  }
  // We're ready to accept another client.
  DoAccept();
}

ObjectManager &Raylet::GetObjectManager() { return object_manager_; }

} // namespace raylet

}  // namespace ray
