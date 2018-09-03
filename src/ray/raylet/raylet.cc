#include "raylet.h"

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

#include "ray/status.h"

namespace ray {

namespace raylet {

Raylet::Raylet(boost::asio::io_service &main_service, const std::string &socket_name,
               const std::string &node_ip_address, const std::string &redis_address,
               int redis_port, const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    : gcs_client_(gcs_client),
      object_manager_(main_service, object_manager_config, gcs_client),
      node_manager_(main_service, node_manager_config, object_manager_, gcs_client_),
      socket_name_(socket_name),
      acceptor_(main_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(main_service),
      object_manager_acceptor_(
          main_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
      object_manager_socket_(main_service),
      node_manager_acceptor_(
          main_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
      node_manager_socket_(main_service) {
  // Start listening for clients.
  DoAccept();
  DoAcceptObjectManager();
  DoAcceptNodeManager();

  RAY_CHECK_OK(RegisterGcs(node_ip_address, socket_name_,
                           object_manager_config.store_socket_name, redis_address,
                           redis_port, main_service, node_manager_config));

  RAY_CHECK_OK(RegisterPeriodicTimer(main_service));
}

Raylet::~Raylet() { RAY_CHECK_OK(gcs_client_->client_table().Disconnect()); }

ray::Status Raylet::RegisterPeriodicTimer(boost::asio::io_service &io_service) {
  boost::posix_time::milliseconds timer_period_ms(100);
  boost::asio::deadline_timer timer(io_service, timer_period_ms);
  return ray::Status::OK();
}

ray::Status Raylet::RegisterGcs(const std::string &node_ip_address,
                                const std::string &raylet_socket_name,
                                const std::string &object_store_socket_name,
                                const std::string &redis_address, int redis_port,
                                boost::asio::io_service &io_service,
                                const NodeManagerConfig &node_manager_config) {
  RAY_RETURN_NOT_OK(gcs_client_->Attach(io_service));

  ClientTableDataT client_info = gcs_client_->client_table().GetLocalClient();
  client_info.node_manager_address = node_ip_address;
  client_info.raylet_socket_name = raylet_socket_name;
  client_info.object_store_socket_name = object_store_socket_name;
  client_info.object_manager_port = object_manager_acceptor_.local_endpoint().port();
  client_info.node_manager_port = node_manager_acceptor_.local_endpoint().port();
  // Add resource information.
  for (const auto &resource_pair : node_manager_config.resource_config.GetResourceMap()) {
    client_info.resources_total_label.push_back(resource_pair.first);
    client_info.resources_total_capacity.push_back(resource_pair.second);
  }

  RAY_LOG(DEBUG) << "Node manager listening on: IP " << client_info.node_manager_address
                 << " port " << client_info.node_manager_port;
  RAY_RETURN_NOT_OK(gcs_client_->client_table().Connect(client_info));

  RAY_RETURN_NOT_OK(node_manager_.RegisterGcs());

  return Status::OK();
}

void Raylet::DoAcceptNodeManager() {
  node_manager_acceptor_.async_accept(node_manager_socket_,
                                      boost::bind(&Raylet::HandleAcceptNodeManager, this,
                                                  boost::asio::placeholders::error));
}

void Raylet::HandleAcceptNodeManager(const boost::system::error_code &error) {
  if (!error) {
    ClientHandler<boost::asio::ip::tcp> client_handler = [this](
        TcpClientConnection &client) { node_manager_.ProcessNewNodeManager(client); };
    MessageHandler<boost::asio::ip::tcp> message_handler = [this](
        std::shared_ptr<TcpClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      node_manager_.ProcessNodeManagerMessage(*client, message_type, message);
    };
    // Accept a new TCP client and dispatch it to the node manager.
    auto new_connection = TcpClientConnection::Create(
        client_handler, message_handler, std::move(node_manager_socket_), "node manager");
  }
  // We're ready to accept another client.
  DoAcceptNodeManager();
}

void Raylet::DoAcceptObjectManager() {
  object_manager_acceptor_.async_accept(
      object_manager_socket_, boost::bind(&Raylet::HandleAcceptObjectManager, this,
                                          boost::asio::placeholders::error));
}

void Raylet::HandleAcceptObjectManager(const boost::system::error_code &error) {
  ClientHandler<boost::asio::ip::tcp> client_handler =
      [this](TcpClientConnection &client) { object_manager_.ProcessNewClient(client); };
  MessageHandler<boost::asio::ip::tcp> message_handler = [this](
      std::shared_ptr<TcpClientConnection> client, int64_t message_type,
      const uint8_t *message) {
    object_manager_.ProcessClientMessage(client, message_type, message);
  };
  // Accept a new TCP client and dispatch it to the node manager.
  auto new_connection =
      TcpClientConnection::Create(client_handler, message_handler,
                                  std::move(object_manager_socket_), "object manager");
  DoAcceptObjectManager();
}

void Raylet::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&Raylet::HandleAccept, this,
                                              boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // TODO: typedef these handlers.
    ClientHandler<boost::asio::local::stream_protocol> client_handler =
        [this](LocalClientConnection &client) { node_manager_.ProcessNewClient(client); };
    MessageHandler<boost::asio::local::stream_protocol> message_handler = [this](
        std::shared_ptr<LocalClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      node_manager_.ProcessClientMessage(client, message_type, message);
    };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = LocalClientConnection::Create(client_handler, message_handler,
                                                        std::move(socket_), "worker");
  }
  // We're ready to accept another client.
  DoAccept();
}

}  // namespace raylet

}  // namespace ray
