#include "raylet.h"

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

#include "ray/common/status.h"

namespace {

const std::vector<std::string> GenerateEnumNames(const char *const *enum_names_ptr,
                                                 int start_index, int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  size_t i = 0;
  while (true) {
    const char *name = enum_names_ptr[i];
    if (name == nullptr) {
      break;
    }
    enum_names.push_back(name);
    i++;
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}

static const std::vector<std::string> node_manager_message_enum =
    GenerateEnumNames(ray::protocol::EnumNamesMessageType(),
                      static_cast<int>(ray::protocol::MessageType::MIN),
                      static_cast<int>(ray::protocol::MessageType::MAX));
}  // namespace

namespace ray {

namespace raylet {

Raylet::Raylet(boost::asio::io_service &main_service, const std::string &socket_name,
               const std::string &node_ip_address, const std::string &redis_address,
               int redis_port, const std::string &redis_password,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::RedisGcsClient> gcs_client)
    : gcs_client_(gcs_client),
      object_directory_(std::make_shared<ObjectDirectory>(main_service, gcs_client_)),
      object_manager_(main_service, object_manager_config, object_directory_),
      node_manager_(main_service, node_manager_config, object_manager_, gcs_client_,
                    object_directory_),
      socket_name_(socket_name),
      acceptor_(main_service, local_stream_protocol::endpoint(socket_name)),
      socket_(main_service) {
  // Start listening for clients.
  DoAccept();

  RAY_CHECK_OK(RegisterGcs(
      node_ip_address, socket_name_, object_manager_config.store_socket_name,
      redis_address, redis_port, redis_password, main_service, node_manager_config));

  RAY_CHECK_OK(RegisterPeriodicTimer(main_service));
}

Raylet::~Raylet() {}

ray::Status Raylet::RegisterPeriodicTimer(boost::asio::io_service &io_service) {
  boost::posix_time::milliseconds timer_period_ms(100);
  boost::asio::deadline_timer timer(io_service, timer_period_ms);
  return ray::Status::OK();
}

ray::Status Raylet::RegisterGcs(const std::string &node_ip_address,
                                const std::string &raylet_socket_name,
                                const std::string &object_store_socket_name,
                                const std::string &redis_address, int redis_port,
                                const std::string &redis_password,
                                boost::asio::io_service &io_service,
                                const NodeManagerConfig &node_manager_config) {
  GcsNodeInfo node_info = gcs_client_->client_table().GetLocalClient();
  node_info.set_node_manager_address(node_ip_address);
  node_info.set_raylet_socket_name(raylet_socket_name);
  node_info.set_object_store_socket_name(object_store_socket_name);
  node_info.set_object_manager_port(object_manager_.GetServerPort());
  node_info.set_node_manager_port(node_manager_.GetServerPort());
  node_info.set_node_manager_hostname(boost::asio::ip::host_name());

  RAY_LOG(DEBUG) << "Node manager " << gcs_client_->client_table().GetLocalClientId()
                 << " started on " << node_info.node_manager_address() << ":"
                 << node_info.node_manager_port() << " object manager at "
                 << node_info.node_manager_address() << ":"
                 << node_info.object_manager_port() << ", hostname "
                 << node_info.node_manager_hostname();
  ;
  RAY_RETURN_NOT_OK(gcs_client_->client_table().Connect(node_info));

  // Add resource information.
  std::unordered_map<std::string, std::shared_ptr<gcs::ResourceTableData>> resources;
  for (const auto &resource_pair : node_manager_config.resource_config.GetResourceMap()) {
    auto resource = std::make_shared<gcs::ResourceTableData>();
    resource->set_resource_capacity(resource_pair.second);
    resources.emplace(resource_pair.first, resource);
  }
  RAY_RETURN_NOT_OK(gcs_client_->resource_table().Update(
      JobID::Nil(), gcs_client_->client_table().GetLocalClientId(), resources, nullptr));

  RAY_RETURN_NOT_OK(node_manager_.RegisterGcs());

  return Status::OK();
}

void Raylet::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&Raylet::HandleAccept, this,
                                              boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // TODO: typedef these handlers.
    ClientHandler<local_stream_protocol> client_handler =
        [this](LocalClientConnection &client) { node_manager_.ProcessNewClient(client); };
    MessageHandler<local_stream_protocol> message_handler =
        [this](std::shared_ptr<LocalClientConnection> client, int64_t message_type,
               const uint8_t *message) {
          node_manager_.ProcessClientMessage(client, message_type, message);
        };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = LocalClientConnection::Create(
        client_handler, message_handler, std::move(socket_), "worker",
        node_manager_message_enum,
        static_cast<int64_t>(protocol::MessageType::DisconnectClient));
  }
  // We're ready to accept another client.
  DoAccept();
}

}  // namespace raylet

}  // namespace ray
