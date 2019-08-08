#include "raylet.h"

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

#include "ray/common/status.h"

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
      raylet_server_("Raylet", socket_name),
      raylet_service_(main_service, node_manager_) {
  raylet_server_.RegisterService(raylet_service_);
  raylet_server_.Run();

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

  RAY_LOG(DEBUG) << "Node manager " << gcs_client_->client_table().GetLocalClientId()
                 << " started on " << node_info.node_manager_address() << ":"
                 << node_info.node_manager_port() << " object manager at "
                 << node_info.node_manager_address() << ":"
                 << node_info.object_manager_port();
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

}  // namespace raylet

}  // namespace ray
