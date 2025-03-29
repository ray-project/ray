#include "ray/gcs/gcs_server/service_discovery.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"

namespace ray {
namespace gcs {

ServiceDiscovery::ServiceDiscovery(GcsServer &gcs_server,
                                 instrumented_io_context &io_context)
    : gcs_server_(gcs_server), io_context_(io_context) {}

Status ServiceDiscovery::Start() {
  // Initialize service registry
  service_registry_.clear();
  return Status::OK();
}

Status ServiceDiscovery::Stop() {
  // Clear service registry
  service_registry_.clear();
  return Status::OK();
}

Status ServiceDiscovery::RegisterService(const std::string &service_id,
                                       const std::string &service_type,
                                       const std::string &address,
                                       int port,
                                       const std::unordered_map<std::string, std::string> &metadata) {
  // Create service info
  ServiceInfo service_info;
  service_info.service_id = service_id;
  service_info.service_type = service_type;
  service_info.address = address;
  service_info.port = port;
  service_info.metadata = metadata;
  service_info.last_heartbeat = std::chrono::system_clock::now();

  // Add to registry
  service_registry_[service_id] = service_info;

  // Add to type index
  service_type_index_[service_type].insert(service_id);

  return Status::OK();
}

Status ServiceDiscovery::DiscoverService(const std::string &service_id,
                                       ServiceInfo *service_info) {
  auto it = service_registry_.find(service_id);
  if (it == service_registry_.end()) {
    return Status::NotFound("Service not found");
  }

  *service_info = it->second;
  return Status::OK();
}

Status ServiceDiscovery::GetAllServices(std::vector<ServiceInfo> *services) {
  services->clear();
  services->reserve(service_registry_.size());

  for (const auto &entry : service_registry_) {
    services->push_back(entry.second);
  }

  return Status::OK();
}

Status ServiceDiscovery::GetServicesByType(const std::string &service_type,
                                         std::vector<ServiceInfo> *services) {
  services->clear();

  auto type_it = service_type_index_.find(service_type);
  if (type_it == service_type_index_.end()) {
    return Status::OK();
  }

  services->reserve(type_it->second.size());
  for (const auto &service_id : type_it->second) {
    auto service_it = service_registry_.find(service_id);
    if (service_it != service_registry_.end()) {
      services->push_back(service_it->second);
    }
  }

  return Status::OK();
}

Status ServiceDiscovery::RemoveService(const std::string &service_id) {
  auto it = service_registry_.find(service_id);
  if (it == service_registry_.end()) {
    return Status::NotFound("Service not found");
  }

  // Remove from type index
  auto type_it = service_type_index_.find(it->second.service_type);
  if (type_it != service_type_index_.end()) {
    type_it->second.erase(service_id);
    if (type_it->second.empty()) {
      service_type_index_.erase(type_it);
    }
  }

  // Remove from registry
  service_registry_.erase(it);

  return Status::OK();
}

void ServiceDiscovery::CleanupExpiredServices() {
  auto now = std::chrono::system_clock::now();
  std::vector<std::string> expired_services;

  // Find expired services
  for (const auto &entry : service_registry_) {
    auto time_since_last_heartbeat = now - entry.second.last_heartbeat;
    if (time_since_last_heartbeat > service_ttl_) {
      expired_services.push_back(entry.first);
    }
  }

  // Remove expired services
  for (const auto &service_id : expired_services) {
    RemoveService(service_id);
  }
}

}  // namespace gcs
}  // namespace ray 