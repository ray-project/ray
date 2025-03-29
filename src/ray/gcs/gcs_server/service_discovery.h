#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/head_node_service.h"

namespace ray {
namespace gcs {

/// ServiceDiscovery manages service registration and discovery across head nodes.
class ServiceDiscovery {
 public:
  /// Constructor.
  ///
  /// \param[in] gcs_server The GCS server instance.
  /// \param[in] io_context The IO context for async operations.
  ServiceDiscovery(GcsServer &gcs_server, instrumented_io_context &io_context)
      : gcs_server_(gcs_server),
        io_context_(io_context),
        services_() {}

  /// Start the service discovery.
  void Start();

  /// Stop the service discovery.
  void Stop();

  /// Register a service.
  ///
  /// \param[in] request The registration request.
  /// \param[out] reply The registration reply.
  /// \return Status indicating success or failure.
  Status RegisterService(const rpc::RegisterServiceRequest &request,
                        rpc::RegisterServiceReply *reply);

  /// Discover services of a given type.
  ///
  /// \param[in] request The discovery request.
  /// \param[out] reply The discovery reply.
  /// \return Status indicating success or failure.
  Status DiscoverService(const rpc::DiscoverServiceRequest &request,
                        rpc::DiscoverServiceReply *reply);

  /// Get all registered services.
  ///
  /// \return Vector of service information.
  std::vector<rpc::ServiceInfo> GetAllServices() const;

  /// Get services of a specific type.
  ///
  /// \param[in] service_type The type of service to get.
  /// \return Vector of service information.
  std::vector<rpc::ServiceInfo> GetServicesByType(const std::string &service_type) const;

  /// Remove a service.
  ///
  /// \param[in] service_id The ID of the service to remove.
  void RemoveService(const std::string &service_id);

 private:
  /// Reference to the GCS server.
  GcsServer &gcs_server_;

  /// The IO context for async operations.
  instrumented_io_context &io_context_;

  /// Map of service ID to service information.
  std::unordered_map<std::string, rpc::ServiceInfo> services_;

  /// Map of service type to service IDs.
  std::unordered_map<std::string, std::vector<std::string>> service_type_to_ids_;
};

}  // namespace gcs
}  // namespace ray 