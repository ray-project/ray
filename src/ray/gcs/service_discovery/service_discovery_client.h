#ifndef RAY_GCS_SERVICE_DISCOVERY_SERVICE_DISCOVERY_CLIENT_H
#define RAY_GCS_SERVICE_DISCOVERY_SERVICE_DISCOVERY_CLIENT_H

#include <boost/asio.hpp>
#include <functional>
#include <string>
#include "absl/synchronization/mutex.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \ServiceDiscoveryClient
/// This class include all the methods to access service information
/// from discovery service.
template <typename ServiceInfo>
class ServiceDiscoveryClient {
 public:
  virtual ~ServiceDiscoveryClient() {}

  /// Init this client.
  ///
  /// \param io_service The event loop for this client.
  /// \return Status
  virtual Status Init() = 0;

  /// Shutdown this client.
  virtual void Shutdown() = 0;

  /// Register service information to discovery service asynchronously.
  /// This interface is used for server to register as a service.
  ///
  /// \param service_name The name of the service that will be registered.
  /// \param service_info The information of the service that will be registered.
  /// \param callback The callback that will be called after the register finishes.
  /// \return Status
  virtual Status AsyncRegisterService(const std::string &service_name,
                                      const ServiceInfo &service_info,
                                      const StatusCallback &callback) = 0;

  /// Listen for service information changes from discovery service.
  /// This interface is used for users to discover service. Non-thread saft.
  /// Should only call this method once.
  ///
  /// \param service_name The name of the service to be listened.
  /// \param callback The callback that will be called when service info changes.
  virtual void RegisterServiceWatcher(const std::string &service_name,
                                      const ItemCallback<ServiceInfo> &callback) = 0;

 protected:
  ServiceDiscoveryClient() {}
};

typedef ServiceDiscoveryClient<rpc::GcsServerInfo> GcsServiceDiscoveryClient;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SERVICE_DISCOVERY_SERVICE_DISCOVERY_CLIENT_H
