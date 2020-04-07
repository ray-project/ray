#ifndef RAY_GCS_SERVICE_DISCOVERY_GCS_SERVICE_DISCOVERY_CLIENT_H
#define RAY_GCS_SERVICE_DISCOVERY_GCS_SERVICE_DISCOVERY_CLIENT_H

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

/// \class GcsServiceDiscoveryClientOptions
/// This class includes all
class GcsServiceDiscoveryClientOptions {
 public:
  GcsServiceDiscoveryClientOptions(const std::string &target_gcs_service_name,
                                   const std::string &discovery_server_address,
                                   int discovery_server_port)
      : target_gcs_service_name_(target_gcs_service_name),
        discovery_server_address_(discovery_server_address),
        discovery_server_port_(discovery_server_port) {}

  /// The name of gcs service to be watched or to be registered.
  /// It's may be the ray cluster name.
  std::string target_gcs_service_name_;

  /// Address of discovery server.
  std::string discovery_server_address_;
  /// Port of discovery server.
  int discovery_server_port_;
};

/// \GcsServiceDiscoveryClient
/// This class include all the methods to access gcs service information
/// from discovery service.
class GcsServiceDiscoveryClient {
 public:
  virtual ~GcsServiceDiscoveryClient() {}

  /// Init this client.
  ///
  /// \param io_service The event loop for this client.
  /// \return Status
  virtual Status Init() = 0;

  /// Shutdown this client.
  virtual void Shutdown() = 0;

  /// Register gcs service information to discovery service asynchronously.
  /// This interface is used for GCS Server to register as a service.
  ///
  /// \param service_info The information of gcs service that will be registered.
  /// \param callback The callback that will be called after the register finishes.
  /// \return Status
  virtual Status AsyncRegisterService(const rpc::GcsServerInfo &service_info,
                                      const StatusCallback &callback) = 0;

  /// This callback is used to receive notifications of service info.
  using ServiceWatcherCallback =
      std::function<void(const rpc::GcsServerInfo &service_info)>;

  /// Listen for gcs service information changes from discovery service.
  /// This interface is used for GCS Client to discover service.
  ///
  /// \param callback The callback that will be called when gcs service info changes.
  virtual void RegisterServiceWatcher(const ServiceWatcherCallback &callback) = 0;

 protected:
  GcsServiceDiscoveryClient(const GcsServiceDiscoveryClientOptions &options,
                            std::shared_ptr<IOServicePool> io_service_pool)
      : options_(options),
        io_service_pool_(std::move(io_service_pool)) {}

  /// Options of this client.
  GcsServiceDiscoveryClientOptions options_;

  std::shared_ptr<IOServicePool> io_service_pool_;

  /// The callback that registered to watch gcs service information.
  ServiceWatcherCallback service_watcher_callback_{nullptr};

  absl::Mutex mutex_;
  /// The gcs service information that received from discovery service.
  rpc::GcsServerInfo received_gcs_service_info_ GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SERVICE_DISCOVERY_GCS_SERVICE_DISCOVERY_CLIENT_H
