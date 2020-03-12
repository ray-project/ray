#ifndef RAY_GCS_GCS_SERVICE_DISCOVERY_CLIENT_H
#define RAY_GCS_GCS_SERVICE_DISCOVERY_CLIENT_H

#include <boost/asio.hpp>
#include <functional>
#include <string>
#include "absl/synchronization/mutex.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs_service.pb.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsServiceDiscoveryClientOptions
/// This class includes all
class GcsServiceDiscoveryClientOptions {
 public:
  GcsServiceDiscoveryClientOptions()
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
  Status Init(boost::asio::io_service &io_service);

  /// Register gcs service information to discovery service synchronously.
  /// This interface is used for GCS Server to register as a service.
  ///
  /// \param service_info The information of gcs service that will be registered.
  /// \return Status
  // TODO(micafan) Maybe change it to asynchronous method.
  Status RegisterService(const rpc::GcsServiceInfo &service_info);

  /// This callback is used to receive notifications of service info.
  using ServiceWatcherCallback =
      std::function<void(const rpc::GcsServiceInfo &service_info)>;

  /// Listen for gcs service information changes from discovery service.
  /// This interface is used for GCS Client to discover service.
  ///
  /// \param callback The callback that will be called when gcs service info changes.
  void RegisterServiceWatcher(const ServiceWatcherCallback &callback);

 protected:
  GcsServiceDiscoveryClient(const GcsServiceDiscoveryOptions &options)
      : options_(options) {}

  /// Options of this client.
  GcsServiceDiscoveryOptions options_;

  /// The event loop for this client.
  boost::asio::io_service *io_service_{nullptr};

  /// The callback that registered to watch gcs service information.
  ServiceWatcherCallback service_watcher_callback_{nullptr};

  absl::Mutex mutex_;
  /// The gcs service information that received from discovery service.
  rpc::GcsServiceInfo received_gcs_service_info_ GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_SERVICE_DISCOVERY_CLIENT_H
