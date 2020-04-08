#ifndef GCS_SERVICE_DISCOVERY_STORE_BASED_SERVICE_DISCOVERY_CLIENT_H
#define GCS_SERVICE_DISCOVERY_STORE_BASED_SERVICE_DISCOVERY_CLIENT_H

#include <memory>
#include "ray/gcs/service_discovery/service_discovery_client.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

/// \class StoreBasedServiceDiscoveryClient
/// `StoreBasedServiceDiscoveryClient` is an implementation of
/// ServiceDiscoveryClient that use storage as the backend discovery service.
template <typename ServiceInfo>
class StoreBasedServiceDiscoveryClient : public ServiceDiscoveryClient<ServiceInfo> {
 public:
  /// Constructor of StoreBasedServiceDiscoveryClient.
  ///
  /// \param options Options of this client.
  /// \param io_service_pool The event loops for this client.
  /// \param store_client The storage client to access service information from.
  StoreBasedServiceDiscoveryClient(
      std::shared_ptr<IOServicePool> io_service_pool,
      std::shared_ptr<StoreClient<ServiceInfo>> store_client);

  virtual ~StoreBasedServiceDiscoveryClient();

  Status Init() override;

  void Shutdown() override;

  Status AsyncRegisterService(const std::string &service_name,
                              const ServiceInfo &service_info,
                              const StatusCallback &callback) override;

  void RegisterServiceWatcher(const std::string &service_name,
                              const ItemCallback<ServiceInfo> &callback) override;

 private:
  /// Process the service information that received from storage.
  ///
  /// \param new_service_info_str The information that received from storage.
  void OnReceiveServiceInfo(const ServiceInfo &new_service_info);

  /// Start timer to poll service information from storage.
  void RunQueryStoreTimer();

 private:
  std::shared_ptr<IOServicePool> io_service_pool_;

  std::shared_ptr<StoreClient<ServiceInfo>> store_client_;
  std::string service_table_name_;

  /// A timer that ticks every fixed milliseconds.
  std::unique_ptr<boost::asio::deadline_timer> query_store_timer_;

  /// The name of the service that to be listened.
  std::string service_name_;
  /// The callback that will be called if  service information changes.
  ItemCallback<ServiceInfo> service_watcher_callback_{nullptr};

  /// The service information that received from discovery service.
  ServiceInfo received_service_info_;
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_SERVICE_DISCOVERY_STORE_BASED_SERVICE_DISCOVERY_CLIENT_H
