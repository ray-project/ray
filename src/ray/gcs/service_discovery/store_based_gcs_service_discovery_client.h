#ifndef GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H
#define GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H

#include <memory>
#include "ray/gcs/service_discovery/gcs_service_discovery_client.h"
#include "ray/gcs/store_client/redis_store_client.h"

namespace ray {

namespace gcs {

/// \class StoreBasedGcsServiceDiscoveryClient
/// `StoreBasedGcsServiceDiscoveryClient`  is an implementation of
/// GcsServiceDiscoveryClient that use storage as the backend discovery service.
class StoreBasedGcsServiceDiscoveryClient : public GcsServiceDiscoveryClient {
 public:
  /// Constructor of StoreBasedGcsServiceDiscoveryClient.
  ///
  /// \param options Options of this client.
  /// \param store_client The storage client to access service information from.
  StoreBasedGcsServiceDiscoveryClient(const GcsServiceDiscoveryClientOptions &options,
                                      std::shared_ptr<StoreClient> store_client);

  virtual ~StoreBasedGcsServiceDiscoveryClient();

  Status Init(boost::asio::io_service &io_service);

  Status RegisterService(const rpc::GcsServiceInfo &service_info);

  void RegisterServiceWatcher(const ServiceWatcherCallback &callback);

 private:
  /// Process the gcs service information that received from storage.
  ///
  /// \param cur_service_info The information that received from storage.
  void OnReceiveGcsServiceInfo(const GcsServiceInfo &cur_service_info);

  /// Get local gcs service info.
  ///
  /// return boost::optional<rpc::GcsServiceInfo>
  boost::optional<rpc::GcsServiceInfo> GetGcsServiceInfo();

  /// Start timer to poll gcs service information from storage.
  void RunQueryStoreTimer();

  std::shared_ptr<StoreClient> store_client_;

  /// A timer that ticks every fixed milliseconds.
  std::unique_ptr<boost::asio::deadline_timer> query_store_timer_;
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H
