#ifndef GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H
#define GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H

#include <memory>
#include "ray/gcs/service_discovery/gcs_service_discovery_client.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/gcs/store_client/store_tables.h"

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
  /// \param io_service_pool The event loops for this client.
  /// \param store_client The storage client to access service information from.
  StoreBasedGcsServiceDiscoveryClient(
      const GcsServiceDiscoveryClientOptions &options,
      std::shared_ptr<IOServicePool> io_service_pool,
      std::shared_ptr<GcsServerInfoTable> gcs_server_table);

  virtual ~StoreBasedGcsServiceDiscoveryClient();

  Status Init() override;

  void Shutdown() override;

  Status AsyncRegisterService(const rpc::GcsServerInfo &service_info,
                              const StatusCallback &callback) override;

  void RegisterServiceWatcher(const ServiceWatcherCallback &callback) override;

 private:
  /// Process the gcs service information that received from storage.
  ///
  /// \param cur_service_info The information that received from storage.
  void OnReceiveGcsServiceInfo(const rpc::GcsServerInfo &cur_service_info);

  /// Get local gcs service info.
  ///
  /// return boost::optional<rpc::GcsServerInfo>
  boost::optional<rpc::GcsServerInfo> GetGcsServiceInfo();

  /// Start timer to poll gcs service information from storage.
  void RunQueryStoreTimer();

  std::shared_ptr<GcsServerInfoTable> gcs_server_table_;

  /// A timer that ticks every fixed milliseconds.
  std::unique_ptr<boost::asio::deadline_timer> query_store_timer_;
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_SERVICE_DISCOVERY_STORE_BASED_GCS_SERVICE_DISCOVERY_CLIENT_H
