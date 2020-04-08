#include "ray/gcs/service_discovery/store_based_service_discovery_client.h"

namespace ray {

namespace gcs {

template <typename ServiceInfo>
StoreBasedServiceDiscoveryClient<ServiceInfo>::StoreBasedServiceDiscoveryClient(
    std::shared_ptr<IOServicePool> io_service_pool,
    std::shared_ptr<StoreClient<ServiceInfo>> store_client)
    : io_service_pool_(std::move(io_service_pool)),
      store_client_(std::move(store_client)) {
  RAY_CHECK(io_service_pool_);
  RAY_CHECK(store_client_);
  // TODO(micafan) Table name should contain ray cluster name.
  // Ensure that different ray clusters do not overwrite each other when sharing storage.
  service_table_name_ = kServiceDiscoveryTableNamePrefix;
}

template <typename ServiceInfo>
StoreBasedServiceDiscoveryClient<ServiceInfo>::~StoreBasedServiceDiscoveryClient() {}

template <typename ServiceInfo>
Status StoreBasedServiceDiscoveryClient<ServiceInfo>::Init() {
  boost::asio::io_service *io_service = io_service_pool_->Get();
  query_store_timer_.reset(new boost::asio::deadline_timer(*io_service));
  return Status::OK();
}

template <typename ServiceInfo>
void StoreBasedServiceDiscoveryClient<ServiceInfo>::Shutdown() {
  if (query_store_timer_) {
    boost::system::error_code ec;
    // TODO(micafan) Confirm whether the timer cancel is sync.
    query_store_timer_->cancel(ec);
    if (ec) {
      RAY_LOG(WARNING) << "An exception occurs when Shutdown, error " << ec.message();
    }
  }
}

template <typename ServiceInfo>
Status StoreBasedServiceDiscoveryClient<ServiceInfo>::AsyncRegisterService(
    const std::string &service_name, const ServiceInfo &service_info,
    const StatusCallback &callback) {
  std::string service_info_str = service_info.SerializeAsString();
  RAY_CHECK(!service_info_str.empty());
  return store_client_->AsyncPut(service_table_name_, service_name, service_info_str,
                                 callback);
}

template <typename ServiceInfo>
void StoreBasedServiceDiscoveryClient<ServiceInfo>::RegisterServiceWatcher(
    const std::string &service_name, const ItemCallback<ServiceInfo> &callback) {
  RAY_CHECK(callback);

  RAY_CHECK(service_name_.empty());
  RAY_CHECK(!service_watcher_callback_);

  service_name_ = service_name;
  service_watcher_callback_ = callback;

  // Start timer task to poll service information from storage.
  RunQueryStoreTimer();
}

template <typename ServiceInfo>
void StoreBasedServiceDiscoveryClient<ServiceInfo>::OnReceiveServiceInfo(
    const ServiceInfo &new_service_info) {
  bool changed = false;

  std::string new_service_info_str = new_service_info.SerializeAsString();
  std::string service_info_str = received_service_info_.SerializeAsString();
  if (new_service_info_str != service_info_str) {
    received_service_info_ = new_service_info;
    changed = true;
  }

  if (changed) {
    RAY_LOG(INFO) << "Receive new service info " << new_service_info.ShortDebugString();
    service_watcher_callback_(new_service_info);
  }
}

template <typename ServiceInfo>
void StoreBasedServiceDiscoveryClient<ServiceInfo>::RunQueryStoreTimer() {
  auto on_get_callback = [this](Status status,
                                const boost::optional<ServiceInfo> &result) {
    if (status.ok() && result) {
      OnReceiveServiceInfo(*result);
    }
    if (!status.ok()) {
      // TODO(micafan) Change RAY_LOG to RAY_LOG_EVERY_N.
      RAY_LOG(INFO) << "Get service info from storage failed, status "
                    << status.ToString();
    }

    auto query_period = boost::posix_time::milliseconds(100);
    query_store_timer_->expires_from_now(query_period);
    query_store_timer_->async_wait([this](const boost::system::error_code &error) {
      RAY_CHECK(!error);
      RunQueryStoreTimer();
    });
  };

  Status status =
      store_client_->AsyncGet(service_table_name_, service_name_, on_get_callback);
  RAY_CHECK_OK(status);
}

}  // namespace gcs

}  // namespace ray