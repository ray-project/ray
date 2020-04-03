#include "ray/gcs/service_discovery/store_based_gcs_service_discovery_client.h"

namespace ray {

namespace gcs {

StoreBasedGcsServiceDiscoveryClient::StoreBasedGcsServiceDiscoveryClient(
    const GcsServiceDiscoveryClientOptions &options,
    std::shared_ptr<StoreClient> store_client)
    : GcsServiceDiscoveryClient(options), store_client_(store_client) {
  RAY_CHECK(store_client_);
  RAY_CHECK(!options_.target_gcs_service_name_.empty());
}

StoreBasedGcsServiceDiscoveryClient::~StoreBasedGcsServiceDiscoveryClient() {}

Status StoreBasedGcsServiceDiscoveryClient::Init(boost::asio::io_service &io_service) {
  query_store_timer_.reset(new boost::asio::deadline_timer(io_service));
}

void StoreBasedGcsServiceDiscoveryClient::Shutdown() {
  if (query_store_timer_) {
    boost::system::error_code ec;
    query_store_timer_->cancel(ec);
    if (ec) {
      RAY_LOG(WARNING) << "An exception occurs when Shutdown, error " << ec.message();
    }
  }
}

Status StoreBasedGcsServiceDiscoveryClient::RegisterService(
    const rpc::GcsServerInfo &service_info) {
  return store_client_->Put(options_.target_gcs_service_name_,
                            service_info.SerializeToString());
}

void StoreBasedGcsServiceDiscoveryClient::RegisterServiceWatcher(
    const ServiceWatcherCallback &callback) {
  RAY_CHECK(callback);
  RAY_CHECK(!!service_watcher_callback_);
  service_watcher_callback_ = callback;

  auto opt_service_info = GetGcsServiceInfo();
  if (opt_service_info) {
    // TODO(micafan) Maybe post the callback to event loop.
    service_watcher_callback_(*opt_service_info);
  }

  // Start timer task to poll gcs service information from storage.
  RunQueryStoreTimer();
}

boost::optional<rpc::GcsServerInfo>
StoreBasedGcsServiceDiscoveryClient::GetGcsServiceInfo() {
  boost::optional<rpc::GcsServerInfo> opt_service_info;
  {
    absl::MutexLock lock(&mutex_);
    if (!received_gcs_service_info_.gcs_server_address_.empty()) {
      *opt_service_info = received_gcs_service_info_;
    }
  }
  return opt_service_info;
}

void StoreBasedGcsServiceDiscoveryClient::OnReceiveGcsServiceInfo(
    const GcsServerInfo &cur_service_info) {
  bool changed = false;
  if (cur_service_info.gcs_server_address_.empty()) {
    RAY_LOG(DEBUG) << "Receive empty gcs service info.";
    return;
  }

  {
    absl::MutexLock lock(&mutex_);
    if (received_gcs_service_info_ != cur_service_info) {
      received_gcs_service_info_ = cur_service_info;
      changed = true;
    }
  }

  if (changed) {
    RAY_LOG(INFO) << "Receive new gcs service info "
                  << cur_service_info.ShortDebugString();
    service_watcher_callback_(cur_service_info);
  }
}

void StoreBasedGcsServiceDiscoveryClient::RunQueryStoreTimer() {
  auto on_get_callback =
      [this](Status status, const boost::optional<std::string> &result) {
        if (status.OK() && result) {
          GcsServerInfo cur_service_info;
          RAY_DCHECK(cur_service_info.ParseFromString(*result));
          OnReceiveGcsServiceInfo(cur_service_info);
        }
        if (!status.OK()) {
          // TODO(micafan) Change RAY_LOG to LOG_EVERY_N.
          RAY_LOG(INFO) << "Get gcs service info from storage failed, status "
                        << status.ToString();
        }

        auto query_period = boost::posix_time::milliseconds(100);
        query_store_timer_.expires_from_now(query_period);
        query_store_timer_.async_wait([this](const boost::system::error_code &error) {
          RAY_CHECK(!error);
          RunQueryStoreTimer();
        });
      }

  Status status =
      store_client_->AsyncGet(options_.target_gcs_service_name_, on_get_callback);
  RAY_CHECK_OK(status);
}

}  // namespace gcs

}  // namespace ray