#include "ray/gcs/service_discovery/store_based_gcs_service_discovery_client.h"

namespace ray {

namespace gcs {

StoreBasedGcsServiceDiscoveryClient::StoreBasedGcsServiceDiscoveryClient(
    const GcsServiceDiscoveryClientOptions &options,
    std::shared_ptr<IOServicePool> io_service_pool,
    std::shared_ptr<GcsServerInfoTable> gcs_server_table)

    : GcsServiceDiscoveryClient(options, std::move(io_service_pool)),
      gcs_server_table_(std::move(gcs_server_table)) {
  RAY_CHECK(gcs_server_table_);
  RAY_CHECK(!options_.target_gcs_service_name_.empty());
}

StoreBasedGcsServiceDiscoveryClient::~StoreBasedGcsServiceDiscoveryClient() {}

Status StoreBasedGcsServiceDiscoveryClient::Init() {
  boost::asio::io_service *io_service = io_service_pool_->Get();
  query_store_timer_.reset(new boost::asio::deadline_timer(*io_service));
  return Status::OK();
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

Status StoreBasedGcsServiceDiscoveryClient::AsyncRegisterService(
    const rpc::GcsServerInfo &service_info,
    const StatusCallback &callback) {
  return gcs_server_table_->AsyncPut(GcsServerID::Nil(), service_info, callback);
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
    if (!received_gcs_service_info_.gcs_server_address().empty()) {
      *opt_service_info = received_gcs_service_info_;
    }
  }
  return opt_service_info;
}

void StoreBasedGcsServiceDiscoveryClient::OnReceiveGcsServiceInfo(
    const rpc::GcsServerInfo &cur_service_info) {
  bool changed = false;
  if (cur_service_info.gcs_server_address().empty()) {
    RAY_LOG(DEBUG) << "Receive empty gcs service info.";
    return;
  }

  {
    absl::MutexLock lock(&mutex_);
    if (received_gcs_service_info_.gcs_server_id() != cur_service_info.gcs_server_id()) {
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
      [this](Status status, const boost::optional<rpc::GcsServerInfo> &result) {
        if (status.ok() && result) {
          OnReceiveGcsServiceInfo(*result);
        }
        if (!status.ok()) {
          // TODO(micafan) Change RAY_LOG to RAY_LOG_EVERY_N.
          RAY_LOG(INFO) << "Get gcs service info from storage failed, status "
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
      gcs_server_table_->AsyncGet(GcsServerID::Nil(), on_get_callback);
  RAY_CHECK_OK(status);
}

}  // namespace gcs

}  // namespace ray