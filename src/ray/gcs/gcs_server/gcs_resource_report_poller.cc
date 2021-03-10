#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"

namespace ray {
namespace gcs {

GcsResourceReportPoller::GcsResourceReportPoller(
    uint64_t max_concurrent_pulls,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool)
    : max_concurrent_pulls_(max_concurrent_pulls),
      inflight_pulls_(0),
      gcs_resource_manager_(gcs_resource_manager),
      raylet_client_pool_(raylet_client_pool),
      poll_period_ms_(boost::posix_time::milliseconds(
          RayConfig::instance().gcs_resource_report_poll_period_ms())) {}

GcsResourceReportPoller::~GcsResourceReportPoller() { Stop(); }

void GcsResourceReportPoller::Start() {
  polling_thread_.reset(new std::thread{[this]() {
    SetThreadName("resource_report_poller");
    boost::asio::io_service::work work(polling_service_);

    polling_service_.run();
    RAY_LOG(DEBUG) << "GCSResourceReportPoller has stopped. This should only happen if "
                      "the cluster has stopped";
  }});
}

void GcsResourceReportPoller::Stop() {
  polling_service_.stop();
  if (polling_thread_->joinable()) {
    polling_thread_->join();
  }
}

void GcsResourceReportPoller::HandleNodeAdded(
    std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  absl::MutexLock guard(&mutex_);
  const auto node_id = NodeID::FromBinary(node_info->node_id());

  RAY_CHECK(!nodes_.count(node_id)) << "Node with id: " << node_id << " was added twice!";

  auto &state = nodes_[node_id];
  state.node_id = node_id;

  state.address.set_raylet_id(node_info->node_id());
  state.address.set_ip_address(node_info->node_manager_address());
  state.address.set_port(node_info->node_manager_port());

  state.last_pull_time = absl::GetCurrentTimeNanos();

  state.next_pull_timer = std::unique_ptr<boost::asio::deadline_timer>(
      new boost::asio::deadline_timer(polling_service_));

  polling_service_.post([this, node_id]() { TryPullResourceReport(node_id); });
}

void GcsResourceReportPoller::HandleNodeRemoved(
    std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  NodeID node_id = NodeID::FromBinary(node_info->node_id());

  {
    absl::MutexLock guard(&mutex_);
    nodes_.erase(node_id);
  }
}

void GcsResourceReportPoller::TryPullResourceReport(const NodeID &node_id) {
  absl::MutexLock guard(&mutex_);

  to_pull_queue_.push_back(node_id);

  while (inflight_pulls_ < max_concurrent_pulls_ && !to_pull_queue_.empty()) {
    const NodeID &to_pull = to_pull_queue_.front();
    to_pull_queue_.pop_front();

    auto it = nodes_.find(to_pull);
    if (it == nodes_.end()) {
      RAY_LOG(DEBUG)
          << "Update finished, but node was already removed from the cluster. Ignoring.";
      continue;
    }
    auto &state = it->second;
    PullResourceReport(state);
  }
}

void GcsResourceReportPoller::PullResourceReport(PullState &state) {
  inflight_pulls_++;
  const auto &node_id = state.node_id;
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(state.address);
  raylet_client->RequestResourceReport(
      [&, node_id](const Status &status, const rpc::RequestResourceReportReply &reply) {
        if (status.ok()) {
          // TODO (Alex): This callback is always posted onto the main thread. Since most
          // of the work is in the callback we should move this callback's execution to
          // the polling thread. We will need to implement locking once we switch threads.
          gcs_resource_manager_->UpdateFromResourceReport(reply.resources());
          polling_service_.post([this, node_id] { NodeResourceReportReceived(node_id); });
        } else {
          RAY_LOG(INFO) << "Couldn't get resource request from raylet " << node_id << ": "
                        << status.ToString();
        }
      });
}

void GcsResourceReportPoller::NodeResourceReportReceived(const NodeID &node_id) {
  absl::MutexLock guard(&mutex_);
  inflight_pulls_--;
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    RAY_LOG(DEBUG)
        << "Update finished, but node was already removed from the cluster. Ignoring.";
    return;
  }

  auto &state = it->second;
  state.next_pull_timer->expires_from_now(poll_period_ms_);
  state.next_pull_timer->async_wait([&, node_id](const boost::system::error_code &error) {
    if (!error) {
      TryPullResourceReport(node_id);
    } else {
      RAY_LOG(INFO) << "GcsResourceReportPoller timer failed: " << error.message() << ".";
    }
  });
}

}  // namespace gcs
}  // namespace ray
