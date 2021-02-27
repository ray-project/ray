#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"

namespace ray {
namespace gcs {

GcsResourceReportPoller::GcsResourceReportPoller(
    uint64_t max_concurrent_pulls,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool)
    : max_concurrent_pulls_(max_concurrent_pulls),
      gcs_resource_manager_(gcs_resource_manager),
      raylet_client_pool_(raylet_client_pool),
      poll_period_ms_(boost::posix_time::milliseconds(
          RayConfig::instance().gcs_resource_report_poll_period_ms())),
      poll_timer_(polling_service_) {}

void GcsResourceReportPoller::Start() {
  polling_thread_ = std::unique_ptr<std::thread>(new std::thread{[&]() {
    RAY_LOG(ERROR) << "Polling thread created!";
    boost::asio::io_service::work work(polling_service_);


    polling_service_.post([&]() { Tick(); });
    polling_service_.run();
    RAY_CHECK(false) << "The polling service should never stop.";
  }});
}

void GcsResourceReportPoller::HandleNodeAdded(
                                              std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  absl::MutexLock guard(&mutex_);
  const auto node_id = NodeID::FromBinary(node_info->node_id());
  RAY_LOG(ERROR) << "Node added, id=" << node_id;

  auto it = nodes_.find(node_id);
  RAY_CHECK(it == nodes_.end()) << "Node with id: " << node_id << " was added twice!";

  auto &state = it->second;
  state.node_id = node_id;

  state.address.set_raylet_id(node_info->node_id());
  state.address.set_ip_address(node_info->node_manager_address());
  state.address.set_port(node_info->node_manager_port());

  state.last_pull_time = absl::GetCurrentTimeNanos();

  // state.next_pull_timer = std::unique_ptr{poll_service_};
  state.next_pull_timer = std::unique_ptr<boost::asio::deadline_timer>(new boost::asio::deadline_timer(polling_service_));

  polling_service_.post([&, node_id] () {
                          PullResourceReport(node_id);
                        });
}

void GcsResourceReportPoller::PullResourceReport(const NodeID &node_id) {
  absl::MutexLock guard(&mutex_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    RAY_LOG(DEBUG) << "Update finished, but node was already removed from the cluster. Ignoring.";
    return;
  }
  auto &state = it->second;
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(state.address);
  raylet_client->RequestResourceReport(
                                        [this, node_id](const Status &status, const rpc::RequestResourceReportReply &reply) {
                                          // TODO (Alex): This callback is always posted onto the main thread. Since most
                                          // of the work is in the callback we should move this callback's execution to
                                          // the polling thread. We will need to implement locking once we switch threads.
                                          gcs_resource_manager_->UpdateFromResourceReport(reply.resources());
                                          polling_service_.post([this, node_id] {
                                                                  NodeResourceReportReceived(node_id);
                                                                });
                                        });
}

void GcsResourceReportPoller::NodeResourceReportReceived(const NodeID &node_id) {
  absl::MutexLock guard(&mutex_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    RAY_LOG(DEBUG) << "Update finished, but node was already removed from the cluster. Ignoring.";
    return;
  }

  auto &state = it->second;
  state.next_pull_timer->expires_from_now(poll_period_ms_);
  state.next_pull_timer->async_wait([&, node_id] (const boost::system::error_code &error){
                                     RAY_CHECK(!error) << "Timer failed for no apparent reason." << error.message();
                                     PullResourceReport(node_id);
                                   });
}









void GcsResourceReportPoller::Tick() {
  GetAllResourceUsage();

}

void GcsResourceReportPoller::GetAllResourceUsage() {
  {
    absl::MutexLock guard(&mutex_);
    for (const auto &pair : nodes_to_poll_) {
      poll_state_.to_pull.push_back(pair.second);
    }
  }
  LaunchPulls();
}

void GcsResourceReportPoller::LaunchPulls() {
  absl::MutexLock guard(&mutex_);
  while (!poll_state_.to_pull.empty() && poll_state_.inflight_pulls.size() < max_concurrent_pulls_) {
    rpc::Address &address = poll_state_.to_pull.back();
    NodeID node_id = NodeID::FromBinary(address.raylet_id());

    auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(address);
    raylet_client->RequestResourceReport(
                                         [this, node_id](const Status &status, const rpc::RequestResourceReportReply &reply) {
                                           // TODO (Alex): This callback is always posted onto the main thread. Since most
                                           // of the work is in the callback we should move this callback's execution to
                                           // the polling thread. We will need to implement locking once we switch threads.
                                           gcs_resource_manager_->UpdateFromResourceReport(reply.resources());
                                           RAY_LOG(ERROR) << "Posting continuation";
                                           polling_service_.post([this, node_id] {
                                                                   NodeResourceReportReceived(node_id);
                                                                 });
                                         });
    poll_state_.to_pull.pop_back();
    poll_state_.inflight_pulls.insert(node_id);
  }

  // Handle the edge case with 0 nodes.
  if (poll_state_.to_pull.empty() && poll_state_.inflight_pulls.empty()) {
    PullRoundDone();
  }
}

// void GcsResourceReportPoller::NodeResourceReportReceived(const NodeID &node_id) {
//   {
//     absl::MutexLock guard(&mutex_);
//     poll_state_.inflight_pulls.erase(node_id);
//   }
//   LaunchPulls();
// }

void GcsResourceReportPoller::PullRoundDone() {
  RAY_LOG(ERROR) << "Round finished.";
  // TODO (Alex): Should this be an open system or closed?
  auto delay = boost::posix_time::milliseconds(100);
  poll_timer_.expires_from_now(delay);
  poll_timer_.async_wait([&](const boost::system::error_code &error) {
                           RAY_CHECK(!error) << "Timer failed for no apparent reason." << error.message();
                           Tick();
                         });
}


// void GcsResourceReportPoller::HandleNodeAdded(
//     std::shared_ptr<rpc::GcsNodeInfo> node_info) {
//   RAY_LOG(ERROR) << "Node added";
//   NodeID node_id = NodeID::FromBinary(node_info->node_id());
//   rpc::Address address;
//   address.set_raylet_id(node_info->node_id());
//   address.set_ip_address(node_info->node_manager_address());
//   address.set_port(node_info->node_manager_port());

//   {
//     absl::MutexLock guard(&mutex_);
//     nodes_to_poll_[node_id] = address;
//   }
// }

void GcsResourceReportPoller::HandleNodeRemoved(
    std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  NodeID node_id = NodeID::FromBinary(node_info->node_id());

  {
    absl::MutexLock guard(&mutex_);
    nodes_to_poll_.erase(node_id);
  }
}

}  // namespace gcs
}  // namespace ray
