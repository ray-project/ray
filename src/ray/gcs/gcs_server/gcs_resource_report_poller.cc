// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"

namespace ray {
namespace gcs {

GcsResourceReportPoller::GcsResourceReportPoller(
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    std::function<void(const rpc::ResourcesData &)> handle_resource_report,
    std::function<int64_t(void)> get_current_time_milli,
    std::function<void(
        const rpc::Address &,
        std::shared_ptr<rpc::NodeManagerClientPool> &,
        std::function<void(const Status &, const rpc::RequestResourceReportReply &)>)>
        request_report)
    : ticker_(polling_service_),
      max_concurrent_pulls_(RayConfig::instance().gcs_max_concurrent_resource_pulls()),
      inflight_pulls_(0),
      raylet_client_pool_(raylet_client_pool),
      handle_resource_report_(handle_resource_report),
      get_current_time_milli_(get_current_time_milli),
      request_report_(request_report),
      poll_period_ms_(RayConfig::instance().gcs_resource_report_poll_period_ms()) {}

GcsResourceReportPoller::~GcsResourceReportPoller() { Stop(); }

void GcsResourceReportPoller::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &pair : gcs_init_data.Nodes()) {
    HandleNodeAdded(pair.second);
  }
}

void GcsResourceReportPoller::Start() {
  polling_thread_.reset(new std::thread{[this]() {
    SetThreadName("resource_poller");
    boost::asio::io_service::work work(polling_service_);

    polling_service_.run();
    RAY_LOG(DEBUG) << "GCSResourceReportPoller has stopped. This should only happen if "
                      "the cluster has stopped";
  }});
  ticker_.RunFnPeriodically(
      [this] { TryPullResourceReport(); },
      10,
      "GcsResourceReportPoller.deadline_timer.pull_resource_report");
}

void GcsResourceReportPoller::Stop() {
  if (polling_thread_ != nullptr) {
    // TODO (Alex): There's technically a race condition here if we start and stop the
    // thread in rapid succession.
    polling_service_.stop();
    if (polling_thread_->joinable()) {
      polling_thread_->join();
    }
  }
}

void GcsResourceReportPoller::HandleNodeAdded(const rpc::GcsNodeInfo &node_info) {
  absl::MutexLock guard(&mutex_);

  rpc::Address address;
  address.set_raylet_id(node_info.node_id());
  address.set_ip_address(node_info.node_manager_address());
  address.set_port(node_info.node_manager_port());

  auto state = std::make_shared<PullState>(NodeID::FromBinary(node_info.node_id()),
                                           std::move(address),
                                           -1,
                                           get_current_time_milli_());

  const auto &node_id = state->node_id;

  RAY_CHECK(!nodes_.count(node_id)) << "Node with id: " << node_id << " was added twice!";

  nodes_[node_id] = state;
  to_pull_queue_.push_front(state);
  RAY_LOG(DEBUG) << "Node was added with id: " << node_id;

  polling_service_.post([this]() { TryPullResourceReport(); },
                        "GcsResourceReportPoller.TryPullResourceReport");
}

void GcsResourceReportPoller::HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  {
    absl::MutexLock guard(&mutex_);
    nodes_.erase(node_id);
    RAY_CHECK(!nodes_.count(node_id));
    RAY_LOG(DEBUG) << "Node removed (node_id: " << node_id
                   << ")# of remaining nodes: " << nodes_.size();
  }
}

void GcsResourceReportPoller::TryPullResourceReport() {
  absl::MutexLock guard(&mutex_);
  int64_t cur_time = get_current_time_milli_();

  while (inflight_pulls_ < max_concurrent_pulls_ && !to_pull_queue_.empty()) {
    auto to_pull = to_pull_queue_.front();
    if (cur_time < to_pull->next_pull_time) {
      break;
    }

    to_pull_queue_.pop_front();

    if (!nodes_.count(to_pull->node_id)) {
      RAY_LOG(DEBUG)
          << "Update finished, but node was already removed from the cluster. Ignoring.";
      continue;
    }

    PullResourceReport(to_pull);
  }
}

void GcsResourceReportPoller::PullResourceReport(const std::shared_ptr<PullState> state) {
  inflight_pulls_++;

  request_report_(
      state->address,
      raylet_client_pool_,
      [this, state](const Status &status, const rpc::RequestResourceReportReply &reply) {
        if (status.ok()) {
          // TODO (Alex): This callback is always posted onto the main thread. Since most
          // of the work is in the callback we should move this callback's execution to
          // the polling thread. We will need to implement locking once we switch threads.
          handle_resource_report_(reply.resources());
        } else {
          RAY_LOG(DEBUG) << "Couldn't get resource request from raylet " << state->node_id
                         << ": " << status.ToString();
        }
        polling_service_.post([this, state]() { NodeResourceReportReceived(state); },
                              "GcsResourceReportPoller.PullResourceReport");
      });
}

void GcsResourceReportPoller::NodeResourceReportReceived(
    const std::shared_ptr<PullState> state) {
  absl::MutexLock guard(&mutex_);
  inflight_pulls_--;

  // Schedule the next pull. The scheduling `TryPullResourceReport` loop will handle
  // validating that this node is still in the cluster.
  state->next_pull_time = get_current_time_milli_() + poll_period_ms_;
  to_pull_queue_.push_back(state);

  polling_service_.post([this] { TryPullResourceReport(); },
                        "GcsResourceReportPoller.TryPullResourceReport");
}

}  // namespace gcs
}  // namespace ray
