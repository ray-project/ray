// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_health_check_manager.h"

#include "ray/stats/metric.h"
DEFINE_stats(health_check_rpc_latency_ms,
             "Latency of rpc request for health check.",
             (),
             ({1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);

namespace ray {
namespace gcs {

GcsHealthCheckManager::GcsHealthCheckManager(
    instrumented_io_context &io_service,
    std::function<void(const NodeID &)> on_node_death_callback,
    int64_t initial_delay_ms,
    int64_t timeout_ms,
    int64_t period_ms,
    int64_t failure_threshold)
    : io_service_(io_service),
      on_node_death_callback_(on_node_death_callback),
      initial_delay_ms_(initial_delay_ms),
      timeout_ms_(timeout_ms),
      period_ms_(period_ms),
      failure_threshold_(failure_threshold) {
  RAY_CHECK(on_node_death_callback != nullptr);
  RAY_CHECK(initial_delay_ms >= 0);
  RAY_CHECK(timeout_ms >= 0);
  RAY_CHECK(period_ms >= 0);
  RAY_CHECK(failure_threshold >= 0);
}

GcsHealthCheckManager::~GcsHealthCheckManager() {}

void GcsHealthCheckManager::RemoveNode(const NodeID &node_id) {
  io_service_.dispatch(
      [this, node_id]() {
        auto iter = health_check_contexts_.find(node_id);
        if (iter == health_check_contexts_.end()) {
          return;
        }
        health_check_contexts_.erase(iter);
      },
      "GcsHealthCheckManager::RemoveNode");
}

void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING) << "Node " << node_id << " is dead because the health check failed.";
  on_node_death_callback_(node_id);
  health_check_contexts_.erase(node_id);
}

std::vector<NodeID> GcsHealthCheckManager::GetAllNodes() const {
  std::vector<NodeID> nodes;
  for (const auto &[node_id, _] : health_check_contexts_) {
    nodes.emplace_back(node_id);
  }
  return nodes;
}

void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;

  context_ = std::make_shared<grpc::ClientContext>();

  auto deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(manager_->timeout_ms_);
  context_->set_deadline(deadline);
  stub_->async()->Check(
      context_.get(),
      &request_,
      &response_,
      [this,
       stopped = this->stopped_,
       context = this->context_,
       now = absl::Now()](::grpc::Status status) {
        // This callback is done in gRPC's thread pool.
        STATS_health_check_rpc_latency_ms.Record(
            absl::ToInt64Milliseconds(absl::Now() - now));
        if (*stopped || status.error_code() == ::grpc::StatusCode::CANCELLED) {
          return;
        }
        manager_->io_service_.post(
            [this, stopped, status]() {
              // Stopped has to be read in the same thread where it's updated.
              if (*stopped) {
                return;
              }
              RAY_LOG(DEBUG) << "Health check status: " << int(response_.status());

              if (status.ok() && response_.status() == HealthCheckResponse::SERVING) {
                // Health check passed
                health_check_remaining_ = manager_->failure_threshold_;
              } else {
                --health_check_remaining_;
                RAY_LOG(WARNING) << "Health check failed for node " << node_id_
                                 << ", remaining checks " << health_check_remaining_;
              }

              if (health_check_remaining_ == 0) {
                manager_->io_service_.post([this, stopped]() {
                  if(*stopped) {
                    return;
                  }
                  manager_->FailNode(node_id_); },
                  "");
              } else {
                // Do another health check.
                timer_.expires_from_now(
                    boost::posix_time::milliseconds(manager_->period_ms_));
                timer_.async_wait([this, stopped](auto ec) {
                  // We need to check stopped here as well since cancel
                  // won't impact the queued tasks.
                  if (ec != boost::asio::error::operation_aborted && !*stopped) {
                    StartHealthCheck();
                  }
                });
              }
            },
            "HealthCheck");
      });
}

void GcsHealthCheckManager::AddNode(const NodeID &node_id,
                                    std::shared_ptr<grpc::Channel> channel) {
  io_service_.dispatch(
      [this, channel, node_id]() {
        RAY_CHECK(health_check_contexts_.count(node_id) == 0);
        auto context = std::make_unique<HealthCheckContext>(this, channel, node_id);
        health_check_contexts_.emplace(std::make_pair(node_id, std::move(context)));
      },
      "GcsHealthCheckManager::AddNode");
}

}  // namespace gcs
}  // namespace ray
