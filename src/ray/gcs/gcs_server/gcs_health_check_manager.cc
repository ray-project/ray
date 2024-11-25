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

#include <string_view>

#include "ray/stats/metric.h"

DEFINE_stats(health_check_rpc_latency_ms,
             "Latency of rpc request for health check.",
             (),
             ({1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);

namespace ray::gcs {

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
  RAY_CHECK_GE(initial_delay_ms, 0);
  RAY_CHECK_GE(timeout_ms, 0);
  RAY_CHECK_GE(period_ms, 0);
  RAY_CHECK_GE(failure_threshold, 0);
}

GcsHealthCheckManager::~GcsHealthCheckManager() = default;

void GcsHealthCheckManager::RemoveNode(const NodeID &node_id) {
  io_service_.dispatch(
      [this, node_id]() {
        thread_checker_.IsOnSameThread();
        auto iter = health_check_contexts_.find(node_id);
        if (iter == health_check_contexts_.end()) {
          return;
        }
        iter->second->Stop();
        health_check_contexts_.erase(iter);
      },
      "GcsHealthCheckManager::RemoveNode");
}

void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING).WithField(node_id) << "Node is dead because the health check failed.";
  thread_checker_.IsOnSameThread();
  auto iter = health_check_contexts_.find(node_id);
  if (iter != health_check_contexts_.end()) {
    on_node_death_callback_(node_id);
    health_check_contexts_.erase(iter);
  }
}

std::vector<NodeID> GcsHealthCheckManager::GetAllNodes() const {
  thread_checker_.IsOnSameThread();
  std::vector<NodeID> nodes;
  nodes.reserve(health_check_contexts_.size());
  for (const auto &[node_id, _] : health_check_contexts_) {
    nodes.emplace_back(node_id);
  }
  return nodes;
}

void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;

  // Reset the context/request/response for the next request.
  context_.~ClientContext();
  new (&context_) grpc::ClientContext();
  response_.Clear();

  const auto now = absl::Now();
  const auto deadline = now + absl::Milliseconds(manager_->timeout_ms_);
  context_.set_deadline(absl::ToChronoTime(deadline));
  stub_->async()->Check(
      &context_, &request_, &response_, [this, start = now](::grpc::Status status) {
        // This callback is done in gRPC's thread pool.
        STATS_health_check_rpc_latency_ms.Record(
            absl::ToInt64Milliseconds(absl::Now() - start));
        manager_->io_service_.post(
            [this, status]() {
              if (stopped_) {
                delete this;
                return;
              }
              RAY_LOG(DEBUG) << "Health check status: "
                             << HealthCheckResponse_ServingStatus_Name(
                                    response_.status());

              if (status.ok() && response_.status() == HealthCheckResponse::SERVING) {
                // Health check passed.
                health_check_remaining_ = manager_->failure_threshold_;
              } else {
                --health_check_remaining_;
                RAY_LOG(WARNING)
                    << "Health check failed for node " << node_id_
                    << ", remaining checks " << health_check_remaining_ << ", status "
                    << status.error_code() << ", response status " << response_.status()
                    << ", status message " << status.error_message()
                    << ", status details " << status.error_details();
              }

              if (health_check_remaining_ == 0) {
                manager_->FailNode(node_id_);
                delete this;
              } else {
                // Do another health check.
                //
                // TODO(hjiang): Able to reduce a few health check based on know resource
                // usage communication between GCS and raylet.
                timer_.expires_from_now(
                    boost::posix_time::milliseconds(manager_->period_ms_));
                timer_.async_wait([this](auto) { StartHealthCheck(); });
              }
            },
            "HealthCheck");
      });
}

void GcsHealthCheckManager::HealthCheckContext::Stop() { stopped_ = true; }

void GcsHealthCheckManager::AddNode(const NodeID &node_id,
                                    std::shared_ptr<grpc::Channel> channel) {
  io_service_.dispatch(
      [this, channel = std::move(channel), node_id]() {
        thread_checker_.IsOnSameThread();
        auto context = new HealthCheckContext(this, channel, node_id);
        auto [_, is_new] = health_check_contexts_.emplace(node_id, context);
        RAY_CHECK(is_new);
      },
      "GcsHealthCheckManager::AddNode");
}

}  // namespace ray::gcs
