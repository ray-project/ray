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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

namespace ray {
namespace gcs {
/*
  A health check manager for GCS.
 */
class GcsHealthCheckManager {
 public:
  GcsHealthCheckManager(
      instrumented_io_context &io_service,
      rpc::NodeManagerClientPool &client_pool,
      std::function<void(const NodeID &)> on_node_death_callback,
      int64_t initial_delay_ms = RayConfig::instance().health_check_initial_delay_ms(),
      int64_t timeout_ms = RayConfig::instance().health_check_timeout_ms(),
      int64_t period_ms = RayConfig::instance().health_check_period_ms(),
      int64_t failure_threshold = RayConfig::instance().health_check_failure_threshold());

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  void AddNode(const rpc::GcsNodeInfo &node_info);

  void RemoveNode(const NodeID &node_id);

 private:
  void FailNode(const NodeID &node_id);

  /// The context for the health check. It's to support unary call.
  /// It can be updated to support streaming call for efficiency.
  class HealthCheckContext {
   public:
    HealthCheckContext(GcsHealthCheckManager *_manager, NodeID node_id)
        : manager(_manager),
          node_id(node_id),
          timer(manager->io_service_),
          health_check_remaining(manager->failure_threshold_) {
      timer.expires_from_now(boost::posix_time::milliseconds(manager->initial_delay_ms_));
      timer.async_wait([this](auto ec) {
        if (ec) {
          StartHealthCheck();
        }
      });
    }

    void StopHealthCheck();

   private:
    void StartHealthCheck();
    GcsHealthCheckManager *manager;
    NodeID node_id;
    std::unique_ptr<::grpc::health::v1::Health::Stub> stub;
    std::unique_ptr<grpc::ClientContext> context;

    ::grpc::health::v1::HealthCheckRequest request;
    ::grpc::health::v1::HealthCheckResponse response;
    boost::asio::deadline_timer timer;

    /// The remaining check left. If it reaches 0, the node will be marked as dead.
    int64_t health_check_remaining;
  };

  instrumented_io_context &io_service_;
  rpc::NodeManagerClientPool &client_pool_;
  std::function<void(const NodeID &)> on_node_death_callback_;

  absl::flat_hash_map<NodeID, HealthCheckContext> inflight_health_checks_;

  const int64_t initial_delay_ms_;
  const int64_t timeout_ms_;
  const int64_t period_ms_;
  const int64_t failure_threshold_;
};

}  // namespace gcs
}  // namespace ray
