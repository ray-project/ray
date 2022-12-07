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

#include <grpcpp/grpcpp.h>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

class GcsHealthCheckManagerTest;

namespace ray {
namespace gcs {

/// GcsHealthCheckManager is used to track the healthiness of the nodes in the ray
/// cluster. The health check is done in pull based way, which means this module will send
/// health check to the raylets to see whether the raylet is healthy or not. If the raylet
/// is not healthy for certain times, the module will think the raylet is dead.
/// When the node is dead a callback passed in the constructor will be called and this
/// node will be removed from GcsHealthCheckManager. The node can be added into this class
/// later. Although the same node id is not supposed to be reused in ray cluster, this is
/// not enforced in this class.
/// TODO (iycheng): Move the GcsHealthCheckManager to ray/common.
class GcsHealthCheckManager {
 public:
  /// Constructor of GcsHealthCheckManager.
  ///
  /// \param io_service The thread where all operations in this class should run.
  /// \param on_node_death_callback The callback function when some node is marked as
  /// failure.
  /// \param initial_delay_ms The delay for the first health check.
  /// \param period_ms The interval between two health checks for the same node.
  /// \param failure_threshold The threshold before a node will be marked as dead due to
  /// health check failure.
  GcsHealthCheckManager(
      instrumented_io_context &io_service,
      std::function<void(const NodeID &)> on_node_death_callback,
      int64_t initial_delay_ms = RayConfig::instance().health_check_initial_delay_ms(),
      int64_t timeout_ms = RayConfig::instance().health_check_timeout_ms(),
      int64_t period_ms = RayConfig::instance().health_check_period_ms(),
      int64_t failure_threshold = RayConfig::instance().health_check_failure_threshold());

  ~GcsHealthCheckManager();

  /// Start to track the healthiness of a node.
  ///
  /// \param node_id The id of the node.
  /// \param channel The gRPC channel to the node.
  void AddNode(const NodeID &node_id, std::shared_ptr<grpc::Channel> channel);

  /// Stop tracking the healthiness of a node.
  ///
  /// \param node_id The id of the node to stop tracking.
  void RemoveNode(const NodeID &node_id);

  /// Return all the nodes monitored.
  ///
  /// \return A list of node id which are being monitored by this class.
  std::vector<NodeID> GetAllNodes() const;

 private:
  /// Fail a node when health check failed. It'll stop the health checking and
  /// call on_node_death_callback.
  ///
  /// \param node_id The id of the node.
  void FailNode(const NodeID &node_id);

  using Timer = boost::asio::deadline_timer;

  /// The context for the health check. It's to support unary call.
  /// It can be updated to support streaming call for efficiency.
  class HealthCheckContext {
   public:
    HealthCheckContext(GcsHealthCheckManager *manager,
                       std::shared_ptr<grpc::Channel> channel,
                       NodeID node_id)
        : manager_(manager),
          node_id_(node_id),
          stopped_(std::make_shared<bool>(false)),
          timer_(manager->io_service_),
          health_check_remaining_(manager->failure_threshold_) {
      request_.set_service(node_id.Hex());
      stub_ = grpc::health::v1::Health::NewStub(channel);
      timer_.expires_from_now(
          boost::posix_time::milliseconds(manager_->initial_delay_ms_));
      timer_.async_wait([this](auto ec) {
        if (ec != boost::asio::error::operation_aborted) {
          StartHealthCheck();
        }
      });
    }

    ~HealthCheckContext() {
      timer_.cancel();
      if (context_ != nullptr) {
        context_->TryCancel();
      }
      *stopped_ = true;
    }

   private:
    void StartHealthCheck();

    GcsHealthCheckManager *manager_;

    NodeID node_id_;

    // Whether the health check has stopped.
    std::shared_ptr<bool> stopped_;

    /// gRPC related fields
    std::unique_ptr<::grpc::health::v1::Health::Stub> stub_;

    // The context is used in the gRPC callback which is in another
    // thread, so we need it to be a shared_ptr.
    std::shared_ptr<grpc::ClientContext> context_;
    ::grpc::health::v1::HealthCheckRequest request_;
    ::grpc::health::v1::HealthCheckResponse response_;

    /// The timer is used to do async wait before the next try.
    Timer timer_;

    /// The remaining check left. If it reaches 0, the node will be marked as dead.
    int64_t health_check_remaining_;
  };

  /// The main service. All method needs to run on this thread.
  instrumented_io_context &io_service_;

  /// Callback when the node failed.
  std::function<void(const NodeID &)> on_node_death_callback_;

  /// The context of the health check for each nodes.
  absl::flat_hash_map<NodeID, std::unique_ptr<HealthCheckContext>> health_check_contexts_;

  /// The delay for the first health check request.
  const int64_t initial_delay_ms_;
  /// Timeout for each health check request.
  const int64_t timeout_ms_;
  /// Intervals between two health check.
  const int64_t period_ms_;
  /// The number of failures before the node is considered as dead.
  const int64_t failure_threshold_;
};

}  // namespace gcs
}  // namespace ray
