
// Copyright 2017 The Ray Authors.
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
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsHeartbeatManager is responsible for monitoring nodes liveness as well as
/// handing heartbeat rpc requests. This class is not thread-safe.
class GcsHeartbeatManager : public rpc::HeartbeatInfoHandler {
 public:
  /// Create a GcsHeartbeatManager.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param on_node_death_callback Callback that will be called when node death is
  /// detected.
  explicit GcsHeartbeatManager(
      instrumented_io_context &io_service,
      std::function<void(const NodeID &)> on_node_death_callback);

  /// Handle heartbeat rpc come from raylet.
  void HandleReportHeartbeat(const rpc::ReportHeartbeatRequest &request,
                             rpc::ReportHeartbeatReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle check alive request for GCS.
  void HandleCheckAlive(const rpc::CheckAliveRequest &request,
                        rpc::CheckAliveReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Start node failure detect loop.
  void Start();

  /// Stop node failure detect loop.
  void Stop();

  /// Register node to this detector.
  /// Only if the node has registered, its heartbeat data will be accepted.
  ///
  /// \param node_id ID of the node to be registered.
  void AddNode(const NodeID &node_id);

 protected:
  /// Check that if any raylet is inactive due to no heartbeat for a period of time.
  /// If found any, mark it as dead.
  void DetectDeadNodes();

 private:
  /// The main event loop for node failure detector.
  instrumented_io_context &io_service_;
  std::unique_ptr<std::thread> io_service_thread_;
  /// The callback of node death.
  std::function<void(const NodeID &)> on_node_death_callback_;
  /// The number of heartbeats that can be missed before a node is removed.
  int64_t num_heartbeats_timeout_;
  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;
  /// For each Raylet that we receive a heartbeat from, the number of ticks
  /// that may pass before the Raylet will be declared dead.
  absl::flat_hash_map<NodeID, int64_t> heartbeats_;
  /// Is the detect started.
  bool is_started_ = false;
};

}  // namespace gcs
}  // namespace ray
