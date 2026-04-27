// Copyright 2025 The Ray Authors.
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

#include <cstdint>
#include <functional>
#include <limits>

#include "absl/time/clock.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"

namespace ray {
namespace gcs {

/// Cluster-wide coordinator for global GC. Handles best-effort RPCs from
/// Raylets and broadcasts a GlobalGC RPC to every alive Raylet, throttled to
/// at most one broadcast per `min_interval_ns`. Requests that arrive within
/// the throttling window are dropped silently (the reply is still OK).
class GlobalGCManager : public rpc::GlobalGCGcsServiceHandler {
 public:
  GlobalGCManager(
      GcsNodeManager &gcs_node_manager,
      rpc::RayletClientPool &raylet_client_pool,
      int64_t min_interval_ns,
      std::function<int64_t()> now_fn = []() { return absl::GetCurrentTimeNanos(); });

  void HandleTriggerGlobalGCBestEffort(
      rpc::TriggerGlobalGCBestEffortRequest request,
      rpc::TriggerGlobalGCBestEffortReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

 private:
  void BroadcastGlobalGC();

  GcsNodeManager &gcs_node_manager_;
  rpc::RayletClientPool &raylet_client_pool_;
  const int64_t min_interval_ns_;
  std::function<int64_t()> now_fn_;
  // Sentinel ensures the first request always passes the throttle check.
  int64_t last_broadcast_ns_ = std::numeric_limits<int64_t>::min() / 2;
};

}  // namespace gcs
}  // namespace ray
