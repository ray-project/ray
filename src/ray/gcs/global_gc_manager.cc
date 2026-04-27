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

#include "ray/gcs/global_gc_manager.h"

#include <utility>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

GlobalGCManager::GlobalGCManager(GcsNodeManager &gcs_node_manager,
                                 rpc::RayletClientPool &raylet_client_pool,
                                 int64_t min_interval_ns,
                                 std::function<int64_t()> now_fn)
    : gcs_node_manager_(gcs_node_manager),
      raylet_client_pool_(raylet_client_pool),
      min_interval_ns_(min_interval_ns),
      now_fn_(std::move(now_fn)) {}

void GlobalGCManager::HandleTriggerGlobalGCBestEffort(
    rpc::TriggerGlobalGCBestEffortRequest request,
    rpc::TriggerGlobalGCBestEffortReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const int64_t now = now_fn_();
  if (now - last_broadcast_ns_ >= min_interval_ns_) {
    last_broadcast_ns_ = now;
    BroadcastGlobalGC();
  } else {
    RAY_LOG(DEBUG) << "Dropping global GC request: within throttling window of "
                   << min_interval_ns_ << "ns since last broadcast.";
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void GlobalGCManager::BroadcastGlobalGC() {
  const auto alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  RAY_LOG(INFO) << "Broadcasting global GC to " << alive_nodes.size() << " Raylet(s).";
  for (const auto &[node_id, node_info] : alive_nodes) {
    auto address = rpc::RayletClientPool::GenerateRayletAddress(
        node_id, node_info->node_manager_address(), node_info->node_manager_port());
    auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(address);
    raylet_client->GlobalGC([](const Status &, const rpc::GlobalGCReply &) {});
  }
}

}  // namespace gcs
}  // namespace ray
