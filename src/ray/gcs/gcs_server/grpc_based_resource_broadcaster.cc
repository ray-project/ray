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

#include "ray/gcs/gcs_server/grpc_based_resource_broadcaster.h"

#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

GrpcBasedResourceBroadcaster::GrpcBasedResourceBroadcaster(
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    std::function<void(const rpc::Address &,
                       std::shared_ptr<rpc::NodeManagerClientPool> &,
                       std::string &,
                       const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
        send_batch

    )
    : seq_no_(absl::GetCurrentTimeNanos()),
      raylet_client_pool_(raylet_client_pool),
      send_batch_(send_batch) {}

GrpcBasedResourceBroadcaster::~GrpcBasedResourceBroadcaster() {}

void GrpcBasedResourceBroadcaster::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &pair : gcs_init_data.Nodes()) {
    HandleNodeAdded(pair.second);
  }
}

void GrpcBasedResourceBroadcaster::HandleNodeAdded(const rpc::GcsNodeInfo &node_info) {
  rpc::Address address;
  address.set_raylet_id(node_info.node_id());
  address.set_ip_address(node_info.node_manager_address());
  address.set_port(node_info.node_manager_port());

  NodeID node_id = NodeID::FromBinary(node_info.node_id());

  absl::MutexLock guard(&mutex_);
  nodes_[node_id] = std::move(address);
}

void GrpcBasedResourceBroadcaster::HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  {
    absl::MutexLock guard(&mutex_);
    nodes_.erase(node_id);
    RAY_LOG(DEBUG) << "Node removed (node_id: " << node_id
                   << ")# of remaining nodes: " << nodes_.size();
  }
}

std::string GrpcBasedResourceBroadcaster::DebugString() {
  size_t node_num = 0;
  {
    absl::MutexLock guard(&mutex_);
    node_num = nodes_.size();
  }
  return absl::StrCat("GrpcBasedResourceBroadcaster:\n- Tracked nodes: ", node_num);
}

void GrpcBasedResourceBroadcaster::SendBroadcast(rpc::ResourceUsageBroadcastData batch) {
  if (batch.batch_size() == 0) {
    return;
  }

  batch.set_seq_no(seq_no_++);

  // Serializing is relatively expensive on large batches, so we should only do it once.
  std::string serialized_batch = batch.SerializeAsString();
  stats::OutboundHeartbeatSizeKB.Record((double)(serialized_batch.size() / 1024.0));

  absl::MutexLock guard(&mutex_);
  for (const auto &pair : nodes_) {
    const auto &address = pair.second;
    double start_time = absl::GetCurrentTimeNanos();
    auto callback = [start_time](const Status &status,
                                 const rpc::UpdateResourceUsageReply &reply) {
      double end_time = absl::GetCurrentTimeNanos();
      double lapsed_time_ms = static_cast<double>(end_time - start_time) / 1e6;
      ray::stats::GcsUpdateResourceUsageTime.Record(lapsed_time_ms);
    };
    send_batch_(address, raylet_client_pool_, serialized_batch, callback);
  }
}

}  // namespace gcs
}  // namespace ray
