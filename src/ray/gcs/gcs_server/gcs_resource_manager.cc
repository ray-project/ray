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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(const GcsNodeManager &gcs_node_manager) {
  gcs_node_manager.AddNodeResourceChangeListener(
      [this](const rpc::HeartbeatTableData &heartbeat) {
        auto node_id = NodeID::FromBinary(heartbeat.client_id());
        cluster_resources_[node_id] =
            ResourceSet(MapFromProtobuf(heartbeat.resources_available()));
      });
}

const absl::flat_hash_map<NodeID, ResourceSet> &GcsResourceManager::GetClusterResources()
    const {
  return cluster_resources_;
}

void GcsResourceManager::StartTransaction() {
  RAY_CHECK(!is_transaction_in_progress_);
  is_transaction_in_progress_ = true;
}

void GcsResourceManager::CommitTransaction() {
  RAY_CHECK(is_transaction_in_progress_);
  resource_changes_during_transaction_.clear();
  is_transaction_in_progress_ = false;
}

void GcsResourceManager::RollbackTransaction() {
  RAY_CHECK(is_transaction_in_progress_);
  for (const auto &resource_changes : resource_changes_during_transaction_) {
    for (const auto &resource : resource_changes) {
      cluster_resources_[node_id].AddResources(resource);
    }
  }
  resource_changes_during_transaction_.clear();
  is_transaction_in_progress_ = false;
}

void GcsResourceManager::AcquireResource(const NodeID &node_id,
                                         const ResourceSet &required_resources) {
  cluster_resources_[node_id].SubtractResourcesStrict(required_resources);
  if (is_transaction_in_progress_) {
    resource_changes_during_transaction_[node_id].push_back(required_resources);
  }
}

void GcsResourceManager::ReleaseResource(const NodeID &node_id,
                                         const ResourceSet &acquired_resources) {
  cluster_resources_[node_id].AddResources(required_resources);
}

}  // namespace gcs
}  // namespace ray
