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
// limitations under the License

#include "ray/raylet/virtual_cluster_resource_manager.h"

#include <cctype>
#include <fstream>
#include <memory>

#include "ray/common/virtual_cluster_node_spec.h"

namespace ray {

namespace raylet {

VirtualClusterResourceManager::VirtualClusterResourceManager(
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler)
    : cluster_resource_scheduler_(cluster_resource_scheduler) {}

bool VirtualClusterResourceManager::PrepareBundle(
    const VirtualClusterNodesSpec &nodes_spec) {
  const auto &vc_id = nodes_spec.vc_id;
  auto iter = vcs_.find(vc_id);
  if (iter != vcs_.end()) {
    if (iter->second->state_ == VirtualClusterTransactionState::CommitState::COMMITTED) {
      // If the bundle state is already committed, it means that prepare request is just
      // stale.
      RAY_LOG(DEBUG) << "Duplicate prepare virtual cluster bundle request, skip it "
                        "directly. This should only happen when GCS restarts.";
      return true;
    } else {
      // If there was a bundle in prepare state, it already locked resources, we will
      // return bundle resources so that we can start from the prepare phase again.
      ReturnBundle(vc_id);
    }
  }

  if (cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining()) {
    return false;
  }

  if (!cluster_resource_scheduler_->GetLocalResourceManager().IsAvailable(
          cluster_resource_scheduler_->GetLocalResourceManager().GetLocalRayletID(),
          nodes_spec.GetRequiredResources())) {
    return false;
  }

  std::vector<std::shared_ptr<TaskResourceInstances>> resource_allocations;
  for (const auto &node_spec : nodes_spec.fixed_size_nodes) {
    auto resource_instances = std::make_shared<TaskResourceInstances>();
    bool allocated =
        cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
            cluster_resource_scheduler_->GetLocalResourceManager().GetLocalRayletID(),
            node_spec.GetRequiredResources(),
            resource_instances);
    RAY_CHECK(allocated);
    resource_allocations.emplace_back(resource_instances);
  }

  vcs_[vc_id] =
      std::make_shared<VirtualClusterTransactionState>(nodes_spec, resource_allocations);
  return true;
}

void VirtualClusterResourceManager::ReturnUnusedBundles(
    const std::unordered_set<VirtualClusterID> &in_use_bundles) {
  for (auto iter = vcs_.begin(); iter != vcs_.end();) {
    VirtualClusterID vc_id = iter->first;
    iter++;
    if (0 == in_use_bundles.count(vc_id)) {
      ReturnBundle(vc_id);
    }
  }
}

void VirtualClusterResourceManager::CommitBundle(VirtualClusterID vc_id) {
  auto it = vcs_.find(vc_id);
  if (it == vcs_.end()) {
    // We should only ever receive a commit for a non-existent virtual cluster when a
    // virtual cluster is created and removed in quick succession.
    RAY_LOG(DEBUG) << "Received a commit message for an unknown vc_id = " << vc_id;
    return;
  } else {
    // Ignore request If the bundle state is already committed.
    if (it->second->state_ == VirtualClusterTransactionState::CommitState::COMMITTED) {
      RAY_LOG(DEBUG) << "Duplicate commit vc bundle request, skip it directly.";
      return;
    }
  }

  const auto &bundle_state = it->second;
  bundle_state->state_ = VirtualClusterTransactionState::CommitState::COMMITTED;

  for (size_t i = 0; i < bundle_state->nodes_spec_.fixed_size_nodes.size(); ++i) {
    const auto &task_resource_instances = *bundle_state->resources_[i];
    auto &node_spec = bundle_state->nodes_spec_.fixed_size_nodes[i];
    NodeResourceInstances node;
    node.total = NodeResourceInstanceSet();
    for (const auto &resource_id : task_resource_instances.ResourceIds()) {
      node.total.Set(resource_id, task_resource_instances.Get(resource_id));
    }
    NodeID node_id = node_spec.GetNodeId();
    node.available = node.total;
    node.labels = MapFromProtobuf(node_spec.GetMessage().labels());
    node.labels[kLabelKeyNodeID] = node_id.Hex();
    node.labels[kLabelKeyVirtualClusterID] = vc_id.Hex();
    node.labels[kLabelKeyRayletID] =
        NodeID::FromBinary(cluster_resource_scheduler_->GetLocalResourceManager()
                               .GetLocalRayletID()
                               .Binary())
            .Hex();
    node.labels[kLabelKeyParentNodeID] =
        NodeID::FromBinary(cluster_resource_scheduler_->GetLocalResourceManager()
                               .GetLocalRayletID()
                               .Binary())
            .Hex();
    cluster_resource_scheduler_->GetLocalResourceManager().AddVirtualNode(
        scheduling::NodeID(node_id.Binary()), node);
  }
}

void VirtualClusterResourceManager::ReturnBundle(VirtualClusterID vc_id) {
  auto it = vcs_.find(vc_id);
  if (it == vcs_.end()) {
    RAY_LOG(DEBUG) << " VirtualClusterResourceManager::ReturnBundle vc_id not found";
    return;
  }
  const auto &bundle_state = it->second;
  if (bundle_state->state_ == VirtualClusterTransactionState::CommitState::PREPARED) {
    // Commit bundle first so that we can remove the bundle with consistent
    // implementation.
    CommitBundle(vc_id);
  }

  // Return original resources to resource allocator `ClusterResourceScheduler`.
  for (size_t i = 0; i < bundle_state->nodes_spec_.fixed_size_nodes.size(); ++i) {
    const auto &task_resource_instances = bundle_state->resources_[i];
    const auto &node_spec = bundle_state->nodes_spec_.fixed_size_nodes[i];
    cluster_resource_scheduler_->GetLocalResourceManager().ReleaseWorkerResources(
        cluster_resource_scheduler_->GetLocalResourceManager().GetLocalRayletID(),
        task_resource_instances);
    cluster_resource_scheduler_->GetLocalResourceManager().RemoveVirtualNode(
        scheduling::NodeID(node_spec.GetNodeId().Binary()));
  }

  // TODO: what if the tasks and actors in the VC has not yet been killed?
  vcs_.erase(it);
}

}  // namespace raylet
}  // namespace ray
