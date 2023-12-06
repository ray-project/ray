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

#include "ray/common/virtual_cluster_bundle_spec.h"

namespace ray {

namespace raylet {

VirtualClusterResourceManager::VirtualClusterResourceManager(
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler)
    : cluster_resource_scheduler_(cluster_resource_scheduler) {}

bool VirtualClusterResourceManager::PrepareBundle(
    const VirtualClusterBundleSpec &bundle_spec, int64_t seqno) {
  const auto vc_id = bundle_spec.GetVirtualClusterId();
  auto iter = vc_bundles_.find({vc_id, seqno});
  if (iter != vc_bundles_.end()) {
    if (iter->second->state_ ==
        VirtualClusterBundleTransactionState::CommitState::COMMITTED) {
      // If the bundle state is already committed, it means that prepare request is just
      // stale.
      RAY_LOG(DEBUG) << "Duplicate prepare virtual cluster bundle request, skip it "
                        "directly. This should only happen when GCS restarts.";
      return true;
    } else {
      // If there was a bundle in prepare state, it already locked resources, we will
      // return bundle resources so that we can start from the prepare phase again.
      ReturnBundle(vc_id, seqno);
    }
  }

  if (cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining()) {
    return false;
  }

  auto resource_instances = std::make_shared<TaskResourceInstances>();
  bool allocated =
      cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
          bundle_spec.GetRequiredResources(), resource_instances);

  if (!allocated) {
    return false;
  }

  vc_bundles_[{vc_id, seqno}] = std::make_shared<VirtualClusterBundleTransactionState>(
      bundle_spec, resource_instances);
  return true;
}

void VirtualClusterResourceManager::ReturnUnusedBundles(
    const std::unordered_set<VirtualClusterID> &in_use_bundles) {
  for (auto iter = vc_bundles_.begin(); iter != vc_bundles_.end();) {
    auto vc_id_and_seqno = iter->first;
    iter++;
    if (0 == in_use_bundles.count(vc_id_and_seqno.first)) {
      ReturnBundle(vc_id_and_seqno.first, vc_id_and_seqno.second);
    }
  }
}

void VirtualClusterResourceManager::CommitBundle(VirtualClusterID vc_id, int64_t seqno) {
  auto it = vc_bundles_.find({vc_id, seqno});
  if (it == vc_bundles_.end()) {
    // We should only ever receive a commit for a non-existent virtual cluster when a
    // virtual cluster is created and removed in quick succession.
    RAY_LOG(DEBUG) << "Received a commit message for an unknown vc_id = " << vc_id;
    return;
  } else {
    // Ignore request If the bundle state is already committed.
    if (it->second->state_ ==
        VirtualClusterBundleTransactionState::CommitState::COMMITTED) {
      RAY_LOG(DEBUG) << "Duplicate commit vc bundle request, skip it directly.";
      return;
    }
  }

  const auto &bundle_state = it->second;
  bundle_state->state_ = VirtualClusterBundleTransactionState::CommitState::COMMITTED;

  const auto &task_resource_instances = *bundle_state->resources_;
  const auto &bundle_spec = bundle_state->bundle_spec_;

  // For each resource {"CPU": 2} allocated, add a {"CPU_vc_vchex": 2}. For the resource
  // "vcbundle" we did not allocate but we will add 1000 anyway. I feel we can write it
  // better by adding a ResourceSet of {"vcbundle":1000} in prepare time so we won't need
  // the `if` here.
  const auto &resources = bundle_spec.GetFormattedResources();
  for (const auto &resource : resources) {
    const std::string &resource_name = resource.first;
    auto label = VirtualClusterBundleResourceLabel::Parse(resource_name);
    RAY_CHECK(label.has_value());
    const std::string &original_resource_name = label->original_resource;
    if (original_resource_name != kVirtualClusterBundle_ResourceLabel) {
      const auto &instances =
          task_resource_instances.Get(ResourceID(original_resource_name));
      cluster_resource_scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
          scheduling::ResourceID{resource_name}, instances);
    } else {
      cluster_resource_scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
          scheduling::ResourceID{resource_name}, {resource.second});
    }
  }
}

void VirtualClusterResourceManager::ReturnBundle(VirtualClusterID vc_id, int64_t seqno) {
  if (seqno == -1) {
    for (const auto &[vc_id_and_seqno, _] : vc_bundles_) {
      if (vc_id_and_seqno.first == vc_id) {
        ReturnBundle(vc_id, seqno);
      }
    }
    return;
  }
  auto it = vc_bundles_.find({vc_id, seqno});
  if (it == vc_bundles_.end()) {
    RAY_LOG(DEBUG) << " VirtualClusterResourceManager::ReturnBundle vc_id not found";
    return;
  }
  const auto &bundle_state = it->second;
  if (bundle_state->state_ ==
      VirtualClusterBundleTransactionState::CommitState::PREPARED) {
    // Commit bundle first so that we can remove the bundle with consistent
    // implementation.
    CommitBundle(vc_id, seqno);
  }

  // Return original resources to resource allocator `ClusterResourceScheduler`.
  auto original_resources = it->second->resources_;
  cluster_resource_scheduler_->GetLocalResourceManager().ReleaseWorkerResources(
      original_resources);

  // Substract virtual cluster resources from resource allocator
  // `ClusterResourceScheduler`.
  const auto &bundle_spec = bundle_state->bundle_spec_;
  // TODO: what if the tasks and actors in the VC has not yet been killed?
  const auto &virtual_cluster_resources = bundle_spec.GetFormattedResources();

  auto resource_instances = std::make_shared<TaskResourceInstances>();
  cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      virtual_cluster_resources, resource_instances);

  for (const auto &resource : virtual_cluster_resources) {
    auto resource_id = scheduling::ResourceID{resource.first};
    if (cluster_resource_scheduler_->GetLocalResourceManager().IsAvailableResourceEmpty(
            resource_id)) {
      RAY_LOG(DEBUG) << "Available bundle resource:[" << resource.first
                     << "] is empty, Will delete it from local resource";
      // Delete local resource if available resource is empty when return bundle, or there
      // will be resource leak.
      cluster_resource_scheduler_->GetLocalResourceManager().DeleteLocalResource(
          resource_id);
    } else {
      RAY_LOG(DEBUG) << "Available bundle resource:[" << resource.first
                     << "] is not empty. Resources are not deleted from the local node.";
    }
  }
  vc_bundles_.erase(it);
}

}  // namespace raylet
}  // namespace ray
