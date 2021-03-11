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

#include "ray/raylet/placement_group_resource_manager.h"

#include <cctype>
#include <fstream>
#include <memory>

namespace ray {

namespace raylet {

void PlacementGroupResourceManager::ReturnUnusedBundle(
    const std::unordered_set<BundleID, pair_hash> &in_use_bundles) {
  for (auto iter = bundle_spec_map_.begin(); iter != bundle_spec_map_.end();) {
    if (0 == in_use_bundles.count(iter->first)) {
      ReturnBundle(*iter->second);
      bundle_spec_map_.erase(iter++);
    } else {
      iter++;
    }
  }
}

NewPlacementGroupResourceManager::NewPlacementGroupResourceManager(
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_)
    : cluster_resource_scheduler_(cluster_resource_scheduler_) {}

bool NewPlacementGroupResourceManager::PrepareBundle(
    const BundleSpecification &bundle_spec) {
  auto iter = pg_bundles_.find(bundle_spec.BundleId());
  if (iter != pg_bundles_.end()) {
    if (iter->second->state_ == CommitState::COMMITTED) {
      // If the bundle state is already committed, it means that prepare request is just
      // stale.
      RAY_LOG(INFO) << "Duplicate prepare bundle request, skip it directly. This should "
                       "only happen when GCS restarts.";
      return true;
    } else {
      // If there was a bundle in prepare state, it already locked resources, we will
      // return bundle resources so that we can start from the prepare phase again.
      ReturnBundle(bundle_spec);
    }
  }

  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  bool allocated = cluster_resource_scheduler_->AllocateLocalTaskResources(
      bundle_spec.GetRequiredResources().GetResourceMap(), resource_instances);

  if (!allocated) {
    return false;
  }

  auto bundle_state =
      std::make_shared<BundleTransactionState>(CommitState::PREPARED, resource_instances);
  pg_bundles_[bundle_spec.BundleId()] = bundle_state;
  bundle_spec_map_.emplace(bundle_spec.BundleId(), std::make_shared<BundleSpecification>(
                                                       bundle_spec.GetMessage()));

  return true;
}

void NewPlacementGroupResourceManager::CommitBundle(
    const BundleSpecification &bundle_spec) {
  auto it = pg_bundles_.find(bundle_spec.BundleId());
  if (it == pg_bundles_.end()) {
    // We should only ever receive a commit for a non-existent placement group when a
    // placement group is created and removed in quick succession.
    RAY_LOG(DEBUG)
        << "Received a commit message for an unknown bundle. The bundle info is "
        << bundle_spec.DebugString();
    return;
  } else {
    // Ignore request If the bundle state is already committed.
    if (it->second->state_ == CommitState::COMMITTED) {
      RAY_LOG(INFO) << "Duplicate committ bundle request, skip it directly.";
      return;
    }
  }

  const auto &bundle_state = it->second;
  bundle_state->state_ = CommitState::COMMITTED;

  for (const auto &resource : bundle_spec.GetFormattedResources()) {
    cluster_resource_scheduler_->AddLocalResource(resource.first, resource.second);
  }
}

void NewPlacementGroupResourceManager::ReturnBundle(
    const BundleSpecification &bundle_spec) {
  auto it = pg_bundles_.find(bundle_spec.BundleId());
  if (it == pg_bundles_.end()) {
    RAY_LOG(INFO) << "Duplicate cancel request, skip it directly.";
    return;
  }
  const auto &bundle_state = it->second;
  if (bundle_state->state_ == CommitState::PREPARED) {
    // Commit bundle first so that we can remove the bundle with consistent
    // implementation.
    CommitBundle(bundle_spec);
  }

  // Return original resources to resource allocator `ClusterResourceScheduler`.
  auto original_resources = it->second->resources_;
  cluster_resource_scheduler_->ReleaseWorkerResources(original_resources);

  // Substract placement group resources from resource allocator
  // `ClusterResourceScheduler`.
  const auto &placement_group_resources = bundle_spec.GetFormattedResources();
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  cluster_resource_scheduler_->AllocateLocalTaskResources(placement_group_resources,
                                                          resource_instances);
  for (const auto &resource : placement_group_resources) {
    if (cluster_resource_scheduler_->IsAvailableResourceEmpty(resource.first)) {
      RAY_LOG(DEBUG) << "Available bundle resource:[" << resource.first
                     << "] is empty, Will delete it from local resource";
      // Delete local resource if available resource is empty when return bundle, or there
      // will be resource leak.
      cluster_resource_scheduler_->DeleteLocalResource(resource.first);
    }
  }
  pg_bundles_.erase(it);
}

}  // namespace raylet
}  // namespace ray
