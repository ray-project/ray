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
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,

    std::function<void(const ray::gcs::NodeResourceInfoAccessor::ResourceMap &resources)>
        update_resources,
    std::function<void(const std::vector<std::string> &resource_names)> delete_resources)
    : cluster_resource_scheduler_(cluster_resource_scheduler),
      update_resources_(update_resources),
      delete_resources_(delete_resources) {}

bool NewPlacementGroupResourceManager::PrepareBundle(
    const BundleSpecification &bundle_spec) {
  auto iter = pg_bundles_.find(bundle_spec.BundleId());
  if (iter != pg_bundles_.end()) {
    if (iter->second->state_ == CommitState::COMMITTED) {
      // If the bundle state is already committed, it means that prepare request is just
      // stale.
      RAY_LOG(DEBUG) << "Duplicate prepare bundle request, skip it directly. This should "
                        "only happen when GCS restarts.";
      return true;
    } else {
      // If there was a bundle in prepare state, it already locked resources, we will
      // return bundle resources so that we can start from the prepare phase again.
      ReturnBundle(bundle_spec);
    }
  }

  auto resource_instances = std::make_shared<TaskResourceInstances>();
  bool allocated =
      cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
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

bool NewPlacementGroupResourceManager::PrepareBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs) {
  std::vector<std::shared_ptr<const BundleSpecification>> prepared_bundles;
  for (const auto &bundle_spec : bundle_specs) {
    if (PrepareBundle(*bundle_spec)) {
      prepared_bundles.emplace_back(bundle_spec);
    } else {
      // Terminate the preparation phase if any of bundle cannot be prepared.
      break;
    }
  }

  if (prepared_bundles.size() != bundle_specs.size()) {
    RAY_LOG(DEBUG) << "There are one or more bundles request resource failed, will "
                      "release the requested resources before.";
    for (const auto &bundle : prepared_bundles) {
      ReturnBundle(*bundle);
      // Erase from `bundle_spec_map_`.
      const auto &iter = bundle_spec_map_.find(bundle->BundleId());
      if (iter != bundle_spec_map_.end()) {
        bundle_spec_map_.erase(iter);
      }
    }
    return false;
  }
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
      RAY_LOG(DEBUG) << "Duplicate committ bundle request, skip it directly.";
      return;
    }
  }

  const auto &bundle_state = it->second;
  bundle_state->state_ = CommitState::COMMITTED;

  const auto &string_id_map = cluster_resource_scheduler_->GetStringIdMap();
  const auto &task_resource_instances = *bundle_state->resources_;

  const auto &resources = bundle_spec.GetFormattedResources();
  for (const auto &resource : resources) {
    const auto &resource_name = resource.first;
    const auto &original_resource_name = GetOriginalResourceName(resource_name);
    if (original_resource_name != kBundle_ResourceLabel) {
      const auto &instances =
          task_resource_instances.Get(original_resource_name, string_id_map);
      cluster_resource_scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
          resource_name, instances);
    } else {
      cluster_resource_scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
          resource_name, {resource.second});
    }
  }
  update_resources_(
      cluster_resource_scheduler_->GetLocalResourceManager().GetResourceTotals(
          /*resource_name_filter*/ resources));
}

void NewPlacementGroupResourceManager::CommitBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs) {
  for (const auto &bundle_spec : bundle_specs) {
    CommitBundle(*bundle_spec);
  }
}

void NewPlacementGroupResourceManager::ReturnBundle(
    const BundleSpecification &bundle_spec) {
  auto it = pg_bundles_.find(bundle_spec.BundleId());
  if (it == pg_bundles_.end()) {
    RAY_LOG(DEBUG) << "Duplicate cancel request, skip it directly.";
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
  cluster_resource_scheduler_->GetLocalResourceManager().ReleaseWorkerResources(
      original_resources);

  // Substract placement group resources from resource allocator
  // `ClusterResourceScheduler`.
  const auto &placement_group_resources = bundle_spec.GetFormattedResources();
  auto resource_instances = std::make_shared<TaskResourceInstances>();
  cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      placement_group_resources, resource_instances);

  std::vector<std::string> deleted;
  for (const auto &resource : placement_group_resources) {
    if (cluster_resource_scheduler_->GetLocalResourceManager().IsAvailableResourceEmpty(
            resource.first)) {
      RAY_LOG(DEBUG) << "Available bundle resource:[" << resource.first
                     << "] is empty, Will delete it from local resource";
      // Delete local resource if available resource is empty when return bundle, or there
      // will be resource leak.
      cluster_resource_scheduler_->GetLocalResourceManager().DeleteLocalResource(
          resource.first);
      deleted.push_back(resource.first);
    } else {
      RAY_LOG(DEBUG) << "Available bundle resource:[" << resource.first
                     << "] is not empty. Resources are not deleted from the local node.";
    }
  }
  pg_bundles_.erase(it);
  delete_resources_(deleted);
}

}  // namespace raylet
}  // namespace ray
