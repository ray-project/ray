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
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/placement_group.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet {

enum CommitState {
  /// Resources are prepared.
  PREPARED,
  /// Resources are COMMITTED.
  COMMITTED
};

struct BundleTransactionState {
  BundleTransactionState(CommitState state,
                         std::shared_ptr<TaskResourceInstances> &resources)
      : state_(state), resources_(resources) {}
  CommitState state_;
  std::shared_ptr<TaskResourceInstances> resources_;
};

/// `PlacementGroupResourceManager` responsible for managing the resources that
/// about allocated for placement group bundles.
class PlacementGroupResourceManager {
 public:
  /// Prepare a list of bundles. It is guaranteed that all bundles are atomically
  /// prepared.
  ///(e.g., if one of bundle cannot be prepared, all bundles are failed to be prepared)
  ///
  /// \param bundle_specs A set of bundles that waiting to be prepared.
  /// \return bool True if all bundles successfully reserved resources, otherwise false.
  virtual bool PrepareBundles(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs) = 0;

  /// Convert the required resources to placement group resources(like CPU ->
  /// CPU_group_i). This is phase two of 2PC.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be commited.
  virtual void CommitBundles(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs) = 0;

  /// Return back all the bundle resource.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be returned.
  virtual void ReturnBundle(const BundleSpecification &bundle_spec) = 0;

  /// Return back all the bundle(which is unused) resource.
  ///
  /// \param bundle_spec A set of bundles which in use.
  void ReturnUnusedBundle(const std::unordered_set<BundleID, pair_hash> &in_use_bundles);

  virtual ~PlacementGroupResourceManager() {}

 protected:
  /// Save `BundleSpecification` for cleaning leaked bundles after GCS restart.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleSpecification>, pair_hash>
      bundle_spec_map_;
};

/// Associated with new scheduler.
class NewPlacementGroupResourceManager : public PlacementGroupResourceManager {
 public:
  /// Create a new placement group resource manager.
  ///
  /// \param cluster_resource_scheduler_: The resource allocator of new scheduler.
  NewPlacementGroupResourceManager(
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler);

  virtual ~NewPlacementGroupResourceManager() = default;

  bool PrepareBundles(const std::vector<std::shared_ptr<const BundleSpecification>>
                          &bundle_specs) override;

  void CommitBundles(const std::vector<std::shared_ptr<const BundleSpecification>>
                         &bundle_specs) override;

  void ReturnBundle(const BundleSpecification &bundle_spec) override;

  const std::shared_ptr<ClusterResourceScheduler> GetResourceScheduler() const {
    return cluster_resource_scheduler_;
  }

 private:
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Tracking placement group bundles and their states. This mapping is the source of
  /// truth for the new scheduler.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleTransactionState>, pair_hash>
      pg_bundles_;

  /// Lock the required resources from local available resources. Note that this is phase
  /// one of 2PC, it will not convert placement group resource(like CPU -> CPU_group_i).
  ///
  /// \param bundle_spec Specification of a bundle whose resources will be prepared.
  /// \return bool True if the bundle successfully reserved resources, otherwise false.
  bool PrepareBundle(const BundleSpecification &bundle_spec);

  /// Convert the normal original resources that were locked in the preparation phase
  /// to the placement group customer resources.
  ///
  /// \param bundle_spec Specification of a bundle whose resources have been locked
  /// successfully before.
  void CommitBundle(const BundleSpecification &bundle_spec);
};

}  // namespace raylet
}  // end namespace ray
