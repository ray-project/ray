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
#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

namespace ray {

namespace raylet {

enum CommitState {
  /// Resources are prepared.
  PREPARED,
  /// Resources are COMMITTED.
  COMMITTED
};

struct BundleState {
  /// Leasing state for 2PC protocol.
  CommitState state;
  /// Resources that are acquired at preparation stage.
  ResourceIdSet acquired_resources;
};

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
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
  /// Lock the required resources from local available resources. Note that this is phase
  /// one of 2PC, it will not convert placement group resource(like CPU -> CPU_group_i).
  ///
  /// \param bundle_spec: Specification of bundle whose resources will be prepared.
  virtual bool PrepareBundle(const BundleSpecification &bundle_spec) = 0;

  /// Convert the required resources to placement group resources(like CPU ->
  /// CPU_group_i). This is phase two of 2PC.
  ///
  /// \param bundle_spec: Specification of bundle whose resources will be commited.
  virtual void CommitBundle(const BundleSpecification &bundle_spec) = 0;

  /// Return back all the bundle resource.
  ///
  /// \param bundle_spec: Specification of bundle whose resources will be returned.
  virtual void ReturnBundle(const BundleSpecification &bundle_spec) = 0;

  /// Return back all the bundle(which is unused) resource.
  ///
  /// \param bundle_spec: A set of bundles which in use.
  void ReturnUnusedBundle(const std::unordered_set<BundleID, pair_hash> &in_use_bundles);

  virtual ~PlacementGroupResourceManager() {}

 protected:
  /// Save `BundleSpecification` for cleaning leaked bundles after GCS restart.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleSpecification>, pair_hash>
      bundle_spec_map_;
};

/// Associated with old scheduler.
class OldPlacementGroupResourceManager : public PlacementGroupResourceManager {
 public:
  /// Create a old placement group resource manager.
  ///
  /// \param local_available_resources_: The resources (IDs specificed) that are currently
  /// available.
  /// \param cluster_resource_map_: The resources (without IDs specificed) that
  /// are currently available.
  /// \param self_node_id_: The related raylet with current
  /// placement group manager.
  OldPlacementGroupResourceManager(
      ResourceIdSet &local_available_resources_,
      std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map_,
      const NodeID &self_node_id_);

  virtual ~OldPlacementGroupResourceManager() = default;

  bool PrepareBundle(const BundleSpecification &bundle_spec);

  void CommitBundle(const BundleSpecification &bundle_spec);

  void ReturnBundle(const BundleSpecification &bundle_spec);

  /// Get all local available resource(IDs specificed).
  const ResourceIdSet &GetAllResourceIdSet() const { return local_available_resources_; };

  /// Get all local available resource(without IDs specificed).
  const SchedulingResources &GetAllResourceSetWithoutId() const {
    return cluster_resource_map_[self_node_id_];
  }

 private:
  /// The resources (and specific resource IDs) that are currently available.
  /// These two resource container is shared with `NodeManager`.
  ResourceIdSet &local_available_resources_;
  std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map_;

  /// Related raylet with current placement group manager.
  NodeID self_node_id_;

  /// This map represents the commit state of 2PC protocol for atomic placement group
  /// creation.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleState>, pair_hash>
      bundle_state_map_;
};

/// Associated with new scheduler.
class NewPlacementGroupResourceManager : public PlacementGroupResourceManager {
 public:
  /// Create a new placement group resource manager.
  ///
  /// \param cluster_resource_scheduler_: The resource allocator of new scheduler.
  NewPlacementGroupResourceManager(
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_);

  virtual ~NewPlacementGroupResourceManager() = default;

  bool PrepareBundle(const BundleSpecification &bundle_spec);

  void CommitBundle(const BundleSpecification &bundle_spec);

  void ReturnBundle(const BundleSpecification &bundle_spec);

  const std::shared_ptr<ClusterResourceScheduler> GetResourceScheduler() const {
    return cluster_resource_scheduler_;
  }

 private:
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Tracking placement group bundles and their states. This mapping is the source of
  /// truth for the new scheduler.
  std::unordered_map<BundleID, std::shared_ptr<BundleTransactionState>, pair_hash>
      pg_bundles_;
};

}  // namespace raylet
}  // end namespace ray
