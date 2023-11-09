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
#include "ray/common/id.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/virtual_cluster.h"
#include "ray/common/virtual_cluster_bundle_spec.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace raylet {

struct VirtualClusterBundleTransactionState {
  enum CommitState {
    /// Resources are prepared.
    PREPARED,
    /// Resources are COMMITTED.
    COMMITTED
  };

  VirtualClusterBundleTransactionState(CommitState state,
                                       std::shared_ptr<TaskResourceInstances> &resources)
      : state_(state), resources_(resources) {}
  CommitState state_;
  std::shared_ptr<TaskResourceInstances> resources_;
};

/// `VirtualClusterResourceManager` responsible for managing the resources that
/// about allocated for virtual cluster bundles.
class VirtualClusterResourceManager {
 public:
  /// Create a virtual cluster resource manager.
  ///
  /// \param cluster_resource_scheduler_: The resource allocator of scheduler.
  VirtualClusterResourceManager(
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler);

  /// Prepare a list of bundles. It is guaranteed that all bundles are atomically
  /// prepared.
  ///(e.g., if one of bundle cannot be prepared, all bundles are failed to be prepared)
  ///
  /// \param bundle_specs A set of bundles that waiting to be prepared.
  /// \return bool True if all bundles successfully reserved resources, otherwise false.
  virtual bool PrepareBundles(
      const std::vector<std::shared_ptr<const VirtualClusterBundleSpec>> &bundle_specs);

  /// Convert the required resources to virtual cluster resources(like CPU ->
  /// CPU_cluster_i). This is phase two of 2PC.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be commited.
  virtual void CommitBundles(
      const std::vector<std::shared_ptr<const VirtualClusterBundleSpec>> &bundle_specs);

  /// Return back all the bundle resource.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be returned.
  virtual void ReturnBundle(const VirtualClusterBundleSpec &bundle_spec);

  /// Return back all the bundle(which is unused) resource.
  ///
  /// \param bundle_spec A set of bundles which in use.
  void ReturnUnusedBundle(
      const std::unordered_set<VirtualClusterBundleID, pair_hash> &in_use_bundles);

  const std::shared_ptr<ClusterResourceScheduler> GetResourceScheduler() const {
    return cluster_resource_scheduler_;
  }

  virtual ~VirtualClusterResourceManager() {}

 private:
  void CommitBundle(const VirtualClusterBundleSpec &bundle_spec);

  bool PrepareBundle(const VirtualClusterBundleSpec &bundle_spec);

  /// Save `VirtualClusterBundleSpec` for cleaning leaked bundles after GCS restart.
  absl::flat_hash_map<VirtualClusterBundleID,
                      std::shared_ptr<VirtualClusterBundleSpec>,
                      pair_hash>
      bundle_spec_map_;

  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Tracking virtual cluster bundles and their states. This mapping is the source of
  /// truth for the scheduler.
  absl::flat_hash_map<VirtualClusterBundleID,
                      std::shared_ptr<VirtualClusterBundleTransactionState>,
                      pair_hash>
      vc_bundles_;
};

}  // namespace raylet
}  // end namespace ray
