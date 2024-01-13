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
#include "ray/common/virtual_cluster_node_spec.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace raylet {

struct VirtualClusterTransactionState {
  enum CommitState {
    // The bundle has been prepared.
    // Invariant: resources_ is non empty, and holds the resources as requested by the
    // bundle.
    PREPARED,
    // The bundle has been committed.
    // Invariant: resources_ is non empty, and holds the resources as requested by the
    // bundle.
    COMMITTED
  };

  explicit VirtualClusterTransactionState(
      const VirtualClusterNodesSpec &nodes_spec,
      std::shared_ptr<TaskResourceInstances> resources)
      : state_(PREPARED), nodes_spec_(nodes_spec), resources_(resources) {}
  CommitState state_;
  VirtualClusterNodesSpec nodes_spec_;
  std::shared_ptr<TaskResourceInstances> resources_;
};

/// `VirtualClusterResourceManager` responsible for managing the resources that
/// about allocated for virtual cluster bundles.
///
/// Each Virtual Cluster may ask to prepare 1 bundle. Lifetime of a bundle:
///
/// - "PrepareBundle": (empty) -> (resources allocated)
/// - "CommitBundle": (resources allocated) -> (renamed resources added)
/// - "ReturnBundle": (renamed resources added) -> (empty)
///
/// TODO: later we will be able to adjust the bundle resources.
class VirtualClusterResourceManager {
 public:
  /// Create a virtual cluster resource manager.
  ///
  /// \param cluster_resource_scheduler_: The resource allocator of scheduler.
  VirtualClusterResourceManager(
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler);

  /// Prepare a virtual cluster.
  /// If this vc_id already exists in `vcs_`, and
  /// - if the pre-existing vc is COMMITTED -> fast return true;
  /// - if the pre-existing vc is PREPARED -> return it first to re-prepare.
  ///
  /// WARNING: no checks on the resource amounts are done. If a 2nd request has a
  /// different amount of resources, it is IGNORED and the resources is kept as the old
  /// request.
  ///
  /// \param nodes_spec A set of bundles that waiting to be prepared.
  /// \return bool True if all bundles successfully reserved resources, otherwise false.
  virtual bool PrepareBundle(const VirtualClusterNodesSpec &nodes_spec);

  /// Convert the required resources to virtual cluster resources(like CPU ->
  /// CPU_cluster_i). This is phase two of 2PC.
  void CommitBundle(VirtualClusterID vc_id);

  /// Return back all the bundle resource.
  /// Removes the added renamed resources, releases the original resources, and erases the
  /// entry in `vcs_`.
  virtual void ReturnBundle(VirtualClusterID vc_id);

  /// Return back all the bundle(which is unused) resource.
  ///
  /// \param bundle_spec A set of bundles which in use.
  void ReturnUnusedBundles(const std::unordered_set<VirtualClusterID> &in_use_bundles);

  const std::shared_ptr<ClusterResourceScheduler> GetResourceScheduler() const {
    return cluster_resource_scheduler_;
  }

  virtual ~VirtualClusterResourceManager() {}

 private:
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Tracking virtual clusters and their states. This mapping is the source of
  /// truth for the scheduler.
  absl::flat_hash_map<VirtualClusterID, std::shared_ptr<VirtualClusterTransactionState>>
      vcs_;
};

}  // namespace raylet
}  // end namespace ray
