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
    // The bundle has been prepared.
    // Invariant: resources_ is non empty, and holds the resources as requested by the
    // bundle.
    PREPARED,
    // The bundle has been committed.
    // Invariant: resources_ is non empty, and holds the resources as requested by the
    // bundle.
    COMMITTED
  };

  explicit VirtualClusterBundleTransactionState(
      const VirtualClusterBundleSpec &bundle_spec,
      std::shared_ptr<TaskResourceInstances> resources)
      : state_(PREPARED), bundle_spec_(bundle_spec), resources_(resources) {}
  CommitState state_;
  VirtualClusterBundleSpec bundle_spec_;
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

  /// Prepare a bundle.
  /// If this vc_id already exists in `vc_bundles_`, and
  /// - if the pre-existing vc is COMMITTED -> fast return true;
  /// - if the pre-existing vc is PREPARED -> return it first to re-prepare.
  ///
  /// WARNING: no checks on the resource amounts are done. If a 2nd request has a
  /// different amount of resources, it is IGNORED and the resources is kept as the old
  /// request.
  ///
  /// \param bundle_specs A set of bundles that waiting to be prepared.
  /// \return bool True if all bundles successfully reserved resources, otherwise false.
  virtual bool PrepareBundle(const VirtualClusterBundleSpec &bundle_spec, int64_t seqno);

  /// Convert the required resources to virtual cluster resources(like CPU ->
  /// CPU_cluster_i). This is phase two of 2PC.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be commited.
  void CommitBundle(VirtualClusterID vc_id, int64_t seqno);

  /// Return back all the bundle resource.
  /// Removes the added renamed resources, releases the original resources, and erases the
  /// entry in `vc_bundles_`.
  ///
  /// \param bundle_spec Specification of bundle whose resources will be returned.
  virtual void ReturnBundle(VirtualClusterID vc_id, int64_t seqno);

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

  /// Tracking virtual cluster bundles and their states. This mapping is the source of
  /// truth for the scheduler.
  absl::flat_hash_map<std::pair<VirtualClusterID, int64_t /*seqno*/>,
                      std::shared_ptr<VirtualClusterBundleTransactionState>>
      vc_bundles_;
};

}  // namespace raylet
}  // end namespace ray
