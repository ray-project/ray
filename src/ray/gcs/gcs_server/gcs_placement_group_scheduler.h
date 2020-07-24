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

#include <queue>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

using ReserveResourceClientFactoryFn =
    std::function<std::shared_ptr<ResourceReserveInterface>(const rpc::Address &address)>;

using ReserveResourceCallback =
    std::function<void(const Status &, const rpc::RequestResourceReserveReply &)>;

typedef std::pair<PlacementGroupID, int64_t> BundleID;
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};
using ScheduleMap = std::unordered_map<BundleID, ClientID, pair_hash>;
class GcsPlacementGroup;

class GcsPlacementGroupSchedulerInterface {
 public:
  /// Schedule the specified placement_group.
  ///
  /// \param placement_group to be scheduled.
  virtual void Schedule(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)>
          schedule_success_handler) = 0;

  virtual ~GcsPlacementGroupSchedulerInterface() {}
};

class GcsScheduleStrategy {
 public:
  virtual ~GcsScheduleStrategy() {}
  virtual ScheduleMap Schedule(
      std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
      const GcsNodeManager &node_manager) = 0;
};

class GcsPackStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const GcsNodeManager &node_manager) override;
};

class GcsSpreadStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const GcsNodeManager &node_manager) override;
};

/// GcsPlacementGroupScheduler is responsible for scheduling placement_groups registered
/// to GcsPlacementGroupManager. This class is not thread-safe.
class GcsPlacementGroupScheduler : public GcsPlacementGroupSchedulerInterface {
 public:
  /// Create a GcsPlacementGroupScheduler
  ///
  /// \param io_context The main event loop.
  /// \param placement_group_info_accessor Used to flush placement_group info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  GcsPlacementGroupScheduler(
      boost::asio::io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      const GcsNodeManager &gcs_node_manager,
      ReserveResourceClientFactoryFn lease_client_factory = nullptr);
  virtual ~GcsPlacementGroupScheduler() = default;
  /// Schedule the specified placement_group.
  /// If there is no available nodes then the `schedule_failed_handler` will be
  /// triggered, otherwise the bundle in placement_group will be add into a queue and
  /// schedule all bundle by calling ReserveResourceFromNode().
  ///
  /// \param placement_group to be scheduled.
  void Schedule(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler)
      override;

 protected:
  /// Lease resource from the specified node for the specified bundle.
  void ReserveResourceFromNode(std::shared_ptr<BundleSpecification> bundle,
                               std::shared_ptr<ray::rpc::GcsNodeInfo> node,
                               ReserveResourceCallback callback);

  /// return resource for the specified node for the specified bundle.
  ///
  /// \param bundle A description of the bundle to return.
  /// \param node The node that the worker will be returned for.
  void CancelResourceReserve(std::shared_ptr<BundleSpecification> bundle_spec,
                             std::shared_ptr<ray::rpc::GcsNodeInfo> node);

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<ResourceReserveInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);
  /// A timer that ticks every cancel resource failure milliseconds.
  boost::asio::deadline_timer return_timer_;
  /// Used to update placement group information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;
  /// The cached node clients which are used to communicate with raylet to lease workers.
  absl::flat_hash_map<ClientID, std::shared_ptr<ResourceReserveInterface>>
      remote_lease_clients_;
  /// Factory for producing new clients to request leases from remote nodes.
  ReserveResourceClientFactoryFn lease_client_factory_;

  /// Map from node ID to the set of bundles for whom we are trying to acquire a lease
  /// from that node. This is needed so that we can retry lease requests from the node
  /// until we receive a reply or the node is removed.
  absl::flat_hash_map<ClientID, absl::flat_hash_set<BundleID>>
      node_to_bundles_when_leasing_;
  /// A vector to store all the schedule strategy.
  std::vector<std::shared_ptr<GcsScheduleStrategy>> scheduler_strategies_;
};

}  // namespace gcs
}  // namespace ray
