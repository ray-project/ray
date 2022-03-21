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
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/ray_syncer.h"
#include "ray/raylet/scheduling/policy/scheduling_context.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class GcsPlacementGroup;

using ClusterResourceScheduler = gcs::GcsResourceScheduler;
using ScheduleContext = raylet_scheduling_policy::BundleSchedulingContext;

using ReserveResourceClientFactoryFn =
    std::function<std::shared_ptr<ResourceReserveInterface>(const rpc::Address &address)>;

using PGSchedulingFailureCallback =
    std::function<void(std::shared_ptr<GcsPlacementGroup>, bool)>;
using PGSchedulingSuccessfulCallback =
    std::function<void(std::shared_ptr<GcsPlacementGroup>)>;

using ScheduleMap = absl::flat_hash_map<BundleID, NodeID, pair_hash>;
using ScheduleResult = std::pair<SchedulingResultStatus, ScheduleMap>;

class GcsPlacementGroupSchedulerInterface {
 public:
  /// Schedule unplaced bundles of the specified placement group.
  ///
  /// \param placement_group The placement group to be scheduled.
  /// \param failure_callback This function is called if the schedule is failed.
  /// \param success_callback This function is called if the schedule is successful.
  virtual void ScheduleUnplacedBundles(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      PGSchedulingFailureCallback failure_callback,
      PGSchedulingSuccessfulCallback success_callback) = 0;

  /// Get bundles belong to the specified node.
  ///
  /// \param node_id ID of the dead node.
  /// \return The bundles belong to the dead node.
  virtual absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) = 0;

  /// Destroy bundle resources from all nodes in the placement group.
  ///
  /// \param placement_group_id The id of the placement group to be destroyed.
  virtual void DestroyPlacementGroupBundleResourcesIfExists(
      const PlacementGroupID &placement_group_id) = 0;

  /// Mark the placement group scheduling is cancelled.
  /// This method will incur check failure if scheduling
  /// is not actually going on to guarantee strong consistency.
  ///
  /// \param placement_group_id The placement group id scheduling is in progress.
  virtual void MarkScheduleCancelled(const PlacementGroupID &placement_group_id) = 0;

  /// Notify raylets to release unused bundles.
  ///
  /// \param node_to_bundles Bundles used by each node.
  virtual void ReleaseUnusedBundles(
      const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles) = 0;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param node_to_bundles Bundles used by each node.
  virtual void Initialize(
      const absl::flat_hash_map<PlacementGroupID,
                                std::vector<std::shared_ptr<BundleSpecification>>>
          &group_to_bundles) = 0;

  virtual ~GcsPlacementGroupSchedulerInterface() {}
};

class GcsScheduleStrategy {
 public:
  virtual ~GcsScheduleStrategy() {}
  virtual ScheduleResult Schedule(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context,
      ClusterResourceScheduler &cluster_resource_scheduler) = 0;

 protected:
  /// Get required resources from bundles.
  ///
  /// \param bundles Bundles to be scheduled.
  /// \return Required resources.
  std::vector<ResourceRequest> GetRequiredResourcesFromBundles(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles);

  /// Generate `ScheduleResult` from bundles and nodes .
  ///
  /// \param bundles Bundles to be scheduled.
  /// \param selected_nodes selected_nodes to be scheduled.
  /// \param status Status of the scheduling result.
  /// \return The scheduling result from the required resource.
  ScheduleResult GenerateScheduleResult(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::vector<scheduling::NodeID> &selected_nodes,
      const SchedulingResultStatus &status);
};

/// The `GcsPackStrategy` is that pack all bundles in one node as much as possible.
/// If one node does not have enough resources, we need to divide bundles to multiple
/// nodes.
class GcsPackStrategy : public GcsScheduleStrategy {
 public:
  ScheduleResult Schedule(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context,
      ClusterResourceScheduler &cluster_resource_scheduler) override;
};

/// The `GcsSpreadStrategy` is that spread all bundles in different nodes.
class GcsSpreadStrategy : public GcsScheduleStrategy {
 public:
  ScheduleResult Schedule(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context,
      ClusterResourceScheduler &cluster_resource_scheduler) override;
};

/// The `GcsStrictPackStrategy` is that all bundles must be scheduled to one node. If one
/// node does not have enough resources, it will fail to schedule.
class GcsStrictPackStrategy : public GcsScheduleStrategy {
 public:
  ScheduleResult Schedule(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context,
      ClusterResourceScheduler &cluster_resource_scheduler) override;
};

/// The `GcsStrictSpreadStrategy` is that spread all bundles in different nodes.
/// A node can only deploy one bundle.
/// If the node resource is insufficient, it will fail to schedule.
class GcsStrictSpreadStrategy : public GcsScheduleStrategy {
 public:
  ScheduleResult Schedule(
      const std::vector<std::shared_ptr<const ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context,
      ClusterResourceScheduler &cluster_resource_scheduler) override;
};

enum class LeasingState {
  /// The first phase of 2PC. It means requests to nodes are sent to prepare resources.
  PREPARING,
  /// The second phase of 2PC. It means that all prepare requests succeed, and GCS is
  /// committing resources to each node.
  COMMITTING,
  /// Placement group has been removed, and this leasing is not valid.
  CANCELLED
};

/// A data structure that encapsulates information regarding bundle resource leasing
/// status.
class LeaseStatusTracker {
 public:
  LeaseStatusTracker(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      const std::vector<std::shared_ptr<const BundleSpecification>> &unplaced_bundles,
      const ScheduleMap &schedule_map);
  ~LeaseStatusTracker() = default;

  /// Indicate the tracker that prepare requests are sent to a specific node.
  ///
  /// \param node_id Id of a node where prepare request is sent.
  /// \param bundle Bundle specification the node is supposed to prepare.
  /// \return False if the prepare phase was already started. True otherwise.
  bool MarkPreparePhaseStarted(const NodeID &node_id,
                               const std::shared_ptr<const BundleSpecification> &bundle);

  /// Indicate the tracker that all prepare requests are returned.
  ///
  /// \param node_id Id of a node where prepare request is returned.
  /// \param bundle Bundle specification the node was supposed to schedule.
  /// \param status Status of the prepare response.
  /// \param void
  void MarkPrepareRequestReturned(
      const NodeID &node_id,
      const std::shared_ptr<const BundleSpecification> &bundle,
      const Status &status);

  /// Used to know if all prepare requests are returned.
  ///
  /// \return True if all prepare requests are returned. False otherwise.
  bool AllPrepareRequestsReturned() const;

  /// Used to know if the prepare phase succeed.
  ///
  /// \return True if all prepare requests were successful.
  bool AllPrepareRequestsSuccessful() const;

  /// Indicate the tracker that the commit request of a bundle from a node has returned.
  ///
  /// \param node_id Id of a node where commit request is returned.
  /// \param bundle Bundle specification the node was supposed to schedule.
  /// \param status Status of the returned commit request.
  void MarkCommitRequestReturned(const NodeID &node_id,
                                 const std::shared_ptr<const BundleSpecification> &bundle,
                                 const Status &status);

  /// Used to know if all commit requests are returend.
  ///
  /// \return True if all commit requests are returned. False otherwise.
  bool AllCommitRequestReturned() const;

  /// Used to know if the commit phase succeed.
  ///
  /// \return True if all commit requests were successful..
  bool AllCommitRequestsSuccessful() const;

  /// Return a placement group this status tracker is associated with.
  ///
  /// \return The placement group of this lease status tracker is tracking.
  const std::shared_ptr<GcsPlacementGroup> &GetPlacementGroup() const;

  /// Return bundles that should be scheduled.
  ///
  /// \return List of bundle specification that are supposed to be scheduled.
  [[nodiscard]] const std::vector<std::shared_ptr<const BundleSpecification>>
      &GetBundlesToSchedule() const;

  /// This method returns bundle locations that succeed to prepare resources.
  ///
  /// \return Location of bundles that succeed to prepare resources on a node.
  const std::shared_ptr<BundleLocations> &GetPreparedBundleLocations() const;

  /// This method returns bundle locations that failed to commit resources.
  ///
  /// \return Location of bundles that failed to commit resources on a node.
  const std::shared_ptr<BundleLocations> &GetUnCommittedBundleLocations() const;

  /// This method returns bundle locations that success to commit resources.
  ///
  /// \return Location of bundles that success to commit resources on a node.
  const std::shared_ptr<BundleLocations> &GetCommittedBundleLocations() const;

  /// This method returns bundle locations.
  ///
  /// \return Location of bundles.
  const std::shared_ptr<BundleLocations> &GetBundleLocations() const;

  /// Return the leasing state.
  ///
  /// \return Leasing state.
  const LeasingState GetLeasingState() const;

  /// Mark that this leasing is cancelled.
  void MarkPlacementGroupScheduleCancelled();

  /// Mark that the commit phase is started.
  /// There's no need to mark commit phase is done because in that case, we won't need the
  /// status tracker anymore.
  void MarkCommitPhaseStarted();

 private:
  /// Method to update leasing states.
  ///
  /// \param leasing_state The state to update.
  /// \return True if succeeds to update. False otherwise.
  bool UpdateLeasingState(LeasingState leasing_state);

  /// Placement group of which this leasing context is associated with.
  std::shared_ptr<GcsPlacementGroup> placement_group_;

  /// Location of bundles that prepare requests were sent.
  /// If prepare succeeds, the decision will be set as schedule_map[bundles[pos]]
  /// else will be set NodeID::Nil().
  std::shared_ptr<BundleLocations> preparing_bundle_locations_;

  /// Location of bundles grouped by node.
  absl::flat_hash_map<NodeID, std::vector<std::shared_ptr<const BundleSpecification>>>
      grouped_preparing_bundle_locations_;

  /// Number of prepare requests that are returned.
  size_t prepare_request_returned_count_ = 0;

  /// Number of commit requests that are returned.
  size_t commit_request_returned_count_ = 0;

  /// Location of bundles that commit requests failed.
  std::shared_ptr<BundleLocations> uncommitted_bundle_locations_;

  /// Location of bundles that committed requests success.
  std::shared_ptr<BundleLocations> committed_bundle_locations_;

  /// The leasing stage. This is used to know the state of current leasing context.
  LeasingState leasing_state_ = LeasingState::PREPARING;

  /// Map from node ID to the set of bundles for whom we are trying to acquire a lease
  /// from that node. This is needed so that we can retry lease requests from the node
  /// until we receive a reply or the node is removed.
  /// TODO(sang): We don't currently handle retry.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<BundleID>>
      node_to_bundles_when_preparing_;

  /// Bundles to schedule.
  std::vector<std::shared_ptr<const BundleSpecification>> bundles_to_schedule_;

  /// Location of bundles.
  std::shared_ptr<BundleLocations> bundle_locations_;
};

/// A data structure that helps fast bundle location lookup.
class BundleLocationIndex {
 public:
  BundleLocationIndex() {}
  ~BundleLocationIndex() = default;

  /// Add bundle locations to index.
  ///
  /// \param placement_group_id
  /// \param bundle_locations Bundle locations that will be associated with the placement
  /// group id.
  void AddBundleLocations(const PlacementGroupID &placement_group_id,
                          std::shared_ptr<BundleLocations> bundle_locations);

  /// Erase bundle locations associated with a given node id.
  ///
  /// \param node_id The id of node.
  /// \return True if succeed. False otherwise.
  bool Erase(const NodeID &node_id);

  /// Erase bundle locations associated with a given placement group id.
  ///
  /// \param placement_group_id Placement group id
  /// \return True if succeed. False otherwise.
  bool Erase(const PlacementGroupID &placement_group_id);

  /// Get BundleLocation of placement group id.
  ///
  /// \param placement_group_id Placement group id of this bundle locations.
  /// \return Bundle locations that are associated with a given placement group id.
  const absl::optional<std::shared_ptr<BundleLocations> const> GetBundleLocations(
      const PlacementGroupID &placement_group_id);

  /// Get BundleLocation of node id.
  ///
  /// \param node_id Node id of this bundle locations.
  /// \return Bundle locations that are associated with a given node id.
  const absl::optional<std::shared_ptr<BundleLocations> const> GetBundleLocationsOnNode(
      const NodeID &node_id);

  /// Update the index to contain new node information. Should be used only when new node
  /// is added to the cluster.
  ///
  /// \param alive_nodes map of alive nodes.
  void AddNodes(
      const absl::flat_hash_map<NodeID, std::shared_ptr<ray::rpc::GcsNodeInfo>> &nodes);

 private:
  /// Map from node ID to the set of bundles. This is used to lookup bundles at each node
  /// when a node is dead.
  absl::flat_hash_map<NodeID, std::shared_ptr<BundleLocations>> node_to_leased_bundles_;

  /// A map from placement group id to bundle locations.
  /// It is used to destroy bundles for the placement group.
  /// NOTE: It is a reverse index of `node_to_leased_bundles`.
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<BundleLocations>>
      placement_group_to_bundle_locations_;
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
  /// \param gcs_resource_manager The resource manager which is used when scheduling.
  /// \param cluster_resource_scheduler The resource scheduler which is used when
  /// scheduling. \param lease_client_factory Factory to create remote lease client.
  GcsPlacementGroupScheduler(
      instrumented_io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      const GcsNodeManager &gcs_node_manager,
      GcsResourceManager &gcs_resource_manager,
      ClusterResourceScheduler &cluster_resource_scheduler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      syncer::RaySyncer &ray_syncer);

  virtual ~GcsPlacementGroupScheduler() = default;

  /// Schedule unplaced bundles of the specified placement group.
  /// If there is no available nodes then the `schedule_failed_handler` will be
  /// triggered, otherwise the bundle in placement_group will be added into a queue and
  /// scheduled to all nodes.
  ///
  /// \param placement_group to be scheduled.
  /// \param failure_callback This function is called if the schedule is failed.
  /// \param success_callback This function is called if the schedule is successful.
  void ScheduleUnplacedBundles(std::shared_ptr<GcsPlacementGroup> placement_group,
                               PGSchedulingFailureCallback failure_handler,
                               PGSchedulingSuccessfulCallback success_handler) override;

  /// Destroy the actual bundle resources or locked resources (for 2PC)
  /// on all nodes associated with this placement group.
  /// The method is idempotent, meaning if all bundles are already cancelled,
  /// this method won't do anything.
  ///
  /// \param placement_group_id The id of a placement group to destroy all bundle
  /// or locked resources.
  void DestroyPlacementGroupBundleResourcesIfExists(
      const PlacementGroupID &placement_group_id) override;

  /// Mark the placement group scheduling is cancelled.
  /// This method will incur check failure if scheduling
  /// is not actually going on to guarantee strong consistency.
  ///
  /// \param placement_group_id The placement group id scheduling is in progress.
  void MarkScheduleCancelled(const PlacementGroupID &placement_group_id) override;

  /// Get bundles belong to the specified node.
  ///
  /// \param node_id ID of the dead node.
  /// \return The bundles belong to the dead node.
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) override;

  /// Notify raylets to release unused bundles.
  ///
  /// \param node_to_bundles Bundles used by each node.
  void ReleaseUnusedBundles(const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>>
                                &node_to_bundles) override;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param node_to_bundles Bundles used by each node.
  void Initialize(
      const absl::flat_hash_map<PlacementGroupID,
                                std::vector<std::shared_ptr<BundleSpecification>>>
          &group_to_bundles) override;

 protected:
  /// Send bundles PREPARE requests to a node. The PREPARE requests will lock resources
  /// on a node until COMMIT or CANCEL requests are sent to a node.
  /// NOTE: All of given bundles will be prepared on the same node. It is guaranteed that
  /// all of bundles are atomically prepared on a given node.
  ///
  /// \param bundles Bundles to be scheduled on a node.
  /// \param node A node to prepare resources for given bundles.
  /// \param callback
  void PrepareResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundles,
      const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
      const StatusCallback &callback);

  /// Send bundles COMMIT request to a node. This means the placement group creation
  /// is ready and GCS will commit resources on a given node.
  ///
  /// \param bundles Bundles to be scheduled on a node.
  /// \param node A node to commit resources for given bundles.
  /// \param callback
  void CommitResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundles,
      const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
      const StatusCallback callback);

  /// Cacnel prepared or committed resources from a node.
  /// Nodes will be in charge of tracking state of a bundle.
  /// This method is supposed to be idempotent.
  ///
  /// \param bundle A description of the bundle to return.
  /// \param node The node that the worker will be returned for.
  void CancelResourceReserve(
      const std::shared_ptr<const BundleSpecification> &bundle_spec,
      const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node);

  /// Get an existing lease client or connect a new one or connect a new one.
  std::shared_ptr<ResourceReserveInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

  /// Get an existing lease client for a given node.
  std::shared_ptr<ResourceReserveInterface> GetLeaseClientFromNode(
      const std::shared_ptr<ray::rpc::GcsNodeInfo> &node);

  /// Called when all prepare requests are returned from nodes.
  void OnAllBundlePrepareRequestReturned(
      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
      const PGSchedulingFailureCallback &schedule_failure_handler,
      const PGSchedulingSuccessfulCallback &schedule_success_handler);

  /// Called when all commit requests are returned from nodes.
  void OnAllBundleCommitRequestReturned(
      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
      const PGSchedulingFailureCallback &schedule_failure_handler,
      const PGSchedulingSuccessfulCallback &schedule_success_handler);

  /// Commit all bundles recorded in lease status tracker.
  void CommitAllBundles(const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
                        const PGSchedulingFailureCallback &schedule_failure_handler,
                        const PGSchedulingSuccessfulCallback &schedule_success_handler);

  /// Destroy the prepared bundle resources with this placement group.
  /// The method is idempotent, meaning if all bundles are already cancelled,
  /// this method won't do anything.
  ///
  /// \param placement_group_id The id of a placement group to destroy all prepared
  /// bundles.
  void DestroyPlacementGroupPreparedBundleResources(
      const PlacementGroupID &placement_group_id);

  /// Destroy the committed bundle resources with this placement group.
  /// The method is idempotent, meaning if all bundles are already cancelled,
  /// this method won't do anything.
  ///
  /// \param placement_group_id The id of a placement group to destroy all committed
  /// bundles.
  void DestroyPlacementGroupCommittedBundleResources(
      const PlacementGroupID &placement_group_id);

  /// Acquire the bundle resources from the cluster resources.
  void AcquireBundleResources(const std::shared_ptr<BundleLocations> &bundle_locations);

  /// Return the bundle resources to the cluster resources.
  void ReturnBundleResources(const std::shared_ptr<BundleLocations> &bundle_locations);

  /// Generate schedule context.
  std::unique_ptr<ScheduleContext> GetScheduleContext(
      const PlacementGroupID &placement_group_id);

  /// A timer that ticks every cancel resource failure milliseconds.
  boost::asio::deadline_timer return_timer_;

  /// Used to update placement group information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;

  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;

  /// Reference of ClusterResourceScheduler.
  ClusterResourceScheduler &cluster_resource_scheduler_;

  /// A vector to store all the schedule strategy.
  std::vector<std::shared_ptr<GcsScheduleStrategy>> scheduler_strategies_;

  /// Index to lookup committed bundle locations of node or placement group.
  BundleLocationIndex committed_bundle_location_index_;

  /// Set of placement group that have lease requests in flight to nodes.
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<LeaseStatusTracker>>
      placement_group_leasing_in_progress_;

  /// The cached raylet clients used to communicate with raylets.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;

  /// The nodes which are releasing unused bundles.
  absl::flat_hash_set<NodeID> nodes_of_releasing_unused_bundles_;

  /// The syncer of resource. This is used to report placement group updates.
  /// TODO (iycheng): Remove this one from pg once we finish the refactor
  syncer::RaySyncer &ray_syncer_;
};

}  // namespace gcs
}  // namespace ray
