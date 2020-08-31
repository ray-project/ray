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

typedef std::pair<PlacementGroupID, int64_t> BundleID;
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};
using ScheduleMap = std::unordered_map<BundleID, ClientID, pair_hash>;
using BundleLocations = absl::flat_hash_map<
    BundleID, std::pair<ClientID, std::shared_ptr<BundleSpecification>>, pair_hash>;

class GcsPlacementGroup;

class GcsPlacementGroupSchedulerInterface {
 public:
  /// Schedule unplaced bundles of the specified placement group.
  ///
  /// \param placement_group The placement group to be scheduled.
  /// \param failure_callback This function is called if the schedule is failed.
  /// \param success_callback This function is called if the schedule is successful.
  virtual void ScheduleUnplacedBundles(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> failure_callback,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> success_callback) = 0;

  /// Get bundles belong to the specified node.
  ///
  /// \param node_id ID of the dead node.
  /// \return The bundles belong to the dead node.
  virtual absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const ClientID &node_id) = 0;

  /// Destroy bundle resources from all nodes in the placement group.
  virtual void DestroyPlacementGroupBundleResourcesIfExists(
      const PlacementGroupID &placement_group_id) = 0;

  /// Mark the placement group schedule as cancelled. Cancelled bundles will be destroyed.
  virtual void MarkScheduleCancelled(const PlacementGroupID &placement_group_id) = 0;

  virtual ~GcsPlacementGroupSchedulerInterface() {}
};

/// ScheduleContext provides information that are needed for bundle scheduling decision.
class ScheduleContext {
 public:
  ScheduleContext(std::shared_ptr<absl::flat_hash_map<ClientID, int64_t>> node_to_bundles,
                  const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations,
                  const GcsNodeManager &node_manager)
      : node_to_bundles_(std::move(node_to_bundles)),
        bundle_locations_(bundle_locations),
        node_manager_(node_manager) {}

  // Key is node id, value is the number of bundles on the node.
  const std::shared_ptr<absl::flat_hash_map<ClientID, int64_t>> node_to_bundles_;
  // The locations of existing bundles for this placement group.
  const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations_;

  const GcsNodeManager &node_manager_;
};

class GcsScheduleStrategy {
 public:
  virtual ~GcsScheduleStrategy() {}
  virtual ScheduleMap Schedule(
      std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
      const std::unique_ptr<ScheduleContext> &context) = 0;
};

/// The `GcsPackStrategy` is that pack all bundles in one node as much as possible.
/// If one node does not have enough resources, we need to divide bundles to multiple
/// nodes.
class GcsPackStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const std::unique_ptr<ScheduleContext> &context) override;
};

/// The `GcsSpreadStrategy` is that spread all bundles in different nodes.
class GcsSpreadStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const std::unique_ptr<ScheduleContext> &context) override;
};

/// The `GcsStrictPackStrategy` is that all bundles must be scheduled to one node. If one
/// node does not have enough resources, it will fail to schedule.
class GcsStrictPackStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const std::unique_ptr<ScheduleContext> &context) override;
};

/// The `GcsStrictSpreadStrategy` is that spread all bundles in different nodes.
/// A node can only deploy one bundle.
/// If the node resource is insufficient, it will fail to schedule.
class GcsStrictSpreadStrategy : public GcsScheduleStrategy {
 public:
  ScheduleMap Schedule(std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
                       const std::unique_ptr<ScheduleContext> &context) override;
};

enum class LeasingState {
  // TODO(sang): Use prepare and commit instead for 2PC.
  /// The phase where lease requests haven't been returned.
  SCHEDULING,
  /// The phase where lease requests have returned
  ALL_RETURNED,
  /// Placement group has been removed, and this leasing is not valid.
  CANCELLED
};

/// A data structure that encapsulates information regarding bundle resource leasing
/// status.
class LeaseStatusTracker {
 public:
  LeaseStatusTracker(std::shared_ptr<GcsPlacementGroup> placement_group,
                     std::vector<std::shared_ptr<BundleSpecification>> &unplaced_bundles);
  ~LeaseStatusTracker() = default;

  bool MarkLeaseStarted(const ClientID &node_id,
                        std::shared_ptr<BundleSpecification> bundle);
  void MarkLeaseReturned(const ClientID &node_id,
                         std::shared_ptr<BundleSpecification> bundle,
                         const Status &status);
  bool IsAllLeaseRequestReturned() const;
  bool IsLeasingSucceed() const;
  const std::shared_ptr<GcsPlacementGroup> &GetPlacementGroup() const;
  const std::vector<std::shared_ptr<BundleSpecification>> &GetUnplacedBundles() const;
  const std::shared_ptr<BundleLocations> &GetBundleLocations() const;
  const LeasingState GetLeasingState() const;
  void MarkPlacementGroupScheduleCancelled();

 private:
  /// Method to update leasing states.
  ///
  /// \param leasing_state The state to update.
  /// \return True if succeeds to update. False otherwise.
  bool UpdateLeasingState(LeasingState leasing_state);
  /// Placement group of which this leasing context is associated with.
  std::shared_ptr<GcsPlacementGroup> placement_group_;
  /// Location of bundles that lease requests were sent.
  /// If schedule success, the decision will be set as schedule_map[bundles[pos]]
  /// else will be set ClientID::Nil().
  std::shared_ptr<BundleLocations> bundle_locations_;
  /// Number of lease requests that are returned.
  size_t returned_count_ = 0;
  /// The leasing stage. This is used to know the state of current leasing context.
  LeasingState leasing_state_ = LeasingState::SCHEDULING;
  /// Map from node ID to the set of bundles for whom we are trying to acquire a lease
  /// from that node. This is needed so that we can retry lease requests from the node
  /// until we receive a reply or the node is removed.
  /// TODO(sang): We don't currently handle retry.
  absl::flat_hash_map<ClientID, absl::flat_hash_set<BundleID>>
      node_to_bundles_when_leasing_;
  /// Unplaced bundle specification for this leasing context.
  std::vector<std::shared_ptr<BundleSpecification>> unplaced_bundles_;
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
  bool Erase(const ClientID &node_id);

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
      const ClientID &node_id);

  /// Update the index to contain new node information. Should be used only when new node
  /// is added to the cluster.
  ///
  /// \param alive_nodes map of alive nodes.
  void AddNodes(
      const absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>> &nodes);

 private:
  /// Map from node ID to the set of bundles. This is used to lookup bundles at each node
  /// when a node is dead.
  absl::flat_hash_map<ClientID, std::shared_ptr<BundleLocations>> node_to_leased_bundles_;

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
  GcsPlacementGroupScheduler(
      boost::asio::io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      const GcsNodeManager &gcs_node_manager,
      ReserveResourceClientFactoryFn lease_client_factory = nullptr);

  virtual ~GcsPlacementGroupScheduler() = default;

  /// Schedule unplaced bundles of the specified placement group.
  /// If there is no available nodes then the `schedule_failed_handler` will be
  /// triggered, otherwise the bundle in placement_group will be add into a queue and
  /// schedule all bundle by calling ReserveResourceFromNode().
  ///
  /// \param placement_group to be scheduled.
  /// \param failure_callback This function is called if the schedule is failed.
  /// \param success_callback This function is called if the schedule is successful.
  void ScheduleUnplacedBundles(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> failure_handler,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> success_handler) override;

  /// Destroy bundle resources from all nodes in the placement group.
  /// This doesn't do anything if bundles are already destroyed.
  ///
  /// \param placement_group_id The id of a placement group to destroy all bundle
  /// resources.
  void DestroyPlacementGroupBundleResourcesIfExists(
      const PlacementGroupID &placement_group_id) override;

  /// Mark the placement group schedule as cancelled.
  /// Cancelled bundles will be destroyed.
  /// \param placement_group_id The id of a placement group to mark that scheduling is
  /// cancelled.
  void MarkScheduleCancelled(const PlacementGroupID &placement_group_id) override;

  /// Get bundles belong to the specified node.
  ///
  /// \param node_id ID of the dead node.
  /// \return The bundles belong to the dead node.
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const ClientID &node_id) override;

 protected:
  /// Lease resource from the specified node for the specified bundle.
  void ReserveResourceFromNode(const std::shared_ptr<BundleSpecification> &bundle,
                               const std::shared_ptr<ray::rpc::GcsNodeInfo> &node,
                               const StatusCallback &callback);

  /// return resource for the specified node for the specified bundle.
  ///
  /// \param bundle A description of the bundle to return.
  /// \param node The node that the worker will be returned for.
  void CancelResourceReserve(const std::shared_ptr<BundleSpecification> &bundle_spec,
                             const std::shared_ptr<ray::rpc::GcsNodeInfo> &node);

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<ResourceReserveInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

  void OnAllBundleSchedulingRequestReturned(
      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
      const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
          &schedule_failure_handler,
      const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
          &schedule_success_handler);

  /// Generate schedule context.
  std::unique_ptr<ScheduleContext> GetScheduleContext(
      const PlacementGroupID &placement_group_id);

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

  /// A vector to store all the schedule strategy.
  std::vector<std::shared_ptr<GcsScheduleStrategy>> scheduler_strategies_;

  /// Index to lookup bundle locations of node or placement group.
  BundleLocationIndex bundle_location_index_;

  /// Set of placement group that have lease requests in flight to nodes.
  /// It is required to know if placement group has been removed or not.
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<LeaseStatusTracker>>
      placement_group_leasing_in_progress_;
};

}  // namespace gcs
}  // namespace ray
