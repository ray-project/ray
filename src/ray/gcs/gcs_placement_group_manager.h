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
#include <gtest/gtest_prod.h>

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_placement_group.h"
#include "ray/gcs/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/util/counter_map.h"
#include "ray/util/exponential_backoff.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsPlacementGroupManager is responsible for managing the lifecycle of all placement
/// group. This class is not thread-safe.
/// The placementGroup will be added into queue and set the status as pending first and
/// use SchedulePendingPlacementGroups(). The SchedulePendingPlacementGroups() will get
/// the head of the queue and schedule it. If schedule success, using the
/// SchedulePendingPlacementGroups() Immediately. else wait for a short time beforw using
/// SchedulePendingPlacementGroups() next time.
class GcsPlacementGroupManager : public rpc::PlacementGroupInfoGcsServiceHandler {
 public:
  /// Create a GcsPlacementGroupManager
  ///
  /// \param io_context The event loop to run the monitor on.
  /// \param scheduler Used to schedule placement group creation tasks.
  /// \param gcs_table_storage Used to flush placement group data to storage.
  /// \param gcs_resource_manager Reference of GcsResourceManager.
  /// \param get_ray_namespace A callback to get the ray namespace.
  GcsPlacementGroupManager(instrumented_io_context &io_context,
                           GcsPlacementGroupSchedulerInterface *scheduler,
                           gcs::GcsTableStorage *gcs_table_storage,
                           GcsResourceManager &gcs_resource_manager,
                           std::function<std::string(const JobID &)> get_ray_namespace);

  ~GcsPlacementGroupManager() override = default;

  void HandleCreatePlacementGroup(rpc::CreatePlacementGroupRequest request,
                                  rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemovePlacementGroup(rpc::RemovePlacementGroupRequest request,
                                  rpc::RemovePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetPlacementGroup(rpc::GetPlacementGroupRequest request,
                               rpc::GetPlacementGroupReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedPlacementGroup(rpc::GetNamedPlacementGroupRequest request,
                                    rpc::GetNamedPlacementGroupReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllPlacementGroup(rpc::GetAllPlacementGroupRequest request,
                                  rpc::GetAllPlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;
  void HandleWaitPlacementGroupUntilReady(
      rpc::WaitPlacementGroupUntilReadyRequest request,
      rpc::WaitPlacementGroupUntilReadyReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Register a callback which will be invoked after successfully created.
  ///
  /// \param placement_group_id The placement group id which we want to listen.
  /// \param callback Will be invoked after the placement group is created successfully or
  /// be invoked if the placement group is deleted before create successfully.
  void WaitPlacementGroup(const PlacementGroupID &placement_group_id,
                          StatusCallback callback);

  /// Register placement_group asynchronously.
  ///
  /// \param placement_group The placement group to be created.
  /// \param callback Will be invoked after the placement_group is created successfully or
  /// be invoked immediately if the placement_group is already registered to
  /// `registered_placement_groups_` and its state is `CREATED`. The callback will not be
  /// called in this case.
  void RegisterPlacementGroup(const std::shared_ptr<GcsPlacementGroup> &placement_group,
                              StatusCallback callback);

  /// Schedule placement_groups in the `pending_placement_groups_` queue.
  /// The method handles all states of placement groups
  /// (e.g., REMOVED states should be properly ignored within the method.)
  void SchedulePendingPlacementGroups();

  /// Get the placement_group ID for the named placement_group. Returns nil if the
  /// placement_group was not found.
  /// \param name The name of the  placement_group to look up.
  /// \returns PlacementGroupID The ID of the placement_group. Nil if the
  /// placement_group was not found.
  PlacementGroupID GetPlacementGroupIDByName(const std::string &name,
                                             const std::string &ray_namespace);

  /// Handle placement_group creation task failure. This should be called when scheduling
  /// an placement_group creation task is infeasible.
  ///
  /// \param placement_group The placement_group whose creation task is infeasible.
  /// \param is_feasible whether the scheduler can be retry or not currently.
  void OnPlacementGroupCreationFailed(std::shared_ptr<GcsPlacementGroup> placement_group,
                                      ExponentialBackoff backoff,
                                      bool is_feasible);

  /// Handle placement_group creation task success. This should be called when the
  /// placement_group creation task has been scheduled successfully.
  ///
  /// \param placement_group The placement_group that has been created.
  void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<GcsPlacementGroup> &placement_group);

  /// Remove the placement group of a given id.
  void RemovePlacementGroup(const PlacementGroupID &placement_group_id,
                            StatusCallback on_placement_group_removed);

  /// Handle a node death. This will reschedule all bundles associated with the
  /// specified node id.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Handle a node register. This will try to reschedule all the infeasible
  /// placement groups.
  ///
  /// \param node_id The specified node id.
  void OnNodeAdd(const NodeID &node_id);

  /// Get bundles on a node.
  ///
  /// \param node_id The specified node id.
  /// \return A map from placement group id to bundles indices on the node.
  virtual absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) const;

  /// Clean placement group that belongs to the job id if necessary.
  ///
  /// This interface is a part of automatic lifecycle management for placement groups.
  /// When a job is killed, this method should be invoked to clean up
  /// placement groups that belong to the given job.
  ///
  /// Calling this method doesn't mean placement groups that belong to the given job
  /// will be cleaned. Placement groups are cleaned only when the creator job AND actor
  /// are both dead.
  ///
  /// NOTE: This method is idempotent.
  ///
  /// \param job_id The job id where placement groups that need to be cleaned belong to.
  void CleanPlacementGroupIfNeededWhenJobDead(const JobID &job_id);

  /// Clean placement group that belongs to the actor id if necessary.
  ///
  /// This interface is a part of automatic lifecycle management for placement groups.
  /// When an actor is killed, this method should be invoked to clean up
  /// placement groups that belong to the given actor.
  ///
  /// Calling this method doesn't mean placement groups that belong to the given actor
  /// will be cleaned. Placement groups are cleaned only when the creator job AND actor
  /// are both dead.
  ///
  /// NOTE: This method is idempotent.
  ///
  /// \param actor_id The actor id where placement groups that need to be cleaned belong
  /// to.
  void CleanPlacementGroupIfNeededWhenActorDead(const ActorID &actor_id);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string DebugString() const;

  /// Record internal metrics of the placement group manager.
  void RecordMetrics() const;

  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) {
    usage_stats_client_ = usage_stats_client;
  }

  /// Get the placement group load information.
  ///
  /// The API guarantees the returned placement groups' states
  /// are either PENDING or RESCHEDULING.
  ///
  /// \return Placement group load information. Users should check if
  /// the returned rpc has any placement_group_data.
  virtual std::shared_ptr<rpc::PlacementGroupLoad> GetPlacementGroupLoad() const;

 protected:
  /// For testing/mocking only.
  explicit GcsPlacementGroupManager(instrumented_io_context &io_context,
                                    GcsResourceManager &gcs_resource_manager);

 private:
  /// Push a placement group to pending queue.
  ///
  /// \param pg The placementgroup we are adding
  /// \param rank The rank for this placement group. Semantically it's the time
  /// this placement group to be scheduled. By default it'll be assigned to be
  /// the current time. If you assign 0, it means it will be scheduled as a highest
  /// priority.
  /// \param exp_backer The exponential backoff. A default one will be given if
  /// it's not set. This will be used to generate the deferred time for this pg.
  void AddToPendingQueue(std::shared_ptr<GcsPlacementGroup> pg,
                         std::optional<int64_t> rank = std::nullopt,
                         std::optional<ExponentialBackoff> exp_backer = std::nullopt);
  void RemoveFromPendingQueue(const PlacementGroupID &pg_id);

  /// Try to create placement group after a short time.
  void RetryCreatingPlacementGroup();

  /// Mark the manager that there's a placement group scheduling going on.
  void MarkSchedulingStarted(const PlacementGroupID placement_group_id) {
    scheduling_in_progress_id_ = placement_group_id;
  }

  /// Mark the manager that there's no more placement group scheduling going on.
  void MarkSchedulingDone() { scheduling_in_progress_id_ = PlacementGroupID::Nil(); }

  /// Check if the placement group of a given id is scheduling.
  bool IsSchedulingInProgress(const PlacementGroupID &placement_group_id) const {
    return scheduling_in_progress_id_ == placement_group_id;
  }

  /// Check if there's any placement group scheduling going on.
  bool IsSchedulingInProgress() const {
    return scheduling_in_progress_id_ != PlacementGroupID::Nil();
  }

  // Method that is invoked every second.
  void Tick();

  // Update placement group load information so that the autoscaler can use it.
  void UpdatePlacementGroupLoad();

  /// Check if this placement group is waiting for scheduling.
  bool IsInPendingQueue(const PlacementGroupID &placement_group_id) const;

  /// Reschedule this placement group if it still has unplaced bundles.
  bool RescheduleIfStillHasUnplacedBundles(const PlacementGroupID &placement_group_id);

  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  instrumented_io_context &io_context_;

  /// Callbacks of pending `RegisterPlacementGroup` requests.
  /// Maps placement group ID to placement group registration callbacks, which is used to
  /// filter duplicated messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<PlacementGroupID, std::vector<StatusCallback>>
      placement_group_to_register_callbacks_;

  /// Callback of `WaitPlacementGroupUntilReady` requests.
  absl::flat_hash_map<PlacementGroupID, std::vector<StatusCallback>>
      placement_group_to_create_callbacks_;

  /// All registered placement_groups (pending placement_groups are also included).
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<GcsPlacementGroup>>
      registered_placement_groups_;

  /// The pending placement_groups which will not be scheduled until there's a
  /// resource change. The pending queue is represented as an ordered map, where
  /// the key is the time to schedule the pg and value if a pair containing the
  /// actual placement group and a exp-backoff.
  /// When error happens, we'll retry it later and this can be simply done by
  /// inserting an element into the queue with a bigger key. With this, we don't
  /// need to post retry job to io context. And when schedule pending placement
  /// group, we always start with the one with the smallest key.
  absl::btree_multimap<int64_t,
                       std::pair<ExponentialBackoff, std::shared_ptr<GcsPlacementGroup>>>
      pending_placement_groups_;

  /// The infeasible placement_groups that can't be scheduled currently.
  std::deque<std::shared_ptr<GcsPlacementGroup>> infeasible_placement_groups_;

  /// The scheduler to schedule all registered placement_groups.
  /// Scheduler's lifecycle lies in [GcsServer].
  gcs::GcsPlacementGroupSchedulerInterface *gcs_placement_group_scheduler_ = nullptr;

  /// Used to update placement group information upon creation, deletion, etc.
  gcs::GcsTableStorage *gcs_table_storage_ = nullptr;

  /// Counter of placement groups broken down by State.
  std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>>
      placement_group_state_counter_;

  /// The placement group id that is in progress of scheduling bundles.
  /// TODO(sang): Currently, only one placement group can be scheduled at a time.
  /// We should probably support concurrenet creation (or batching).
  PlacementGroupID scheduling_in_progress_id_ = PlacementGroupID::Nil();

  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;

  UsageStatsClient *usage_stats_client_;

  /// Get ray namespace.
  std::function<std::string(const JobID &)> get_ray_namespace_;

  /// Maps placement group names to their placement group ID for lookups by
  /// name, first keyed by namespace.
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, PlacementGroupID>>
      named_placement_groups_;

  /// Total number of successfully created placement groups in the cluster lifetime.
  int64_t lifetime_num_placement_groups_created_ = 0;

  // Debug info.
  enum CountType {
    CREATE_PLACEMENT_GROUP_REQUEST = 0,
    REMOVE_PLACEMENT_GROUP_REQUEST = 1,
    GET_PLACEMENT_GROUP_REQUEST = 2,
    GET_ALL_PLACEMENT_GROUP_REQUEST = 3,
    WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST = 4,
    GET_NAMED_PLACEMENT_GROUP_REQUEST = 5,
    SCHEDULING_PENDING_PLACEMENT_GROUP = 6,
    CountType_MAX = 7,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityReschedule);
  FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityFailed);
  FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityOrder);
};

}  // namespace gcs
}  // namespace ray
