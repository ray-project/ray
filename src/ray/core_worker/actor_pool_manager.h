// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/actor_pool_work_queue.h"
#include "ray/core_worker/common.h"
#include "ray/util/time.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

// Forward declarations
class ActorManager;
class ActorTaskSubmitterInterface;
class LocalityDataProviderInterface;
class TaskManagerInterface;

/// Callback type for task completion (success or failure).
/// \param status The status of the task.
/// \param error_info Error information if the task failed (nullptr on success).
using TaskCompletionCallback =
    std::function<void(const Status &status, const rpc::RayErrorInfo *error_info)>;

/// Callback type for submitting an actor task via CoreWorker.
/// This allows ActorPoolManager to delegate TaskSpec building to CoreWorker.
///
/// \param actor_id The actor to submit the task to.
/// \param function The function to execute.
/// \param args The task arguments.
/// \param task_options Task options.
/// \param on_complete Callback to invoke when the task completes.
/// \param pool_id The actor pool this task belongs to.
/// \param work_item_id The work item ID for pool-level tracking.
/// \return Object references for the task's return values.
using SubmitActorTaskCallback = std::function<std::vector<rpc::ObjectReference>(
    const ActorID &actor_id,
    const RayFunction &function,
    std::vector<std::unique_ptr<TaskArg>> args,
    const TaskOptions &task_options,
    TaskCompletionCallback on_complete,
    const ActorPoolID &pool_id,
    const TaskID &work_item_id)>;

/// Configuration for an actor pool.
struct ActorPoolConfig {
  /// Retry configuration
  int32_t max_retry_attempts = 3;
  int32_t retry_backoff_ms = 1000;
  float retry_backoff_multiplier = 2.0f;
  int32_t max_retry_backoff_ms = 60000;
  bool retry_on_system_errors = true;

  /// Max concurrent tasks per actor (controls admission in SelectActorFromPool).
  int32_t max_tasks_in_flight_per_actor = 1;

  /// Autoscaling configuration
  int32_t min_size = 1;
  int32_t max_size = -1;  // -1 = unbounded
  int32_t initial_size = 1;
};

/// State of an actor within a pool.
struct ActorPoolActorState {
  /// Number of tasks currently in flight for this actor.
  int32_t num_tasks_in_flight = 0;

  /// Location of the actor (for locality-aware scheduling).
  NodeID location;

  /// Whether this actor is alive and can accept work.
  bool is_alive = true;

  /// Number of consecutive failures for circuit breaking.
  int32_t consecutive_failures = 0;
};

/// Information about an actor pool.
struct ActorPoolInfo {
  /// Pool configuration.
  ActorPoolConfig config;

  /// List of actor IDs in this pool.
  std::vector<ActorID> actor_ids;

  /// State for each actor in the pool.
  absl::flat_hash_map<ActorID, ActorPoolActorState> actor_states;

  /// Total number of tasks submitted to this pool.
  int64_t total_tasks_submitted = 0;

  /// Total number of tasks that failed.
  int64_t total_tasks_failed = 0;

  /// Total number of tasks that were retried.
  int64_t total_tasks_retried = 0;
};

/// Statistics for an actor pool.
struct PoolStats {
  /// Total tasks submitted to the pool.
  int64_t total_tasks_submitted = 0;

  /// Total tasks that failed.
  int64_t total_tasks_failed = 0;

  /// Total tasks that were retried.
  int64_t total_tasks_retried = 0;

  /// Current number of actors in the pool.
  int32_t num_actors = 0;

  /// Current backlog size (queued work items).
  size_t backlog_size = 0;

  /// Total in-flight tasks across all actors.
  int32_t total_in_flight = 0;
};

/// Manages actor pools for cross-actor retry and load balancing.
///
/// This class provides pool-level task submission where the pool (not the caller)
/// selects which actor to execute the task. When a task fails, the pool can retry
/// it on a different actor, avoiding the thundering herd problem of actor-bound retries.
///
/// Task completion is notified via OnPoolTaskComplete(), which is called by the
/// callback wired through ActorTaskSubmitter::SetPoolTaskCompletionCallback().
/// This enables the cross-actor retry mechanism:
///   1. Task submitted to actor via SubmitToActor()
///   2. Task completes (success or failure)
///   3. ActorTaskSubmitter::HandlePushTaskReply() detects pool task and notifies
///   4. OnPoolTaskComplete() called, which invokes OnTaskSucceeded/OnTaskFailed
///   5. On failure, task can be re-enqueued and retried on a different actor
///
/// This class is thread-safe.
class ActorPoolManager {
 public:
  /// Constructor (minimal, for testing without full CoreWorker integration).
  ///
  /// \param actor_manager Reference to the ActorManager.
  /// \param task_submitter Reference to the ActorTaskSubmitter.
  /// \param task_manager Reference to the TaskManager.
  ActorPoolManager(ActorManager &actor_manager,
                   ActorTaskSubmitterInterface &task_submitter,
                   TaskManagerInterface &task_manager);

  /// Constructor with full CoreWorker integration.
  ///
  /// \param actor_manager Reference to the ActorManager.
  /// \param task_submitter Reference to the ActorTaskSubmitter.
  /// \param task_manager Reference to the TaskManager.
  /// \param io_service Reference to the IO service for delayed retry scheduling.
  /// \param submit_actor_task_fn Callback to submit actor tasks via CoreWorker.
  /// \param locality_data_provider Provider for object locality data (nullable).
  ActorPoolManager(ActorManager &actor_manager,
                   ActorTaskSubmitterInterface &task_submitter,
                   TaskManagerInterface &task_manager,
                   instrumented_io_context &io_service,
                   SubmitActorTaskCallback submit_actor_task_fn,
                   LocalityDataProviderInterface *locality_data_provider = nullptr);

  ~ActorPoolManager() = default;

  /// Register a new actor pool.
  ///
  /// \param config Pool configuration (retry, ordering, autoscaling).
  /// \param initial_actors Optional initial set of actor IDs to add to the pool.
  /// \return The ID of the newly created pool.
  ActorPoolID RegisterPool(const ActorPoolConfig &config,
                           const std::vector<ActorID> &initial_actors = {});

  /// Unregister an actor pool and clean up resources.
  ///
  /// \param pool_id The ID of the pool to unregister.
  void UnregisterPool(const ActorPoolID &pool_id);

  /// Add an actor to a pool.
  ///
  /// \param pool_id The ID of the pool.
  /// \param actor_id The ID of the actor to add.
  /// \param location The node ID where the actor is located.
  void AddActorToPool(const ActorPoolID &pool_id,
                      const ActorID &actor_id,
                      const NodeID &location);

  /// Remove an actor from a pool.
  ///
  /// \param pool_id The ID of the pool.
  /// \param actor_id The ID of the actor to remove.
  void RemoveActorFromPool(const ActorPoolID &pool_id, const ActorID &actor_id);

  /// Submit a task to an actor pool.
  /// The pool will select an appropriate actor based on load and locality.
  ///
  /// \param pool_id The ID of the pool to submit to.
  /// \param function The function to execute.
  /// \param args The task arguments.
  /// \param task_options Task options (num_returns, resources, etc).
  /// \return Object references for the task's return values.
  std::vector<rpc::ObjectReference> SubmitTaskToPool(
      const ActorPoolID &pool_id,
      const RayFunction &function,
      std::vector<std::unique_ptr<TaskArg>> args,
      const TaskOptions &task_options);

  /// Get all actor IDs in a pool.
  ///
  /// \param pool_id The ID of the pool.
  /// \return Vector of actor IDs in the pool.
  std::vector<ActorID> GetPoolActors(const ActorPoolID &pool_id) const;

  /// Get statistics for a pool.
  ///
  /// \param pool_id The ID of the pool.
  /// \return Pool statistics.
  PoolStats GetPoolStats(const ActorPoolID &pool_id) const;

  /// Check if a pool exists.
  ///
  /// \param pool_id The ID of the pool to check.
  /// \return True if the pool exists.
  bool HasPool(const ActorPoolID &pool_id) const;

  /// Returns in_flight + backlog — single number for backpressure decisions.
  ///
  /// \param pool_id The ID of the pool.
  /// \return Total occupied task slots (in-flight tasks + queued work items).
  int64_t GetOccupiedTaskSlots(const ActorPoolID &pool_id) const;

  /// Returns number of actors with num_tasks_in_flight > 0.
  ///
  /// \param pool_id The ID of the pool.
  /// \return Number of actors currently processing tasks.
  int32_t GetNumActiveActors(const ActorPoolID &pool_id) const;

  /// Returns the number of tasks currently in flight for a specific actor.
  ///
  /// \param pool_id The ID of the pool.
  /// \param actor_id The ID of the actor.
  /// \return Number of tasks currently in flight for the actor.
  int32_t GetActorTasksInFlight(const ActorPoolID &pool_id,
                                const ActorID &actor_id) const;

  /// Select an actor from the pool for task execution or reconstruction.
  /// Thread-safe wrapper around SelectActorFromPool.
  ///
  /// \param pool_id The ID of the pool.
  /// \param arg_ids Object IDs of task arguments (for locality).
  /// \param exclude_actor_id Actor to exclude from selection (e.g. the actor
  ///   that just failed). Pass Nil to exclude nothing.
  /// \return The selected actor ID, or Nil if no actors are available.
  ActorID SelectActorForTask(const ActorPoolID &pool_id,
                             const std::vector<ObjectID> &arg_ids = {},
                             const ActorID &exclude_actor_id = ActorID::Nil());

  /// Called when a pool task terminally completes (success or exhausted retries).
  /// Updates num_tasks_in_flight and drains queued work on success.
  /// Called by ActorTaskSubmitter via MaybeNotifyPoolTaskComplete.
  ///
  /// \param pool_id The ID of the pool the task belongs to.
  /// \param work_item_id The ID of the work item within the pool.
  /// \param task_id The ID of the completed task.
  /// \param actor_id The ID of the actor that executed the task.
  /// \param status The completion status (OK for success, error for failure).
  /// \param error_info Error information if the task failed (nullptr on success).
  void OnPoolTaskComplete(const ActorPoolID &pool_id,
                          const TaskID &work_item_id,
                          const TaskID &task_id,
                          const ActorID &actor_id,
                          const Status &status,
                          const rpc::RayErrorInfo *error_info);

  /// Notify that a task was pushed to an actor in a pool.
  /// Transitions the work item from pending-submission to in-flight.
  ///
  /// \param actor_id The actor that received the task.
  /// \param work_item_id The work item that was pushed.
  void OnTaskSubmitted(const ActorID &actor_id,
                       const TaskID &work_item_id = TaskID::Nil());

  /// Notify that an actor has come alive (e.g. after restart).
  /// Called from ActorManager::HandleActorStateNotification when state = ALIVE.
  /// Marks the actor as alive and drains the work queue to dispatch queued tasks.
  ///
  /// \param actor_id The actor that is now alive.
  /// \param node_id The node the actor is running on.
  void OnActorAlive(const ActorID &actor_id, const NodeID &node_id);

  /// Notify that an actor is dead or restarting.
  /// Called from ActorManager::HandleActorStateNotification when state = RESTARTING/DEAD.
  /// Marks the actor as not alive so SelectActorFromPool skips it.
  ///
  /// \param actor_id The actor that is dead or restarting.
  void OnActorDead(const ActorID &actor_id);

 private:
  FRIEND_TEST(ActorPoolManagerTest, RegisterUnregisterPool);
  FRIEND_TEST(ActorPoolManagerTest, AddRemoveActors);
  FRIEND_TEST(ActorPoolManagerTest, SelectLeastLoadedActor);
  FRIEND_TEST(ActorPoolManagerTest, CrossActorRetry);
  FRIEND_TEST(ActorPoolManagerLocalityTest, LocalitySelectsDataLocalActor);
  FRIEND_TEST(ActorPoolManagerLocalityTest, LocalityTiebreakerIsLoad);
  FRIEND_TEST(ActorPoolManagerLocalityTest, NoLocalityDataFallsBackToLoad);
  FRIEND_TEST(ActorPoolManagerLocalityTest, EmptyArgIdsFallsBackToLoad);
  FRIEND_TEST(ActorPoolManagerLocalityTest, LargerDataNodeWins);
  FRIEND_TEST(ActorPoolManagerLocalityTest, NullProviderFallsBackToLoad);
  FRIEND_TEST(ActorPoolManagerTest, GetOccupiedTaskSlots);
  FRIEND_TEST(ActorPoolManagerTest, GetNumActiveActors);
  FRIEND_TEST(ActorPoolManagerTest, UnregisterPoolCleansTrackedWorkItems);
  FRIEND_TEST(ActorPoolManagerTest, UnregisterPoolOnlyCleansItsOwnTrackedWorkItems);
  FRIEND_TEST(ActorPoolManagerTest,
              LateTaskCompletionAfterUnregisterIsIgnoredWithoutLeak);
  FRIEND_TEST(ActorPoolManagerTest, OnTaskFailedMarksActorDead);
  FRIEND_TEST(ActorPoolManagerTest, OnActorAliveDrainsQueue);
  FRIEND_TEST(ActorPoolManagerTest, OnActorDeadMarksActorNotAlive);

  /// Select the best actor from a pool based on load and locality.
  ///
  /// \param pool_id The ID of the pool.
  /// \param arg_ids Object IDs of task arguments (for locality).
  /// \param exclude_actor_id Actor to skip during selection.
  /// \return The selected actor ID, or Nil if no actors are available.
  ActorID SelectActorFromPool(const ActorPoolID &pool_id,
                              const std::vector<ObjectID> &arg_ids,
                              const ActorID &exclude_actor_id = ActorID::Nil())
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Rank an actor for scheduling (lower is better).
  /// Returns (locality_rank, load) pair for lexicographic comparison.
  /// locality_rank is -total_bytes for data-local nodes, INT64_MAX for remote.
  ///
  /// \param actor_id The actor to rank.
  /// \param node_bytes Pre-computed map of NodeID → total data bytes.
  /// \param pool_info Pool information.
  /// \return (locality_rank, load) pair.
  std::pair<int64_t, int32_t> RankActor(
      const ActorID &actor_id,
      const absl::flat_hash_map<NodeID, uint64_t> &node_bytes,
      const ActorPoolInfo &pool_info) const;

  /// Compute a map of NodeID → total bytes of task arguments on that node.
  /// Hoisted out of per-candidate loop for efficiency.
  ///
  /// \param arg_ids Object IDs of task arguments.
  /// \return Map from node ID to total bytes of arguments on that node.
  absl::flat_hash_map<NodeID, uint64_t> ComputeNodeLocalityMap(
      const std::vector<ObjectID> &arg_ids) const;

  /// Submit a work item to a specific actor.
  ///
  /// \param pool_id The pool ID.
  /// \param actor_id The actor to submit to.
  /// \param work_item The work item to submit.
  /// \return Object references for the task's return values.
  std::vector<rpc::ObjectReference> SubmitToActor(const ActorPoolID &pool_id,
                                                  const ActorID &actor_id,
                                                  PoolWorkItem work_item)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Handle a task failure and potentially retry on a different actor.
  ///
  /// \param pool_id The pool ID.
  /// \param work_item_id The work item ID.
  /// \param failed_actor_id The actor that failed.
  /// \param error_info Error information.
  void OnTaskFailed(const ActorPoolID &pool_id,
                    const TaskID &work_item_id,
                    const ActorID &failed_actor_id,
                    const rpc::RayErrorInfo &error_info)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Handle a task success.
  ///
  /// \param pool_id The pool ID.
  /// \param actor_id The actor that succeeded.
  void OnTaskSucceeded(const ActorPoolID &pool_id, const ActorID &actor_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Schedule a retry for a work item with backoff.
  ///
  /// \param pool_id The pool ID.
  /// \param work_item The work item to retry.
  /// \param backoff_ms Backoff time in milliseconds.
  void ScheduleRetry(const ActorPoolID &pool_id,
                     PoolWorkItem work_item,
                     int64_t backoff_ms) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Retry a work item (select actor and submit).
  ///
  /// \param pool_id The pool ID.
  /// \param work_item The work item to retry.
  void RetryWorkItem(const ActorPoolID &pool_id, PoolWorkItem work_item)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Determine if a task should be retried based on error type.
  ///
  /// \param config Pool configuration.
  /// \param error_info Error information.
  /// \return True if the task should be retried.
  bool ShouldRetryTask(const ActorPoolConfig &config,
                       const rpc::RayErrorInfo &error_info) const;

  /// Calculate backoff time for a retry attempt.
  ///
  /// \param attempt_number The attempt number (1-indexed).
  /// \param base_backoff_ms Base backoff time.
  /// \param multiplier Backoff multiplier.
  /// \param max_backoff_ms Maximum backoff time.
  /// \return Backoff time in milliseconds.
  int64_t CalculateBackoff(int32_t attempt_number,
                           int32_t base_backoff_ms,
                           float multiplier,
                           int32_t max_backoff_ms) const;

  /// Fail a work item permanently.
  ///
  /// \param work_item_id The work item ID.
  /// \param error_info Error information.
  void FailWorkItem(const TaskID &work_item_id, const rpc::RayErrorInfo &error_info)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Track a submitted work item by ID and owning pool.
  void TrackWorkItem(PoolWorkItem work_item) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Erase a tracked work item and remove it from the reverse index.
  void EraseTrackedWorkItem(const TaskID &work_item_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Take ownership of a tracked work item and remove it from the reverse index.
  std::optional<PoolWorkItem> TakeTrackedWorkItem(const TaskID &work_item_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Cleanup all tracked work items owned by a pool.
  void CleanupTrackedWorkItemsForPool(const ActorPoolID &pool_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Drain the work queue for a pool, submitting queued items to available actors.
  /// Called when new capacity becomes available (task succeeded, actor added).
  ///
  /// \param pool_id The pool ID whose queue to drain.
  void DrainWorkQueue(const ActorPoolID &pool_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Clone task arguments for retry (since we need to keep a copy).
  std::vector<std::unique_ptr<TaskArg>> CloneArgs(
      const std::vector<std::unique_ptr<TaskArg>> &args) const;

  /// Reference to the actor manager.
  ActorManager &actor_manager_;

  /// Reference to the actor task submitter.
  ActorTaskSubmitterInterface &task_submitter_;

  /// Reference to the task manager.
  TaskManagerInterface &task_manager_;

  /// Reference to the IO service for delayed retry scheduling.
  /// May be nullptr if using the minimal constructor.
  instrumented_io_context *io_service_ = nullptr;

  /// Callback to submit actor tasks via CoreWorker.
  /// May be nullptr if using the minimal constructor.
  SubmitActorTaskCallback submit_actor_task_fn_;

  /// Provider for object locality data (for locality-aware scheduling).
  /// May be nullptr if locality data is unavailable.
  LocalityDataProviderInterface *locality_data_provider_ = nullptr;

  /// Mutex protecting all pool state.
  mutable absl::Mutex mu_;

  /// Registry of all actor pools.
  absl::flat_hash_map<ActorPoolID, ActorPoolInfo> pools_ ABSL_GUARDED_BY(mu_);

  /// Map from actor ID to pool ID (for reverse lookup).
  absl::flat_hash_map<ActorID, ActorPoolID> actor_to_pool_ ABSL_GUARDED_BY(mu_);

  /// Work queues for each pool.
  absl::flat_hash_map<ActorPoolID, std::unique_ptr<PoolWorkQueue>> work_queues_
      ABSL_GUARDED_BY(mu_);

  /// Map from work item ID to work item (for retry tracking).
  absl::flat_hash_map<TaskID, PoolWorkItem> work_items_ ABSL_GUARDED_BY(mu_);

  /// Reverse index from pool ID to tracked work item IDs for teardown cleanup.
  absl::flat_hash_map<ActorPoolID, absl::flat_hash_set<TaskID>> pool_to_work_items_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace core
}  // namespace ray
