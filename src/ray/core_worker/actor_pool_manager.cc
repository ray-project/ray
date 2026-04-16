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

#include "ray/core_worker/actor_pool_manager.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "boost/asio/steady_timer.hpp"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/task_manager_interface.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

namespace {}  // namespace

ActorPoolManager::ActorPoolManager(ActorManager &actor_manager,
                                   TaskManagerInterface &task_manager)
    : actor_manager_(actor_manager),
      task_manager_(task_manager),
      io_service_(nullptr),
      submit_actor_task_fn_(nullptr) {
  RAY_LOG(DEBUG) << "ActorPoolManager initialized (minimal mode, no callbacks)";
}

ActorPoolManager::ActorPoolManager(ActorManager &actor_manager,
                                   TaskManagerInterface &task_manager,
                                   instrumented_io_context &io_service,
                                   SubmitActorTaskCallback submit_actor_task_fn,
                                   WorkerContext &worker_context,
                                   const rpc::Address &rpc_address,
                                   LocalityDataProviderInterface *locality_data_provider)
    : actor_manager_(actor_manager),
      task_manager_(task_manager),
      io_service_(&io_service),
      submit_actor_task_fn_(std::move(submit_actor_task_fn)),
      locality_data_provider_(locality_data_provider),
      worker_context_(&worker_context),
      rpc_address_(rpc_address) {
  RAY_LOG(DEBUG) << "ActorPoolManager initialized (full mode with CoreWorker callbacks)";
}

ActorPoolID ActorPoolManager::RegisterPool(const ActorPoolConfig &config,
                                           const std::vector<ActorID> &initial_actors) {
  absl::MutexLock lock(&mu_);

  ActorPoolID pool_id = ActorPoolID::FromRandom();

  ActorPoolInfo pool_info;
  pool_info.config = config;

  auto task_queue = std::make_unique<FifoPoolTaskQueue>();

  for (const auto &actor_id : initial_actors) {
    pool_info.actor_ids.push_back(actor_id);
    pool_info.actor_states[actor_id] = ActorPoolActorState{};
    actor_to_pool_[actor_id] = pool_id;
  }

  pools_[pool_id] = std::move(pool_info);
  task_queues_[pool_id] = std::move(task_queue);

  RAY_LOG(INFO) << "Registered actor pool " << pool_id << " with "
                << initial_actors.size() << " actors";

  return pool_id;
}

void ActorPoolManager::UnregisterPool(const ActorPoolID &pool_id) {
  absl::MutexLock lock(&mu_);

  auto it = pools_.find(pool_id);
  if (it == pools_.end()) {
    RAY_LOG(WARNING) << "Attempted to unregister non-existent pool: " << pool_id;
    return;
  }

  for (const auto &actor_id : it->second.actor_ids) {
    actor_to_pool_.erase(actor_id);
  }

  task_queues_.erase(pool_id);
  CleanupTrackedPoolTasksForPool(pool_id);
  pools_.erase(it);

  RAY_LOG(INFO) << "Unregistered actor pool " << pool_id;
}

void ActorPoolManager::AddActorToPool(const ActorPoolID &pool_id,
                                      const ActorID &actor_id,
                                      const NodeID &location) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  RAY_CHECK(pool_it != pools_.end()) << "Pool not found: " << pool_id;

  auto &pool_info = pool_it->second;

  // If actor already in pool, just update location.
  auto state_it = pool_info.actor_states.find(actor_id);
  if (state_it != pool_info.actor_states.end()) {
    if (!location.IsNil()) {
      state_it->second.location = location;
    }
    return;
  }

  pool_info.actor_ids.push_back(actor_id);
  pool_info.actor_states[actor_id] = ActorPoolActorState{
      .num_tasks_in_flight = 0, .location = location, .is_alive = true};
  actor_to_pool_[actor_id] = pool_id;

  RAY_LOG(DEBUG) << "Added actor " << actor_id << " to pool " << pool_id;

  DrainTaskQueue(pool_id);
}

void ActorPoolManager::RemoveActorFromPool(const ActorPoolID &pool_id,
                                           const ActorID &actor_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(WARNING) << "Pool not found: " << pool_id;
    return;
  }

  auto &pool_info = pool_it->second;

  auto &actor_ids = pool_info.actor_ids;
  actor_ids.erase(std::remove(actor_ids.begin(), actor_ids.end(), actor_id),
                  actor_ids.end());

  pool_info.actor_states.erase(actor_id);
  actor_to_pool_.erase(actor_id);

  RAY_LOG(DEBUG) << "Removed actor " << actor_id << " from pool " << pool_id;
}

std::vector<rpc::ObjectReference> ActorPoolManager::SubmitTaskToPool(
    const ActorPoolID &pool_id,
    const RayFunction &function,
    std::vector<std::unique_ptr<TaskArg>> args,
    const TaskOptions &task_options) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(ERROR) << "Pool not found: " << pool_id;
    return {};
  }

  auto &task_queue = task_queues_[pool_id];

  // Generate pool-scoped TaskID before actor selection.
  TaskID pool_task_id;
  std::vector<rpc::ObjectReference> return_refs;
  if (worker_context_) {
    pool_task_id = TaskID::ForPoolTask(worker_context_->GetCurrentJobID(),
                                       worker_context_->GetCurrentInternalTaskId(),
                                       worker_context_->GetNextTaskIndex(),
                                       pool_id);

    // Pre-register ObjectRefs with TaskManager so caller gets refs immediately.
    bool is_streaming = (task_options.num_returns == -1);
    size_t num_returns = is_streaming ? 1 : static_cast<size_t>(task_options.num_returns);
    return_refs = task_manager_.RegisterPoolTaskReturnValues(rpc_address_,
                                                             pool_task_id,
                                                             num_returns,
                                                             /*call_site=*/"actor_pool",
                                                             is_streaming);
  } else {
    // Minimal mode (for tests): generate a pool-scoped ID without WorkerContext.
    // Use a monotonic counter for uniqueness.
    RAY_LOG(WARNING)
        << "Generating pool-scoped TaskID for test mode, not to be used in production";
    static std::atomic<size_t> test_counter{0};
    pool_task_id =
        TaskID::ForPoolTask(JobID::FromInt(0), TaskID::Nil(), test_counter++, pool_id);
    // Build return refs without TaskManager registration.
    size_t num_returns = (task_options.num_returns == -1)
                             ? 1
                             : static_cast<size_t>(task_options.num_returns);
    for (size_t i = 0; i < num_returns; i++) {
      rpc::ObjectReference ref;
      ref.set_object_id(ObjectID::FromIndex(pool_task_id, i + 1).Binary());
      return_refs.push_back(std::move(ref));
    }
  }

  PoolTask pool_task;
  pool_task.pool_id = pool_id;
  pool_task.pool_task_id = pool_task_id;
  pool_task.function = function;
  pool_task.args = std::move(args);
  pool_task.options = task_options;
  pool_task.return_refs = return_refs;
  pool_task.attempt_number = 0;
  pool_task.enqueued_at_ms = current_time_ms();

  // Precompute by-reference arg ObjectIDs for locality-aware scheduling.
  // Stored once in the pool task so DrainTaskQueue and RetryPoolTask
  // can reuse them without re-serializing args.
  for (const auto &arg : pool_task.args) {
    auto obj_id = arg->GetObjectId();
    if (!obj_id.IsNil()) {
      pool_task.arg_ids.push_back(obj_id);
    }
  }

  // Only dispatch to actors with available capacity. When all actors are at
  // max_tasks_in_flight, the task queues at the pool level instead of
  // overloading individual actors.
  ActorID selected_actor = SelectActorFromPool(pool_id,
                                               pool_task.arg_ids,
                                               /*exclude_actor_id=*/ActorID::Nil(),
                                               /*require_available_capacity=*/true);

  if (selected_actor.IsNil()) {
    RAY_LOG(DEBUG) << "No actors with free capacity in pool " << pool_id
                   << ", enqueueing pool task " << pool_task_id;
    task_queue->Push(std::move(pool_task));
    // Return pool-scoped refs even when queued — caller can wait on them.
    return return_refs;
  }

  SubmitToActor(pool_id, selected_actor, std::move(pool_task));
  return return_refs;
}

std::vector<ActorID> ActorPoolManager::GetPoolActors(const ActorPoolID &pool_id) const {
  absl::MutexLock lock(&mu_);

  auto it = pools_.find(pool_id);
  if (it == pools_.end()) {
    return {};
  }

  return it->second.actor_ids;
}

PoolStats ActorPoolManager::GetPoolStats(const ActorPoolID &pool_id) const {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return PoolStats{};
  }

  const auto &pool_info = pool_it->second;
  auto tq_it = task_queues_.find(pool_id);
  if (tq_it == task_queues_.end()) {
    return PoolStats{};
  }
  const auto &task_queue = tq_it->second;

  PoolStats stats;
  stats.total_tasks_submitted = pool_info.total_tasks_submitted;
  stats.total_tasks_failed = pool_info.total_tasks_failed;
  stats.num_actors = static_cast<int32_t>(pool_info.actor_ids.size());
  stats.backlog_size = task_queue->Size();

  int32_t total_in_flight = 0;
  for (const auto &[actor_id, state] : pool_info.actor_states) {
    total_in_flight += state.num_tasks_in_flight;
  }
  stats.total_in_flight = total_in_flight;

  return stats;
}

bool ActorPoolManager::HasPool(const ActorPoolID &pool_id) const {
  absl::MutexLock lock(&mu_);
  return pools_.find(pool_id) != pools_.end();
}

int64_t ActorPoolManager::GetOccupiedTaskSlots(const ActorPoolID &pool_id) const {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return 0;
  }

  const auto &pool_info = pool_it->second;
  int64_t total_in_flight = 0;
  for (const auto &[actor_id, state] : pool_info.actor_states) {
    total_in_flight += state.num_tasks_in_flight;
  }

  auto tq_it = task_queues_.find(pool_id);
  int64_t backlog = (tq_it != task_queues_.end()) ? tq_it->second->Size() : 0;

  // Count pool tasks dispatched to ActorTaskSubmitter but not yet pushed to the
  // actor via gRPC (OnTaskSubmitted hasn't fired yet). Without this, tasks are
  // invisible between SubmitToActor and the async OnTaskSubmitted callback,
  // allowing the Python scheduling loop to over-submit.
  int64_t pending_submissions = 0;
  auto ptt_it = pool_to_tasks_.find(pool_id);
  if (ptt_it != pool_to_tasks_.end()) {
    for (const auto &pool_task_id : ptt_it->second) {
      auto pt_it = pool_tasks_.find(pool_task_id);
      if (pt_it != pool_tasks_.end() && !pt_it->second.pushed_to_actor) {
        pending_submissions++;
      }
    }
  }

  return total_in_flight + backlog + pending_submissions;
}

int32_t ActorPoolManager::GetNumActiveActors(const ActorPoolID &pool_id) const {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return 0;
  }

  int32_t active = 0;
  for (const auto &[actor_id, state] : pool_it->second.actor_states) {
    if (state.num_tasks_in_flight > 0) {
      active++;
    }
  }
  return active;
}

int32_t ActorPoolManager::GetActorTasksInFlight(const ActorPoolID &pool_id,
                                                const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return 0;
  }

  auto state_it = pool_it->second.actor_states.find(actor_id);
  if (state_it == pool_it->second.actor_states.end()) {
    return 0;
  }

  return state_it->second.num_tasks_in_flight;
}

ActorID ActorPoolManager::SelectActorForTask(const ActorPoolID &pool_id,
                                             const std::vector<ObjectID> &arg_ids,
                                             const ActorID &exclude_actor_id,
                                             bool require_available_capacity) {
  absl::MutexLock lock(&mu_);
  return SelectActorFromPool(
      pool_id, arg_ids, exclude_actor_id, require_available_capacity);
}

void ActorPoolManager::OnPoolTaskComplete(const ActorPoolID &pool_id,
                                          const TaskID &pool_task_id,
                                          const TaskID &task_id,
                                          const ActorID &actor_id,
                                          const Status &status,
                                          const rpc::RayErrorInfo *error_info) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(DEBUG) << "Pool " << pool_id << " no longer exists, ignoring task completion";
    return;
  }
  auto tq_it = task_queues_.find(pool_id);
  RAY_CHECK(tq_it != task_queues_.end());
  if (status.ok()) {
    OnTaskSucceeded(pool_id, actor_id);
    EraseTrackedPoolTask(pool_task_id);
  } else {
    if (error_info != nullptr) {
      OnTaskFailed(pool_id, pool_task_id, actor_id, *error_info);
    } else {
      rpc::RayErrorInfo generic_error;
      generic_error.set_error_type(rpc::ErrorType::ACTOR_DIED);
      generic_error.set_error_message("Task failed with unknown error: " +
                                      status.ToString());
      OnTaskFailed(pool_id, pool_task_id, actor_id, generic_error);
    }
  }
}

void ActorPoolManager::OnTaskSubmitted(const ActorID &actor_id,
                                       const TaskID &pool_task_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = actor_to_pool_.find(actor_id);
  if (pool_it == actor_to_pool_.end()) {
    return;
  }

  auto info_it = pools_.find(pool_it->second);
  if (info_it == pools_.end()) {
    return;
  }

  auto state_it = info_it->second.actor_states.find(actor_id);
  if (state_it == info_it->second.actor_states.end()) {
    return;
  }

  state_it->second.num_tasks_in_flight++;

  // Mark the pool task as pushed so it is no longer counted as a pending
  // submission in GetOccupiedTaskSlots.
  if (!pool_task_id.IsNil()) {
    auto pt_it = pool_tasks_.find(pool_task_id);
    if (pt_it != pool_tasks_.end()) {
      pt_it->second.pushed_to_actor = true;
    }
  }
}

void ActorPoolManager::OnActorAlive(const ActorID &actor_id, const NodeID &node_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = actor_to_pool_.find(actor_id);
  if (pool_it == actor_to_pool_.end()) {
    return;  // Actor not in any pool.
  }

  const auto &pool_id = pool_it->second;
  auto info_it = pools_.find(pool_id);
  if (info_it == pools_.end()) {
    return;
  }

  auto &pool_info = info_it->second;
  auto state_it = pool_info.actor_states.find(actor_id);
  if (state_it == pool_info.actor_states.end()) {
    return;
  }

  auto &actor_state = state_it->second;
  actor_state.is_alive = true;
  actor_state.consecutive_failures = 0;
  // Tasks previously assigned to an older incarnation of this actor will
  // either fail and retry elsewhere or have already completed, so any stale
  // in-flight count for the previous incarnation is cleared here.
  actor_state.num_tasks_in_flight = 0;
  if (!node_id.IsNil()) {
    actor_state.location = node_id;
  }

  DrainTaskQueue(pool_id);
}

void ActorPoolManager::OnActorDead(const ActorID &actor_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = actor_to_pool_.find(actor_id);
  if (pool_it == actor_to_pool_.end()) {
    return;
  }

  const auto &pool_id = pool_it->second;
  auto info_it = pools_.find(pool_id);
  if (info_it == pools_.end()) {
    return;
  }

  auto state_it = info_it->second.actor_states.find(actor_id);
  if (state_it == info_it->second.actor_states.end()) {
    return;
  }

  state_it->second.is_alive = false;
  state_it->second.num_tasks_in_flight = 0;
}

ActorID ActorPoolManager::SelectActorFromPool(const ActorPoolID &pool_id,
                                              const std::vector<ObjectID> &arg_ids,
                                              const ActorID &exclude_actor_id,
                                              bool require_available_capacity) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(WARNING) << "Pool not found: " << pool_id;
    return ActorID::Nil();
  }

  auto &pool_info = pool_it->second;  // non-const: next_selection_index is mutated

  // Prefer alive actors with available capacity.
  std::vector<ActorID> candidates;
  std::vector<ActorID> alive_actors;
  for (const auto &actor_id : pool_info.actor_ids) {
    auto state_it = pool_info.actor_states.find(actor_id);
    if (state_it == pool_info.actor_states.end()) {
      continue;
    }

    const auto &state = state_it->second;
    if (!state.is_alive) {
      continue;
    }
    alive_actors.push_back(actor_id);
    if (actor_id == exclude_actor_id) {
      continue;
    }

    const int32_t max_concurrency = pool_info.config.max_tasks_in_flight_per_actor;
    if (state.num_tasks_in_flight < max_concurrency) {
      candidates.push_back(actor_id);
    }
  }

  // Fall back to all alive actors when normal submissions have exhausted the
  // configured per-actor limit.
  if (candidates.empty() && !require_available_capacity) {
    // If exclusion filtered out all candidates, fall back to the full
    // alive set (including the excluded actor).  This handles single-actor
    // pools and the case where all other actors are dead.
    candidates = std::move(alive_actors);
  }

  if (candidates.empty()) {
    RAY_LOG(DEBUG) << "No alive actors in pool " << pool_id;
    return ActorID::Nil();
  }

  auto node_bytes = ComputeNodeLocalityMap(arg_ids);
  auto best_rank = std::make_pair(INT64_MAX, INT32_MAX);
  std::vector<ActorID> best_actors;
  best_actors.reserve(candidates.size());
  for (const auto &candidate : candidates) {
    const auto rank = RankActor(candidate, node_bytes, pool_info);
    if (rank < best_rank) {
      best_rank = rank;
      best_actors.clear();
      best_actors.push_back(candidate);
    } else if (rank == best_rank) {
      best_actors.push_back(candidate);
    }
  }
  RAY_CHECK(!best_actors.empty());
  const auto selected_index = pool_info.next_selection_index++ % best_actors.size();
  auto best_actor = best_actors[selected_index];

  RAY_LOG(DEBUG) << "Selected actor " << best_actor << " from pool " << pool_id;
  return best_actor;
}

std::pair<int64_t, int32_t> ActorPoolManager::RankActor(
    const ActorID &actor_id,
    const absl::flat_hash_map<NodeID, uint64_t> &node_bytes,
    const ActorPoolInfo &pool_info) const {
  auto state_it = pool_info.actor_states.find(actor_id);
  if (state_it == pool_info.actor_states.end()) {
    return {INT64_MAX, INT32_MAX};
  }

  const auto &state = state_it->second;
  int32_t load = state.num_tasks_in_flight;

  if (node_bytes.empty()) {
    return {0, load};
  }

  // Rank by (-local_bytes, load): actors on nodes holding more task data rank higher.
  auto bytes_it = node_bytes.find(state.location);
  if (bytes_it != node_bytes.end()) {
    return {-static_cast<int64_t>(bytes_it->second), load};
  }

  return {INT64_MAX, load};
}

absl::flat_hash_map<NodeID, uint64_t> ActorPoolManager::ComputeNodeLocalityMap(
    const std::vector<ObjectID> &arg_ids) const {
  absl::flat_hash_map<NodeID, uint64_t> node_bytes;
  if (!locality_data_provider_ || arg_ids.empty()) {
    return node_bytes;
  }

  for (const auto &arg_id : arg_ids) {
    auto locality = locality_data_provider_->GetLocalityData(arg_id);
    if (!locality.has_value()) {
      continue;
    }
    for (const auto &node_id : locality->nodes_containing_object) {
      node_bytes[node_id] += locality->object_size;
    }
  }

  return node_bytes;
}

std::vector<rpc::ObjectReference> ActorPoolManager::SubmitToActor(
    const ActorPoolID &pool_id, const ActorID &actor_id, PoolTask pool_task) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(ERROR) << "Pool not found: " << pool_id;
    return {};
  }

  auto &pool_info = pool_it->second;

  pool_info.total_tasks_submitted++;

  TaskID pool_task_id = pool_task.pool_task_id;

  if (!submit_actor_task_fn_) {
    RAY_LOG(WARNING) << "SubmitToActor called without submit callback (minimal mode)";
    pool_info.actor_states[actor_id].num_tasks_in_flight++;
    TrackPoolTask(std::move(pool_task));
    return {};
  }

  // Clone args before moving pool_task into pool_tasks_ (we need args for submission).
  auto args_for_submit = CloneArgs(pool_task.args);
  RayFunction function = pool_task.function;
  TaskOptions options = pool_task.options;

  TrackPoolTask(std::move(pool_task));

  // Completion is NOT handled via this callback — it flows through
  // ActorTaskSubmitter::HandlePushTaskReply() → SetPoolTaskCompletionCallback()
  // → OnPoolTaskComplete(). We pass nullptr to avoid duplicate handling.
  return submit_actor_task_fn_(actor_id,
                               function,
                               std::move(args_for_submit),
                               options,
                               nullptr,
                               pool_id,
                               pool_task_id);
}

void ActorPoolManager::OnTaskFailed(const ActorPoolID &pool_id,
                                    const TaskID &pool_task_id,
                                    const ActorID &failed_actor_id,
                                    const rpc::RayErrorInfo &error_info) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(WARNING) << "Pool not found: " << pool_id;
    return;
  }

  auto &pool_info = pool_it->second;

  auto actor_state_it = pool_info.actor_states.find(failed_actor_id);
  if (actor_state_it != pool_info.actor_states.end()) {
    auto &actor_state = actor_state_it->second;
    if (actor_state.num_tasks_in_flight > 0) {
      actor_state.num_tasks_in_flight--;
    }
    actor_state.consecutive_failures++;

    // Mark actor as not alive on system errors (actor death, node death).
    // The actor may come back via OnActorAlive when GCS notifies of a restart.
    // Note: with max_retries=-1, actor death errors are retried by the
    // ActorTaskSubmitter and the pool callback never fires for those, so this
    // code path is only reached for non-retried failures. Kept as a safety net
    // and for OnActorDead which is the primary liveness tracker.
    if (error_info.error_type() == rpc::ErrorType::ACTOR_DIED ||
        error_info.error_type() == rpc::ErrorType::ACTOR_UNAVAILABLE ||
        error_info.error_type() == rpc::ErrorType::WORKER_DIED ||
        error_info.error_type() == rpc::ErrorType::NODE_DIED) {
      actor_state.is_alive = false;
    }
  }

  pool_info.total_tasks_failed++;

  RAY_LOG(INFO) << "Pool task " << pool_task_id << " failed terminally on actor "
                << failed_actor_id << ". Error: " << error_info.error_message();
  FailPoolTask(pool_task_id, error_info);

  // A slot freed up on this actor (or the actor died) — drain queued tasks.
  DrainTaskQueue(pool_id);
}

void ActorPoolManager::OnTaskSucceeded(const ActorPoolID &pool_id,
                                       const ActorID &actor_id) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return;
  }

  auto state_it = pool_it->second.actor_states.find(actor_id);
  if (state_it == pool_it->second.actor_states.end()) {
    // Actor was removed from pool; ignore late-arriving success callback.
    return;
  }
  auto &actor_state = state_it->second;
  if (actor_state.num_tasks_in_flight > 0) {
    actor_state.num_tasks_in_flight--;
    actor_state.consecutive_failures = 0;
  }

  DrainTaskQueue(pool_id);
}

void ActorPoolManager::FailPoolTask(const TaskID &pool_task_id,
                                    const rpc::RayErrorInfo &error_info) {
  EraseTrackedPoolTask(pool_task_id);

  RAY_LOG(INFO) << "Pool task " << pool_task_id
                << " failed permanently. Error: " << error_info.error_message();

  // The actual task failure is surfaced by TaskManager via the reply callback.
  // We only clean up pool-level tracking state here.
}

std::vector<std::unique_ptr<TaskArg>> ActorPoolManager::CloneArgs(
    const std::vector<std::unique_ptr<TaskArg>> &args) const {
  std::vector<std::unique_ptr<TaskArg>> cloned_args;
  cloned_args.reserve(args.size());

  for (const auto &arg : args) {
    rpc::TaskArg arg_proto;
    arg->ToProto(&arg_proto);

    if (arg_proto.has_object_ref()) {
      cloned_args.push_back(std::make_unique<TaskArgByReference>(
          ObjectID::FromBinary(arg_proto.object_ref().object_id()),
          rpc::Address(arg_proto.object_ref().owner_address()),
          arg_proto.object_ref().call_site()));
    } else if (!arg_proto.data().empty() || !arg_proto.metadata().empty()) {
      // const_cast is safe: copy_data=true makes LocalMemoryBuffer deep-copy immediately.
      std::shared_ptr<LocalMemoryBuffer> data = nullptr;
      if (!arg_proto.data().empty()) {
        data = std::make_shared<LocalMemoryBuffer>(
            reinterpret_cast<uint8_t *>(const_cast<char *>(arg_proto.data().data())),
            arg_proto.data().size(),
            /*copy_data=*/true);
      }

      std::shared_ptr<LocalMemoryBuffer> metadata = nullptr;
      if (!arg_proto.metadata().empty()) {
        metadata = std::make_shared<LocalMemoryBuffer>(
            reinterpret_cast<uint8_t *>(const_cast<char *>(arg_proto.metadata().data())),
            arg_proto.metadata().size(),
            /*copy_data=*/true);
      }

      std::vector<rpc::ObjectReference> nested_refs;
      nested_refs.reserve(arg_proto.nested_inlined_refs_size());
      for (const auto &nested_ref : arg_proto.nested_inlined_refs()) {
        nested_refs.push_back(nested_ref);
      }

      cloned_args.push_back(std::make_unique<TaskArgByValue>(
          std::make_shared<RayObject>(data, metadata, nested_refs, /*copy_data=*/true)));
    }
  }

  return cloned_args;
}

void ActorPoolManager::TrackPoolTask(PoolTask pool_task) {
  RAY_CHECK(!pool_task.pool_id.IsNil()) << "Tracked pool task must belong to a pool";

  const auto pool_id = pool_task.pool_id;
  const auto pool_task_id = pool_task.pool_task_id;
  pool_to_tasks_[pool_id].insert(pool_task_id);
  pool_tasks_[pool_task_id] = std::move(pool_task);
}

void ActorPoolManager::EraseTrackedPoolTask(const TaskID &pool_task_id) {
  auto pool_task_it = pool_tasks_.find(pool_task_id);
  if (pool_task_it == pool_tasks_.end()) {
    return;
  }

  const auto pool_id = pool_task_it->second.pool_id;
  pool_tasks_.erase(pool_task_it);

  auto pool_it = pool_to_tasks_.find(pool_id);
  if (pool_it == pool_to_tasks_.end()) {
    return;
  }

  pool_it->second.erase(pool_task_id);
  if (pool_it->second.empty()) {
    pool_to_tasks_.erase(pool_it);
  }
}

std::optional<PoolTask> ActorPoolManager::TakeTrackedPoolTask(
    const TaskID &pool_task_id) {
  auto pool_task_it = pool_tasks_.find(pool_task_id);
  if (pool_task_it == pool_tasks_.end()) {
    return std::nullopt;
  }

  const auto pool_id = pool_task_it->second.pool_id;
  PoolTask pool_task = std::move(pool_task_it->second);
  pool_tasks_.erase(pool_task_it);

  auto pool_it = pool_to_tasks_.find(pool_id);
  if (pool_it != pool_to_tasks_.end()) {
    pool_it->second.erase(pool_task_id);
    if (pool_it->second.empty()) {
      pool_to_tasks_.erase(pool_it);
    }
  }

  return pool_task;
}

void ActorPoolManager::CleanupTrackedPoolTasksForPool(const ActorPoolID &pool_id) {
  auto pool_it = pool_to_tasks_.find(pool_id);
  if (pool_it == pool_to_tasks_.end()) {
    return;
  }

  for (const auto &pool_task_id : pool_it->second) {
    pool_tasks_.erase(pool_task_id);
  }
  pool_to_tasks_.erase(pool_it);
}

void ActorPoolManager::DrainTaskQueue(const ActorPoolID &pool_id) {
  auto wq_it = task_queues_.find(pool_id);
  if (wq_it == task_queues_.end()) {
    return;
  }

  auto &task_queue = wq_it->second;
  while (task_queue->HasWork()) {
    auto pool_task = task_queue->Pop();
    if (!pool_task.has_value()) {
      break;
    }

    ActorID actor = SelectActorFromPool(pool_id,
                                        pool_task->arg_ids,
                                        /*exclude_actor_id=*/ActorID::Nil(),
                                        /*require_available_capacity=*/true);
    if (actor.IsNil()) {
      // No actors with capacity — push the item back and stop draining.
      task_queue->PushFront(std::move(*pool_task));
      break;
    }

    RAY_LOG(DEBUG) << "Draining queued pool task " << pool_task->pool_task_id
                   << " to actor " << actor << " in pool " << pool_id;
    SubmitToActor(pool_id, actor, std::move(*pool_task));
  }
}

}  // namespace core
}  // namespace ray
