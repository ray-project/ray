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
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "boost/asio/steady_timer.hpp"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/task_manager_interface.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

namespace {

std::string PoolStateDebugString(const ActorPoolID &pool_id,
                                 const ActorPoolInfo &pool_info,
                                 size_t backlog_size) {
  int32_t total_in_flight = 0;
  int32_t alive_actors = 0;
  for (const auto &[actor_id, state] : pool_info.actor_states) {
    total_in_flight += state.num_tasks_in_flight;
    if (state.is_alive) {
      alive_actors++;
    }
  }

  std::ostringstream stream;
  stream << "pool_id=" << pool_id << " alive_actors=" << alive_actors << "/"
         << pool_info.actor_ids.size() << " total_in_flight=" << total_in_flight
         << " backlog=" << backlog_size
         << " total_submitted=" << pool_info.total_tasks_submitted
         << " total_failed=" << pool_info.total_tasks_failed
         << " total_retried=" << pool_info.total_tasks_retried;
  return stream.str();
}

}  // namespace

ActorPoolManager::ActorPoolManager(ActorManager &actor_manager,
                                   ActorTaskSubmitterInterface &task_submitter,
                                   TaskManagerInterface &task_manager)
    : actor_manager_(actor_manager),
      task_submitter_(task_submitter),
      task_manager_(task_manager),
      io_service_(nullptr),
      submit_actor_task_fn_(nullptr) {
  RAY_LOG(DEBUG) << "ActorPoolManager initialized (minimal mode, no callbacks)";
}

ActorPoolManager::ActorPoolManager(ActorManager &actor_manager,
                                   ActorTaskSubmitterInterface &task_submitter,
                                   TaskManagerInterface &task_manager,
                                   instrumented_io_context &io_service,
                                   SubmitActorTaskCallback submit_actor_task_fn,
                                   LocalityDataProviderInterface *locality_data_provider)
    : actor_manager_(actor_manager),
      task_submitter_(task_submitter),
      task_manager_(task_manager),
      io_service_(&io_service),
      submit_actor_task_fn_(std::move(submit_actor_task_fn)),
      locality_data_provider_(locality_data_provider) {
  RAY_LOG(DEBUG) << "ActorPoolManager initialized (full mode with CoreWorker callbacks)";
}

ActorPoolID ActorPoolManager::RegisterPool(const ActorPoolConfig &config,
                                           const std::vector<ActorID> &initial_actors) {
  absl::MutexLock lock(&mu_);

  ActorPoolID pool_id = ActorPoolID::FromRandom();

  ActorPoolInfo pool_info;
  pool_info.config = config;

  auto work_queue = std::make_unique<UnorderedPoolWorkQueue>();

  for (const auto &actor_id : initial_actors) {
    pool_info.actor_ids.push_back(actor_id);
    pool_info.actor_states[actor_id] = ActorPoolActorState{};
    actor_to_pool_[actor_id] = pool_id;
  }

  pools_[pool_id] = std::move(pool_info);
  work_queues_[pool_id] = std::move(work_queue);

  RAY_LOG(INFO) << "ActorPoolDebug register-pool "
                << PoolStateDebugString(pool_id, pools_[pool_id], /*backlog_size=*/0);

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

  work_queues_.erase(pool_id);
  CleanupTrackedWorkItemsForPool(pool_id);
  pools_.erase(it);

  RAY_LOG(INFO) << "ActorPoolDebug unregister-pool pool_id=" << pool_id;
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

  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug add-actor actor_id=" << actor_id
                  << " location=" << location << " "
                  << PoolStateDebugString(pool_id, pool_info, wq_it->second->Size());
  }

  DrainWorkQueue(pool_id);
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

  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug remove-actor actor_id=" << actor_id << " "
                  << PoolStateDebugString(pool_id, pool_info, wq_it->second->Size());
  }
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

  auto &work_queue = work_queues_[pool_id];

  // Work item ID is an opaque unique key for retry tracking — it is NOT the
  // actual TaskID used for submission (SubmitActorTaskForPool creates that).
  // The nil JobID is fine because this ID never enters the task lineage.
  TaskID work_item_id = TaskID::FromRandom(JobID());
  PoolWorkItem work_item;
  work_item.pool_id = pool_id;
  work_item.work_item_id = work_item_id;
  work_item.function = function;
  work_item.args = std::move(args);
  work_item.options = task_options;
  work_item.attempt_number = 0;
  work_item.enqueued_at_ms = current_time_ms();

  // Precompute by-reference arg ObjectIDs for locality-aware scheduling.
  // Stored once in the work item so DrainWorkQueue and RetryWorkItem
  // can reuse them without re-serializing args.
  for (const auto &arg : work_item.args) {
    rpc::TaskArg arg_proto;
    arg->ToProto(&arg_proto);
    if (arg_proto.has_object_ref()) {
      work_item.arg_ids.push_back(
          ObjectID::FromBinary(arg_proto.object_ref().object_id()));
    }
  }

  ActorID selected_actor = SelectActorFromPool(pool_id, work_item.arg_ids);

  if (selected_actor.IsNil()) {
    RAY_LOG(DEBUG) << "No actors available in pool " << pool_id
                   << ", enqueueing work item " << work_item_id;
    work_queue->Push(std::move(work_item));
    return {};
  }

  return SubmitToActor(pool_id, selected_actor, std::move(work_item));
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

  auto &pool_info = pool_it->second;
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it == work_queues_.end()) {
    return PoolStats{};
  }
  const auto &work_queue = wq_it->second;

  PoolStats stats;
  stats.total_tasks_submitted = pool_info.total_tasks_submitted;
  stats.total_tasks_failed = pool_info.total_tasks_failed;
  stats.total_tasks_retried = pool_info.total_tasks_retried;
  stats.num_actors = static_cast<int32_t>(pool_info.actor_ids.size());
  stats.backlog_size = work_queue->Size();

  int32_t total_in_flight = 0;
  for (const auto &[actor_id, state] : pool_info.actor_states) {
    total_in_flight += state.num_tasks_in_flight;
  }
  stats.total_in_flight = total_in_flight;
  stats.waiting_for_actor_retries =
      static_cast<int32_t>(pool_info.waiting_for_actor_retry_task_ids.size());

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

  auto &pool_info = pool_it->second;
  int64_t total_in_flight = 0;
  for (const auto &[actor_id, state] : pool_info.actor_states) {
    total_in_flight += state.num_tasks_in_flight;
  }

  auto wq_it = work_queues_.find(pool_id);
  int64_t backlog = (wq_it != work_queues_.end()) ? wq_it->second->Size() : 0;

  // Count work items dispatched to ActorTaskSubmitter but not yet pushed to the
  // actor via gRPC (OnTaskSubmitted hasn't fired yet). Without this, tasks are
  // invisible between SubmitToActor and the async OnTaskSubmitted callback,
  // allowing the Python scheduling loop to over-submit.
  int64_t pending_submissions = 0;
  auto pw_it = pool_to_work_items_.find(pool_id);
  if (pw_it != pool_to_work_items_.end()) {
    for (const auto &work_item_id : pw_it->second) {
      auto wi_it = work_items_.find(work_item_id);
      if (wi_it != work_items_.end() && !wi_it->second.pushed_to_actor) {
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

void ActorPoolManager::MarkRetryWaitingForActor(const ActorPoolID &pool_id,
                                                const TaskID &task_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return;
  }

  pool_it->second.waiting_for_actor_retry_task_ids.insert(task_id);
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug retry-waiting-for-actor task_id=" << task_id << " "
                  << PoolStateDebugString(
                         pool_id, pool_it->second, wq_it->second->Size());
  }
}

void ActorPoolManager::ClearRetryWaitingForActor(const ActorPoolID &pool_id,
                                                 const TaskID &task_id) {
  absl::MutexLock lock(&mu_);

  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return;
  }

  pool_it->second.waiting_for_actor_retry_task_ids.erase(task_id);
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug retry-no-longer-waiting-for-actor task_id="
                  << task_id << " "
                  << PoolStateDebugString(
                         pool_id, pool_it->second, wq_it->second->Size());
  }
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
                                          const TaskID &work_item_id,
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
  auto wq_it = work_queues_.find(pool_id);
  RAY_CHECK(wq_it != work_queues_.end());
  RAY_LOG(INFO) << "ActorPoolDebug pool-task-complete work_item_id=" << work_item_id
                << " task_id=" << task_id << " actor_id=" << actor_id
                << " status=" << status.ToString() << " error_type="
                << (error_info == nullptr ? "OK"
                                          : rpc::ErrorType_Name(error_info->error_type()))
                << " "
                << PoolStateDebugString(pool_id, pool_it->second, wq_it->second->Size());

  if (status.ok()) {
    OnTaskSucceeded(pool_id, actor_id);
    EraseTrackedWorkItem(work_item_id);
  } else {
    if (error_info != nullptr) {
      OnTaskFailed(pool_id, work_item_id, actor_id, *error_info);
    } else {
      rpc::RayErrorInfo generic_error;
      generic_error.set_error_type(rpc::ErrorType::ACTOR_DIED);
      generic_error.set_error_message("Task failed with unknown error: " +
                                      status.ToString());
      OnTaskFailed(pool_id, work_item_id, actor_id, generic_error);
    }
  }
}

void ActorPoolManager::OnTaskSubmitted(const ActorID &actor_id,
                                       const TaskID &work_item_id) {
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

  // Mark the work item as pushed so it is no longer counted as a pending
  // submission in GetOccupiedTaskSlots.
  if (!work_item_id.IsNil()) {
    auto wi_it = work_items_.find(work_item_id);
    if (wi_it != work_items_.end()) {
      wi_it->second.pushed_to_actor = true;
    }
  }

  auto wq_it = work_queues_.find(pool_it->second);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug task-submitted actor_id=" << actor_id
                  << " work_item_id=" << work_item_id << " "
                  << PoolStateDebugString(
                         pool_it->second, info_it->second, wq_it->second->Size());
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

  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug actor-alive actor_id=" << actor_id
                  << " node_id=" << node_id << " "
                  << PoolStateDebugString(pool_id, pool_info, wq_it->second->Size());
  }

  DrainWorkQueue(pool_id);
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

  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug actor-dead actor_id=" << actor_id << " "
                  << PoolStateDebugString(
                         pool_id, info_it->second, wq_it->second->Size());
  }
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

  auto &pool_info = pool_it->second;

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
    RAY_LOG(INFO) << "ActorPoolDebug select-actor-none pool_id=" << pool_id
                  << " exclude_actor_id=" << exclude_actor_id
                  << " require_available_capacity=" << require_available_capacity
                  << " arg_count=" << arg_ids.size() << " "
                  << PoolStateDebugString(pool_id, pool_info, /*backlog_size=*/0);
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
    const ActorPoolID &pool_id, const ActorID &actor_id, PoolWorkItem work_item) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(ERROR) << "Pool not found: " << pool_id;
    return {};
  }

  auto &pool_info = pool_it->second;

  pool_info.total_tasks_submitted++;

  TaskID work_item_id = work_item.work_item_id;
  int32_t attempt_number = work_item.attempt_number;

  auto wq_it = work_queues_.find(pool_id);
  RAY_CHECK(wq_it != work_queues_.end());
  RAY_LOG(INFO) << "ActorPoolDebug submit-to-actor work_item_id=" << work_item_id
                << " actor_id=" << actor_id << " attempt=" << attempt_number << " "
                << PoolStateDebugString(pool_id, pool_info, wq_it->second->Size());

  if (!submit_actor_task_fn_) {
    RAY_LOG(WARNING) << "SubmitToActor called without submit callback (minimal mode)";
    pool_info.actor_states[actor_id].num_tasks_in_flight++;
    TrackWorkItem(std::move(work_item));
    return {};
  }

  // Clone args before moving work_item into work_items_ (we need args for submission).
  auto args_for_submit = CloneArgs(work_item.args);
  RayFunction function = work_item.function;
  TaskOptions options = work_item.options;

  TrackWorkItem(std::move(work_item));

  // Completion is NOT handled via this callback — it flows through
  // ActorTaskSubmitter::HandlePushTaskReply() → SetPoolTaskCompletionCallback()
  // → OnPoolTaskComplete(). We pass nullptr to avoid duplicate handling.
  return submit_actor_task_fn_(actor_id,
                               function,
                               std::move(args_for_submit),
                               options,
                               nullptr,
                               pool_id,
                               work_item_id);
}

void ActorPoolManager::OnTaskFailed(const ActorPoolID &pool_id,
                                    const TaskID &work_item_id,
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
  auto wq_it = work_queues_.find(pool_id);
  RAY_CHECK(wq_it != work_queues_.end());

  bool should_retry = ShouldRetryTask(pool_info.config, error_info);
  RAY_LOG(INFO) << "ActorPoolDebug task-failed work_item_id=" << work_item_id
                << " actor_id=" << failed_actor_id << " should_retry=" << should_retry
                << " error_type=" << rpc::ErrorType_Name(error_info.error_type()) << " "
                << PoolStateDebugString(pool_id, pool_info, wq_it->second->Size());

  if (!should_retry) {
    RAY_LOG(INFO) << "Work item " << work_item_id << " failed with non-retriable error, "
                  << "not retrying. Error: " << error_info.error_message();
    FailWorkItem(work_item_id, error_info);
    return;
  }

  auto work_item = TakeTrackedWorkItem(work_item_id);
  if (!work_item.has_value()) {
    RAY_LOG(WARNING) << "Work item " << work_item_id << " not found for retry";
    return;
  }

  work_item->attempt_number++;

  if (pool_info.config.max_retry_attempts >= 0 &&
      work_item->attempt_number > pool_info.config.max_retry_attempts) {
    RAY_LOG(INFO) << "Work item " << work_item_id << " exceeded max retry attempts ("
                  << pool_info.config.max_retry_attempts << "), failing permanently";
    FailWorkItem(work_item_id, error_info);
    return;
  }

  pool_info.total_tasks_retried++;

  int64_t backoff_ms = CalculateBackoff(work_item->attempt_number,
                                        pool_info.config.retry_backoff_ms,
                                        pool_info.config.retry_backoff_multiplier,
                                        pool_info.config.max_retry_backoff_ms);

  RAY_LOG(INFO) << "Work item " << work_item_id << " failed on actor " << failed_actor_id
                << ", retrying (attempt " << work_item->attempt_number << ") after "
                << backoff_ms << "ms on different actor in pool " << pool_id;

  ScheduleRetry(pool_id, std::move(*work_item), backoff_ms);
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
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it != work_queues_.end()) {
    RAY_LOG(INFO) << "ActorPoolDebug task-succeeded actor_id=" << actor_id << " "
                  << PoolStateDebugString(
                         pool_id, pool_it->second, wq_it->second->Size());
  }

  DrainWorkQueue(pool_id);
}

void ActorPoolManager::ScheduleRetry(const ActorPoolID &pool_id,
                                     PoolWorkItem work_item,
                                     int64_t backoff_ms) {
  if (backoff_ms <= 0) {
    RetryWorkItem(pool_id, std::move(work_item));
    return;
  }

  if (!io_service_) {
    RAY_LOG(DEBUG) << "No io_service available, performing immediate retry for work item "
                   << work_item.work_item_id;
    RetryWorkItem(pool_id, std::move(work_item));
    return;
  }

  RAY_LOG(DEBUG) << "Scheduling retry for work item " << work_item.work_item_id
                 << " with backoff " << backoff_ms << "ms";

  // shared_ptr so the timer outlives this scope until the callback fires.
  auto timer = std::make_shared<boost::asio::steady_timer>(
      *io_service_, std::chrono::milliseconds(backoff_ms));

  // Captures `this` — safe because ActorPoolManager outlives CoreWorker's io_service.
  timer->async_wait([this, pool_id, work_item = std::move(work_item), timer](
                        const boost::system::error_code &ec) mutable {
    if (ec) {
      RAY_LOG(WARNING) << "Timer error during retry scheduling: " << ec.message();
      return;
    }
    absl::MutexLock lock(&mu_);
    RetryWorkItem(pool_id, std::move(work_item));
  });
}

void ActorPoolManager::RetryWorkItem(const ActorPoolID &pool_id, PoolWorkItem work_item) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(WARNING) << "Pool not found during retry: " << pool_id;
    return;
  }

  auto &work_queue = work_queues_[pool_id];

  ActorID selected_actor = SelectActorFromPool(pool_id, work_item.arg_ids);

  if (selected_actor.IsNil()) {
    RAY_LOG(DEBUG) << "No actors available for retry of work item "
                   << work_item.work_item_id << ", re-enqueueing";
    work_queue->Push(std::move(work_item));
    return;
  }

  RAY_LOG(INFO) << "Retrying work item " << work_item.work_item_id << " on actor "
                << selected_actor << " (attempt " << work_item.attempt_number << ")";

  SubmitToActor(pool_id, selected_actor, std::move(work_item));
}

bool ActorPoolManager::ShouldRetryTask(const ActorPoolConfig &config,
                                       const rpc::RayErrorInfo &error_info) const {
  if (!config.retry_on_system_errors) {
    return false;
  }

  switch (error_info.error_type()) {
  case rpc::ErrorType::ACTOR_DIED:
  case rpc::ErrorType::ACTOR_UNAVAILABLE:
  case rpc::ErrorType::NODE_DIED:
  case rpc::ErrorType::WORKER_DIED:
    return true;

  case rpc::ErrorType::TASK_CANCELLED:
    return false;

  case rpc::ErrorType::TASK_EXECUTION_EXCEPTION:
  case rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED:
  case rpc::ErrorType::OUT_OF_MEMORY:
    // Retrying OOM on a different actor is unlikely to help.
    return false;

  default:
    RAY_LOG(WARNING) << "Unknown error type: " << error_info.error_type()
                     << ", not retrying";
    return false;
  }
}

int64_t ActorPoolManager::CalculateBackoff(int32_t attempt_number,
                                           int32_t base_backoff_ms,
                                           float multiplier,
                                           int32_t max_backoff_ms) const {
  if (attempt_number <= 1) {
    return base_backoff_ms;
  }

  // Exponential backoff: base * multiplier^(attempt-1)
  int64_t backoff = base_backoff_ms;
  for (int32_t i = 1; i < attempt_number; i++) {
    backoff = static_cast<int64_t>(backoff * multiplier);
    if (backoff > max_backoff_ms) {
      backoff = max_backoff_ms;
      break;
    }
  }

  return std::min(backoff, static_cast<int64_t>(max_backoff_ms));
}

void ActorPoolManager::FailWorkItem(const TaskID &work_item_id,
                                    const rpc::RayErrorInfo &error_info) {
  EraseTrackedWorkItem(work_item_id);

  RAY_LOG(INFO) << "Work item " << work_item_id
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

void ActorPoolManager::TrackWorkItem(PoolWorkItem work_item) {
  RAY_CHECK(!work_item.pool_id.IsNil()) << "Tracked work item must belong to a pool";

  const auto pool_id = work_item.pool_id;
  const auto work_item_id = work_item.work_item_id;
  pool_to_work_items_[pool_id].insert(work_item_id);
  work_items_[work_item_id] = std::move(work_item);
}

void ActorPoolManager::EraseTrackedWorkItem(const TaskID &work_item_id) {
  auto work_item_it = work_items_.find(work_item_id);
  if (work_item_it == work_items_.end()) {
    return;
  }

  const auto pool_id = work_item_it->second.pool_id;
  work_items_.erase(work_item_it);

  auto pool_it = pool_to_work_items_.find(pool_id);
  if (pool_it == pool_to_work_items_.end()) {
    return;
  }

  pool_it->second.erase(work_item_id);
  if (pool_it->second.empty()) {
    pool_to_work_items_.erase(pool_it);
  }
}

std::optional<PoolWorkItem> ActorPoolManager::TakeTrackedWorkItem(
    const TaskID &work_item_id) {
  auto work_item_it = work_items_.find(work_item_id);
  if (work_item_it == work_items_.end()) {
    return std::nullopt;
  }

  const auto pool_id = work_item_it->second.pool_id;
  PoolWorkItem work_item = std::move(work_item_it->second);
  work_items_.erase(work_item_it);

  auto pool_it = pool_to_work_items_.find(pool_id);
  if (pool_it != pool_to_work_items_.end()) {
    pool_it->second.erase(work_item_id);
    if (pool_it->second.empty()) {
      pool_to_work_items_.erase(pool_it);
    }
  }

  return work_item;
}

void ActorPoolManager::CleanupTrackedWorkItemsForPool(const ActorPoolID &pool_id) {
  auto pool_it = pool_to_work_items_.find(pool_id);
  if (pool_it == pool_to_work_items_.end()) {
    return;
  }

  for (const auto &work_item_id : pool_it->second) {
    work_items_.erase(work_item_id);
  }
  pool_to_work_items_.erase(pool_it);
}

void ActorPoolManager::DrainWorkQueue(const ActorPoolID &pool_id) {
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it == work_queues_.end()) {
    return;
  }

  auto &work_queue = wq_it->second;
  while (work_queue->HasWork()) {
    auto work_item = work_queue->Pop();
    if (!work_item.has_value()) {
      break;
    }

    ActorID actor = SelectActorFromPool(pool_id, work_item->arg_ids);
    if (actor.IsNil()) {
      // No actors available — push the item back and stop draining.
      work_queue->Push(std::move(*work_item));
      break;
    }

    RAY_LOG(DEBUG) << "Draining queued work item " << work_item->work_item_id
                   << " to actor " << actor << " in pool " << pool_id;
    SubmitToActor(pool_id, actor, std::move(*work_item));
  }
}

}  // namespace core
}  // namespace ray
