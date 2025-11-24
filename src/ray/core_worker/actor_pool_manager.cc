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

#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/task_manager_interface.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

ActorPoolManager::ActorPoolManager(ActorManager &actor_manager,
                                   ActorTaskSubmitterInterface &task_submitter,
                                   TaskManagerInterface &task_manager)
    : actor_manager_(actor_manager),
      task_submitter_(task_submitter),
      task_manager_(task_manager) {
  RAY_LOG(INFO) << "ActorPoolManager initialized";
}

ActorPoolID ActorPoolManager::RegisterPool(const ActorPoolConfig &config,
                                           const std::vector<ActorID> &initial_actors) {
  absl::MutexLock lock(&mu_);
  
  // Generate a new pool ID
  ActorPoolID pool_id = ActorPoolID::FromRandom();
  
  // Create pool info
  ActorPoolInfo pool_info;
  pool_info.config = config;
  
  // Create work queue based on ordering mode
  std::unique_ptr<PoolWorkQueue> work_queue;
  switch (config.ordering_mode) {
    case PoolOrderingMode::UNORDERED:
      work_queue = std::make_unique<UnorderedPoolWorkQueue>();
      break;
    case PoolOrderingMode::PER_KEY_FIFO:
      // Phase 2: Implement PerKeyOrderedPoolWorkQueue
      RAY_LOG(FATAL) << "PER_KEY_FIFO ordering not yet implemented";
      break;
    case PoolOrderingMode::GLOBAL_FIFO:
      // Phase 2: Could use UnorderedPoolWorkQueue with single key
      RAY_LOG(FATAL) << "GLOBAL_FIFO ordering not yet implemented";
      break;
  }
  
  // Add initial actors
  for (const auto &actor_id : initial_actors) {
    pool_info.actor_ids.push_back(actor_id);
    pool_info.actor_states[actor_id] = ActorPoolActorState{};
    actor_to_pool_[actor_id] = pool_id;
  }
  
  // Register the pool
  pools_[pool_id] = std::move(pool_info);
  work_queues_[pool_id] = std::move(work_queue);
  
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
  
  // Remove actor-to-pool mappings
  for (const auto &actor_id : it->second.actor_ids) {
    actor_to_pool_.erase(actor_id);
  }
  
  // Remove pool
  work_queues_.erase(pool_id);
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
  
  // Check if actor already in pool
  if (pool_info.actor_states.find(actor_id) != pool_info.actor_states.end()) {
    RAY_LOG(WARNING) << "Actor " << actor_id << " already in pool " << pool_id;
    return;
  }
  
  // Add actor
  pool_info.actor_ids.push_back(actor_id);
  pool_info.actor_states[actor_id] = ActorPoolActorState{
      .num_tasks_in_flight = 0, .location = location, .is_alive = true};
  actor_to_pool_[actor_id] = pool_id;
  
  RAY_LOG(DEBUG) << "Added actor " << actor_id << " to pool " << pool_id;
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
  
  // Remove from actor list
  auto &actor_ids = pool_info.actor_ids;
  actor_ids.erase(std::remove(actor_ids.begin(), actor_ids.end(), actor_id),
                  actor_ids.end());
  
  // Remove state
  pool_info.actor_states.erase(actor_id);
  actor_to_pool_.erase(actor_id);
  
  RAY_LOG(DEBUG) << "Removed actor " << actor_id << " from pool " << pool_id;
}

std::vector<rpc::ObjectReference> ActorPoolManager::SubmitTaskToPool(
    const ActorPoolID &pool_id,
    const RayFunction &function,
    std::vector<std::unique_ptr<TaskArg>> args,
    const TaskOptions &task_options,
    const std::string &key) {
  absl::MutexLock lock(&mu_);
  
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(ERROR) << "Pool not found: " << pool_id;
    return {};
  }
  
  auto &pool_info = pool_it->second;
  auto &work_queue = work_queues_[pool_id];
  
  // Extract argument object IDs for locality-aware scheduling
  std::vector<ObjectID> arg_ids;
  for (const auto &arg : args) {
    if (arg->IsPassedByReference()) {
      arg_ids.push_back(arg->GetReference().OwnedByAddress()
                            ? arg->GetReference().ObjectID()
                            : ObjectID::Nil());
    }
  }
  
  // Create work item
  // Note: This will need proper JobID from WorkerContext when integrated with CoreWorker
  TaskID work_item_id = TaskID::ForNormalTask(JobID(), TaskID(), 0);
  PoolWorkItem work_item;
  work_item.work_item_id = work_item_id;
  work_item.function = function;
  work_item.args = std::move(args);
  work_item.options = task_options;
  work_item.key = key;
  work_item.attempt_number = 0;
  work_item.enqueued_at_ms = current_time_ms();
  
  // Select actor from pool
  ActorID selected_actor = SelectActorFromPool(pool_id, arg_ids);
  
  if (selected_actor.IsNil()) {
    // No actors available, enqueue work
    RAY_LOG(DEBUG) << "No actors available in pool " << pool_id
                   << ", enqueueing work item " << work_item_id;
    work_queue->Push(std::move(work_item));
    return {};  // Return empty refs; work will be submitted when actor becomes available
  }
  
  // Store work item for retry tracking before submission
  auto work_item_copy = std::move(work_item);
  
  // Submit to selected actor
  return SubmitToActor(pool_id, selected_actor, std::move(work_item_copy));
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
  const auto &work_queue = work_queues_.at(pool_id);
  
  PoolStats stats;
  stats.total_tasks_submitted = pool_info.total_tasks_submitted;
  stats.total_tasks_failed = pool_info.total_tasks_failed;
  stats.total_tasks_retried = pool_info.total_tasks_retried;
  stats.num_actors = static_cast<int32_t>(pool_info.actor_ids.size());
  stats.backlog_size = work_queue->Size();
  
  // Calculate total in-flight
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

ActorID ActorPoolManager::SelectActorFromPool(const ActorPoolID &pool_id,
                                               const std::vector<ObjectID> &arg_ids) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(WARNING) << "Pool not found: " << pool_id;
    return ActorID::Nil();
  }
  
  const auto &pool_info = pool_it->second;
  
  // Filter: only alive actors with available capacity
  std::vector<ActorID> candidates;
  for (const auto &actor_id : pool_info.actor_ids) {
    auto state_it = pool_info.actor_states.find(actor_id);
    if (state_it == pool_info.actor_states.end()) {
      continue;
    }
    
    const auto &state = state_it->second;
    // TODO: Get actual max_concurrency from actor handle
    // For now, assume max_concurrency = 1 (single-threaded actors)
    const int32_t max_concurrency = 1;
    
    if (state.is_alive && state.num_tasks_in_flight < max_concurrency) {
      candidates.push_back(actor_id);
    }
  }
  
  if (candidates.empty()) {
    RAY_LOG(DEBUG) << "No available actors in pool " << pool_id;
    return ActorID::Nil();
  }
  
  // Select the actor with the lowest rank (best choice)
  auto best_actor =
      *std::min_element(candidates.begin(),
                        candidates.end(),
                        [&](const ActorID &a, const ActorID &b) {
                          return RankActor(a, arg_ids, pool_info) <
                                 RankActor(b, arg_ids, pool_info);
                        });
  
  RAY_LOG(DEBUG) << "Selected actor " << best_actor << " from pool " << pool_id;
  return best_actor;
}

int32_t ActorPoolManager::RankActor(const ActorID &actor_id,
                                    const std::vector<ObjectID> &arg_ids,
                                    const ActorPoolInfo &pool_info) const {
  auto state_it = pool_info.actor_states.find(actor_id);
  if (state_it == pool_info.actor_states.end()) {
    return INT32_MAX;  // Worst rank
  }
  
  const auto &state = state_it->second;
  
  // Simple ranking: lower is better
  // For Phase 1, rank purely by load (number of in-flight tasks)
  // Phase 2 can add locality awareness
  int32_t rank = state.num_tasks_in_flight;
  
  // TODO(Phase 2): Add locality-aware ranking
  // int32_t locality_rank = GetLocalityRank(state.location, arg_ids);
  // rank = locality_rank * 10000 + state.num_tasks_in_flight;
  
  return rank;
}

std::vector<rpc::ObjectReference> ActorPoolManager::SubmitToActor(
    const ActorPoolID &pool_id,
    const ActorID &actor_id,
    PoolWorkItem work_item) {
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(ERROR) << "Pool not found: " << pool_id;
    return {};
  }
  
  auto &pool_info = pool_it->second;
  
  // Update actor state
  auto &actor_state = pool_info.actor_states[actor_id];
  actor_state.num_tasks_in_flight++;
  pool_info.total_tasks_submitted++;
  
  RAY_LOG(DEBUG) << "Submitting work item " << work_item.work_item_id 
                 << " to actor " << actor_id << " in pool " << pool_id
                 << " (attempt " << work_item.attempt_number << ")";
  
  // TODO(Phase 1, To-do #10): Full implementation requires CoreWorker integration
  // Will need to:
  // 1. Get actor handle from actor_manager_
  // 2. Build TaskSpec with pool metadata (actor_pool_id, actor_pool_work_item_id)
  // 3. Register task callback for failure handling
  // 4. Submit via task_submitter_
  // 5. Return object references
  //
  // Stub for now - this will be implemented when ActorPoolManager is integrated
  // into CoreWorker (To-do #10)
  
  RAY_LOG(WARNING) << "SubmitToActor is a stub pending CoreWorker integration";
  return {};
}

void ActorPoolManager::OnTaskFailed(const ActorPoolID &pool_id,
                                    const TaskID &work_item_id,
                                    const ActorID &failed_actor_id,
                                    const rpc::RayErrorInfo &error_info) {
  // TODO(Phase 1): Implement failure handling and retry
  // This will be implemented in To-do #9
  RAY_LOG(FATAL) << "OnTaskFailed not yet implemented";
}

void ActorPoolManager::OnTaskSucceeded(const ActorPoolID &pool_id,
                                       const ActorID &actor_id) {
  // Decrement in-flight count
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    return;
  }
  
  auto &actor_state = pool_it->second.actor_states[actor_id];
  if (actor_state.num_tasks_in_flight > 0) {
    actor_state.num_tasks_in_flight--;
    actor_state.consecutive_failures = 0;  // Reset on success
  }
}

void ActorPoolManager::ScheduleRetry(const ActorPoolID &pool_id,
                                     PoolWorkItem work_item,
                                     int64_t backoff_ms) {
  // TODO(Phase 1): Implement retry scheduling
  // This will be implemented in To-do #9
  RAY_LOG(FATAL) << "ScheduleRetry not yet implemented";
}

void ActorPoolManager::RetryWorkItem(const ActorPoolID &pool_id,
                                     PoolWorkItem work_item) {
  // TODO(Phase 1): Implement work item retry
  // This will be implemented in To-do #9
  RAY_LOG(FATAL) << "RetryWorkItem not yet implemented";
}

bool ActorPoolManager::ShouldRetryTask(const ActorPoolConfig &config,
                                       const rpc::RayErrorInfo &error_info) const {
  // TODO(Phase 1): Implement error classification
  // This will be implemented in To-do #9
  return false;
}

int64_t ActorPoolManager::CalculateBackoff(int32_t attempt_number,
                                           int32_t base_backoff_ms,
                                           float multiplier,
                                           int32_t max_backoff_ms) const {
  // TODO(Phase 1): Implement backoff calculation
  // This will be implemented in To-do #9
  return 0;
}

void ActorPoolManager::FailWorkItem(const TaskID &work_item_id,
                                    const rpc::RayErrorInfo &error_info) {
  // TODO(Phase 1): Implement permanent failure
  // This will be implemented in To-do #9
  RAY_LOG(FATAL) << "FailWorkItem not yet implemented";
}

}  // namespace core
}  // namespace ray
