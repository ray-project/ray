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
#include <string>
#include <utility>
#include <vector>

#include "boost/asio/steady_timer.hpp"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/task_manager_interface.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

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
                                   SubmitActorTaskCallback submit_actor_task_fn)
    : actor_manager_(actor_manager),
      task_submitter_(task_submitter),
      task_manager_(task_manager),
      io_service_(&io_service),
      submit_actor_task_fn_(std::move(submit_actor_task_fn)) {
  RAY_LOG(DEBUG) << "ActorPoolManager initialized (full mode with CoreWorker callbacks)";
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

  // TODO(C2): Clean up work_items_ belonging to this pool. Currently work items
  // for unregistered pools leak (TaskArg objects held). Need to add pool_id field
  // to PoolWorkItem or maintain a reverse index for efficient cleanup.

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

  // If actor already in pool, update location if provided.
  auto state_it = pool_info.actor_states.find(actor_id);
  if (state_it != pool_info.actor_states.end()) {
    if (!location.IsNil()) {
      state_it->second.location = location;
    }
    return;
  }

  // Add actor
  pool_info.actor_ids.push_back(actor_id);
  pool_info.actor_states[actor_id] = ActorPoolActorState{
      .num_tasks_in_flight = 0, .location = location, .is_alive = true};
  actor_to_pool_[actor_id] = pool_id;

  RAY_LOG(DEBUG) << "Added actor " << actor_id << " to pool " << pool_id;

  // New actor has capacity — drain any queued work.
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

  auto &work_queue = work_queues_[pool_id];

  // Extract argument object IDs for locality-aware scheduling
  std::vector<ObjectID> arg_ids;
  for (const auto &arg : args) {
    // Check if this is a by-reference argument
    rpc::TaskArg arg_proto;
    arg->ToProto(&arg_proto);
    if (arg_proto.has_object_ref()) {
      arg_ids.push_back(ObjectID::FromBinary(arg_proto.object_ref().object_id()));
    }
  }

  // Create unique work item ID
  // Note: Using empty JobID since this is just an internal tracking ID
  TaskID work_item_id = TaskID::FromRandom(JobID());
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

  // Submit to selected actor
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

  const auto &pool_info = pool_it->second;
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

ActorID ActorPoolManager::SelectActorForTask(const ActorPoolID &pool_id,
                                             const std::vector<ObjectID> &arg_ids) {
  absl::MutexLock lock(&mu_);
  return SelectActorFromPool(pool_id, arg_ids);
}

void ActorPoolManager::OnPoolTaskComplete(const ActorPoolID &pool_id,
                                          const TaskID &work_item_id,
                                          const TaskID &task_id,
                                          const ActorID &actor_id,
                                          const Status &status,
                                          const rpc::RayErrorInfo *error_info) {
  RAY_LOG(DEBUG) << "Pool task complete: pool=" << pool_id
                 << ", work_item=" << work_item_id << ", task=" << task_id
                 << ", actor=" << actor_id << ", status=" << status.ToString();

  absl::MutexLock lock(&mu_);

  // Check if pool still exists
  auto pool_it = pools_.find(pool_id);
  if (pool_it == pools_.end()) {
    RAY_LOG(DEBUG) << "Pool " << pool_id << " no longer exists, ignoring task completion";
    return;
  }

  if (status.ok()) {
    OnTaskSucceeded(pool_id, actor_id);
    // Remove the work item from tracking
    work_items_.erase(work_item_id);
  } else {
    if (error_info != nullptr) {
      OnTaskFailed(pool_id, work_item_id, actor_id, *error_info);
    } else {
      // Create a generic error info if none provided
      rpc::RayErrorInfo generic_error;
      generic_error.set_error_type(rpc::ErrorType::ACTOR_DIED);
      generic_error.set_error_message("Task failed with unknown error: " +
                                      status.ToString());
      OnTaskFailed(pool_id, work_item_id, actor_id, generic_error);
    }
  }
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
    const int32_t max_concurrency = pool_info.config.max_tasks_in_flight_per_actor;

    if (state.is_alive && state.num_tasks_in_flight < max_concurrency) {
      candidates.push_back(actor_id);
    }
  }

  if (candidates.empty()) {
    RAY_LOG(DEBUG) << "No available actors in pool " << pool_id;
    return ActorID::Nil();
  }

  // Select the actor with the lowest rank (best choice)
  auto best_actor = *std::min_element(
      candidates.begin(), candidates.end(), [&](const ActorID &a, const ActorID &b) {
        return RankActor(a, arg_ids, pool_info) < RankActor(b, arg_ids, pool_info);
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
    const ActorPoolID &pool_id, const ActorID &actor_id, PoolWorkItem work_item) {
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

  TaskID work_item_id = work_item.work_item_id;
  int32_t attempt_number = work_item.attempt_number;

  RAY_LOG(DEBUG) << "Submitting work item " << work_item_id << " to actor " << actor_id
                 << " in pool " << pool_id << " (attempt " << attempt_number << ")";

  // Check if we have the submit callback (full CoreWorker integration)
  if (!submit_actor_task_fn_) {
    RAY_LOG(WARNING) << "SubmitToActor called without submit callback (minimal mode)";
    // Store work item for potential retry tracking even in minimal mode
    work_items_[work_item_id] = std::move(work_item);
    return {};
  }

  // Clone args before moving work_item into work_items_ (we need args for submission)
  auto args_for_submit = CloneArgs(work_item.args);
  RayFunction function = work_item.function;
  TaskOptions options = work_item.options;

  // Store work item for retry tracking
  work_items_[work_item_id] = std::move(work_item);

  // Note: We don't pass a completion callback here because task completion is now
  // handled via ActorTaskSubmitter::HandlePushTaskReply() → MaybeNotifyPoolTaskComplete()
  // → CoreWorker callback → ActorPoolManager::OnPoolTaskComplete().
  // This avoids duplicate completion handling.

  // Submit via CoreWorker callback (which builds TaskSpec properly)
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

  // Update actor state
  auto actor_state_it = pool_info.actor_states.find(failed_actor_id);
  if (actor_state_it != pool_info.actor_states.end()) {
    auto &actor_state = actor_state_it->second;
    if (actor_state.num_tasks_in_flight > 0) {
      actor_state.num_tasks_in_flight--;
    }
    actor_state.consecutive_failures++;
  }

  pool_info.total_tasks_failed++;

  // Classify error to determine if we should retry
  bool should_retry = ShouldRetryTask(pool_info.config, error_info);

  if (!should_retry) {
    RAY_LOG(INFO) << "Work item " << work_item_id << " failed with non-retriable error, "
                  << "not retrying. Error: " << error_info.error_message();
    FailWorkItem(work_item_id, error_info);
    return;
  }

  // Get work item
  auto work_item_it = work_items_.find(work_item_id);
  if (work_item_it == work_items_.end()) {
    RAY_LOG(WARNING) << "Work item " << work_item_id << " not found for retry";
    return;
  }

  auto work_item = std::move(work_item_it->second);
  work_items_.erase(work_item_it);

  // Increment attempt number
  work_item.attempt_number++;

  // Check if we've exceeded max retries
  if (pool_info.config.max_retry_attempts >= 0 &&
      work_item.attempt_number > pool_info.config.max_retry_attempts) {
    RAY_LOG(INFO) << "Work item " << work_item_id << " exceeded max retry attempts ("
                  << pool_info.config.max_retry_attempts << "), failing permanently";
    FailWorkItem(work_item_id, error_info);
    return;
  }

  pool_info.total_tasks_retried++;

  // Calculate backoff for retry
  int64_t backoff_ms = CalculateBackoff(work_item.attempt_number,
                                        pool_info.config.retry_backoff_ms,
                                        pool_info.config.retry_backoff_multiplier,
                                        pool_info.config.max_retry_backoff_ms);

  RAY_LOG(INFO) << "Work item " << work_item_id << " failed on actor " << failed_actor_id
                << ", retrying (attempt " << work_item.attempt_number << ") after "
                << backoff_ms << "ms on different actor in pool " << pool_id;

  // Schedule retry with backoff
  ScheduleRetry(pool_id, std::move(work_item), backoff_ms);
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

  // Now that an actor has capacity, drain any queued work.
  DrainWorkQueue(pool_id);
}

void ActorPoolManager::ScheduleRetry(const ActorPoolID &pool_id,
                                     PoolWorkItem work_item,
                                     int64_t backoff_ms) {
  if (backoff_ms <= 0) {
    // Immediate retry
    RetryWorkItem(pool_id, std::move(work_item));
    return;
  }

  // Check if we have io_service for delayed scheduling
  if (!io_service_) {
    RAY_LOG(DEBUG) << "No io_service available, performing immediate retry for work item "
                   << work_item.work_item_id;
    RetryWorkItem(pool_id, std::move(work_item));
    return;
  }

  RAY_LOG(DEBUG) << "Scheduling retry for work item " << work_item.work_item_id
                 << " with backoff " << backoff_ms << "ms";

  // Create a shared_ptr to the timer so it stays alive until the callback fires
  auto timer = std::make_shared<boost::asio::steady_timer>(
      *io_service_, std::chrono::milliseconds(backoff_ms));

  // Schedule the delayed retry
  // Note: We capture 'this' and rely on ActorPoolManager outliving the timer
  // In practice, ActorPoolManager lives as long as CoreWorker
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

  // Extract arg IDs for locality-aware scheduling
  std::vector<ObjectID> arg_ids;
  for (const auto &arg : work_item.args) {
    rpc::TaskArg arg_proto;
    arg->ToProto(&arg_proto);
    if (arg_proto.has_object_ref()) {
      arg_ids.push_back(ObjectID::FromBinary(arg_proto.object_ref().object_id()));
    }
  }

  // Select DIFFERENT actor (likely, due to load balancing)
  ActorID selected_actor = SelectActorFromPool(pool_id, arg_ids);

  if (selected_actor.IsNil()) {
    // No actors available, re-enqueue to wait for capacity
    RAY_LOG(DEBUG) << "No actors available for retry of work item "
                   << work_item.work_item_id << ", re-enqueueing";
    work_queue->Push(std::move(work_item));
    return;
  }

  RAY_LOG(INFO) << "Retrying work item " << work_item.work_item_id << " on actor "
                << selected_actor << " (attempt " << work_item.attempt_number << ")";

  // Submit to (likely different) actor
  SubmitToActor(pool_id, selected_actor, std::move(work_item));
}

bool ActorPoolManager::ShouldRetryTask(const ActorPoolConfig &config,
                                       const rpc::RayErrorInfo &error_info) const {
  if (!config.retry_on_system_errors) {
    return false;
  }

  // Classify error types
  switch (error_info.error_type()) {
  case rpc::ErrorType::ACTOR_DIED:
  case rpc::ErrorType::ACTOR_UNAVAILABLE:
  case rpc::ErrorType::NODE_DIED:
  case rpc::ErrorType::WORKER_DIED:
    // System errors - should retry on a different actor
    return true;

  case rpc::ErrorType::TASK_CANCELLED:
    // Don't retry cancelled tasks
    return false;

  case rpc::ErrorType::TASK_EXECUTION_EXCEPTION:
  case rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED:
  case rpc::ErrorType::OUT_OF_MEMORY:
    // User/resource errors - don't retry (retrying OOM on a different actor
    // is unlikely to help since the task will likely OOM again).
    return false;

  default:
    // Unknown error - be conservative, don't retry
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
  // Remove work item from tracking
  work_items_.erase(work_item_id);

  RAY_LOG(INFO) << "Work item " << work_item_id
                << " failed permanently. Error: " << error_info.error_message();

  // Note: The actual task failure is handled by the TaskManager via the completion
  // callback. We just need to clean up our tracking state here.
}

std::vector<std::unique_ptr<TaskArg>> ActorPoolManager::CloneArgs(
    const std::vector<std::unique_ptr<TaskArg>> &args) const {
  std::vector<std::unique_ptr<TaskArg>> cloned_args;
  cloned_args.reserve(args.size());

  for (const auto &arg : args) {
    // Serialize to proto and create a new TaskArg from it
    rpc::TaskArg arg_proto;
    arg->ToProto(&arg_proto);

    if (arg_proto.has_object_ref()) {
      // By-reference argument
      cloned_args.push_back(std::make_unique<TaskArgByReference>(
          ObjectID::FromBinary(arg_proto.object_ref().object_id()),
          rpc::Address(arg_proto.object_ref().owner_address()),
          arg_proto.object_ref().call_site()));
    } else if (!arg_proto.data().empty() || !arg_proto.metadata().empty()) {
      // By-value argument (has data or metadata)
      // Note: const_cast is safe here because copy_data=true causes
      // LocalMemoryBuffer to make a deep copy of the data immediately.
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

      // Extract nested refs (as ObjectReference, not ObjectID)
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

void ActorPoolManager::DrainWorkQueue(const ActorPoolID &pool_id) {
  auto wq_it = work_queues_.find(pool_id);
  if (wq_it == work_queues_.end()) {
    return;
  }

  auto &work_queue = wq_it->second;
  while (work_queue->HasWork()) {
    // Try to find an available actor with capacity.
    ActorID actor = SelectActorFromPool(pool_id, /*arg_ids=*/{});
    if (actor.IsNil()) {
      // No actors with capacity — stop draining.
      break;
    }

    auto work_item = work_queue->Pop();
    if (!work_item.has_value()) {
      break;
    }

    RAY_LOG(DEBUG) << "Draining queued work item " << work_item->work_item_id
                   << " to actor " << actor << " in pool " << pool_id;
    SubmitToActor(pool_id, actor, std::move(*work_item));
  }
}

}  // namespace core
}  // namespace ray
