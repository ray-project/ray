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

#include <list>
#include <memory>
#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/task_execution/actor_task_execution_queue_interface.h"
#include "ray/core_worker/task_execution/common.h"
#include "ray/core_worker/task_execution/concurrency_group_manager.h"
#include "ray/core_worker/task_execution/thread_pool.h"

namespace ray {
namespace core {

/// Used to ensure serial order of task execution per actor handle.
/// See core_worker.proto for a description of the ordering protocol.
class OrderedActorTaskExecutionQueue : public ActorTaskExecutionQueueInterface {
 public:
  OrderedActorTaskExecutionQueue(
      instrumented_io_context &task_execution_service,
      ActorTaskExecutionArgWaiterInterface &waiter,
      worker::TaskEventBuffer &task_event_buffer,
      ray::observability::RayEventRecorderInterface &ray_task_event_recorder,
      std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
      int64_t reorder_wait_seconds,
      ExecuteTaskCallback execute_task,
      CancelTaskCallback cancel_task);

  void Stop() override;

  void EnqueueTask(int64_t seq_no,
                   int64_t client_processed_up_to,
                   TaskToExecute task) override;

  /// Cancel the actor task in the queue.
  /// Tasks are in the queue if it is either queued, or executing.
  /// Return true if a task is in the queue. False otherwise.
  /// This method has to be THREAD-SAFE.
  bool CancelTaskIfFound(TaskID task_id) override;

 private:
  /// Cancel all tasks queued for execution.
  void CancelAllQueuedTasks(const std::string &msg);

  /// Executes as many queued tasks as are ready to execute.
  void ExecuteQueuedTasks();

  /// Accept the given TaskToExecute or reject it if the task attempt is
  /// canceled via CancelTaskIfFound.
  void AcceptRequestOrRejectIfCanceled(const TaskAttempt &task_attempt,
                                       TaskToExecute &request);

  /// Whether any pending attempt of the given task is already marked canceled.
  bool IsTaskCanceledLocked(const TaskID &task_id) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Drop one attempt's cancellation entry, and the task's entry with it once no
  /// attempt of that task is pending.
  void EraseTaskAttemptLocked(const TaskID &task_id, int32_t attempt_number)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void ExecuteRequest(TaskToExecute &&request);

  /// Per-concurrency-group ordering state.
  struct ConcurrencyGroupOrderingState {
    explicit ConcurrencyGroupOrderingState(instrumented_io_context &io_context)
        : wait_timer_(io_context) {}

    /// Sorted map of task callbacks keyed by their per-group sequence number.
    absl::btree_map<int64_t, TaskToExecute> pending_tasks;
    /// List of task retry requests (unordered within the group).
    std::list<TaskToExecute> pending_retry_tasks;
    /// Set of sequence numbers that can be skipped because they were retry seq no's.
    absl::flat_hash_set<int64_t> seq_no_to_skip;
    /// The next sequence number we are waiting for to arrive in this group.
    int64_t next_seq_no = 0;
    /// Waiting for an earlier seq no to arrive for this group. If this times out
    /// for any group, we will cancel all tasks across ALL groups for this client.
    boost::asio::deadline_timer wait_timer_;
  };

  instrumented_io_context &task_execution_service_;

  /// Max time in seconds to wait for an earlier seq no to arrive.
  const int64_t reorder_wait_seconds_;

  /// Per-concurrency-group ordering states.
  absl::flat_hash_map<std::string, ConcurrencyGroupOrderingState> group_states_;

  /// The id of the thread that constructed this scheduling queue.
  std::thread::id main_thread_id_;

  ActorTaskExecutionArgWaiterInterface &waiter_;

  worker::TaskEventBuffer &task_event_buffer_;

  /// Records task events to the event aggregator.
  ray::observability::RayEventRecorderInterface &ray_task_event_recorder_;

  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager_;

  /// Callbacks used to execute a queued task or reply that it's canceled.
  ExecuteTaskCallback execute_task_;
  CancelTaskCallback cancel_task_;

  /// Mutex to protect attributes used for thread safe APIs.
  absl::Mutex mu_;

  /// A map of actor task ids -> attempt number -> is_canceled
  /// Pending means tasks are queued or running. Attempts of one task are tracked
  /// independently, because two of them can be pending at the same time. A task's
  /// entry is erased once none of its attempts is pending, so an entry that exists
  /// always holds at least one attempt.
  absl::flat_hash_map<TaskID, absl::flat_hash_map<int32_t, bool>>
      pending_task_attempt_to_is_canceled ABSL_GUARDED_BY(mu_);

  friend class OrderedActorTaskExecutionQueueTest;
};

}  // namespace core
}  // namespace ray
