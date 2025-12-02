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

#include <memory>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/task_execution/concurrency_group_manager.h"
#include "ray/core_worker/task_execution/fiber.h"
#include "ray/core_worker/task_execution/scheduling_queue.h"
#include "ray/core_worker/task_execution/scheduling_util.h"
#include "ray/core_worker/task_execution/thread_pool.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray {
namespace core {

/// This queue schedule the actor tasks as soon as the dependency is resolved,
/// and ignores the ordering (sequence_no) by the submitting client.
class OutOfOrderActorSchedulingQueue : public SchedulingQueue {
 public:
  OutOfOrderActorSchedulingQueue(
      instrumented_io_context &task_execution_service,
      DependencyWaiter &waiter,
      worker::TaskEventBuffer &task_event_buffer,
      std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
      std::shared_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager,
      bool is_asyncio,
      int fiber_max_concurrency,
      const std::vector<ConcurrencyGroup> &concurrency_groups);

  void Stop() override;

  bool TaskQueueEmpty() const override;

  size_t Size() const override;

  /// Add a new actor task's callbacks to the worker queue.
  void Add(int64_t seq_no,
           int64_t client_processed_up_to,
           std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
               accept_request,
           std::function<void(const TaskSpecification &,
                              const Status &,
                              rpc::SendReplyCallback)> reject_request,
           rpc::SendReplyCallback send_reply_callback,
           TaskSpecification task_spec) override;

  /// Cancel the actor task in the queue.
  /// Tasks are in the queue if it is either queued, or executing.
  /// Return true if a task is in the queue. False otherwise.
  /// This method has to be THREAD-SAFE.
  bool CancelTaskIfFound(TaskID task_id) override;

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() override;

  /// Cancel all pending (not yet accepted/executing) requests in the queue.
  void CancelAllPending(const Status &status) override;

 private:
  void RunRequest(InboundRequest request);

  void RunRequestWithResolvedDependencies(InboundRequest &request);

  /// Accept the given InboundRequest or reject it if a task id is canceled via
  /// CancelTaskIfFound.
  void AcceptRequestOrRejectIfCanceled(TaskID task_id, InboundRequest &request);

  instrumented_io_context &task_execution_service_;
  /// The id of the thread that constructed this scheduling queue.
  std::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  worker::TaskEventBuffer &task_event_buffer_;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager_;
  /// Manage the running fiber states of actors in this worker. It works with
  /// python asyncio if this is an asyncio actor.
  std::shared_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool is_asyncio_ = false;
  /// Mutext to protect attributes used for thread safe APIs.
  absl::Mutex mu_;
  /// This stores all the tasks that have previous attempts that are pending.
  /// They are queued and will be executed after the previous attempt finishes.
  /// This can happen if transient network error happens after an actor
  /// task is submitted and received by the actor and the caller retries
  /// the same task.
  absl::flat_hash_map<TaskID, InboundRequest> queued_actor_tasks_ ABSL_GUARDED_BY(mu_);
  /// A map of actor task IDs -> is_canceled.
  // Pending means tasks are queued or running.
  absl::flat_hash_map<TaskID, bool> pending_task_id_to_is_canceled ABSL_GUARDED_BY(mu_);

  FRIEND_TEST(OutOfOrderActorSchedulingQueueTest, TestSameTaskMultipleAttempts);
  FRIEND_TEST(OutOfOrderActorSchedulingQueueTest,
              TestSameTaskMultipleAttemptsCancellation);
};

}  // namespace core
}  // namespace ray
