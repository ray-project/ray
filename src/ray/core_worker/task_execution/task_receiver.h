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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/task_execution/actor_task_execution_queue_interface.h"
#include "ray/core_worker/task_execution/concurrency_group_manager.h"
#include "ray/core_worker/task_execution/fiber.h"
#include "ray/core_worker/task_execution/normal_task_execution_queue.h"
#include "ray/core_worker/task_execution/thread_pool.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

class TaskReceiver {
 public:
  // Runs a single task. The handler reads its inputs (task spec, resource ids, reply)
  // from the TaskExecutionMetadata and writes the task's outputs back into the same
  // object's fields (return objects, errors, etc.).
  using TaskHandler = std::function<Status(TaskExecutionMetadata &task)>;

  TaskReceiver(instrumented_io_context &task_execution_service,
               worker::TaskEventBuffer &task_event_buffer,
               TaskHandler task_handler,
               ActorTaskExecutionArgWaiter &actor_task_execution_arg_waiter,
               std::function<std::function<void()>()> initialize_thread_callback)
      : task_handler_(std::move(task_handler)),
        task_execution_service_(task_execution_service),
        task_event_buffer_(task_event_buffer),
        waiter_(actor_task_execution_arg_waiter),
        initialize_thread_callback_(std::move(initialize_thread_callback)),
        execute_task_callback_(
            [this](TaskExecutionMetadata &task) { ExecuteTask(task); }),
        cancel_task_callback_([this](const TaskExecutionMetadata &task,
                                     const Status &status) { CancelTask(task, status); }),
        normal_task_execution_queue_(std::make_unique<NormalTaskExecutionQueue>(
            execute_task_callback_, cancel_task_callback_)),
        pool_manager_(std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>()),
        fiber_state_manager_(nullptr) {}

  /// Enqueue a task for execution that was received via `PushTask`.
  ///
  /// For actor tasks: the task will be enqueued and requests will be scheduled to begin
  /// execution if possible.
  ///
  /// For normal tasks: the task will only be enqueued and the caller must call
  /// `RunNormalTasksFromQueue` separately.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The reply callback.
  void QueueTaskForExecution(rpc::PushTaskRequest request,
                             rpc::PushTaskReply *reply,
                             rpc::SendReplyCallback send_reply_callback);

  /// Execute as many tasks from the queue as are available.
  void ExecuteQueuedNormalTasks();

  bool CancelQueuedNormalTask(TaskID task_id);

  /// Cancel an actor task that is queued for execution, but hasn't started executing yet.
  ///
  /// Returns true if the task is present in the executor at all. If false, it means the
  /// task either hasn't been received yet or has already finished executing.
  ///
  /// This method is idempotent.
  bool CancelQueuedActorTask(const WorkerID &caller_worker_id, const TaskID &task_id);

  void Stop();

 private:
  /// Set up the configs for an actor.
  /// This should be called once for the actor creation task.
  void SetupActor(bool is_asyncio,
                  int fiber_max_concurrency,
                  bool allow_out_of_order_execution);

  /// Populate the reply and trigger send_reply_callback based on the outputs of a
  /// completed task execution held in `task`.
  void HandleTaskExecutionResult(Status status, const TaskExecutionMetadata &task);

  /// Execute a task that was queued for execution. Invoked by the execution queues via
  /// `execute_task_callback_`. Reads all per-request state from `task`.
  void ExecuteTask(TaskExecutionMetadata &task);

  /// Reply that a queued task was canceled before it started executing. Invoked by the
  /// execution queues via `cancel_task_callback_`. Reads per-request state from `task`.
  void CancelTask(const TaskExecutionMetadata &task, const Status &status);

  // True once shutdown begins. Requests to execute new tasks will be rejected.
  std::atomic<bool> stopping_ = false;

  /// The callback function to process a task.
  TaskHandler task_handler_;

  /// The event loop for running tasks on.
  instrumented_io_context &task_execution_service_;

  worker::TaskEventBuffer &task_event_buffer_;

  /// Shared waiter for dependencies required by incoming tasks.
  ActorTaskExecutionArgWaiter &waiter_;

  /// The language-specific callback function that initializes threads.
  std::function<std::function<void()>()> initialize_thread_callback_;

  /// Queue-level callbacks passed to each execution queue at construction time. They
  /// capture only `this` and read all per-request state from the TaskExecutionMetadata
  /// argument. Declared before the queues so they are constructed first and outlive them.
  ExecuteTaskCallback execute_task_callback_;
  CancelTaskCallback cancel_task_callback_;

  /// Queue of actor tasks waiting to execute, keyed on the ID of the worker that
  /// submitted the task.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  absl::flat_hash_map<WorkerID, std::unique_ptr<ActorTaskExecutionQueueInterface>>
      actor_task_execution_queues_;

  // Queue of normal (non-actor) tasks waiting to execute.
  std::unique_ptr<NormalTaskExecutionQueue> normal_task_execution_queue_;

  /// The max number of concurrent calls to allow for fiber mode.
  /// 0 indicates that the value is not set yet.
  int fiber_max_concurrency_ = 0;

  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager_;

  /// If async calls are allowed, holds the fibers for executing async tasks.
  /// Only populated if this actor is async.
  std::shared_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager_;

  /// Whether this actor use asyncio for concurrency.
  bool is_asyncio_ = false;

  /// Whether this actor executes tasks out of order with respect to client submission
  /// order.
  bool allow_out_of_order_execution_ = false;

  /// The concurrency groups of this worker's actor, computed from actor creation task
  /// spec.
  std::vector<ConcurrencyGroup> concurrency_groups_;
};

}  // namespace core
}  // namespace ray
