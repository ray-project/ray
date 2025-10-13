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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/task_execution/actor_scheduling_queue.h"
#include "ray/core_worker/task_execution/concurrency_group_manager.h"
#include "ray/core_worker/task_execution/fiber.h"
#include "ray/core_worker/task_execution/normal_scheduling_queue.h"
#include "ray/core_worker/task_execution/out_of_order_actor_scheduling_queue.h"
#include "ray/core_worker/task_execution/thread_pool.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;
using RepeatedObjectRefCount =
    ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>;

class TaskReceiver {
 public:
  using TaskHandler = std::function<Status(
      const TaskSpecification &task_spec,
      std::optional<ResourceMappingType> resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
          *dynamic_return_objects,
      std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
      RepeatedObjectRefCount *borrower_refs,
      bool *is_retryable_error,
      std::string *application_error)>;

  using OnActorCreationTaskDone = std::function<Status()>;

  TaskReceiver(instrumented_io_context &task_execution_service,
               worker::TaskEventBuffer &task_event_buffer,
               TaskHandler task_handler,
               DependencyWaiter &dependency_waiter,
               std::function<std::function<void()>()> initialize_thread_callback,
               OnActorCreationTaskDone actor_creation_task_done)
      : task_handler_(std::move(task_handler)),
        task_execution_service_(task_execution_service),
        task_event_buffer_(task_event_buffer),
        waiter_(dependency_waiter),
        initialize_thread_callback_(std::move(initialize_thread_callback)),
        actor_creation_task_done_(std::move(actor_creation_task_done)),
        pool_manager_(std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>()),
        fiber_state_manager_(nullptr) {}

  /// Handle a `PushTask` request. If it's an actor request, this function will enqueue
  /// the task and then start scheduling the requests to begin the execution. If it's a
  /// non-actor request, this function will just enqueue the task.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleTask(rpc::PushTaskRequest request,
                  rpc::PushTaskReply *reply,
                  rpc::SendReplyCallback send_reply_callback);

  /// Pop tasks from the queue and execute them sequentially
  void RunNormalTasksFromQueue();

  bool CancelQueuedNormalTask(TaskID task_id);

  /// Cancel an actor task that is queued for execution, but hasn't started executing yet.
  ///
  /// Returns true if the task is present in the executor at all. If false, it means the
  /// task either hasn't been received yet or has already finished executing.
  ///
  /// This method is idempotent.
  bool CancelQueuedActorTask(const WorkerID &caller_worker_id, const TaskID &task_id);

  void Stop();

  /// Set the actor repr name for an actor.
  ///
  /// The actor repr name is only available after actor creation task has been run since
  /// the repr name could include data only initialized during the creation task.
  void SetActorReprName(const std::string &repr_name);

 private:
  /// Guard for shutdown state.
  absl::Mutex stop_mu_;
  // True once shutdown begins. Requests to execute new tasks will be rejected.
  bool stopping_ ABSL_GUARDED_BY(stop_mu_) = false;
  /// Set up the configs for an actor.
  /// This should be called once for the actor creation task.
  void SetupActor(bool is_asyncio,
                  int fiber_max_concurrency,
                  bool allow_out_of_order_execution);

  /// The callback function to process a task.
  TaskHandler task_handler_;

  /// The event loop for running tasks on.
  instrumented_io_context &task_execution_service_;

  worker::TaskEventBuffer &task_event_buffer_;

  /// Shared waiter for dependencies required by incoming tasks.
  DependencyWaiter &waiter_;

  /// The language-specific callback function that initializes threads.
  std::function<std::function<void()>()> initialize_thread_callback_;

  /// The callback function to be invoked when finishing a task.
  OnActorCreationTaskDone actor_creation_task_done_;

  /// Queue of pending requests per actor handle.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  absl::flat_hash_map<WorkerID, std::unique_ptr<SchedulingQueue>>
      actor_scheduling_queues_;

  // Queue of pending normal (non-actor) tasks.
  std::unique_ptr<SchedulingQueue> normal_scheduling_queue_ =
      std::make_unique<NormalSchedulingQueue>();

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

  /// The repr name of the actor instance for an anonymous actor.
  /// This is only available after the actor creation task.
  std::string actor_repr_name_;

  /// The concurrency groups of this worker's actor, computed from actor creation task
  /// spec.
  std::vector<ConcurrencyGroup> concurrency_groups_;
};

}  // namespace core
}  // namespace ray
