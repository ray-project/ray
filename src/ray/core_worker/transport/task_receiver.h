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
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/actor_scheduling_queue.h"
#include "ray/core_worker/transport/actor_task_submitter.h"
#include "ray/core_worker/transport/concurrency_group_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/normal_scheduling_queue.h"
#include "ray/core_worker/transport/out_of_order_actor_scheduling_queue.h"
#include "ray/core_worker/transport/thread_pool.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace core {

class TaskReceiver {
 public:
  using TaskHandler = std::function<Status(
      const TaskSpecification &task_spec,
      std::optional<ResourceMappingType> resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
          *dynamic_return_objects,
      std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
      ReferenceCounter::ReferenceTableProto *borrower_refs,
      bool *is_retryable_error,
      std::string *application_error)>;

  using OnActorCreationTaskDone = std::function<Status()>;

  TaskReceiver(instrumented_io_context &task_execution_service,
               worker::TaskEventBuffer &task_event_buffer,
               TaskHandler task_handler,
               std::function<std::function<void()>()> initialize_thread_callback,
               const OnActorCreationTaskDone &actor_creation_task_done)
      : task_handler_(std::move(task_handler)),
        task_execution_service_(task_execution_service),
        task_event_buffer_(task_event_buffer),
        initialize_thread_callback_(std::move(initialize_thread_callback)),
        actor_creation_task_done_(actor_creation_task_done),
        pool_manager_(std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>()),
        fiber_state_manager_(nullptr) {}

  /// Initialize this receiver. This must be called prior to use.
  void Init(std::shared_ptr<rpc::CoreWorkerClientPool>,
            rpc::Address rpc_address,
            DependencyWaiter *dependency_waiter);

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
  /// Set up the configs for an actor.
  /// This should be called once for the actor creation task.
  void SetupActor(bool is_asyncio, int fiber_max_concurrency, bool execute_out_of_order);

 protected:
  /// Cache the concurrency groups of actors.
  // TODO(ryw): remove the ActorID key since we only ever handle 1 actor.
  absl::flat_hash_map<ActorID, std::vector<ConcurrencyGroup>> concurrency_groups_cache_;

 private:
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The event loop for running tasks on.
  instrumented_io_context &task_execution_service_;
  worker::TaskEventBuffer &task_event_buffer_;
  /// The language-specific callback function that initializes threads.
  std::function<std::function<void()>()> initialize_thread_callback_;
  /// The callback function to be invoked when finishing a task.
  OnActorCreationTaskDone actor_creation_task_done_;
  /// Shared pool for producing new core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool_;
  /// Address of our RPC server.
  rpc::Address rpc_address_;
  /// Shared waiter for dependencies required by incoming tasks.
  DependencyWaiter *waiter_ = nullptr;
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
  bool execute_out_of_order_ = false;
  /// The repr name of the actor instance for an anonymous actor.
  /// This is only available after the actor creation task.
  std::string actor_repr_name_;
};

}  // namespace core
}  // namespace ray
