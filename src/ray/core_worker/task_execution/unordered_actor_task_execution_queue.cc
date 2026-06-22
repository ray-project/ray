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

#include "ray/core_worker/task_execution/unordered_actor_task_execution_queue.h"

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

UnorderedActorTaskExecutionQueue::UnorderedActorTaskExecutionQueue(
    instrumented_io_context &io_service,
    std::shared_ptr<Postable> default_postable,
    ActorTaskExecutionArgWaiterInterface &waiter,
    worker::TaskEventBuffer &task_event_buffer,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    std::shared_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager,
    bool is_asyncio,
    int fiber_max_concurrency,
    const std::vector<ConcurrencyGroup> &concurrency_groups)
    : io_service_(io_service),
      default_postable_(std::move(default_postable)),
      main_thread_id_(std::this_thread::get_id()),
      waiter_(waiter),
      task_event_buffer_(task_event_buffer),
      pool_manager_(pool_manager),
      fiber_state_manager_(fiber_state_manager),
      is_asyncio_(is_asyncio) {
  if (is_asyncio_) {
    std::stringstream ss;
    ss << "Setting actor as asyncio with max_concurrency=" << fiber_max_concurrency
       << ", and defined concurrency groups are:" << std::endl;
    for (const auto &concurrency_group : concurrency_groups) {
      ss << "\t" << concurrency_group.name_ << " : "
         << concurrency_group.max_concurrency_;
    }
    RAY_LOG(INFO) << ss.str();
  }
}

void UnorderedActorTaskExecutionQueue::CancelAllQueuedTasks(const std::string &msg) {
  absl::MutexLock lock(&mu_);

  Status status = Status::SchedulingCancelled(msg);

  while (!queued_actor_tasks_.empty()) {
    auto it = queued_actor_tasks_.begin();
    it->second.Cancel(status);
    pending_task_id_to_is_canceled.erase(it->first);
    queued_actor_tasks_.erase(it);
  }
}

void UnorderedActorTaskExecutionQueue::Stop() {
  if (pool_manager_) {
    pool_manager_->Stop();
  }
  if (fiber_state_manager_) {
    fiber_state_manager_->Stop();
  }

  CancelAllQueuedTasks("Actor task execution queue stopped; canceling all queued tasks.");
}

void UnorderedActorTaskExecutionQueue::EnqueueTask(int64_t seq_no,
                                                   int64_t client_processed_up_to,
                                                   TaskToExecute task) {
  // Add and execute a task. For different attempts of the same
  // task id, if an attempt is running, the other attempt will
  // wait until the first attempt finishes so that no more
  // than one attempt of the same task run at the same time.
  // The reason why we don't run multiple attempts of the same
  // task concurrently is that it's not safe to assume user's
  // code can handle concurrent execution of the same actor method.
  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
  TaskID task_id = task.TaskID();
  bool run_task = true;
  std::optional<TaskToExecute> task_to_cancel;
  {
    absl::MutexLock lock(&mu_);
    if (pending_task_id_to_is_canceled.contains(task_id)) {
      // There is a previous attempt of the same task running,
      // queue the current attempt.
      run_task = false;

      auto it = queued_actor_tasks_.find(task_id);
      if (it != queued_actor_tasks_.end()) {
        // There is already an attempt of the same task queued,
        // keep the one with larger attempt number and cancel the other one.
        RAY_CHECK_NE(it->second.AttemptNumber(), task.AttemptNumber());
        if (it->second.AttemptNumber() > task.AttemptNumber()) {
          // This can happen if the PushTaskRequest arrives out of order.
          task_to_cancel = std::move(task);
        } else {
          task_to_cancel = std::move(it->second);
          queued_actor_tasks_.insert_or_assign(task_id, std::move(task));
        }
      } else {
        queued_actor_tasks_.emplace(task_id, std::move(task));
      }
    } else {
      pending_task_id_to_is_canceled.emplace(task_id, false);
      run_task = true;
    }
  }

  if (run_task) {
    RunRequest(std::move(task));
  }

  if (task_to_cancel.has_value()) {
    task_to_cancel->Cancel(Status::SchedulingCancelled(
        "In favor of the same task with larger attempt number"));
  }
}

bool UnorderedActorTaskExecutionQueue::CancelTaskIfFound(TaskID task_id) {
  absl::MutexLock lock(&mu_);
  if (pending_task_id_to_is_canceled.find(task_id) !=
      pending_task_id_to_is_canceled.end()) {
    // Mark the task is canceled.
    pending_task_id_to_is_canceled[task_id] = true;
    return true;
  } else {
    return false;
  }
}

void UnorderedActorTaskExecutionQueue::RunRequestWithResolvedDependencies(
    TaskToExecute request) {
  RAY_CHECK(request.DependenciesResolved());
  const auto task_id = request.TaskID();

  // Pick where to run the user task body: fiber, concurrency-group pool,
  // or the default postable.
  std::shared_ptr<Postable> post_execute;
  if (is_asyncio_) {
    post_execute = fiber_state_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                                     request.FunctionDescriptor());
  } else {
    RAY_CHECK(pool_manager_ != nullptr);
    auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                           request.FunctionDescriptor());
    post_execute = pool ? std::shared_ptr<Postable>(std::move(pool)) : default_postable_;
  }
  AcceptRequestOrRejectIfCanceled(task_id, std::move(request), std::move(post_execute));
}

void UnorderedActorTaskExecutionQueue::RunRequest(TaskToExecute request) {
  const TaskSpecification &task_spec = request.TaskSpec();
  if (!request.PendingDependencies().empty()) {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_spec.TaskId(),
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH,
        /* include_task_info */ false));
    // Make a copy since request is going to be moved.
    auto dependencies = request.PendingDependencies();
    waiter_.AsyncWait(dependencies, [this, request = std::move(request)]() mutable {
      RAY_CHECK_EQ(std::this_thread::get_id(), main_thread_id_);

      const TaskSpecification &task = request.TaskSpec();
      RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
          task.TaskId(),
          task.JobId(),
          task.AttemptNumber(),
          task,
          rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
          /* include_task_info */ false));

      request.MarkDependenciesResolved();
      RunRequestWithResolvedDependencies(std::move(request));
    });
  } else {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_spec.TaskId(),
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
        /* include_task_info */ false));
    request.MarkDependenciesResolved();
    RunRequestWithResolvedDependencies(std::move(request));
  }
}

void UnorderedActorTaskExecutionQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, TaskToExecute request, std::shared_ptr<Postable> post_execute) {
  bool is_canceled = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_task_id_to_is_canceled.find(task_id);
    if (it != pending_task_id_to_is_canceled.end()) {
      is_canceled = it->second;
    }
  }

  auto post_task_cleanup = [this, task_id]() {
    std::optional<TaskToExecute> request_to_run;
    {
      absl::MutexLock lock(&mu_);
      auto it = queued_actor_tasks_.find(task_id);
      if (it != queued_actor_tasks_.end()) {
        request_to_run = std::move(it->second);
        queued_actor_tasks_.erase(it);
      } else {
        pending_task_id_to_is_canceled.erase(task_id);
      }
    }
    if (request_to_run.has_value()) {
      io_service_.post(
          [this, request = std::move(*request_to_run)]() mutable {
            RunRequest(std::move(request));
          },
          "UnorderedActorTaskExecutionQueue.RunRequest");
    }
  };

  if (is_canceled) {
    request.Cancel(
        Status::SchedulingCancelled("Task is canceled before it is scheduled."));
    post_task_cleanup();
    return;
  }

  // Execute can be very long, and we shouldn't hold a lock.
  auto execute_handler = [request = std::move(request), post_task_cleanup]() mutable {
    request.Execute();
    post_task_cleanup();
  };
  if (is_asyncio_) {
    // For asyncio actors post_execute is a FiberState, whose Post() blocks the
    // caller until the fiber runner picks the task up (unbuffered fiber-channel
    // rendezvous, see fiber.h). This code runs on io_service_, the single thread
    // that also services health-check and other RPCs, so blocking here can stall
    // the whole worker. Hand the blocking dispatch to task_execution_service_
    // instead; arg fetching still runs on io_service_, so pipelining is unaffected.
    default_postable_->Post([post_execute = std::move(post_execute),
                             execute_handler = std::move(execute_handler)]() mutable {
      post_execute->Post(std::move(execute_handler));
    });
  } else {
    post_execute->Post(std::move(execute_handler));
  }
}

}  // namespace core
}  // namespace ray
