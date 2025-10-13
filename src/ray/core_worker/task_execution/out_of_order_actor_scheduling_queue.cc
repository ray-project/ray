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

#include "ray/core_worker/task_execution/out_of_order_actor_scheduling_queue.h"

#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

OutOfOrderActorSchedulingQueue::OutOfOrderActorSchedulingQueue(
    instrumented_io_context &task_execution_service,
    DependencyWaiter &waiter,
    worker::TaskEventBuffer &task_event_buffer,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    std::shared_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager,
    bool is_asyncio,
    int fiber_max_concurrency,
    const std::vector<ConcurrencyGroup> &concurrency_groups)
    : task_execution_service_(task_execution_service),
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

void OutOfOrderActorSchedulingQueue::Stop() {
  if (pool_manager_) {
    pool_manager_->Stop();
  }
  if (fiber_state_manager_) {
    fiber_state_manager_->Stop();
  }
  CancelAllPending(Status::SchedulingCancelled(
      "Out-of-order actor scheduling queue stopped; canceling pending tasks"));
}

bool OutOfOrderActorSchedulingQueue::TaskQueueEmpty() const {
  RAY_LOG(FATAL) << "TaskQueueEmpty() not implemented for actor queues";
  return false;
}

size_t OutOfOrderActorSchedulingQueue::Size() const {
  RAY_LOG(FATAL) << "Size() not implemented for actor queues";
  return 0;
}

void OutOfOrderActorSchedulingQueue::ScheduleRequests() {
  RAY_LOG(FATAL) << "ScheduleRequests() not implemented for actor queues";
}

void OutOfOrderActorSchedulingQueue::Add(
    int64_t seq_no,
    int64_t client_processed_up_to,
    std::function<void(const TaskSpecification &, rpc::SendReplyCallback)> accept_request,
    std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
        reject_request,
    rpc::SendReplyCallback send_reply_callback,
    TaskSpecification task_spec) {
  // Add and execute a task. For different attempts of the same
  // task id, if an attempt is running, the other attempt will
  // wait until the first attempt finishes so that no more
  // than one attempt of the same task run at the same time.
  // The reason why we don't run multiple attempts of the same
  // task concurrently is that it's not safe to assume user's
  // code can handle concurrent execution of the same actor method.
  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
  auto task_id = task_spec.TaskId();
  auto request = InboundRequest(std::move(accept_request),
                                std::move(reject_request),
                                std::move(send_reply_callback),
                                std::move(task_spec));
  bool run_request = true;
  std::optional<InboundRequest> request_to_cancel;
  {
    absl::MutexLock lock(&mu_);
    if (pending_task_id_to_is_canceled.contains(task_id)) {
      // There is a previous attempt of the same task running,
      // queue the current attempt.
      run_request = false;

      if (queued_actor_tasks_.contains(task_id)) {
        // There is already an attempt of the same task queued,
        // keep the one with larger attempt number and cancel the other one.
        RAY_CHECK_NE(queued_actor_tasks_[task_id].AttemptNumber(),
                     request.AttemptNumber());
        if (queued_actor_tasks_[task_id].AttemptNumber() > request.AttemptNumber()) {
          // This can happen if the PushTaskRequest arrives out of order.
          request_to_cancel = request;
        } else {
          request_to_cancel = queued_actor_tasks_[task_id];
          queued_actor_tasks_[task_id] = request;
        }
      } else {
        queued_actor_tasks_[task_id] = request;
      }
    } else {
      pending_task_id_to_is_canceled.emplace(task_id, false);
      run_request = true;
    }
  }

  if (run_request) {
    RunRequest(std::move(request));
  }

  if (request_to_cancel.has_value()) {
    request_to_cancel->Cancel(Status::SchedulingCancelled(
        "In favor of the same task with larger attempt number"));
  }
}

bool OutOfOrderActorSchedulingQueue::CancelTaskIfFound(TaskID task_id) {
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

void OutOfOrderActorSchedulingQueue::RunRequestWithResolvedDependencies(
    InboundRequest &request) {
  RAY_CHECK(request.DependenciesResolved());
  const auto task_id = request.TaskID();
  if (is_asyncio_) {
    // Process async actor task.
    auto fiber = fiber_state_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                                   request.FunctionDescriptor());
    fiber->EnqueueFiber([this, request, task_id]() mutable {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    });
  } else {
    // Process actor tasks.
    RAY_CHECK(pool_manager_ != nullptr);
    auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                           request.FunctionDescriptor());
    if (pool == nullptr) {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    } else {
      pool->Post([this, request, task_id]() mutable {
        AcceptRequestOrRejectIfCanceled(task_id, request);
      });
    }
  }
}

void OutOfOrderActorSchedulingQueue::RunRequest(InboundRequest request) {
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
    waiter_.Wait(dependencies, [this, request = std::move(request)]() mutable {
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
      RunRequestWithResolvedDependencies(request);
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
    RunRequestWithResolvedDependencies(request);
  }
}

void OutOfOrderActorSchedulingQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, InboundRequest &request) {
  bool is_canceled = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_task_id_to_is_canceled.find(task_id);
    if (it != pending_task_id_to_is_canceled.end()) {
      is_canceled = it->second;
    }
  }

  // Accept can be very long, and we shouldn't hold a lock.
  if (is_canceled) {
    request.Cancel(
        Status::SchedulingCancelled("Task is canceled before it is scheduled."));
  } else {
    request.Accept();
  }

  std::optional<InboundRequest> request_to_run;
  {
    absl::MutexLock lock(&mu_);
    if (queued_actor_tasks_.contains(task_id)) {
      request_to_run = queued_actor_tasks_[task_id];
      queued_actor_tasks_.erase(task_id);
    } else {
      pending_task_id_to_is_canceled.erase(task_id);
    }
  }

  if (request_to_run.has_value()) {
    task_execution_service_.post(
        [this, request = std::move(*request_to_run)]() mutable {
          RunRequest(std::move(request));
        },
        "OutOfOrderActorSchedulingQueue.RunRequest");
  }
}

void OutOfOrderActorSchedulingQueue::CancelAllPending(const Status &status) {
  absl::MutexLock lock(&mu_);
  while (!queued_actor_tasks_.empty()) {
    auto it = queued_actor_tasks_.begin();
    it->second.Cancel(status);
    pending_task_id_to_is_canceled.erase(it->first);
    queued_actor_tasks_.erase(it);
  }
}

}  // namespace core
}  // namespace ray
