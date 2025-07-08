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

#include "ray/core_worker/transport/actor_scheduling_queue.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace ray {
namespace core {

ActorSchedulingQueue::ActorSchedulingQueue(
    instrumented_io_context &task_execution_service,
    DependencyWaiter &waiter,
    worker::TaskEventBuffer &task_event_buffer,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    int64_t reorder_wait_seconds)
    : reorder_wait_seconds_(reorder_wait_seconds),
      wait_timer_(task_execution_service),
      main_thread_id_(std::this_thread::get_id()),
      waiter_(waiter),
      task_event_buffer_(task_event_buffer),
      pool_manager_(std::move(pool_manager)) {}

void ActorSchedulingQueue::Stop() { pool_manager_->Stop(); }

bool ActorSchedulingQueue::TaskQueueEmpty() const {
  RAY_CHECK(false) << "TaskQueueEmpty() not implemented for actor queues";
  // The return instruction will never be executed, but we need to include it
  // nonetheless because this is a non-void function.
  return false;
}

size_t ActorSchedulingQueue::Size() const {
  RAY_CHECK(false) << "Size() not implemented for actor queues";
  // The return instruction will never be executed, but we need to include it
  // nonetheless because this is a non-void function.
  return 0;
}

/// Add a new actor task's callbacks to the worker queue.
void ActorSchedulingQueue::Add(
    int64_t seq_no,
    int64_t client_processed_up_to,
    std::function<void(const TaskSpecification &, rpc::SendReplyCallback)> accept_request,
    std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
        reject_request,
    rpc::SendReplyCallback send_reply_callback,
    TaskSpecification task_spec) {
  // A seq_no of -1 means no ordering constraint. Actor tasks that are not retries must be
  // executed in order.
  RAY_CHECK(seq_no != -1 || task_spec.IsRetry());

  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
  if (client_processed_up_to >= next_seq_no_) {
    RAY_LOG(INFO) << "client skipping requests " << next_seq_no_ << " to "
                  << client_processed_up_to;
    next_seq_no_ = client_processed_up_to + 1;
  }
  auto task_id = task_spec.TaskId();
  RAY_LOG(DEBUG).WithField(task_id) << "Enqueuing in order actor task, seq_no=" << seq_no
                                    << ", next_seq_no_=" << next_seq_no_;

  const auto dependencies = task_spec.GetDependencies();
  if (seq_no == -1 && !dependencies.empty()) {
    retries_pending_args_fetch_.emplace(task_id,
                                        InboundRequest(std::move(accept_request),
                                                       std::move(reject_request),
                                                       std::move(send_reply_callback),
                                                       task_spec));
  } else if (seq_no == -1) {
    retries_pending_execution_.emplace(task_id,
                                       InboundRequest(std::move(accept_request),
                                                      std::move(reject_request),
                                                      std::move(send_reply_callback),
                                                      task_spec));
  } else {
    RAY_CHECK(pending_actor_tasks_
                  .emplace(seq_no,
                           InboundRequest(std::move(accept_request),
                                          std::move(reject_request),
                                          std::move(send_reply_callback),
                                          task_spec))
                  .second);
  }
  {
    absl::MutexLock lock(&mu_);
    pending_task_id_to_is_canceled.emplace(task_spec.TaskId(), false);
  }

  if (!dependencies.empty()) {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_spec.TaskId(),
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH,
        /* include_task_info */ false));
    waiter_.Wait(dependencies, [this, seq_no, task_id]() mutable {
      RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
      InboundRequest *inbound_request = nullptr;
      if (auto it = pending_actor_tasks_.find(seq_no); it != pending_actor_tasks_.end()) {
        inbound_request = &it->second;
      } else if (auto it = retries_pending_args_fetch_.find(task_id);
                 it != retries_pending_args_fetch_.end()) {
        auto [new_it, inserted] =
            retries_pending_execution_.emplace(task_id, std::move(it->second));
        retries_pending_args_fetch_.erase(it);
        RAY_CHECK(inserted);
        inbound_request = &new_it->second;
      }
      if (inbound_request != nullptr) {
        const auto &task_spec = inbound_request->TaskSpec();
        RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
            task_spec.TaskId(),
            task_spec.JobId(),
            task_spec.AttemptNumber(),
            task_spec,
            rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
            /* include_task_info */ false));
        inbound_request->MarkDependenciesSatisfied();
        ScheduleRequests();
      }
    });
  } else {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_spec.TaskId(),
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
        /* include_task_info */ false));
  }

  ScheduleRequests();
}

bool ActorSchedulingQueue::CancelTaskIfFound(TaskID task_id) {
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

/// Schedules as many requests as possible in sequence.
void ActorSchedulingQueue::ScheduleRequests() {
  // Cancel any stale requests that the client doesn't need any longer.
  while (!pending_actor_tasks_.empty() &&
         pending_actor_tasks_.begin()->first < next_seq_no_) {
    auto head = pending_actor_tasks_.begin();
    RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                   << pending_actor_tasks_.begin()->first << " < " << next_seq_no_;
    head->second.Cancel(Status::Invalid("client cancelled stale rpc"));
    {
      absl::MutexLock lock(&mu_);
      pending_task_id_to_is_canceled.erase(head->second.TaskID());
    }
    pending_actor_tasks_.erase(head);
  }

  // Process as many in-order requests as we can.
  while (!retries_pending_execution_.empty() ||
         (!pending_actor_tasks_.empty() &&
          pending_actor_tasks_.begin()->first == next_seq_no_ &&
          pending_actor_tasks_.begin()->second.CanExecute())) {
    InboundRequest request;
    TaskID task_id;
    if (!retries_pending_execution_.empty()) {
      auto it = retries_pending_execution_.begin();
      task_id = it->second.TaskID();
      request = std::move(it->second);
      retries_pending_execution_.erase(it);
    } else {
      auto it = pending_actor_tasks_.begin();
      task_id = it->second.TaskID();
      request = std::move(it->second);
      pending_actor_tasks_.erase(it);
      next_seq_no_++;
    }
    // Process actor tasks.
    auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                           request.FunctionDescriptor());
    if (pool == nullptr) {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    } else {
      pool->Post([this, request = std::move(request), task_id]() mutable {
        AcceptRequestOrRejectIfCanceled(task_id, request);
      });
    }
  }

  if (pending_actor_tasks_.empty() ||
      !pending_actor_tasks_.begin()->second.CanExecute()) {
    // No timeout for object dependency waits.
    wait_timer_.cancel();
  } else {
    // Set a timeout on the queued tasks to avoid an infinite wait on failure.
    wait_timer_.expires_from_now(boost::posix_time::seconds(reorder_wait_seconds_));
    RAY_LOG(DEBUG) << "waiting for " << next_seq_no_ << " queue size "
                   << pending_actor_tasks_.size();
    wait_timer_.async_wait([this](const boost::system::error_code &error) {
      if (error == boost::asio::error::operation_aborted) {
        return;  // time deadline was adjusted
      }
      OnSequencingWaitTimeout();
    });
  }
}

/// Called when we time out waiting for an earlier task to show up.
void ActorSchedulingQueue::OnSequencingWaitTimeout() {
  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
  RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                 << ", cancelling all queued tasks";
  while (!pending_actor_tasks_.empty()) {
    auto head = pending_actor_tasks_.begin();
    head->second.Cancel(Status::Invalid("client cancelled stale rpc"));
    next_seq_no_ = std::max(next_seq_no_, head->first + 1);
    {
      absl::MutexLock lock(&mu_);
      pending_task_id_to_is_canceled.erase(head->second.TaskID());
    }
    pending_actor_tasks_.erase(head);
  }
}

void ActorSchedulingQueue::AcceptRequestOrRejectIfCanceled(TaskID task_id,
                                                           InboundRequest &request) {
  bool is_canceled = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_task_id_to_is_canceled.find(task_id);
    if (it != pending_task_id_to_is_canceled.end()) {
      is_canceled = it->second;
      pending_task_id_to_is_canceled.erase(it);
    }
  }

  // Accept can be very long, and we shouldn't hold a lock.
  if (is_canceled) {
    request.Cancel(
        Status::SchedulingCancelled("Task is canceled before it is scheduled."));
  } else {
    request.Accept();
  }
}

}  // namespace core
}  // namespace ray
