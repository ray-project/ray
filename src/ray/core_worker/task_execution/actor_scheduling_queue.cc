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

#include "ray/core_worker/task_execution/actor_scheduling_queue.h"

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

void ActorSchedulingQueue::Stop() {
  pool_manager_->Stop();
  CancelAllPending(Status::SchedulingCancelled(
      "Actor scheduling queue stopped; canceling pending tasks"));
}

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
  // A seq_no of -1 means no ordering constraint. Non-retry Actor tasks must be executed
  // in order.
  RAY_CHECK(seq_no != -1);

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
  InboundRequest inbound_request(std::move(accept_request),
                                 std::move(reject_request),
                                 std::move(send_reply_callback),
                                 task_spec);
  const bool is_retry = task_spec.IsRetry();
  InboundRequest *retry_request = nullptr;
  if (is_retry) {
    retry_request = &pending_retry_actor_tasks_.emplace_back(std::move(inbound_request));
  } else {
    RAY_CHECK(pending_actor_tasks_.emplace(seq_no, std::move(inbound_request)).second);
  }

  if (is_retry) {
    seq_no_to_skip_.insert(seq_no);
  }
  {
    absl::MutexLock lock(&mu_);
    pending_task_id_to_is_canceled.emplace(task_id, false);
  }

  if (!dependencies.empty()) {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_id,
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH,
        /* include_task_info */ false));
    waiter_.Wait(dependencies, [this, seq_no, is_retry, retry_request]() mutable {
      InboundRequest *inbound_req = nullptr;
      if (is_retry) {
        // retry_request is guaranteed to be a valid pointer for retries because it
        // won't be erased from the retry list until its dependencies are fetched and
        // ExecuteRequest happens.
        inbound_req = retry_request;
      } else if (auto it = pending_actor_tasks_.find(seq_no);
                 it != pending_actor_tasks_.end()) {
        // For non-retry tasks, we need to check if the task is still in the map because
        // it can be erased due to being canceled via a higher `client_processed_up_to_`.
        inbound_req = &it->second;
      }

      if (inbound_req != nullptr) {
        const auto &inbound_req_task_spec = inbound_req->TaskSpec();
        RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
            inbound_req_task_spec.TaskId(),
            inbound_req_task_spec.JobId(),
            inbound_req_task_spec.AttemptNumber(),
            inbound_req_task_spec,
            rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
            /* include_task_info */ false));
        inbound_req->MarkDependenciesResolved();
        ScheduleRequests();
      }
    });
  } else {
    RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_id,
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
  // This happens when the client sends an RPC with the client_processed_up_to
  // sequence number higher than the lowest sequence number of a pending actor task.
  // In that case, the client no longer needs the task to execute (e.g., it has been
  // retried).
  while (!pending_actor_tasks_.empty() &&
         pending_actor_tasks_.begin()->first < next_seq_no_) {
    auto head = pending_actor_tasks_.begin();
    RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                   << pending_actor_tasks_.begin()->first << " < " << next_seq_no_;
    head->second.Cancel(
        Status::Invalid("Task canceled due to stale sequence number. The client "
                        "intentionally discarded this task."));
    {
      absl::MutexLock lock(&mu_);
      pending_task_id_to_is_canceled.erase(head->second.TaskID());
    }
    pending_actor_tasks_.erase(head);
  }

  // Process as many retry requests as we can.
  // Retry requests do not respect sequence number ordering, so we execute them as soon as
  // they are ready to execute.
  auto retry_iter = pending_retry_actor_tasks_.begin();
  while (retry_iter != pending_retry_actor_tasks_.end()) {
    auto &request = *retry_iter;
    if (!request.DependenciesResolved()) {
      retry_iter++;
      continue;
    }
    ExecuteRequest(std::move(request));
    pending_retry_actor_tasks_.erase(retry_iter++);
  }

  // Process as many in-order requests as we can.
  while (!pending_actor_tasks_.empty()) {
    auto begin_it = pending_actor_tasks_.begin();
    auto &[seq_no, request] = *begin_it;
    if (seq_no == next_seq_no_) {
      if (request.DependenciesResolved()) {
        ExecuteRequest(std::move(request));
        pending_actor_tasks_.erase(begin_it);
        next_seq_no_++;
      } else {
        // next_seq_no_ can't execute so break
        break;
      }
    } else if (seq_no_to_skip_.erase(next_seq_no_) > 0) {
      next_seq_no_++;
    } else {
      break;
    }
  }

  if (pending_actor_tasks_.empty() ||
      !pending_actor_tasks_.begin()->second.DependenciesResolved()) {
    // Either there are no tasks to execute, or the head of the line is blocked waiting
    // for its dependencies. We do not set a timeout waiting for dependency resolution.
    wait_timer_.cancel();
  } else {
    // We are waiting for a task with an earlier seq_no from the client.
    // The client always sends tasks in seq_no order, so in the majority of cases we
    // should receive the expected message soon, but messages can come in out of order.
    //
    // We set a generous timeout in case the expected seq_no is never received to avoid
    // hanging. This should happen only if the client crashes or misbehaves. After the
    // timeout, all tasks will be canceled and the client (if alive) must retry.
    wait_timer_.expires_from_now(boost::posix_time::seconds(reorder_wait_seconds_));
    RAY_LOG(DEBUG) << "waiting for " << next_seq_no_ << " queue size "
                   << pending_actor_tasks_.size();
    wait_timer_.async_wait([this](const boost::system::error_code &error) {
      if (error == boost::asio::error::operation_aborted) {
        return;  // Timer deadline was adjusted.
      }
      RAY_LOG(ERROR) << "Timed out waiting for task with seq_no=" << next_seq_no_
                     << ", canceling all queued tasks.";
      while (!pending_actor_tasks_.empty()) {
        auto head = pending_actor_tasks_.begin();
        head->second.Cancel(
            Status::Invalid(absl::StrCat("Server timed out after waiting ",
                                         reorder_wait_seconds_,
                                         " seconds for an earlier seq_no.")));
        next_seq_no_ = std::max(next_seq_no_, head->first + 1);
        {
          absl::MutexLock lock(&mu_);
          pending_task_id_to_is_canceled.erase(head->second.TaskID());
        }
        pending_actor_tasks_.erase(head);
      }
    });
  }
}

void ActorSchedulingQueue::CancelAllPending(const Status &status) {
  absl::MutexLock lock(&mu_);
  // Cancel in-order pending tasks
  while (!pending_actor_tasks_.empty()) {
    auto head = pending_actor_tasks_.begin();
    head->second.Cancel(status);
    pending_task_id_to_is_canceled.erase(head->second.TaskID());
    pending_actor_tasks_.erase(head);
  }
  // Cancel retry tasks
  while (!pending_retry_actor_tasks_.empty()) {
    auto &req = pending_retry_actor_tasks_.front();
    req.Cancel(status);
    pending_task_id_to_is_canceled.erase(req.TaskID());
    pending_retry_actor_tasks_.pop_front();
  }
}

void ActorSchedulingQueue::ExecuteRequest(InboundRequest &&request) {
  auto task_id = request.TaskID();
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

void ActorSchedulingQueue::AcceptRequestOrRejectIfCanceled(TaskID task_id,
                                                           InboundRequest &request) {
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

  absl::MutexLock lock(&mu_);
  pending_task_id_to_is_canceled.erase(task_id);
}

}  // namespace core
}  // namespace ray
