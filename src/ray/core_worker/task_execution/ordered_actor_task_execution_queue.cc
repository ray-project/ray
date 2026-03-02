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

#include "ray/core_worker/task_execution/ordered_actor_task_execution_queue.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

namespace ray {
namespace core {

OrderedActorTaskExecutionQueue::OrderedActorTaskExecutionQueue(
    instrumented_io_context &task_execution_service,
    ActorTaskExecutionArgWaiterInterface &waiter,
    worker::TaskEventBuffer &task_event_buffer,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    int64_t reorder_wait_seconds)
    : task_execution_service_(task_execution_service),
      reorder_wait_seconds_(reorder_wait_seconds),
      main_thread_id_(std::this_thread::get_id()),
      waiter_(waiter),
      task_event_buffer_(task_event_buffer),
      pool_manager_(std::move(pool_manager)) {}

void OrderedActorTaskExecutionQueue::CancelAllQueuedTasks(const std::string &msg) {
  absl::MutexLock lock(&mu_);

  Status status = Status::SchedulingCancelled(msg);

  for (auto &[_, group_state] : group_states_) {
    // Cancel queued ordered tasks.
    while (!group_state.pending_tasks.empty()) {
      auto head = group_state.pending_tasks.begin();
      head->second.Cancel(status);
      pending_task_id_to_is_canceled.erase(head->second.TaskID());
      group_state.pending_tasks.erase(head);
    }

    // Cancel queued retry tasks.
    while (!group_state.pending_retry_tasks.empty()) {
      auto &req = group_state.pending_retry_tasks.front();
      req.Cancel(status);
      pending_task_id_to_is_canceled.erase(req.TaskID());
      group_state.pending_retry_tasks.pop_front();
    }
  }
}

void OrderedActorTaskExecutionQueue::Stop() {
  pool_manager_->Stop();
  CancelAllQueuedTasks("Actor task execution queue stopped; canceling all queued tasks.");
}

void OrderedActorTaskExecutionQueue::EnqueueTask(int64_t seq_no,
                                                 int64_t client_processed_up_to,
                                                 TaskToExecute task) {
  // A seq_no of -1 means no ordering constraint. Non-retry Actor tasks must be executed
  // in order.
  RAY_CHECK(seq_no != -1);

  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);

  // Make a copy of the task spec because `task` is moved below.
  TaskSpecification task_spec = task.TaskSpec();
  const std::string &group = task_spec.ConcurrencyGroupName();
  auto [iter, _] = group_states_.try_emplace(
      group, ConcurrencyGroupOrderingState(task_execution_service_));
  auto &group_state = iter->second;

  if (client_processed_up_to >= group_state.next_seq_no) {
    RAY_LOG(INFO) << "client skipping requests " << group_state.next_seq_no << " to "
                  << client_processed_up_to << " for concurrency group '" << group << "'";
    group_state.next_seq_no = client_processed_up_to + 1;
  }

  RAY_LOG(DEBUG).WithField(task_spec.TaskId())
      << "Enqueuing in order actor task, seq_no=" << seq_no
      << ", next_seq_no_=" << group_state.next_seq_no << ", group='" << group << "'";

  const auto dependencies = task_spec.GetDependencies();
  const bool is_retry = task_spec.IsRetry();
  TaskToExecute *retry_task = nullptr;
  if (is_retry) {
    retry_task = &group_state.pending_retry_tasks.emplace_back(std::move(task));
  } else {
    RAY_CHECK(group_state.pending_tasks.emplace(seq_no, std::move(task)).second);
  }

  if (is_retry) {
    group_state.seq_no_to_skip.insert(seq_no);
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
    waiter_.AsyncWait(dependencies, [this, seq_no, is_retry, retry_task, group]() {
      TaskToExecute *ready_task = nullptr;
      if (is_retry) {
        // retry_task is guaranteed to be a valid pointer for retries
        // because it won't be erased from the retry list until its
        // dependencies are fetched and ExecuteRequest happens.
        ready_task = retry_task;
      } else {
        auto &group_state = group_states_.at(group);
        auto it = group_state.pending_tasks.find(seq_no);
        if (it != group_state.pending_tasks.end()) {
          // For non-retry tasks, we need to check if the task is
          // still in the map because it can be erased due to being
          // canceled via a higher `client_processed_up_to`.
          ready_task = &it->second;
        }
      }

      if (ready_task != nullptr) {
        const auto &ready_task_spec = ready_task->TaskSpec();
        RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
            ready_task_spec.TaskId(),
            ready_task_spec.JobId(),
            ready_task_spec.AttemptNumber(),
            ready_task_spec,
            rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
            /* include_task_info */ false));
        ready_task->MarkDependenciesResolved();
        ExecuteQueuedTasks();
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

  ExecuteQueuedTasks();
}

bool OrderedActorTaskExecutionQueue::CancelTaskIfFound(TaskID task_id) {
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

void OrderedActorTaskExecutionQueue::ExecuteQueuedTasks() {
  for (auto &[group_name, group_state] : group_states_) {
    // Cancel any stale requests that the client doesn't need any longer.
    // This happens when the client sends an RPC with the client_processed_up_to
    // sequence number higher than the lowest sequence number of a pending actor task.
    // In that case, the client no longer needs the task to execute (e.g., it has been
    // retried).
    while (!group_state.pending_tasks.empty() &&
           group_state.pending_tasks.begin()->first < group_state.next_seq_no) {
      auto head = group_state.pending_tasks.begin();
      RAY_LOG(ERROR) << "Cancelling stale RPC with seqno " << head->first << " < "
                     << group_state.next_seq_no << " in group '" << group_name << "'";
      head->second.Cancel(
          Status::Invalid("Task canceled due to stale sequence number. The client "
                          "intentionally discarded this task."));
      {
        absl::MutexLock lock(&mu_);
        pending_task_id_to_is_canceled.erase(head->second.TaskID());
      }
      group_state.pending_tasks.erase(head);
    }

    // Process as many retry requests as we can.
    // Retry requests do not respect sequence number ordering, so we execute them as soon
    // as they are ready to execute.
    auto retry_iter = group_state.pending_retry_tasks.begin();
    while (retry_iter != group_state.pending_retry_tasks.end()) {
      auto &request = *retry_iter;
      if (!request.DependenciesResolved()) {
        retry_iter++;
        continue;
      }
      ExecuteRequest(std::move(request));
      group_state.pending_retry_tasks.erase(retry_iter++);
    }

    // Process as many in-order requests as we can.
    while (!group_state.pending_tasks.empty()) {
      auto begin_it = group_state.pending_tasks.begin();
      auto &[seq_no, request] = *begin_it;
      if (seq_no == group_state.next_seq_no) {
        if (request.DependenciesResolved()) {
          ExecuteRequest(std::move(request));
          group_state.pending_tasks.erase(begin_it);
          group_state.next_seq_no++;
        } else {
          // next_seq_no can't execute so break
          break;
        }
      } else if (group_state.seq_no_to_skip.erase(group_state.next_seq_no) > 0) {
        group_state.next_seq_no++;
      } else {
        break;
      }
    }

    // If there are tasks to execute and the head of the line is not blocked waiting
    // for its dependencies, we are waiting for a previous seq no, so start the wait
    // timer.
    if (!group_state.pending_tasks.empty() &&
        group_state.pending_tasks.begin()->second.DependenciesResolved()) {
      // We are waiting for a task with an earlier seq_no from the client.
      // The client always sends tasks in seq_no order, so in the majority of cases we
      // should receive the expected message soon, but messages can come in out of order.
      //
      // We set a generous timeout in case the expected seq_no is never received to avoid
      // hanging. This should happen only if the client crashes or misbehaves. After the
      // timeout, all tasks will be canceled and the client (if alive) must retry.
      group_state.wait_timer_.expires_from_now(
          boost::posix_time::seconds(reorder_wait_seconds_));

      // Redefining both below because structured bindings can't be captured in lambdas
      // until C++20
      auto &group_name_in = group_name;
      auto next_seq_no = group_state.next_seq_no;
      group_state.wait_timer_.async_wait(
          [this, group_name_in, next_seq_no](const boost::system::error_code &error) {
            if (error == boost::asio::error::operation_aborted) {
              return;  // Timer deadline was adjusted.
            }
            std::string error_message = absl::StrFormat(
                "Timed out waiting for seq_no %d in concurrency group %s, "
                "after waiting for %d seconds. Cancelling all queued tasks. "
                "This means an expected task failed to arrive at the actor via RPC. This "
                "could be due to network issues, submitter death, or resource contention "
                "(resource contention can cause RPC failures).",
                next_seq_no,
                group_name_in,
                reorder_wait_seconds_);
            RAY_LOG(ERROR) << error_message;
            auto invalid_status = Status::Invalid(error_message);
            // Cancel tasks in ALL groups if the client didn't send over the task with the
            // expected seq no
            for (auto &[_, group_state_in] : group_states_) {
              while (!group_state_in.pending_tasks.empty()) {
                auto head = group_state_in.pending_tasks.begin();
                head->second.Cancel(invalid_status);
                group_state_in.next_seq_no =
                    std::max(group_state_in.next_seq_no, head->first + 1);
                {
                  absl::MutexLock lock(&mu_);
                  pending_task_id_to_is_canceled.erase(head->second.TaskID());
                }
                group_state_in.pending_tasks.erase(head);
              }
            }
          });
    } else {
      // We can cancel the wait timer because the head of line task is not waiting for the
      // previous seq no
      group_state.wait_timer_.cancel();
    }
  }
}

void OrderedActorTaskExecutionQueue::ExecuteRequest(TaskToExecute &&request) {
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

void OrderedActorTaskExecutionQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, TaskToExecute &request) {
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
    request.Execute();
  }

  absl::MutexLock lock(&mu_);
  pending_task_id_to_is_canceled.erase(task_id);
}

}  // namespace core
}  // namespace ray
