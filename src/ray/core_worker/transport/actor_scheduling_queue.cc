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

namespace ray {
namespace core {

ActorSchedulingQueue::ActorSchedulingQueue(
    instrumented_io_context &main_io_service,
    DependencyWaiter &waiter,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    bool is_asyncio,
    int fiber_max_concurrency,
    const std::vector<ConcurrencyGroup> &concurrency_groups,
    int64_t reorder_wait_seconds)
    : reorder_wait_seconds_(reorder_wait_seconds),
      wait_timer_(main_io_service),
      main_thread_id_(boost::this_thread::get_id()),
      waiter_(waiter),
      pool_manager_(pool_manager),
      is_asyncio_(is_asyncio) {
  if (is_asyncio_) {
    std::stringstream ss;
    ss << "Setting actor as asyncio with max_concurrency=" << fiber_max_concurrency
       << ", and defined concurrency groups are:" << std::endl;
    for (const auto &concurrency_group : concurrency_groups) {
      ss << "\t" << concurrency_group.name << " : " << concurrency_group.max_concurrency;
    }
    RAY_LOG(INFO) << ss.str();
    fiber_state_manager_ = std::make_unique<ConcurrencyGroupManager<FiberState>>(
        concurrency_groups, fiber_max_concurrency);
  }
}

void ActorSchedulingQueue::Stop() {
  if (pool_manager_) {
    pool_manager_->Stop();
  }
  if (fiber_state_manager_) {
    fiber_state_manager_->Stop();
  }
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
void ActorSchedulingQueue::Add(int64_t seq_no,
                               int64_t client_processed_up_to,
                               std::function<void(rpc::SendReplyCallback)> accept_request,
                               std::function<void(rpc::SendReplyCallback)> reject_request,
                               rpc::SendReplyCallback send_reply_callback,
                               const std::string &concurrency_group_name,
                               const ray::FunctionDescriptor &function_descriptor,
                               TaskID task_id,
                               const std::vector<rpc::ObjectReference> &dependencies) {
  // A seq_no of -1 means no ordering constraint. Actor tasks must be executed in order.
  RAY_CHECK(seq_no != -1);

  RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
  if (client_processed_up_to >= next_seq_no_) {
    RAY_LOG(ERROR) << "client skipping requests " << next_seq_no_ << " to "
                   << client_processed_up_to;
    next_seq_no_ = client_processed_up_to + 1;
  }
  RAY_LOG(DEBUG) << "Enqueue " << seq_no << " cur seqno " << next_seq_no_;

  pending_actor_tasks_[seq_no] = InboundRequest(std::move(accept_request),
                                                std::move(reject_request),
                                                std::move(send_reply_callback),
                                                task_id,
                                                dependencies.size() > 0,
                                                concurrency_group_name,
                                                function_descriptor);

  if (dependencies.size() > 0) {
    waiter_.Wait(dependencies, [seq_no, this]() {
      RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
      auto it = pending_actor_tasks_.find(seq_no);
      if (it != pending_actor_tasks_.end()) {
        it->second.MarkDependenciesSatisfied();
        ScheduleRequests();
      }
    });
  }
  ScheduleRequests();
}

// We don't allow the cancellation of actor tasks, so invoking CancelTaskIfFound
// results in a fatal error.
bool ActorSchedulingQueue::CancelTaskIfFound(TaskID task_id) {
  RAY_CHECK(false) << "Cannot cancel actor tasks";
  // The return instruction will never be executed, but we need to include it
  // nonetheless because this is a non-void function.
  return false;
}

/// Schedules as many requests as possible in sequence.
void ActorSchedulingQueue::ScheduleRequests() {
  // Cancel any stale requests that the client doesn't need any longer.
  while (!pending_actor_tasks_.empty() &&
         pending_actor_tasks_.begin()->first < next_seq_no_) {
    auto head = pending_actor_tasks_.begin();
    RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                   << pending_actor_tasks_.begin()->first << " < " << next_seq_no_;
    head->second.Cancel();
    pending_actor_tasks_.erase(head);
  }

  // Process as many in-order requests as we can.
  while (!pending_actor_tasks_.empty() &&
         pending_actor_tasks_.begin()->first == next_seq_no_ &&
         pending_actor_tasks_.begin()->second.CanExecute()) {
    auto head = pending_actor_tasks_.begin();
    auto request = head->second;

    if (is_asyncio_) {
      // Process async actor task.
      auto fiber = fiber_state_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                                     request.FunctionDescriptor());
      fiber->EnqueueFiber([request]() mutable { request.Accept(); });
    } else {
      // Process actor tasks.
      RAY_CHECK(pool_manager_ != nullptr);
      auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                             request.FunctionDescriptor());
      if (pool == nullptr) {
        request.Accept();
      } else {
        pool->PostBlocking([request]() mutable { request.Accept(); });
      }
    }
    pending_actor_tasks_.erase(head);
    next_seq_no_++;
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
  RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
  RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                 << ", cancelling all queued tasks";
  while (!pending_actor_tasks_.empty()) {
    auto head = pending_actor_tasks_.begin();
    head->second.Cancel();
    next_seq_no_ = std::max(next_seq_no_, head->first + 1);
    pending_actor_tasks_.erase(head);
  }
}

}  // namespace core
}  // namespace ray
