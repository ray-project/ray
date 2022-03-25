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

#include "ray/core_worker/transport/out_of_order_actor_scheduling_queue.h"

namespace ray {
namespace core {

OutOfOrderActorSchedulingQueue::OutOfOrderActorSchedulingQueue(
    instrumented_io_context &main_io_service,
    DependencyWaiter &waiter,
    std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
    bool is_asyncio,
    int fiber_max_concurrency,
    const std::vector<ConcurrencyGroup> &concurrency_groups)
    : main_thread_id_(boost::this_thread::get_id()),
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

void OutOfOrderActorSchedulingQueue::Stop() {
  if (pool_manager_) {
    pool_manager_->Stop();
  }
  if (fiber_state_manager_) {
    fiber_state_manager_->Stop();
  }
}

bool OutOfOrderActorSchedulingQueue::TaskQueueEmpty() const {
  RAY_CHECK(false) << "TaskQueueEmpty() not implemented for actor queues";
  return false;
}

size_t OutOfOrderActorSchedulingQueue::Size() const {
  RAY_CHECK(false) << "Size() not implemented for actor queues";
  return 0;
}

void OutOfOrderActorSchedulingQueue::Add(
    int64_t seq_no,
    int64_t client_processed_up_to,
    std::function<void(rpc::SendReplyCallback)> accept_request,
    std::function<void(rpc::SendReplyCallback)> reject_request,
    rpc::SendReplyCallback send_reply_callback,
    const std::string &concurrency_group_name,
    const ray::FunctionDescriptor &function_descriptor,
    TaskID task_id,
    const std::vector<rpc::ObjectReference> &dependencies) {
  RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
  auto request = InboundRequest(std::move(accept_request),
                                std::move(reject_request),
                                std::move(send_reply_callback),
                                task_id,
                                dependencies.size() > 0,
                                concurrency_group_name,
                                function_descriptor);

  if (dependencies.size() > 0) {
    waiter_.Wait(dependencies, [this, request = std::move(request)]() mutable {
      RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
      request.MarkDependenciesSatisfied();
      pending_actor_tasks_.push_back(std::move(request));
      ScheduleRequests();
    });
  } else {
    request.MarkDependenciesSatisfied();
    pending_actor_tasks_.push_back(std::move(request));
    ScheduleRequests();
  }
}

bool OutOfOrderActorSchedulingQueue::CancelTaskIfFound(TaskID task_id) {
  RAY_CHECK(false) << "Cannot cancel actor tasks";
  return false;
}

/// Schedules as many requests as possible in sequence.
void OutOfOrderActorSchedulingQueue::ScheduleRequests() {
  while (!pending_actor_tasks_.empty()) {
    auto request = pending_actor_tasks_.front();
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
    pending_actor_tasks_.pop_front();
  }
}

}  // namespace core
}  // namespace ray
