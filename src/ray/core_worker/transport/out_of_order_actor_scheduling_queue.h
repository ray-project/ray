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

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/transport/actor_scheduling_util.h"
#include "ray/core_worker/transport/concurrency_group_manager.h"
#include "ray/core_worker/transport/scheduling_queue.h"
#include "ray/core_worker/transport/thread_pool.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// This queue schedule the actor tasks as soon as the dependency is resolved,
/// and ignores the ordering (sequence_no) by the submitting client.
class OutOfOrderActorSchedulingQueue : public SchedulingQueue {
 public:
  OutOfOrderActorSchedulingQueue(
      instrumented_io_context &main_io_service,
      DependencyWaiter &waiter,
      std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager =
          std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
      bool is_asyncio = false,
      int fiber_max_concurrency = 1,
      const std::vector<ConcurrencyGroup> &concurrency_groups = {});

  void Stop() override;

  bool TaskQueueEmpty() const override;

  size_t Size() const override;

  /// Add a new actor task's callbacks to the worker queue.
  void Add(int64_t seq_no,
           int64_t client_processed_up_to,
           std::function<void(rpc::SendReplyCallback)> accept_request,
           std::function<void(rpc::SendReplyCallback)> reject_request,
           rpc::SendReplyCallback send_reply_callback,
           const std::string &concurrency_group_name,
           const ray::FunctionDescriptor &function_descriptor,
           TaskID task_id = TaskID::Nil(),
           const std::vector<rpc::ObjectReference> &dependencies = {}) override;

  // We don't allow the cancellation of actor tasks, so invoking CancelTaskIfFound
  // results in a fatal error.
  bool CancelTaskIfFound(TaskID task_id) override;

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() override;

 private:
  /// The queue stores all the pending tasks.
  std::deque<InboundRequest> pending_actor_tasks_;
  /// The id of the thread that constructed this scheduling queue.
  boost::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool is_asyncio_ = false;
  /// Manage the running fiber states of actors in this worker. It works with
  /// python asyncio if this is an asyncio actor.
  std::unique_ptr<ConcurrencyGroupManager<FiberState>> fiber_state_manager_;

  friend class SchedulingQueueTest;
};

}  // namespace core
}  // namespace ray
