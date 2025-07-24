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

#include <map>
#include <memory>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/transport/concurrency_group_manager.h"
#include "ray/core_worker/transport/scheduling_queue.h"
#include "ray/core_worker/transport/scheduling_util.h"
#include "ray/core_worker/transport/thread_pool.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// Used to ensure serial order of task execution per actor handle.
/// See core_worker.proto for a description of the ordering protocol.
class ActorSchedulingQueue : public SchedulingQueue {
 public:
  ActorSchedulingQueue(
      instrumented_io_context &task_execution_service,
      DependencyWaiter &waiter,
      worker::TaskEventBuffer &task_event_buffer,
      std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager,
      int64_t reorder_wait_seconds);

  void Stop() override;

  bool TaskQueueEmpty() const override;

  size_t Size() const override;

  /// Add a new actor task's callbacks to the worker queue.
  void Add(int64_t seq_no,
           int64_t client_processed_up_to,
           std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
               accept_request,
           std::function<void(const TaskSpecification &,
                              const Status &,
                              rpc::SendReplyCallback)> reject_request,
           rpc::SendReplyCallback send_reply_callback,
           TaskSpecification task_spec) override;

  /// Cancel the actor task in the queue.
  /// Tasks are in the queue if it is either queued, or executing.
  /// Return true if a task is in the queue. False otherwise.
  /// This method has to be THREAD-SAFE.
  bool CancelTaskIfFound(TaskID task_id) override;

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() override;

 private:
  /// Accept the given InboundRequest or reject it if a task id is canceled via
  /// CancelTaskIfFound.
  void AcceptRequestOrRejectIfCanceled(TaskID task_id, InboundRequest &request);

  /// Called when we time out waiting for an earlier task to show up.
  void OnSequencingWaitTimeout();
  /// Max time in seconds to wait for dependencies to show up.
  const int64_t reorder_wait_seconds_;
  /// Sorted map of (accept, rej) task callbacks keyed by their sequence number.
  std::map<int64_t, InboundRequest> pending_actor_tasks_;
  /// The next sequence number we are waiting for to arrive.
  int64_t next_seq_no_ = 0;
  /// Timer for waiting on dependencies. Note that this is set on the task main
  /// io service, which is fine since it only ever fires if no tasks are running.
  boost::asio::deadline_timer wait_timer_;
  /// The id of the thread that constructed this scheduling queue.
  std::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  worker::TaskEventBuffer &task_event_buffer_;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<ConcurrencyGroupManager<BoundedExecutor>> pool_manager_;
  /// Mutext to protect attributes used for thread safe APIs.
  absl::Mutex mu_;
  /// A map of actor task IDs -> is_canceled
  /// Pending means tasks are queued or running.
  absl::flat_hash_map<TaskID, bool> pending_task_id_to_is_canceled ABSL_GUARDED_BY(mu_);

  friend class SchedulingQueueTest;
};

}  // namespace core
}  // namespace ray
