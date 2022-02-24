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
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/transport/actor_scheduling_util.h"
#include "ray/core_worker/transport/scheduling_queue.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// Used to implement the non-actor task queue. These tasks do not have ordering
/// constraints.
class NormalSchedulingQueue : public SchedulingQueue {
 public:
  NormalSchedulingQueue();

  void Stop() override;
  bool TaskQueueEmpty() const override;
  size_t Size() const override;

  /// Add a new task's callbacks to the worker queue.
  void Add(
      int64_t seq_no, int64_t client_processed_up_to,
      std::function<void(rpc::SendReplyCallback)> accept_request,
      std::function<void(rpc::SendReplyCallback)> reject_request,
      rpc::SendReplyCallback send_reply_callback,
      const std::string &concurrency_group_name = "",
      const FunctionDescriptor &function_descriptor = FunctionDescriptorBuilder::Empty(),
      TaskID task_id = TaskID::Nil(),
      const std::vector<rpc::ObjectReference> &dependencies = {}) override;

  // Search for an InboundRequest associated with the task that we are trying to cancel.
  // If found, remove the InboundRequest from the queue and return true. Otherwise,
  // return false.
  bool CancelTaskIfFound(TaskID task_id) override;

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() override;

 private:
  /// Protects access to the dequeue below.
  mutable absl::Mutex mu_;
  /// Queue with (accept, rej) callbacks for non-actor tasks
  std::deque<InboundRequest> pending_normal_tasks_ GUARDED_BY(mu_);
  friend class SchedulingQueueTest;
};

}  // namespace core
}  // namespace ray
