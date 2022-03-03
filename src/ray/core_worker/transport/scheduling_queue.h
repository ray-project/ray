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

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// Used to implement task queueing at the worker. Abstraction to provide a common
/// interface for actor tasks as well as normal ones.
class SchedulingQueue {
 public:
  virtual ~SchedulingQueue() = default;
  virtual void Add(int64_t seq_no, int64_t client_processed_up_to,
                   std::function<void(rpc::SendReplyCallback)> accept_request,
                   std::function<void(rpc::SendReplyCallback)> reject_request,
                   rpc::SendReplyCallback send_reply_callback,
                   const std::string &concurrency_group_name,
                   const ray::FunctionDescriptor &function_descriptor,
                   TaskID task_id = TaskID::Nil(),
                   const std::vector<rpc::ObjectReference> &dependencies = {}) = 0;
  virtual void ScheduleRequests() = 0;
  virtual bool TaskQueueEmpty() const = 0;
  virtual size_t Size() const = 0;
  virtual void Stop() = 0;
  virtual bool CancelTaskIfFound(TaskID task_id) = 0;
};

}  // namespace core
}  // namespace ray
