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

#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

/// Wraps the state and callbacks associated with a task queued for execution on this
/// worker.
class TaskToExecute {
 public:
  TaskToExecute();
  TaskToExecute(std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
                    accept_callback,
                std::function<void(const TaskSpecification &,
                                   const Status &,
                                   rpc::SendReplyCallback)> reject_callback,
                rpc::SendReplyCallback send_reply_callback,
                TaskSpecification task_spec);

  void Accept();
  void Cancel(const Status &status);
  ray::TaskID TaskID() const;
  uint64_t AttemptNumber() const;
  const std::string &ConcurrencyGroupName() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  bool DependenciesResolved() const;
  void MarkDependenciesResolved();
  const std::vector<rpc::ObjectReference> &PendingDependencies() const;
  const TaskSpecification &TaskSpec() const;

 private:
  std::function<void(const TaskSpecification &, rpc::SendReplyCallback)> accept_callback_;
  std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
      reject_callback_;
  rpc::SendReplyCallback send_reply_callback_;

  TaskSpecification task_spec_;
  std::vector<rpc::ObjectReference> pending_dependencies_;
};

class ActorTaskExecutionArgWaiterInterface {
 public:
  virtual ~ActorTaskExecutionArgWaiterInterface() = default;

  /// Asynchronously wait for the specified arguments and call `on_args_ready` when they
  /// are ready. Trigger an async wait for the specified arguments.
  ///
  /// \param[in] on_args_ready The callback to call when arguments are ready.
  virtual void AsyncWait(const std::vector<rpc::ObjectReference> &args,
                         std::function<void()> on_args_ready) = 0;
};

class ActorTaskExecutionArgWaiter : public ActorTaskExecutionArgWaiterInterface {
 public:
  // Callback to trigger the asynchronous wait.
  // The caller is expected to call `MarkReady` with the provided tag when the associated
  // arguments are ready.
  using AsyncWaitForArgs =
      std::function<void(const std::vector<rpc::ObjectReference> &args, int64_t tag)>;

  explicit ActorTaskExecutionArgWaiter(AsyncWaitForArgs async_wait_for_args);

  void AsyncWait(const std::vector<rpc::ObjectReference> &args,
                 std::function<void()> on_args_ready) override;

  void MarkReady(int64_t tag);

 private:
  uint64_t next_tag_ = 0;
  absl::flat_hash_map<int64_t, std::function<void()>> in_flight_waits_;
  AsyncWaitForArgs async_wait_for_args_;
};

}  // namespace core
}  // namespace ray
