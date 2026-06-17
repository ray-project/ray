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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// Holds all state associated with a single task execution on this worker: the
/// per-request input needed to execute it or reply that it was canceled (task spec,
/// resource ids, reply, send_reply_callback), and the outputs produced once it runs
/// (return objects, errors, etc.). A single instance travels with the task through the
/// execution queues; the queue's execute callback fills in the output fields.
class TaskExecutionMetadata {
 public:
  TaskExecutionMetadata(TaskSpecification task_spec,
                        std::optional<ResourceMappingType> resource_ids,
                        rpc::PushTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback)
      : task_spec_(std::move(task_spec)),
        pending_dependencies_(task_spec_.GetDependencies()),
        resource_ids_(std::move(resource_ids)),
        reply_(reply),
        send_reply_callback_(std::move(send_reply_callback)) {
    RAY_CHECK(reply_ != nullptr) << "reply must not be null.";
    RAY_CHECK(send_reply_callback_ != nullptr) << "send_reply_callback must not be null.";
  }

  ray::TaskID TaskID() const { return task_spec_.TaskId(); }
  uint64_t AttemptNumber() const { return task_spec_.AttemptNumber(); }
  bool IsRetry() const { return task_spec_.IsRetry(); }
  const std::string &ConcurrencyGroupName() const {
    return task_spec_.ConcurrencyGroupName();
  }
  ray::FunctionDescriptor FunctionDescriptor() const {
    return task_spec_.FunctionDescriptor();
  }
  bool DependenciesResolved() const { return pending_dependencies_.empty(); }
  void MarkDependenciesResolved() { pending_dependencies_.clear(); }
  const std::vector<rpc::ObjectReference> &PendingDependencies() const {
    return pending_dependencies_;
  }
  const TaskSpecification &TaskSpec() const { return task_spec_; }

  // Per-request state used by the queue's execute / cancel callbacks. `resource_ids`
  // is mutable because the execute path moves it into the task handler.
  std::optional<ResourceMappingType> &resource_ids() { return resource_ids_; }
  rpc::PushTaskReply *reply() const { return reply_; }
  const rpc::SendReplyCallback &send_reply_callback() const {
    return send_reply_callback_;
  }

  // Outputs populated by the execute callback once the task runs.

  // Human-readable name for the actor in this process.
  // This is only expected to be populated for actor creation tasks.
  std::string actor_repr_name;
  // Detailed string containing information about any application error
  // that occurred.
  std::string application_error;
  // Indicates if the error is retryable or not. This is determined by the language
  // frontend (e.g., the `retry_exceptions` parameter in Python).
  bool is_retryable_error = false;
  // Objects returned by the task. Must be populated to match `task_spec.NumReturns()`
  // if the task succeeded.
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> return_objects;
  // Dynamic return objects that are determined on the first execution of a task.
  // Subsequent executions must match the same number of returns as the first execution.
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
  // Map of metadata associated with streaming generator outputs.
  // The value is set to `true` if the object was written to plasma (not inlined).
  std::vector<std::pair<ObjectID, bool>> streaming_generator_returns;

 private:
  TaskSpecification task_spec_;
  std::vector<rpc::ObjectReference> pending_dependencies_;
  std::optional<ResourceMappingType> resource_ids_;
  rpc::PushTaskReply *reply_;
  rpc::SendReplyCallback send_reply_callback_;
};

// Queue-level callbacks invoked to run a task or reply that it has been canceled.
using ExecuteTaskCallback = std::function<void(TaskExecutionMetadata &)>;
using CancelTaskCallback =
    std::function<void(const TaskExecutionMetadata &, const Status &)>;

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
