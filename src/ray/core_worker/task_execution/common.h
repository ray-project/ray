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
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

/// Wraps the state and callbacks associated with a task queued for execution on this
/// worker.
class TaskToExecute {
 public:
  TaskToExecute(
      std::function<void(const TaskSpecification &)> execute_callback,
      std::function<void(const TaskSpecification &, const Status &)> cancel_callback,
      TaskSpecification task_spec);

  void Execute();
  void Cancel(const Status &status);
  ray::TaskID TaskID() const;
  uint64_t AttemptNumber() const;
  bool IsRetry() const;
  const std::string &ConcurrencyGroupName() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  bool DependenciesResolved() const;
  void MarkDependenciesResolved();
  const std::vector<rpc::ObjectReference> &PendingDependencies() const;
  const TaskSpecification &TaskSpec() const;

 private:
  // Callbacks to execute the task or reply that it has been canceled and will not be
  // executed.
  // Only one invocation of these callbacks should ever be called.
  std::function<void(const TaskSpecification &)> execute_callback_;
  std::function<void(const TaskSpecification &, const Status &)> cancel_callback_;

  TaskSpecification task_spec_;
  std::vector<rpc::ObjectReference> pending_dependencies_;
};

// Container for metadata and outputs corresponding to a completed task execution.
struct TaskExecutionResult {
  TaskExecutionResult() = default;

  // Disable copy and move constructors defensively. This struct is only currently
  // used as an output parameter that's passed by reference.
  TaskExecutionResult(const TaskExecutionResult &) = delete;
  TaskExecutionResult &operator=(const TaskExecutionResult &) = delete;

  TaskExecutionResult(TaskExecutionResult &&) = delete;
  TaskExecutionResult &operator=(TaskExecutionResult &&) = delete;

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
};

class ActorTaskExecutionArgWaiterInterface {
 public:
  virtual ~ActorTaskExecutionArgWaiterInterface() = default;

  /// Issue an async args-fetch IPC to the raylet. Returns a tag identifying the
  /// fetch. The caller is expected to later call `OnArgsReady` with the same tag
  /// to register a callback that will run when the args become ready.
  ///
  /// THREAD-SAFE: may be called from any thread (typically the gRPC handler
  /// thread, so the IPC is not blocked behind in-progress work on the task
  /// execution service).
  virtual int64_t BeginArgsFetch(const std::vector<rpc::ObjectReference> &args) = 0;

  /// Register the callback to invoke when the args identified by `tag` are
  /// ready. If the args are already ready (i.e., `MarkReady` was called for
  /// `tag` before this call), the callback is invoked synchronously and the
  /// entry is cleaned up.
  ///
  /// NOT THREAD-SAFE: must be called from the task execution service thread
  /// (same thread as `MarkReady`).
  virtual void OnArgsReady(int64_t tag, std::function<void()> on_args_ready) = 0;
};

class ActorTaskExecutionArgWaiter : public ActorTaskExecutionArgWaiterInterface {
 public:
  // Callback that performs the underlying async wait for the args.
  // The implementor is expected to call `MarkReady` with the provided tag when
  // the associated arguments are ready.
  using AsyncWaitForArgs =
      std::function<void(const std::vector<rpc::ObjectReference> &args, int64_t tag)>;

  explicit ActorTaskExecutionArgWaiter(AsyncWaitForArgs async_wait_for_args);

  int64_t BeginArgsFetch(const std::vector<rpc::ObjectReference> &args) override;

  void OnArgsReady(int64_t tag, std::function<void()> on_args_ready) override;

  /// Called by the args-ready notification path on the task execution service
  /// thread. If a callback was registered for `tag` via `OnArgsReady`, invokes
  /// it. Otherwise records that the args have arrived; the callback will run
  /// synchronously when `OnArgsReady` is called for `tag` later.
  void MarkReady(int64_t tag);

 private:
  // An entry holds at most one of: a registered callback OR an
  // `execute_immediate` flag set by `MarkReady` arriving first.
  // `OnArgsReady` and `MarkReady` create entries,
  // and whichever runs second consumes the entry and erases it.
  struct WaitEntry {
    std::function<void()> on_args_ready;
    bool execute_immediate = false;
  };

  // Bumped by `BeginArgsFetch`
  std::atomic<int64_t> next_tag_{0};

  // Touched only by `OnArgsReady` and `MarkReady`, both on the task execution
  // service thread. Therefore, no mutex guarding this.
  absl::flat_hash_map<int64_t, WaitEntry> in_flight_waits_;

  AsyncWaitForArgs async_wait_for_args_;
};

}  // namespace core
}  // namespace ray
