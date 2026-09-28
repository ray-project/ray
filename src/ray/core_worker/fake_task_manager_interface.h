// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "ray/core_worker/task_manager_interface.h"

namespace ray {
namespace core {

// Hand-written fake for TaskManagerInterface. Methods default to trivial return
// values; public fields let tests program return values and inspect recorded
// calls (replaces gmock return/EXPECT_CALL usage).
class FakeTaskManagerInterface : public TaskManagerInterface {
 public:
  std::vector<rpc::ObjectReference> AddPendingTask(const rpc::Address &caller_address,
                                                   const TaskSpecification &spec,
                                                   const std::string &call_site,
                                                   int max_retries = 0) override {
    add_pending_task_calls.push_back(spec.TaskId());
    if (add_pending_task_hook) {
      return add_pending_task_hook(caller_address, spec, call_site, max_retries);
    }
    return add_pending_task_return;
  }

  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply,
                           const rpc::Address &actor_addr,
                           bool is_application_error) override {
    complete_pending_task_calls.push_back(task_id);
  }

  bool RetryTaskIfPossible(const TaskID &task_id,
                           const rpc::RayErrorInfo &error_info) override {
    retry_task_if_possible_calls.push_back(task_id);
    return retry_task_if_possible_return;
  }

  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status = nullptr,
                       const rpc::RayErrorInfo *ray_error_info = nullptr) override {
    fail_pending_task_calls.push_back(task_id);
    fail_pending_task_error_types.push_back(error_type);
  }

  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true,
                              bool fail_immediately = false) override {
    fail_or_retry_pending_task_calls.push_back(task_id);
    fail_or_retry_pending_task_error_types.push_back(error_type);
    return fail_or_retry_pending_task_return;
  }

  std::optional<rpc::ErrorType> ResubmitTask(const TaskID &task_id,
                                             std::vector<ObjectID> *task_deps) override {
    resubmit_task_calls.push_back(task_id);
    return resubmit_task_return;
  }

  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id,
                                   const WorkerID &worker_id) override {
    mark_task_waiting_for_execution_calls.push_back(task_id);
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {
    on_task_dependencies_inlined_calls.push_back(inlined_dependency_ids);
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {
    mark_dependencies_resolved_calls.push_back(task_id);
  }

  void MarkTaskNoRetry(const TaskID &task_id) override {
    mark_task_no_retry_calls.push_back(task_id);
  }

  void MarkTaskCanceled(const TaskID &task_id) override {
    mark_task_canceled_calls.push_back(task_id);
  }

  bool IsTaskCanceled(const TaskID &task_id) const override {
    return is_task_canceled_return;
  }

  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    return get_task_spec_return;
  }

  bool IsTaskPending(const TaskID &task_id) const override {
    return is_task_pending_return;
  }

  void MarkGeneratorFailedAndResubmit(const TaskID &task_id) override {
    mark_generator_failed_and_resubmit_calls.push_back(task_id);
  }

  // Recorded calls (salient arg per invocation).
  std::vector<TaskID> add_pending_task_calls;
  std::vector<TaskID> complete_pending_task_calls;
  std::vector<TaskID> retry_task_if_possible_calls;
  std::vector<TaskID> fail_pending_task_calls;
  std::vector<rpc::ErrorType> fail_pending_task_error_types;
  std::vector<TaskID> fail_or_retry_pending_task_calls;
  std::vector<rpc::ErrorType> fail_or_retry_pending_task_error_types;
  std::vector<TaskID> resubmit_task_calls;
  std::vector<TaskID> mark_task_waiting_for_execution_calls;
  std::vector<std::vector<ObjectID>> on_task_dependencies_inlined_calls;
  std::vector<TaskID> mark_dependencies_resolved_calls;
  std::vector<TaskID> mark_task_no_retry_calls;
  std::vector<TaskID> mark_task_canceled_calls;
  std::vector<TaskID> mark_generator_failed_and_resubmit_calls;

  // Settable return values.
  std::vector<rpc::ObjectReference> add_pending_task_return;
  bool retry_task_if_possible_return = false;
  bool fail_or_retry_pending_task_return = false;
  std::optional<rpc::ErrorType> resubmit_task_return = std::nullopt;
  bool is_task_canceled_return = false;
  std::optional<TaskSpecification> get_task_spec_return = std::nullopt;
  bool is_task_pending_return = true;

  // Optional behavior-injection hook.
  std::function<std::vector<rpc::ObjectReference>(
      const rpc::Address &, const TaskSpecification &, const std::string &, int)>
      add_pending_task_hook;
};

}  // namespace core
}  // namespace ray
