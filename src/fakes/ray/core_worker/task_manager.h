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
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/task_manager_interface.h"

namespace ray {
namespace core {

class FakeTaskManager : public TaskManagerInterface {
 public:
  std::vector<rpc::ObjectReference> AddPendingTask(const rpc::Address &caller_address,
                                                   const TaskSpecification &spec,
                                                   const std::string &call_site,
                                                   int max_retries) override {
    return std::vector<rpc::ObjectReference>{};
  }

  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply,
                           const rpc::Address &actor_addr,
                           bool is_application_error) override {}

  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status,
                       const rpc::RayErrorInfo *ray_error_info) override {}

  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status,
                              const rpc::RayErrorInfo *ray_error_info,
                              bool mark_task_object_failed,
                              bool fail_immediately) override {
    return false;
  }

  std::optional<rpc::ErrorType> ResubmitTask(const TaskID &task_id,
                                             std::vector<ObjectID> *task_deps) override {
    return std::nullopt;
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {}

  void MarkTaskCanceled(const TaskID &task_id) override {}

  void MarkTaskNoRetry(const TaskID &task_id) override {}

  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    return std::nullopt;
  }

  bool RetryTaskIfPossible(const TaskID &task_id,
                           const rpc::RayErrorInfo &error_info) override {
    return false;
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {}

  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id,
                                   const WorkerID &worker_id) override {}

  bool IsTaskPending(const TaskID &task_id) const override { return false; }

  void MarkGeneratorFailedAndResubmit(const TaskID &task_id) override {}

  void DrainAndShutdown(std::function<void()> shutdown) override {
    if (shutdown) {
      shutdown();
    }
  }

  void RecordMetrics() override {}

  std::vector<TaskID> GetPendingChildrenTasks(
      const TaskID &parent_task_id) const override {
    return std::vector<TaskID>{};
  }

  bool TryDelObjectRefStream(const ObjectID &generator_id) override { return true; }

  Status TryReadObjectRefStream(const ObjectID &generator_id,
                                ObjectID *object_id_out) override {
    if (object_id_out) {
      *object_id_out = ObjectID::Nil();
    }
    return Status::OK();
  }

  bool StreamingGeneratorIsFinished(const ObjectID &generator_id) const override {
    return true;
  }

  std::pair<ObjectID, bool> PeekObjectRefStream(const ObjectID &generator_id) override {
    return std::make_pair(ObjectID::Nil(), false);
  }

  bool HandleReportGeneratorItemReturns(
      const rpc::ReportGeneratorItemReturnsRequest &request,
      const std::function<void(Status, int64_t)> &execution_signal_callback) override {
    if (execution_signal_callback) {
      execution_signal_callback(Status::OK(), 0);
    }
    return false;
  }

  bool ObjectRefStreamExists(const ObjectID &generator_id) override { return false; }

  bool TemporarilyOwnGeneratorReturnRefIfNeeded(const ObjectID &object_id,
                                                const ObjectID &generator_id) override {
    return false;
  }

  ObjectID TaskGeneratorId(const TaskID &task_id) const override {
    return ObjectID::Nil();
  }

  std::unordered_map<rpc::LineageReconstructionTask, uint64_t>
  GetOngoingLineageReconstructionTasks(const ActorManager &actor_manager) const override {
    return std::unordered_map<rpc::LineageReconstructionTask, uint64_t>{};
  }

  size_t NumSubmissibleTasks() const override { return 0; }

  void AddTaskStatusInfo(rpc::CoreWorkerStats *stats) const override {}

  void FillTaskInfo(rpc::GetCoreWorkerStatsReply *reply,
                    const int64_t limit) const override {}

  size_t NumPendingTasks() const override { return 0; }
};

}  // namespace core
}  // namespace ray
