// Copyright 2021 The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/core_worker/task_manager_interface.h"

namespace ray {
namespace core {

class MockTaskManagerInterface : public TaskManagerInterface {
 public:
  MOCK_METHOD(std::vector<rpc::ObjectReference>,
              AddPendingTask,
              (const rpc::Address &caller_address,
               const TaskSpecification &spec,
               const std::string &call_site,
               int max_retries),
              (override));
  MOCK_METHOD(void,
              CompletePendingTask,
              (const TaskID &task_id,
               const rpc::PushTaskReply &reply,
               const rpc::Address &actor_addr,
               bool is_application_error),
              (override));
  MOCK_METHOD(void,
              FailPendingTask,
              (const TaskID &task_id,
               rpc::ErrorType error_type,
               const Status *status,
               const rpc::RayErrorInfo *ray_error_info),
              (override));
  MOCK_METHOD(bool,
              FailOrRetryPendingTask,
              (const TaskID &task_id,
               rpc::ErrorType error_type,
               const Status *status,
               const rpc::RayErrorInfo *ray_error_info,
               bool mark_task_object_failed,
               bool fail_immediately),
              (override));
  MOCK_METHOD(std::optional<rpc::ErrorType>,
              ResubmitTask,
              (const TaskID &task_id, std::vector<ObjectID> *task_deps),
              (override));
  MOCK_METHOD(void,
              OnTaskDependenciesInlined,
              (const std::vector<ObjectID> &inlined_dependency_ids,
               const std::vector<ObjectID> &contained_ids),
              (override));
  MOCK_METHOD(void, MarkTaskCanceled, (const TaskID &task_id), (override));
  MOCK_METHOD(void, MarkTaskNoRetry, (const TaskID &task_id), (override));
  MOCK_METHOD(std::optional<TaskSpecification>,
              GetTaskSpec,
              (const TaskID &task_id),
              (const, override));
  MOCK_METHOD(bool,
              RetryTaskIfPossible,
              (const TaskID &task_id, const rpc::RayErrorInfo &error_info),
              (override));
  MOCK_METHOD(void, MarkDependenciesResolved, (const TaskID &task_id), (override));
  MOCK_METHOD(void,
              MarkTaskWaitingForExecution,
              (const TaskID &task_id, const NodeID &node_id, const WorkerID &worker_id),
              (override));
  MOCK_METHOD(bool, IsTaskPending, (const TaskID &task_id), (const, override));
  MOCK_METHOD(void, MarkGeneratorFailedAndResubmit, (const TaskID &task_id), (override));

  MOCK_METHOD(void, DrainAndShutdown, (std::function<void()> shutdown), (override));

  MOCK_METHOD(void, RecordMetrics, (), (override));

  MOCK_METHOD(std::vector<TaskID>,
              GetPendingChildrenTasks,
              (const TaskID &parent_task_id),
              (const, override));

  MOCK_METHOD(bool, TryDelObjectRefStream, (const ObjectID &generator_id), (override));

  MOCK_METHOD(Status,
              TryReadObjectRefStream,
              (const ObjectID &generator_id, ObjectID *object_id_out),
              (override));

  MOCK_METHOD(bool,
              StreamingGeneratorIsFinished,
              (const ObjectID &generator_id),
              (const, override));

  MOCK_METHOD((std::pair<ObjectID, bool>),
              PeekObjectRefStream,
              (const ObjectID &generator_id),
              (override));

  MOCK_METHOD(bool,
              HandleReportGeneratorItemReturns,
              (const rpc::ReportGeneratorItemReturnsRequest &request,
               const std::function<void(Status, int64_t)> &execution_signal_callback),
              (override));

  MOCK_METHOD(bool, ObjectRefStreamExists, (const ObjectID &generator_id), (override));

  MOCK_METHOD(bool,
              TemporarilyOwnGeneratorReturnRefIfNeeded,
              (const ObjectID &object_id, const ObjectID &generator_id),
              (override));

  MOCK_METHOD(ObjectID, TaskGeneratorId, (const TaskID &task_id), (const, override));

  MOCK_METHOD((std::unordered_map<rpc::LineageReconstructionTask, uint64_t>),
              GetOngoingLineageReconstructionTasks,
              (const ActorManager &actor_manager),
              (const, override));

  MOCK_METHOD(size_t, NumSubmissibleTasks, (), (const, override));

  MOCK_METHOD(void, AddTaskStatusInfo, (rpc::CoreWorkerStats * stats), (const, override));

  MOCK_METHOD(void,
              FillTaskInfo,
              (rpc::GetCoreWorkerStatsReply * reply, const int64_t limit),
              (const, override));

  MOCK_METHOD(size_t, NumPendingTasks, (), (const, override));
};

}  // namespace core
}  // namespace ray
