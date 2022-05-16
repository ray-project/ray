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
#include "gmock/gmock.h"
namespace ray {
namespace core {

class MockTaskFinisherInterface : public TaskFinisherInterface {
 public:
  MOCK_METHOD(void,
              CompletePendingTask,
              (const TaskID &task_id,
               const rpc::PushTaskReply &reply,
               const rpc::Address &actor_addr),
              (override));
  MOCK_METHOD(void,
              FailPendingTask,
              (const TaskID &task_id,
               rpc::ErrorType error_type,
               const Status *status,
               const rpc::RayErrorInfo *ray_error_info,
               bool mark_task_object_failed),
              (override));
  MOCK_METHOD(bool,
              FailOrRetryPendingTask,
              (const TaskID &task_id,
               rpc::ErrorType error_type,
               const Status *status,
               const rpc::RayErrorInfo *ray_error_info,
               bool mark_task_object_failed),
              (override));
  MOCK_METHOD(void,
              OnTaskDependenciesInlined,
              (const std::vector<ObjectID> &inlined_dependency_ids,
               const std::vector<ObjectID> &contained_ids),
              (override));
  MOCK_METHOD(bool, MarkTaskCanceled, (const TaskID &task_id), (override));
  MOCK_METHOD(void,
              MarkTaskReturnObjectsFailed,
              (const TaskSpecification &spec,
               rpc::ErrorType error_type,
               const rpc::RayErrorInfo *ray_error_info),
              (override));
  MOCK_METHOD(absl::optional<TaskSpecification>,
              GetTaskSpec,
              (const TaskID &task_id),
              (const, override));
  MOCK_METHOD(bool, RetryTaskIfPossible, (const TaskID &task_id), (override));
  MOCK_METHOD(void, MarkDependenciesResolved, (const TaskID &task_id), (override));
  MOCK_METHOD(void, MarkTaskWaitingForExecution, (const TaskID &task_id), (override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockTaskResubmissionInterface : public TaskResubmissionInterface {
 public:
  MOCK_METHOD(bool,
              ResubmitTask,
              (const TaskID &task_id, std::vector<ObjectID> *task_deps),
              (override));
};

}  // namespace core
}  // namespace ray
