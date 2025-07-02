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

#include "gmock/gmock.h"
#include "ray/raylet/scheduling/local_task_manager_interface.h"

namespace ray::raylet {
class MockLocalTaskManager : public ILocalTaskManager {
 public:
  MOCK_METHOD(void,
              QueueAndScheduleTask,
              (std::shared_ptr<internal::Work> work),
              (override));
  MOCK_METHOD(void, ScheduleAndDispatchTasks, (), (override));
  MOCK_METHOD(bool,
              CancelTasks,
              (std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
               rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
               const std::string &scheduling_failure_message),
              (override));
  MOCK_METHOD((const absl::flat_hash_map<SchedulingClass,
                                         std::deque<std::shared_ptr<internal::Work>>> &),
              GetTaskToDispatch,
              (),
              (const, override));
  MOCK_METHOD((const absl::flat_hash_map<SchedulingClass,
                                         absl::flat_hash_map<WorkerID, int64_t>> &),
              GetBackLogTracker,
              (),
              (const, override));
  MOCK_METHOD(void,
              SetWorkerBacklog,
              (SchedulingClass scheduling_class,
               const WorkerID &worker_id,
               int64_t backlog_size),
              (override));
  MOCK_METHOD(void, ClearWorkerBacklog, (const WorkerID &worker_id), (override));
  MOCK_METHOD(const RayTask *,
              AnyPendingTasksForResourceAcquisition,
              (int *num_pending_actor_creation, int *num_pending_tasks),
              (const, override));
  MOCK_METHOD(void,
              TaskFinished,
              (std::shared_ptr<WorkerInterface> worker, RayTask *task),
              (override));
  MOCK_METHOD(void, TasksUnblocked, (const std::vector<TaskID> &ready_ids), (override));
  MOCK_METHOD(void,
              ReleaseWorkerResources,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReleaseCpuResourcesFromBlockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReturnCpuResourcesToUnblockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(ResourceSet, CalcNormalTaskResources, (), (const, override));
  MOCK_METHOD(void, RecordMetrics, (), (const, override));
  MOCK_METHOD(void, DebugStr, (std::stringstream & buffer), (const, override));
  MOCK_METHOD(size_t, GetNumTaskSpilled, (), (const, override));
  MOCK_METHOD(size_t, GetNumWaitingTaskSpilled, (), (const, override));
  MOCK_METHOD(size_t, GetNumUnschedulableTaskSpilled, (), (const, override));
};

}  // namespace ray::raylet
