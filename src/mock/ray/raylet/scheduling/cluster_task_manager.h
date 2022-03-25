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

namespace ray {
namespace raylet {

class MockWork : public Work {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockClusterTaskManager : public ClusterTaskManager {
 public:
  MOCK_METHOD(void,
              QueueAndScheduleTask,
              (const RayTask &task,
               rpc::RequestWorkerLeaseReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, TasksUnblocked, (const std::vector<TaskID> &ready_ids), (override));
  MOCK_METHOD(void,
              TaskFinished,
              (std::shared_ptr<WorkerInterface> worker, RayTask *task),
              (override));
  MOCK_METHOD(bool,
              CancelTask,
              (const TaskID &task_id, bool runtime_env_setup_failed),
              (override));
  MOCK_METHOD(void,
              FillPendingActorInfo,
              (rpc::GetNodeStatsReply * reply),
              (const, override));
  MOCK_METHOD(void,
              FillResourceUsage,
              (rpc::ResourcesData & data,
               const std::shared_ptr<SchedulingResources> &last_reported_resources),
              (override));
  MOCK_METHOD(bool,
              AnyPendingTasksForResourceAcquisition,
              (RayTask * exemplar,
               bool *any_pending,
               int *num_pending_actor_creation,
               int *num_pending_tasks),
              (const, override));
  MOCK_METHOD(void,
              ReleaseWorkerResources,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReleaseCpuResourcesFromUnblockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReturnCpuResourcesToBlockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(void, ScheduleAndDispatchTasks, (), (override));
  MOCK_METHOD(void, RecordMetrics, (), (override));
  MOCK_METHOD(std::string, DebugStr, (), (const, override));
  MOCK_METHOD(ResourceSet, CalcNormalTaskResources, (), (const, override));
};

}  // namespace raylet
}  // namespace ray
