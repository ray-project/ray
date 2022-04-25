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

class MockWorkerInterface : public WorkerInterface {
 public:
  MOCK_METHOD(rpc::WorkerType, GetWorkerType, (), (const, override));
  MOCK_METHOD(void, MarkDead, (), (override));
  MOCK_METHOD(bool, IsDead, (), (const, override));
  MOCK_METHOD(void, MarkBlocked, (), (override));
  MOCK_METHOD(void, MarkUnblocked, (), (override));
  MOCK_METHOD(bool, IsBlocked, (), (const, override));
  MOCK_METHOD(WorkerID, WorkerId, (), (const, override));
  MOCK_METHOD(Process, GetProcess, (), (const, override));
  MOCK_METHOD(void, SetProcess, (Process proc), (override));
  MOCK_METHOD(Language, GetLanguage, (), (const, override));
  MOCK_METHOD(const std::string, IpAddress, (), (const, override));
  MOCK_METHOD(void, Connect, (int port), (override));
  MOCK_METHOD(void,
              Connect,
              (std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client),
              (override));
  MOCK_METHOD(int, Port, (), (const, override));
  MOCK_METHOD(int, AssignedPort, (), (const, override));
  MOCK_METHOD(void, SetAssignedPort, (int port), (override));
  MOCK_METHOD(void, AssignTaskId, (const TaskID &task_id), (override));
  MOCK_METHOD(const TaskID &, GetAssignedTaskId, (), (const, override));
  MOCK_METHOD(bool, AddBlockedTaskId, (const TaskID &task_id), (override));
  MOCK_METHOD(bool, RemoveBlockedTaskId, (const TaskID &task_id), (override));
  MOCK_METHOD(const std::unordered_set<TaskID> &,
              GetBlockedTaskIds,
              (),
              (const, override));
  MOCK_METHOD(const JobID &, GetAssignedJobId, (), (const, override));
  MOCK_METHOD(int, GetRuntimeEnvHash, (), (const, override));
  MOCK_METHOD(void, AssignActorId, (const ActorID &actor_id), (override));
  MOCK_METHOD(const ActorID &, GetActorId, (), (const, override));
  MOCK_METHOD(void, MarkDetachedActor, (), (override));
  MOCK_METHOD(bool, IsDetachedActor, (), (const, override));
  MOCK_METHOD(const std::shared_ptr<ClientConnection>, Connection, (), (const, override));
  MOCK_METHOD(void, SetOwnerAddress, (const rpc::Address &address), (override));
  MOCK_METHOD(const rpc::Address &, GetOwnerAddress, (), (const, override));
  MOCK_METHOD(void, DirectActorCallArgWaitComplete, (int64_t tag), (override));
  MOCK_METHOD(const BundleID &, GetBundleId, (), (const, override));
  MOCK_METHOD(void, SetBundleId, (const BundleID &bundle_id), (override));
  MOCK_METHOD(void,
              SetAllocatedInstances,
              (const std::shared_ptr<TaskResourceInstances> &allocated_instances),
              (override));
  MOCK_METHOD(std::shared_ptr<TaskResourceInstances>,
              GetAllocatedInstances,
              (),
              (override));
  MOCK_METHOD(void, ClearAllocatedInstances, (), (override));
  MOCK_METHOD(void,
              SetLifetimeAllocatedInstances,
              (const std::shared_ptr<TaskResourceInstances> &allocated_instances),
              (override));
  MOCK_METHOD(std::shared_ptr<TaskResourceInstances>,
              GetLifetimeAllocatedInstances,
              (),
              (override));
  MOCK_METHOD(void, ClearLifetimeAllocatedInstances, (), (override));
  MOCK_METHOD(RayTask &, GetAssignedTask, (), (override));
  MOCK_METHOD(void, SetAssignedTask, (const RayTask &assigned_task), (override));
  MOCK_METHOD(bool, IsRegistered, (), (override));
  MOCK_METHOD(rpc::CoreWorkerClientInterface *, rpc_client, (), (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockWorker : public Worker {
 public:
};

}  // namespace raylet
}  // namespace ray
