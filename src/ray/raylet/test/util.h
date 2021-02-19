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

#include "ray/raylet/worker.h"

namespace ray {

namespace raylet {

class MockWorker : public WorkerInterface {
 public:
  MockWorker(WorkerID worker_id, int port)
      : worker_id_(worker_id), port_(port), is_detached_actor_(false) {}

  WorkerID WorkerId() const { return worker_id_; }

  rpc::WorkerType GetWorkerType() const { return rpc::WorkerType::WORKER; }

  int Port() const { return port_; }

  void SetOwnerAddress(const rpc::Address &address) { address_ = address; }

  void AssignTaskId(const TaskID &task_id) {}

  void SetAssignedTask(const Task &assigned_task) { task_ = assigned_task; }

  const std::string IpAddress() const { return address_.ip_address(); }

  void SetAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    allocated_instances_ = allocated_instances;
  }

  void SetLifetimeAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    lifetime_allocated_instances_ = allocated_instances;
  }

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() {
    return allocated_instances_;
  }
  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() {
    return lifetime_allocated_instances_;
  }

  void MarkDead() { RAY_CHECK(false) << "Method unused"; }
  bool IsDead() const {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  void MarkBlocked() { blocked_ = true; }
  void MarkUnblocked() { blocked_ = false; }
  bool IsBlocked() const { return blocked_; }

  Process GetProcess() const { return Process::CreateNewDummy(); }
  void SetProcess(Process proc) { RAY_CHECK(false) << "Method unused"; }
  Language GetLanguage() const {
    RAY_CHECK(false) << "Method unused";
    return Language::PYTHON;
  }

  void Connect(int port) { RAY_CHECK(false) << "Method unused"; }

  int AssignedPort() const {
    RAY_CHECK(false) << "Method unused";
    return -1;
  }
  void SetAssignedPort(int port) { RAY_CHECK(false) << "Method unused"; }
  const TaskID &GetAssignedTaskId() const {
    RAY_CHECK(false) << "Method unused";
    return TaskID::Nil();
  }
  bool AddBlockedTaskId(const TaskID &task_id) {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  bool RemoveBlockedTaskId(const TaskID &task_id) {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new std::unordered_set<TaskID>();
    return *t;
  }
  const JobID &GetAssignedJobId() const {
    RAY_CHECK(false) << "Method unused";
    return JobID::Nil();
  }
  void AssignActorId(const ActorID &actor_id) { RAY_CHECK(false) << "Method unused"; }
  const ActorID &GetActorId() const {
    RAY_CHECK(false) << "Method unused";
    return ActorID::Nil();
  }
  void MarkDetachedActor() { is_detached_actor_ = true; }
  bool IsDetachedActor() const { return is_detached_actor_; }
  const std::shared_ptr<ClientConnection> Connection() const {
    RAY_CHECK(false) << "Method unused";
    return nullptr;
  }
  const rpc::Address &GetOwnerAddress() const {
    RAY_CHECK(false) << "Method unused";
    return address_;
  }

  const ResourceIdSet &GetLifetimeResourceIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids) {
    RAY_CHECK(false) << "Method unused";
  }
  void ResetLifetimeResourceIds() { RAY_CHECK(false) << "Method unused"; }

  const ResourceIdSet &GetTaskResourceIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void SetTaskResourceIds(ResourceIdSet &resource_ids) {
    RAY_CHECK(false) << "Method unused";
  }
  void ResetTaskResourceIds() { RAY_CHECK(false) << "Method unused"; }
  ResourceIdSet ReleaseTaskCpuResources() {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources) {
    RAY_CHECK(false) << "Method unused";
  }

  Status AssignTask(const Task &task, const ResourceIdSet &resource_id_set) {
    RAY_CHECK(false) << "Method unused";
    Status s;
    return s;
  }
  void DirectActorCallArgWaitComplete(int64_t tag) {
    RAY_CHECK(false) << "Method unused";
  }

  void ClearAllocatedInstances() { allocated_instances_ = nullptr; }

  void ClearLifetimeAllocatedInstances() { lifetime_allocated_instances_ = nullptr; }

  const BundleID &GetBundleId() const {
    RAY_CHECK(false) << "Method unused";
    return bundle_id_;
  }

  void SetBundleId(const BundleID &bundle_id) { bundle_id_ = bundle_id; }

  Task &GetAssignedTask() { return task_; }

  bool IsRegistered() {
    RAY_CHECK(false) << "Method unused";
    return false;
  }

  rpc::CoreWorkerClientInterface *rpc_client() {
    RAY_CHECK(false) << "Method unused";
    return nullptr;
  }

 private:
  WorkerID worker_id_;
  int port_;
  rpc::Address address_;
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  std::vector<double> borrowed_cpu_instances_;
  bool is_detached_actor_;
  BundleID bundle_id_;
  bool blocked_ = false;
  Task task_;
};

}  // namespace raylet

}  // namespace ray
