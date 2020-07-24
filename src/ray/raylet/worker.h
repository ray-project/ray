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

#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerInterface {
 public:
  /// A destructor responsible for freeing all worker state.
  virtual ~WorkerInterface() {}
  virtual void MarkDead() = 0;
  virtual bool IsDead() const = 0;
  virtual void MarkBlocked() = 0;
  virtual void MarkUnblocked() = 0;
  virtual bool IsBlocked() const = 0;
  /// Return the worker's ID.
  virtual WorkerID WorkerId() const = 0;
  /// Return the worker process.
  virtual Process GetProcess() const = 0;
  virtual void SetProcess(Process proc) = 0;
  virtual Language GetLanguage() const = 0;
  virtual const std::string IpAddress() const = 0;
  /// Connect this worker's gRPC client.
  virtual void Connect(int port) = 0;
  virtual int Port() const = 0;
  virtual int AssignedPort() const = 0;
  virtual void SetAssignedPort(int port) = 0;
  virtual void AssignTaskId(const TaskID &task_id) = 0;
  virtual const TaskID &GetAssignedTaskId() const = 0;
  virtual bool AddBlockedTaskId(const TaskID &task_id) = 0;
  virtual bool RemoveBlockedTaskId(const TaskID &task_id) = 0;
  virtual const std::unordered_set<TaskID> &GetBlockedTaskIds() const = 0;
  virtual void AssignJobId(const JobID &job_id) = 0;
  virtual const JobID &GetAssignedJobId() const = 0;
  virtual void AssignActorId(const ActorID &actor_id) = 0;
  virtual const ActorID &GetActorId() const = 0;
  virtual void MarkDetachedActor() = 0;
  virtual bool IsDetachedActor() const = 0;
  virtual const std::shared_ptr<ClientConnection> Connection() const = 0;
  virtual void SetOwnerAddress(const rpc::Address &address) = 0;
  virtual const rpc::Address &GetOwnerAddress() const = 0;

  virtual const ResourceIdSet &GetLifetimeResourceIds() const = 0;
  virtual void SetLifetimeResourceIds(ResourceIdSet &resource_ids) = 0;
  virtual void ResetLifetimeResourceIds() = 0;

  virtual const ResourceIdSet &GetTaskResourceIds() const = 0;
  virtual void SetTaskResourceIds(ResourceIdSet &resource_ids) = 0;
  virtual void ResetTaskResourceIds() = 0;
  virtual ResourceIdSet ReleaseTaskCpuResources() = 0;
  virtual void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources) = 0;

  virtual Status AssignTask(const Task &task, const ResourceIdSet &resource_id_set) = 0;
  virtual void DirectActorCallArgWaitComplete(int64_t tag) = 0;

  // Setter, geter, and clear methods  for allocated_instances_.
  virtual void SetAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;

  virtual std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() = 0;

  virtual void ClearAllocatedInstances() = 0;

  virtual void SetLifetimeAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;
  virtual std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() = 0;

  virtual void ClearLifetimeAllocatedInstances() = 0;

  virtual void SetBorrowedCPUInstances(std::vector<double> &cpu_instances) = 0;

  virtual std::vector<double> &GetBorrowedCPUInstances() = 0;

  virtual void ClearBorrowedCPUInstances() = 0;

  virtual Task &GetAssignedTask() = 0;

  virtual void SetAssignedTask(Task &assigned_task) = 0;

  virtual bool IsRegistered() = 0;

  virtual rpc::CoreWorkerClient *rpc_client() = 0;
};

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker : public WorkerInterface {
 public:
  /// A constructor that initializes a worker object.
  /// NOTE: You MUST manually set the worker process.
  Worker(const WorkerID &worker_id, const Language &language,
         const std::string &ip_address, std::shared_ptr<ClientConnection> connection,
         rpc::ClientCallManager &client_call_manager);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  void MarkDead();
  bool IsDead() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's ID.
  WorkerID WorkerId() const;
  /// Return the worker process.
  Process GetProcess() const;
  void SetProcess(Process proc);
  Language GetLanguage() const;
  const std::string IpAddress() const;
  /// Connect this worker's gRPC client.
  void Connect(int port);
  int Port() const;
  int AssignedPort() const;
  void SetAssignedPort(int port);
  void AssignTaskId(const TaskID &task_id);
  const TaskID &GetAssignedTaskId() const;
  bool AddBlockedTaskId(const TaskID &task_id);
  bool RemoveBlockedTaskId(const TaskID &task_id);
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;
  void AssignJobId(const JobID &job_id);
  const JobID &GetAssignedJobId() const;
  void AssignActorId(const ActorID &actor_id);
  const ActorID &GetActorId() const;
  void MarkDetachedActor();
  bool IsDetachedActor() const;
  const std::shared_ptr<ClientConnection> Connection() const;
  void SetOwnerAddress(const rpc::Address &address);
  const rpc::Address &GetOwnerAddress() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

  Status AssignTask(const Task &task, const ResourceIdSet &resource_id_set);
  void DirectActorCallArgWaitComplete(int64_t tag);

  // Setter, geter, and clear methods  for allocated_instances_.
  void SetAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() {
    return allocated_instances_;
  };

  void ClearAllocatedInstances() { allocated_instances_ = nullptr; };

  void SetLifetimeAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    lifetime_allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() {
    return lifetime_allocated_instances_;
  };

  void ClearLifetimeAllocatedInstances() { lifetime_allocated_instances_ = nullptr; };

  void SetBorrowedCPUInstances(std::vector<double> &cpu_instances) {
    borrowed_cpu_instances_ = cpu_instances;
  };

  std::vector<double> &GetBorrowedCPUInstances() { return borrowed_cpu_instances_; };

  void ClearBorrowedCPUInstances() { return borrowed_cpu_instances_.clear(); };

  Task &GetAssignedTask() { return assigned_task_; };

  void SetAssignedTask(Task &assigned_task) { assigned_task_ = assigned_task; };

  bool IsRegistered() { return rpc_client_ != nullptr; }

  rpc::CoreWorkerClient *rpc_client() {
    RAY_CHECK(IsRegistered());
    return rpc_client_.get();
  }

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's process.
  Process proc_;
  /// The language type of this worker.
  Language language_;
  /// IP address of this worker.
  std::string ip_address_;
  /// Port assigned to this worker by the raylet. If this is 0, the actual
  /// port the worker listens (port_) on will be a random one. This is required
  /// because a worker could crash before announcing its port, in which case
  /// we still need to be able to mark that port as free.
  int assigned_port_;
  /// Port that this worker listens on.
  int port_;
  /// Connection state of a worker.
  std::shared_ptr<ClientConnection> connection_;
  /// The worker's currently assigned task.
  TaskID assigned_task_id_;
  /// Job ID for the worker's current assigned task.
  JobID assigned_job_id_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Whether the worker is dead.
  bool dead_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The specific resource IDs that this worker owns for its lifetime. This is
  /// only used for actors.
  ResourceIdSet lifetime_resource_ids_;
  /// The specific resource IDs that this worker currently owns for the duration
  // of a task.
  ResourceIdSet task_resource_ids_;
  std::unordered_set<TaskID> blocked_task_ids_;
  /// The `ClientCallManager` object that is shared by `CoreWorkerClient` from all
  /// workers.
  rpc::ClientCallManager &client_call_manager_;
  /// The rpc client to send tasks to this worker.
  std::unique_ptr<rpc::CoreWorkerClient> rpc_client_;
  /// Whether the worker is detached. This is applies when the worker is actor.
  /// Detached actor means the actor's creator can exit without killing this actor.
  bool is_detached_actor_;
  /// The address of this worker's owner. The owner is the worker that
  /// currently holds the lease on this worker, if any.
  rpc::Address owner_address_;
  /// The capacity of each resource instance allocated to this worker in order
  /// to satisfy the resource requests of the task is currently running.
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  /// The capacity of each resource instance allocated to this worker
  /// when running as an actor.
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  /// CPUs borrowed by the worker. This happens in the following scenario:
  /// 1) Worker A is blocked, so it donates its CPUs back to the node.
  /// 2) Other workers are scheduled and are allocated some of the CPUs donated by A.
  /// 3) Task A is unblocked, but it cannot get all CPUs back. At this point,
  /// the node is oversubscribed. borrowed_cpu_instances_ represents the number
  /// of CPUs this node is oversubscribed by.
  /// TODO (Ion): Investigate a more intuitive alternative to track these Cpus.
  std::vector<double> borrowed_cpu_instances_;
  /// Task being assigned to this worker.
  Task assigned_task_;
};

}  // namespace raylet

}  // namespace ray
