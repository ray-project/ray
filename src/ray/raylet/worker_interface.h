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

#include <memory>
#include <optional>
#include <string>

#include "absl/time/time.h"
#include "ray/common/id.h"
#include "ray/util/process.h"

class instrumented_io_context;

namespace ray {

class ClientConnection;
class RayLease;
class TaskResourceInstances;

namespace rpc {
class Address;
class CoreWorkerClientInterface;
enum Language : int;
enum WorkerType : int;
}  // namespace rpc

namespace raylet {

/// \class WorkerInterface
///
/// Interface for worker implementations. Used for dependency injection and testing.
class WorkerInterface {
 public:
  /// A destructor responsible for freeing all worker state.
  virtual ~WorkerInterface() = default;
  virtual rpc::WorkerType GetWorkerType() const = 0;
  virtual void MarkDead() = 0;
  virtual bool IsDead() const = 0;
  virtual void KillAsync(instrumented_io_context &io_service, bool force = false) = 0;
  virtual void MarkBlocked() = 0;
  virtual void MarkUnblocked() = 0;
  virtual bool IsBlocked() const = 0;
  /// Return the worker's ID.
  virtual WorkerID WorkerId() const = 0;
  /// Return the worker process.
  virtual Process GetProcess() const = 0;
  /// Return the worker process's startup token
  virtual StartupToken GetStartupToken() const = 0;
  virtual void SetProcess(Process proc) = 0;
  virtual rpc::Language GetLanguage() const = 0;
  virtual const std::string IpAddress() const = 0;
  virtual void AsyncNotifyGCSRestart() = 0;
  /// Connect this worker's gRPC client.
  virtual void Connect(int port) = 0;
  /// Testing-only
  virtual void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) = 0;
  virtual int Port() const = 0;
  virtual int AssignedPort() const = 0;
  virtual void SetAssignedPort(int port) = 0;
  virtual void GrantLeaseId(const LeaseID &lease_id) = 0;
  virtual const LeaseID &GetGrantedLeaseId() const = 0;
  virtual const JobID &GetAssignedJobId() const = 0;
  virtual const RayLease &GetGrantedLease() const = 0;
  virtual std::optional<bool> GetIsGpu() const = 0;
  virtual std::optional<bool> GetIsActorWorker() const = 0;
  virtual int GetRuntimeEnvHash() const = 0;
  virtual void AssignActorId(const ActorID &actor_id) = 0;
  virtual const ActorID &GetActorId() const = 0;
  virtual const std::string GetLeaseIdAsDebugString() const = 0;
  virtual bool IsDetachedActor() const = 0;
  virtual const std::shared_ptr<ClientConnection> Connection() const = 0;
  virtual void SetOwnerAddress(const rpc::Address &address) = 0;
  virtual const rpc::Address &GetOwnerAddress() const = 0;
  /// Optional saved process group id (PGID) for this worker's process group.
  /// Set at registration time from getpgid(pid) and used for safe cleanup.
  virtual std::optional<pid_t> GetSavedProcessGroupId() const = 0;
  virtual void SetSavedProcessGroupId(pid_t pgid) = 0;

  virtual void ActorCallArgWaitComplete(int64_t tag) = 0;

  virtual const BundleID &GetBundleId() const = 0;
  virtual void SetBundleId(const BundleID &bundle_id) = 0;

  // Setter, geter, and clear methods  for allocated_instances_.
  virtual void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;

  virtual std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() = 0;

  virtual void ClearAllocatedInstances() = 0;

  virtual void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;
  virtual std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() = 0;

  virtual void ClearLifetimeAllocatedInstances() = 0;

  virtual RayLease &GetGrantedLease() = 0;

  virtual void GrantLease(const RayLease &granted_lease) = 0;

  virtual bool IsRegistered() = 0;

  virtual rpc::CoreWorkerClientInterface *rpc_client() = 0;

  /// Return True if the worker is available for scheduling a task or actor.
  virtual bool IsAvailableForScheduling() const = 0;

  /// Time when the last task was assigned to this worker.
  virtual absl::Time GetGrantedLeaseTime() const = 0;

  virtual void SetJobId(const JobID &job_id) = 0;

  virtual const ActorID &GetRootDetachedActorId() const = 0;

 protected:
  virtual void SetStartupToken(StartupToken startup_token) = 0;

  FRIEND_TEST(WorkerPoolDriverRegisteredTest, PopWorkerMultiTenancy);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, TestWorkerCapping);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest,
              TestWorkerCappingLaterNWorkersNotOwningObjects);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, TestJobFinishedForceKillIdleWorker);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, TestJobFinishedForPopWorker);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest,
              WorkerFromAliveJobDoesNotBlockWorkerFromDeadJobFromGettingKilled);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, TestWorkerCappingWithExitDelay);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, MaximumStartupConcurrency);
  FRIEND_TEST(WorkerPoolDriverRegisteredTest, HandleWorkerRegistration);
};

}  // namespace raylet
}  // namespace ray
