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
#include <optional>
#include <string>

#include "absl/time/time.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/util/process.h"

namespace ray {

namespace rpc {
class ClientCallManager;
}

namespace raylet {

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerInterface {
 public:
  /// A destructor responsible for freeing all worker state.
  virtual ~WorkerInterface() {}
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

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker : public std::enable_shared_from_this<Worker>, public WorkerInterface {
 public:
  /// A constructor that initializes a worker object.
  /// NOTE: You MUST manually set the worker process.
  Worker(const JobID &job_id,
         int runtime_env_hash,
         const WorkerID &worker_id,
         const rpc::Language &language,
         rpc::WorkerType worker_type,
         const std::string &ip_address,
         std::shared_ptr<ClientConnection> connection,
         rpc::ClientCallManager &client_call_manager,
         StartupToken startup_token);

  rpc::WorkerType GetWorkerType() const override;
  void MarkDead() override;
  bool IsDead() const override;
  /// Kill the worker process. This is idempotent.
  /// \param io_service for scheduling the graceful period timer.
  /// \param force true to kill immediately, false to give time for the worker to clean up
  /// and exit gracefully.
  void KillAsync(instrumented_io_context &io_service, bool force = false) override;
  void MarkBlocked() override;
  void MarkUnblocked() override;
  bool IsBlocked() const override;
  /// Return the worker's ID.
  WorkerID WorkerId() const override;
  /// Return the worker process.
  Process GetProcess() const override;
  /// Return the worker process's startup token
  StartupToken GetStartupToken() const override;
  void SetProcess(Process proc) override;
  rpc::Language GetLanguage() const override;
  const std::string IpAddress() const override;
  void AsyncNotifyGCSRestart() override;
  /// Connect this worker's gRPC client.
  void Connect(int port) override;
  /// Testing-only
  void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) override;
  int Port() const override;
  int AssignedPort() const override;
  void SetAssignedPort(int port) override;
  void GrantLeaseId(const LeaseID &lease_id) override;
  const LeaseID &GetGrantedLeaseId() const override;
  const JobID &GetAssignedJobId() const override;
  const RayLease &GetGrantedLease() const override;
  std::optional<bool> GetIsGpu() const override;
  std::optional<bool> GetIsActorWorker() const override;
  int GetRuntimeEnvHash() const override;
  void AssignActorId(const ActorID &actor_id) override;
  const ActorID &GetActorId() const override;
  // Creates the debug string for the ID of the lease and the actor ID if it exists.
  const std::string GetLeaseIdAsDebugString() const override;
  bool IsDetachedActor() const override;
  const std::shared_ptr<ClientConnection> Connection() const override;
  void SetOwnerAddress(const rpc::Address &address) override;
  const rpc::Address &GetOwnerAddress() const override;

  std::optional<pid_t> GetSavedProcessGroupId() const override;
  void SetSavedProcessGroupId(pid_t pgid) override;

  void ActorCallArgWaitComplete(int64_t tag) override;

  const BundleID &GetBundleId() const override;
  void SetBundleId(const BundleID &bundle_id) override;

  // Setter, geter, and clear methods  for allocated_instances_.
  void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {
    allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() override {
    return allocated_instances_;
  };

  void ClearAllocatedInstances() override { allocated_instances_ = nullptr; };

  void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {
    lifetime_allocated_instances_ = allocated_instances;
  };

  const ActorID &GetRootDetachedActorId() const override {
    return root_detached_actor_id_;
  }

  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() override {
    return lifetime_allocated_instances_;
  };

  void ClearLifetimeAllocatedInstances() override {
    lifetime_allocated_instances_ = nullptr;
  };

  RayLease &GetGrantedLease() override { return granted_lease_; };

  void GrantLease(const RayLease &granted_lease) override {
    const auto &lease_spec = granted_lease.GetLeaseSpecification();
    SetJobId(lease_spec.JobId());
    SetBundleId(lease_spec.PlacementGroupBundleId());
    SetOwnerAddress(lease_spec.CallerAddress());
    GrantLeaseId(lease_spec.LeaseId());
    SetIsGpu(lease_spec.GetRequiredResources().Get(scheduling::ResourceID::GPU()) > 0);
    SetIsActorWorker(lease_spec.IsActorCreationTask());
    granted_lease_ = granted_lease;
    root_detached_actor_id_ = granted_lease.GetLeaseSpecification().RootDetachedActorId();
  }

  absl::Time GetGrantedLeaseTime() const override { return lease_grant_time_; };

  bool IsRegistered() override { return rpc_client_ != nullptr; }

  bool IsAvailableForScheduling() const override {
    return !IsDead()  // Not dead
           && !GetGrantedLeaseId()
                   .IsNil()  // Has assigned lease. This is intentionally incorrect since
                             // Ray Data relies on this for GC #56155
           && !IsBlocked()   // Not blocked
           && GetActorId().IsNil();  // No assigned actor
  }

  rpc::CoreWorkerClientInterface *rpc_client() override {
    RAY_CHECK(IsRegistered());
    return rpc_client_.get();
  }
  void SetJobId(const JobID &job_id) override;
  void SetIsGpu(bool is_gpu);
  void SetIsActorWorker(bool is_actor_worker);

 protected:
  void SetStartupToken(StartupToken startup_token) override;

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's process.
  Process proc_;
  /// The worker's process's startup_token
  StartupToken startup_token_;
  /// The language type of this worker.
  rpc::Language language_;
  /// The type of the worker.
  rpc::WorkerType worker_type_;
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
  /// The lease id of the worker's currently assigned lease.
  /// It is always Nil for the driver.
  LeaseID lease_id_;
  /// Job ID for the worker's current assigned lease.
  JobID assigned_job_id_;
  /// The hash of the worker's assigned runtime env.  We use this in the worker
  /// pool to cache and reuse workers with the same runtime env, because
  /// installing runtime envs from scratch can be slow.
  const int runtime_env_hash_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Root detached actor ID for the worker's last assigned lease.
  ActorID root_detached_actor_id_;
  /// The worker's placement group bundle. It is used to detect if the worker is
  /// associated with a placement group bundle.
  BundleID bundle_id_;
  /// Whether the worker is being killed by the KillAsync or MarkDead method.
  std::atomic<bool> killing_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The `ClientCallManager` object that is shared by `CoreWorkerClient` from all
  /// workers.
  rpc::ClientCallManager &client_call_manager_;
  /// The rpc client to send tasks to this worker.
  std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client_;
  /// The address of this worker's owner. The owner is the worker that
  /// currently holds the lease on this worker, if any.
  rpc::Address owner_address_;
  /// The capacity of each resource instance allocated to this worker in order
  /// to satisfy the resource requests of the granted lease.
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  /// The capacity of each resource instance allocated to this worker
  /// when running as an actor.
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  /// RayLease being assigned to this worker.
  RayLease granted_lease_;
  /// Time when the last lease was granted to this worker.
  absl::Time lease_grant_time_;
  /// Whether this worker ever holded a GPU resource. Once it holds a GPU or non-GPU lease
  /// it can't switch to the other type.
  std::optional<bool> is_gpu_ = std::nullopt;
  /// Whether this worker can hold an actor. Once it holds an actor or a normal lease, it
  /// can't switch to the other type.
  std::optional<bool> is_actor_worker_ = std::nullopt;
  /// If true, a RPC need to be sent to notify the worker about GCS restarting.
  bool notify_gcs_restarted_ = false;
  /// Saved process group id captured at registration time. Used for process-group
  /// cleanup validation at disconnect/stop.
  std::optional<pid_t> saved_pgid_ = std::nullopt;
};

}  // namespace raylet

}  // namespace ray
