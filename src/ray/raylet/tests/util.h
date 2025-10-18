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
#include <string>
#include <utility>
#include <vector>

#include "ray/raylet/worker_interface.h"

namespace ray {

namespace raylet {

class MockWorker : public WorkerInterface {
 public:
  MockWorker(WorkerID worker_id, int port, int runtime_env_hash = 0)
      : worker_id_(worker_id),
        port_(port),
        runtime_env_hash_(runtime_env_hash),
        job_id_(JobID::FromInt(859)),
        proc_(Process::CreateNewDummy()) {}

  WorkerID WorkerId() const override { return worker_id_; }

  rpc::WorkerType GetWorkerType() const override { return rpc::WorkerType::WORKER; }

  int Port() const override { return port_; }

  void SetOwnerAddress(const rpc::Address &address) override { address_ = address; }

  void GrantLease(const RayLease &granted_lease) override {
    lease_ = granted_lease;
    lease_grant_time_ = absl::Now();
    root_detached_actor_id_ = granted_lease.GetLeaseSpecification().RootDetachedActorId();
    const auto &lease_spec = granted_lease.GetLeaseSpecification();
    SetJobId(lease_spec.JobId());
    SetBundleId(lease_spec.PlacementGroupBundleId());
    SetOwnerAddress(lease_spec.CallerAddress());
    GrantLeaseId(lease_spec.LeaseId());
  };

  void GrantLeaseId(const LeaseID &lease_id) override { lease_id_ = lease_id; }

  const RayLease &GetGrantedLease() const override { return lease_; }

  absl::Time GetGrantedLeaseTime() const override { return lease_grant_time_; };

  std::optional<bool> GetIsGpu() const override { return is_gpu_; }

  std::optional<bool> GetIsActorWorker() const override { return is_actor_worker_; }

  const std::string IpAddress() const override { return address_.ip_address(); }

  void AsyncNotifyGCSRestart() override {}

  void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {
    allocated_instances_ = allocated_instances;
  }

  void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {
    lifetime_allocated_instances_ = allocated_instances;
  }

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() override {
    return allocated_instances_;
  }
  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() override {
    return lifetime_allocated_instances_;
  }

  void MarkDead() override { RAY_CHECK(false) << "Method unused"; }
  bool IsDead() const override { return killing_.load(std::memory_order_acquire); }
  void KillAsync(instrumented_io_context &io_service, bool force) override {
    bool expected = false;
    killing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
  }
  bool IsKilled() const { return killing_.load(std::memory_order_acquire); }
  void MarkBlocked() override { blocked_ = true; }
  void MarkUnblocked() override { blocked_ = false; }
  bool IsBlocked() const override { return blocked_; }

  Process GetProcess() const override { return proc_; }
  StartupToken GetStartupToken() const override { return 0; }
  void SetProcess(Process proc) override { proc_ = std::move(proc); }

  Language GetLanguage() const override {
    RAY_CHECK(false) << "Method unused";
    return Language::PYTHON;
  }

  void Connect(int port) override { RAY_CHECK(false) << "Method unused"; }

  void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) override {
    rpc_client_ = rpc_client;
  }

  int AssignedPort() const override {
    RAY_CHECK(false) << "Method unused";
    return -1;
  }
  void SetAssignedPort(int port) override { RAY_CHECK(false) << "Method unused"; }
  const LeaseID &GetGrantedLeaseId() const override { return lease_id_; }
  const JobID &GetAssignedJobId() const override { return job_id_; }
  int GetRuntimeEnvHash() const override { return runtime_env_hash_; }
  void AssignActorId(const ActorID &actor_id) override {
    RAY_CHECK(false) << "Method unused";
  }
  const ActorID &GetActorId() const override {
    RAY_CHECK(false) << "Method unused";
    return ActorID::Nil();
  }
  const std::string GetLeaseIdAsDebugString() const override {
    RAY_CHECK(false) << "Method unused";
    return "";
  }

  bool IsDetachedActor() const override {
    return lease_.GetLeaseSpecification().IsDetachedActor();
  }

  const std::shared_ptr<ClientConnection> Connection() const override { return nullptr; }
  const rpc::Address &GetOwnerAddress() const override { return address_; }
  std::optional<pid_t> GetSavedProcessGroupId() const override { return std::nullopt; }
  void SetSavedProcessGroupId(pid_t pgid) override { (void)pgid; }

  void ActorCallArgWaitComplete(int64_t tag) override {
    RAY_CHECK(false) << "Method unused";
  }

  void ClearAllocatedInstances() override { allocated_instances_ = nullptr; }

  void ClearLifetimeAllocatedInstances() override {
    lifetime_allocated_instances_ = nullptr;
  }

  const BundleID &GetBundleId() const override {
    RAY_CHECK(false) << "Method unused";
    return bundle_id_;
  }

  void SetBundleId(const BundleID &bundle_id) override { bundle_id_ = bundle_id; }

  RayLease &GetGrantedLease() override { return lease_; }

  bool IsRegistered() override {
    RAY_CHECK(false) << "Method unused";
    return false;
  }

  rpc::CoreWorkerClientInterface *rpc_client() override { return rpc_client_.get(); }

  bool IsAvailableForScheduling() const override {
    RAY_CHECK(false) << "Method unused";
    return true;
  }

  void SetJobId(const JobID &job_id) override { job_id_ = job_id; }

  const ActorID &GetRootDetachedActorId() const override {
    return root_detached_actor_id_;
  }

 protected:
  void SetStartupToken(StartupToken startup_token) override {
    RAY_CHECK(false) << "Method unused";
  };

 private:
  WorkerID worker_id_;
  int port_;
  rpc::Address address_;
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  std::vector<double> borrowed_cpu_instances_;
  std::optional<bool> is_gpu_;
  std::optional<bool> is_actor_worker_;
  BundleID bundle_id_;
  bool blocked_ = false;
  RayLease lease_;
  absl::Time lease_grant_time_;
  int runtime_env_hash_;
  LeaseID lease_id_;
  JobID job_id_;
  ActorID root_detached_actor_id_;
  Process proc_;
  std::atomic<bool> killing_ = false;
  std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client_;
};

}  // namespace raylet

}  // namespace ray
