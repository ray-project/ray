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

#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/lease/lease.h"
#include "ray/raylet/worker_interface.h"
#include "ray/util/fake_process.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

namespace raylet {

class MockWorker : public WorkerInterface {
 public:
  MockWorker(WorkerID worker_id, int port, int runtime_env_hash = 0)
      : worker_id_(worker_id),
        port_(port),
        runtime_env_hash_(runtime_env_hash),
        job_id_(JobID::FromInt(859)),
        proc_(std::make_unique<FakeProcess>()) {}

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

  const ProcessInterface &GetProcess() const override { return *proc_; }
  void SetProcess(std::unique_ptr<ProcessInterface> proc) override {
    proc_ = std::move(proc);
  }

  rpc::Language GetLanguage() const override {
    RAY_CHECK(false) << "Method unused";
    return rpc::Language::PYTHON;
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
  std::unique_ptr<ProcessInterface> proc_;
  std::atomic<bool> killing_ = false;
  std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client_;
};

/**
 * @brief Creates a MockWorker for a normal or actor creation task.
 *
 * @param owner_id The parent task ID for the lease.
 * @param max_retries The maximum number of task retries allowed.
 * For actor creation tasks, this is the maximum number of actor restarts allowed.
 * @param port The port number for the worker.
 * @param task_type The type of task to create.
 * Only supports NORMAL_TASK and ACTOR_CREATION_TASK.
 * @return A shared pointer to the created worker.
 */
inline std::shared_ptr<WorkerInterface> CreateTaskWorker(TaskID owner_id,
                                                         int32_t max_retries,
                                                         int32_t port,
                                                         rpc::TaskType task_type) {
  rpc::LeaseSpec message;
  message.set_lease_id(LeaseID::FromRandom().Binary());
  message.set_parent_task_id(owner_id.Binary());
  message.set_type(task_type);
  if (task_type == rpc::TaskType::NORMAL_TASK) {
    message.set_max_retries(max_retries);
  } else if (task_type == rpc::TaskType::ACTOR_CREATION_TASK) {
    message.set_max_actor_restarts(max_retries);
  } else {
    RAY_CHECK(false) << absl::StrFormat(
        "Unexpected task type: %d received when creating mock task worker. "
        "Only supports NORMAL_TASK and ACTOR_CREATION_TASK.",
        static_cast<int>(task_type));
  }
  LeaseSpecification lease_spec(message);
  RayLease lease(lease_spec);
  auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port);
  worker->GrantLease(lease);
  return worker;
}

/**
 * @brief Kills a worker's process by replacing it with a dead FakeProcess.
 * @note This function assumes that the worker is mocked and SetProcess can be called
 *       more than once.
 *
 * @param worker The worker whose process should be killed.
 */
inline void KillWorkerProcess(std::shared_ptr<WorkerInterface> worker) {
  std::unique_ptr<FakeProcess> fake_process = std::make_unique<FakeProcess>();
  fake_process->SetAlive(false);
  worker->SetProcess(std::move(fake_process));
}

}  // namespace raylet

}  // namespace ray
