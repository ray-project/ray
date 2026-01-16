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
#include <string>
#include <utility>
#include <vector>

#include "ray/raylet/worker.h"
#include "ray/raylet_ipc_client/client_connection.h"

namespace ray {
namespace raylet {

class FakeWorker : public WorkerInterface {
 public:
  FakeWorker(WorkerID worker_id, int port, instrumented_io_context &io_context)
      : worker_id_(worker_id),
        port_(port),
        proc_(Process::CreateNewDummy()),
        connection_([&io_context]() {
          local_stream_socket socket(io_context);
          return ClientConnection::Create(
              [](std::shared_ptr<ClientConnection>,
                 int64_t,
                 const std::vector<uint8_t> &) {},
              [](std::shared_ptr<ClientConnection>, const boost::system::error_code &) {},
              std::move(socket),
              "fake_worker_connection",
              {});
        }()) {}

  WorkerID WorkerId() const override { return worker_id_; }
  rpc::WorkerType GetWorkerType() const override { return rpc::WorkerType::WORKER; }
  int Port() const override { return port_; }
  void SetOwnerAddress(const rpc::Address &address) override {}
  void GrantLease(const RayLease &granted_lease) override {}
  void GrantLeaseId(const LeaseID &lease_id) override { lease_id_ = lease_id; }
  const RayLease &GetGrantedLease() const override { return granted_lease_; }
  absl::Time GetGrantedLeaseTime() const override { return absl::InfiniteFuture(); }
  std::optional<bool> GetIsGpu() const override { return std::nullopt; }
  std::optional<bool> GetIsActorWorker() const override { return std::nullopt; }
  const std::string IpAddress() const override { return "127.0.0.1"; }
  void AsyncNotifyGCSRestart() override {}
  void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {}
  void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) override {}
  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() override {
    return nullptr;
  }
  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() override {
    return nullptr;
  }
  void MarkDead() override {}
  bool IsDead() const override { return false; }
  void KillAsync(instrumented_io_context &io_service, bool force) override {}
  void MarkBlocked() override {}
  void MarkUnblocked() override {}
  bool IsBlocked() const override { return false; }
  Process GetProcess() const override { return proc_; }
  StartupToken GetStartupToken() const override { return 0; }
  void SetProcess(Process proc) override {}
  Language GetLanguage() const override { return Language::PYTHON; }
  void Connect(int port) override {}
  void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) override {}
  int AssignedPort() const override { return -1; }
  void SetAssignedPort(int port) override {}
  const LeaseID &GetGrantedLeaseId() const override { return lease_id_; }
  const JobID &GetAssignedJobId() const override { return job_id_; }
  int GetRuntimeEnvHash() const override { return 0; }
  void AssignActorId(const ActorID &actor_id) override {}
  const ActorID &GetActorId() const override { return actor_id_; }
  const std::string GetLeaseIdAsDebugString() const override { return ""; }
  bool IsDetachedActor() const override { return false; }
  const std::shared_ptr<ClientConnection> Connection() const override {
    return connection_;
  }
  const rpc::Address &GetOwnerAddress() const override { return owner_address_; }
  std::optional<pid_t> GetSavedProcessGroupId() const override { return std::nullopt; }
  void SetSavedProcessGroupId(pid_t pgid) override {}
  void ActorCallArgWaitComplete(int64_t tag) override {}
  void ClearAllocatedInstances() override {}
  void ClearLifetimeAllocatedInstances() override {}
  const BundleID &GetBundleId() const override { return bundle_id_; }
  void SetBundleId(const BundleID &bundle_id) override { bundle_id_ = bundle_id; }
  RayLease &GetGrantedLease() override { return granted_lease_; }
  bool IsRegistered() override { return false; }
  rpc::CoreWorkerClientInterface *rpc_client() override { return nullptr; }
  bool IsAvailableForScheduling() const override { return true; }
  void SetJobId(const JobID &job_id) override {}
  const ActorID &GetRootDetachedActorId() const override {
    return root_detached_actor_id_;
  }

 protected:
  void SetStartupToken(StartupToken startup_token) override {}

 private:
  WorkerID worker_id_;
  int port_;
  LeaseID lease_id_;
  BundleID bundle_id_;
  Process proc_;
  std::shared_ptr<ClientConnection> connection_;
  RayLease granted_lease_;
  JobID job_id_;
  ActorID actor_id_;
  rpc::Address owner_address_;
  ActorID root_detached_actor_id_;
};

}  // namespace raylet
}  // namespace ray
