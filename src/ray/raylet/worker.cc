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

#include <boost/bind/bind.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#if defined(__linux__)
#include <poll.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

// Generic syscall numbers; older kernel headers lack the definitions.
#ifndef SYS_pidfd_send_signal
#define SYS_pidfd_send_signal 424
#endif
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#endif

#include "ray/core_worker_rpc_client/core_worker_client.h"
#include "ray/flatbuffers/node_manager_generated.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

namespace raylet {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(const JobID &job_id,
               int runtime_env_hash,
               const WorkerID &worker_id,
               const rpc::Language &language,
               rpc::WorkerType worker_type,
               const std::string &ip_address,
               std::shared_ptr<ClientConnection> connection,
               rpc::ClientCallManager &client_call_manager,
               ClockInterface &clock)
    : worker_id_(worker_id),
      language_(language),
      worker_type_(worker_type),
      ip_address_(ip_address),
      assigned_port_(-1),
      port_(-1),
      connection_(std::move(connection)),
      assigned_job_id_(job_id),
      runtime_env_hash_(runtime_env_hash),
      bundle_id_(std::make_pair(PlacementGroupID::Nil(), -1)),
      killing_(false),
      blocked_(false),
      client_call_manager_(client_call_manager),
      clock_(clock) {}

rpc::WorkerType Worker::GetWorkerType() const { return worker_type_; }

void Worker::MarkDead() {
  bool expected = false;
  killing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

bool Worker::IsDead() const { return killing_.load(std::memory_order_acquire); }

void Worker::KillAsync(instrumented_io_context &io_service, bool force) {
  bool expected = false;
  if (!killing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return;  // This is not the first time calling KillAsync or MarkDead, do nothing.
  }
  const auto worker = shared_from_this();
  if (force) {
    SignalProcess(/*force=*/true);
    return;
  }
  // Attempt to gracefully shutdown the worker before force killing it.
  SignalProcess(/*force=*/false);

  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service);
  auto timeout = RayConfig::instance().kill_worker_timeout_milliseconds();
  auto retry_duration = boost::posix_time::milliseconds(timeout);
  retry_timer->expires_from_now(retry_duration);
  retry_timer->async_wait(
      [timeout, retry_timer, worker](const boost::system::error_code &error) {
#ifdef _WIN32
#else
        if (worker->IsProcessAlive()) {
          RAY_LOG(INFO) << "Worker with PID=" << worker->proc_->GetId()
                        << " did not exit after " << timeout
                        << "ms, force killing with SIGKILL.";
        } else {
          return;
        }
#endif
        // Force kill worker
        worker->SignalProcess(/*force=*/true);
      });
}

Worker::~Worker() {
#if defined(__linux__)
  if (pidfd_ >= 0) {
    close(pidfd_);
  }
#endif
}

bool Worker::IsProcessAlive() const {
#if defined(__linux__)
  if (process_exited_) {
    return false;
  }
  if (pidfd_ >= 0) {
    // A pidfd becomes readable once its process has exited.
    pollfd pfd = {pidfd_, POLLIN, 0};
    const int ready = poll(&pfd, 1, 0);
    if (ready >= 0) {
      return ready == 0;
    }
  }
#endif
  return proc_->IsAlive();
}

void Worker::SignalProcess(bool force) {
#if defined(__linux__)
  if (process_exited_) {
    return;
  }
  if (pidfd_ >= 0) {
    const int sig = force ? SIGKILL : SIGTERM;
    if (syscall(SYS_pidfd_send_signal, pidfd_, sig, nullptr, 0) != 0 && errno != ESRCH) {
      RAY_LOG(WARNING) << "Failed to send signal " << sig
                       << " to worker with PID=" << proc_->GetId() << ": "
                       << strerror(errno);
    }
    return;
  }
#endif
  if (force) {
    proc_->Kill();
    return;
  }
#ifndef _WIN32
  kill(proc_->GetId(), SIGTERM);
#endif
}

#if defined(__linux__)
bool Worker::IsConnectionPeerAlive() const {
  if (connection_ == nullptr) {
    return true;
  }
  // The worker holds its end of the socket until it exits, so a hang-up means it is gone.
  pollfd pfd = {connection_->GetNativeHandle(), POLLRDHUP, 0};
  return poll(&pfd, 1, 0) == 0;
}
#endif

void Worker::MarkBlocked() { blocked_ = true; }

void Worker::MarkUnblocked() { blocked_ = false; }

bool Worker::IsBlocked() const { return blocked_; }

WorkerID Worker::WorkerId() const { return worker_id_; }

const ProcessInterface &Worker::GetProcess() const { return *proc_; }

void Worker::SetProcess(std::unique_ptr<ProcessInterface> proc) {
  RAY_CHECK(proc != nullptr) << absl::StrFormat(
      "Failed to set process for worker: %s because the process is null. "
      "Was the process spawned successfully?",
      worker_id_.Hex());
  RAY_CHECK(proc_ == nullptr || proc_->IsNull()) << absl::StrFormat(
      "Failed to set process: %d on worker: %s because it already has a process: %d",
      proc->GetId(),
      worker_id_.Hex(),
      proc_->GetId());
  proc_ = std::move(proc);
#if defined(__linux__)
  // Pin the process identity while the PID is known to be this worker. After the worker
  // exits, its PID can be recycled (even as a thread ID of another process), and a
  // PID-based kill would then terminate that unrelated process.
  if (proc_->IsValid()) {
    pidfd_ = static_cast<int>(syscall(SYS_pidfd_open, proc_->GetId(), 0));
    if (pidfd_ < 0) {
      // Only ENOSYS (no pidfd support) may fall back to PID-based signals. Any other
      // failure (PID gone, recycled as a thread ID, fd exhaustion) leaves the worker
      // unsignaled.
      const int err = errno;
      process_exited_ = err != ENOSYS;
      RAY_LOG(DEBUG) << "pidfd_open(" << proc_->GetId() << ") failed: " << strerror(err);
    } else if (!IsConnectionPeerAlive()) {
      // The worker exited before pidfd_open, so the pidfd may refer to a process that
      // reused its PID.
      close(pidfd_);
      pidfd_ = -1;
      process_exited_ = true;
    }
  }
#endif
}

rpc::Language Worker::GetLanguage() const { return language_; }

std::string Worker::IpAddress() const { return ip_address_; }

int Worker::Port() const {
  // NOTE(kfstorm): Since `RayletClient::AnnounceWorkerPort` is an asynchronous
  // operation, the worker may crash before the `AnnounceWorkerPort` request is received
  // by raylet. In this case, Accessing `Worker::Port` in
  // `NodeManager::ProcessDisconnectClientMessage` will fail the check. So disable the
  // check here.
  // RAY_CHECK(port_ > 0);
  return port_;
}

int Worker::AssignedPort() const { return assigned_port_; }

void Worker::SetAssignedPort(int port) { assigned_port_ = port; };

void Worker::AsyncNotifyGCSRestart() {
  if (rpc_client_) {
    rpc::RayletNotifyGCSRestartRequest request;
    rpc_client_->RayletNotifyGCSRestart(request, [](Status status, auto reply) {
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Failed to notify worker about GCS restarting: "
                       << status.ToString();
      }
    });
  } else {
    notify_gcs_restarted_ = true;
  }
}

void Worker::Connect(int port) {
  RAY_CHECK(port > 0);
  port_ = port;
  rpc::Address addr;
  addr.set_ip_address(ip_address_);
  addr.set_port(port_);
  rpc_client_ = std::make_unique<rpc::CoreWorkerClient>(addr, client_call_manager_, []() {
    RAY_LOG(FATAL) << "Raylet doesn't call any retryable core worker grpc methods.";
  });
  Connect(rpc_client_);
}

void Worker::Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) {
  rpc_client_ = rpc_client;
  if (notify_gcs_restarted_) {
    // We need to send RPC to notify about the GCS restarts
    AsyncNotifyGCSRestart();
    notify_gcs_restarted_ = false;
  }
}

std::optional<pid_t> Worker::GetSavedProcessGroupId() const { return saved_pgid_; }

void Worker::SetSavedProcessGroupId(pid_t pgid) { saved_pgid_ = pgid; }

void Worker::GrantLeaseId(const LeaseID &lease_id) {
  lease_id_ = lease_id;
  if (!lease_id.IsNil()) {
    RAY_CHECK(worker_type_ != rpc::WorkerType::DRIVER);
    last_lease_grant_time_ = clock_.Now();
  }
};

const LeaseID &Worker::GetGrantedLeaseId() const { return lease_id_; }

const JobID &Worker::GetAssignedJobId() const { return assigned_job_id_; }

std::optional<bool> Worker::GetIsGpu() const { return is_gpu_; }

std::optional<bool> Worker::GetIsActorWorker() const { return is_actor_worker_; }

int Worker::GetRuntimeEnvHash() const { return runtime_env_hash_; }

void Worker::AssignActorId(const ActorID &actor_id) {
  RAY_CHECK(actor_id_.IsNil())
      << "A worker that is already an actor cannot be assigned an actor ID again.";
  RAY_CHECK(!actor_id.IsNil());
  actor_id_ = actor_id;
}

const ActorID &Worker::GetActorId() const { return actor_id_; }

const std::string Worker::GetLeaseIdAsDebugString() const {
  std::stringstream id_ss;
  if (GetActorId().IsNil()) {
    id_ss << "actor ID: " << GetActorId();
  }
  id_ss << "lease ID: " << GetGrantedLeaseId();
  return id_ss.str();
}

bool Worker::IsDetachedActor() const {
  RAY_CHECK(granted_lease_.has_value());
  return granted_lease_->GetLeaseSpecification().IsDetachedActor();
}

const std::shared_ptr<ClientConnection> Worker::Connection() const { return connection_; }

void Worker::SetOwnerAddress(const rpc::Address &address) { owner_address_ = address; }
const rpc::Address &Worker::GetOwnerAddress() const { return owner_address_; }

void Worker::ActorCallArgWaitComplete(const TaskID &task_id, int32_t attempt_number) {
  RAY_CHECK(port_ > 0);
  rpc::ActorCallArgWaitCompleteRequest request;
  request.set_task_id(task_id.Binary());
  request.set_attempt_number(attempt_number);
  request.set_intended_worker_id(worker_id_.Binary());
  rpc_client_->ActorCallArgWaitComplete(
      request, [](Status status, const rpc::ActorCallArgWaitCompleteReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send wait complete: " << status.ToString();
        }
      });
}

const BundleID &Worker::GetBundleId() const { return bundle_id_; }

void Worker::SetBundleId(const BundleID &bundle_id) { bundle_id_ = bundle_id; }

void Worker::SetJobId(const JobID &job_id) {
  if (assigned_job_id_.IsNil()) {
    assigned_job_id_ = job_id;
  }

  RAY_CHECK(assigned_job_id_ == job_id)
      << "Job_id mismatch, assigned: " << assigned_job_id_.Hex()
      << ", actual: " << job_id.Hex();
}

void Worker::SetIsGpu(bool is_gpu) {
  if (!is_gpu_.has_value()) {
    is_gpu_ = is_gpu;
  }
  RAY_CHECK_EQ(is_gpu_.value(), is_gpu)
      << "is_gpu mismatch, assigned: " << is_gpu_.value() << ", actual: " << is_gpu;
}

void Worker::SetIsActorWorker(bool is_actor_worker) {
  if (!is_actor_worker_.has_value()) {
    is_actor_worker_ = is_actor_worker;
  }
  RAY_CHECK_EQ(is_actor_worker_.value(), is_actor_worker)
      << "is_actor_worker mismatch, assigned: " << is_actor_worker_.value()
      << ", actual: " << is_actor_worker;
}

}  // namespace raylet

}  // end namespace ray
