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

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/raylet.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

namespace raylet {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(const JobID &job_id,
               const int runtime_env_hash,
               const WorkerID &worker_id,
               const Language &language,
               rpc::WorkerType worker_type,
               const std::string &ip_address,
               std::shared_ptr<ClientConnection> connection,
               rpc::ClientCallManager &client_call_manager,
               StartupToken startup_token)
    : worker_id_(worker_id),
      startup_token_(startup_token),
      language_(language),
      worker_type_(worker_type),
      ip_address_(ip_address),
      assigned_port_(-1),
      port_(-1),
      connection_(connection),
      assigned_job_id_(job_id),
      runtime_env_hash_(runtime_env_hash),
      bundle_id_(std::make_pair(PlacementGroupID::Nil(), -1)),
      dead_(false),
      blocked_(false),
      client_call_manager_(client_call_manager),
      is_detached_actor_(false) {}

rpc::WorkerType Worker::GetWorkerType() const { return worker_type_; }

void Worker::MarkDead() { dead_ = true; }

bool Worker::IsDead() const { return dead_; }

void Worker::MarkBlocked() { blocked_ = true; }

void Worker::MarkUnblocked() { blocked_ = false; }

bool Worker::IsBlocked() const { return blocked_; }

WorkerID Worker::WorkerId() const { return worker_id_; }

Process Worker::GetProcess() const { return proc_; }

StartupToken Worker::GetStartupToken() const { return startup_token_; }

void Worker::SetProcess(Process proc) {
  RAY_CHECK(proc_.IsNull());  // this procedure should not be called multiple times
  proc_ = std::move(proc);
}

void Worker::SetStartupToken(StartupToken startup_token) {
  startup_token_ = startup_token;
}

Language Worker::GetLanguage() const { return language_; }

const std::string Worker::IpAddress() const { return ip_address_; }

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
  rpc_client_ = std::make_unique<rpc::CoreWorkerClient>(addr, client_call_manager_);
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

void Worker::AssignTaskId(const TaskID &task_id) {
  if (!task_id.IsNil() && !task_id.ActorId().IsNil()) {
    SetJobId(task_id.JobId());
  }
  assigned_task_id_ = task_id;
}

const TaskID &Worker::GetAssignedTaskId() const { return assigned_task_id_; }

bool Worker::AddBlockedTaskId(const TaskID &task_id) {
  auto inserted = blocked_task_ids_.insert(task_id);
  return inserted.second;
}

bool Worker::RemoveBlockedTaskId(const TaskID &task_id) {
  auto erased = blocked_task_ids_.erase(task_id);
  return erased == 1;
}

const std::unordered_set<TaskID> &Worker::GetBlockedTaskIds() const {
  return blocked_task_ids_;
}

const JobID &Worker::GetAssignedJobId() const { return assigned_job_id_; }

int Worker::GetRuntimeEnvHash() const { return runtime_env_hash_; }

void Worker::AssignActorId(const ActorID &actor_id) {
  RAY_CHECK(actor_id_.IsNil())
      << "A worker that is already an actor cannot be assigned an actor ID again.";
  RAY_CHECK(!actor_id.IsNil());
  actor_id_ = actor_id;
  if (!actor_id.IsNil()) {
    SetJobId(actor_id.JobId());
  }
}

const ActorID &Worker::GetActorId() const { return actor_id_; }

const std::string Worker::GetTaskOrActorIdAsDebugString() const {
  std::stringstream id_ss;
  if (GetActorId().IsNil()) {
    id_ss << "task ID: " << GetAssignedTaskId();
  } else {
    id_ss << "actor ID: " << GetActorId();
  }
  return id_ss.str();
}

void Worker::MarkDetachedActor() { is_detached_actor_ = true; }

bool Worker::IsDetachedActor() const { return is_detached_actor_; }

const std::shared_ptr<ClientConnection> Worker::Connection() const { return connection_; }

void Worker::SetOwnerAddress(const rpc::Address &address) { owner_address_ = address; }
const rpc::Address &Worker::GetOwnerAddress() const { return owner_address_; }

void Worker::DirectActorCallArgWaitComplete(int64_t tag) {
  RAY_CHECK(port_ > 0);
  rpc::DirectActorCallArgWaitCompleteRequest request;
  request.set_tag(tag);
  request.set_intended_worker_id(worker_id_.Binary());
  rpc_client_->DirectActorCallArgWaitComplete(
      request, [](Status status, const rpc::DirectActorCallArgWaitCompleteReply &reply) {
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

  RAY_CHECK(assigned_job_id_ == job_id);
}

}  // namespace raylet

}  // end namespace ray
