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
#include <vector>

#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"

namespace ray {
namespace ipc {

class FakeRayletIpcClient : public RayletIpcClientInterface {
 public:
  Status RegisterClient(const WorkerID &worker_id,
                        rpc::WorkerType worker_type,
                        const JobID &job_id,
                        int runtime_env_hash,
                        const rpc::Language &language,
                        const std::string &ip_address,
                        const std::string &serialized_job_config,
                        const StartupToken &startup_token,
                        NodeID *node_id,
                        int *assigned_port) override {
    return Status::OK();
  }

  Status Disconnect(const rpc::WorkerExitType &exit_type,
                    const std::string &exit_detail,
                    const std::shared_ptr<LocalMemoryBuffer>
                        &creation_task_exception_pb_bytes) override {
    return Status::OK();
  }

  Status AnnounceWorkerPortForWorker(int port) override { return Status::OK(); }

  Status AnnounceWorkerPortForDriver(int port, const std::string &entrypoint) override {
    return Status::OK();
  }

  Status ActorCreationTaskDone() override { return Status::OK(); }

  StatusOr<ScopedResponse> AsyncGetObjects(
      const std::vector<ObjectID> &object_ids,
      const std::vector<rpc::Address> &owner_addresses,
      int64_t get_request_id) override {
    return ScopedResponse();
  }

  StatusOr<absl::flat_hash_set<ObjectID>> Wait(
      const std::vector<ObjectID> &object_ids,
      const std::vector<rpc::Address> &owner_addresses,
      int num_returns,
      int64_t timeout_milliseconds) override {
    return absl::flat_hash_set<ObjectID>();
  }

  Status CancelGetRequest(int64_t request_id) override { return Status::OK(); }

  Status NotifyWorkerBlocked() override { return Status::OK(); }

  Status NotifyWorkerUnblocked() override { return Status::OK(); }

  Status WaitForActorCallArgs(const std::vector<rpc::ObjectReference> &references,
                              int64_t tag) override {
    return Status::OK();
  }

  Status PushError(const JobID &job_id,
                   const std::string &type,
                   const std::string &error_message,
                   double timestamp) override {
    return Status::OK();
  }

  Status FreeObjects(const std::vector<ObjectID> &object_ids, bool local_only) override {
    return Status::OK();
  }

  void SubscribePlasmaReady(const ObjectID &object_id,
                            const rpc::Address &owner_address) override {}
};

}  // namespace ipc
}  // namespace ray
