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

#include "ray/raylet/runtime_env_manager_client.h"

#include "ray/rpc/runtime_env/runtime_env_client.h"

namespace ray {
namespace raylet {

namespace {
class ThinWrapperRuntimeEnvManagerClient : public RuntimeEnvManagerClient {
 public:
  ThinWrapperRuntimeEnvManagerClient(
      std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client)
      : runtime_env_agent_client_(runtime_env_agent_client) {}
  ~ThinWrapperRuntimeEnvManagerClient() {}

  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             const std::string &serialized_allocated_resource_instances,
                             GetOrCreateRuntimeEnvCallback callback) override {
    rpc::GetOrCreateRuntimeEnvRequest request;
    request.set_job_id(job_id.Hex());
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
    request.set_serialized_allocated_resource_instances(
        serialized_allocated_resource_instances);
    runtime_env_agent_client_->GetOrCreateRuntimeEnv(
        request,
        [serialized_runtime_env,
         runtime_env_config,
         serialized_allocated_resource_instances,
         job_id,
         callback = std::move(callback)](const Status &status,
                                         const rpc::GetOrCreateRuntimeEnvReply &reply) {
          if (status.ok()) {
            if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
              callback(true,
                       reply.serialized_runtime_env_context(),
                       /*setup_error_message*/ "");
            } else {
              RAY_LOG(INFO) << "Failed to create runtime env for job " << job_id
                            << ", error message: " << reply.error_message();
              RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                             << serialized_runtime_env;
              callback(false,
                       reply.serialized_runtime_env_context(),
                       /*setup_error_message*/ reply.error_message());
            }

          } else {
            RAY_LOG(INFO)
                << "Failed to create runtime env for job " << job_id
                << ", status = " << status
                << ", maybe there are some network problems, will fail the request.";
            RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                           << serialized_runtime_env;
            callback(false, "", "Failed to request agent.");
          }
        });
  }

  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback) override {
    rpc::DeleteRuntimeEnvIfPossibleRequest request;
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.set_source_process("raylet");
    runtime_env_agent_client_->DeleteRuntimeEnvIfPossible(
        request,
        [serialized_runtime_env, callback = std::move(callback)](
            Status status, const rpc::DeleteRuntimeEnvIfPossibleReply &reply) {
          if (status.ok()) {
            if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
              callback(true);
            } else {
              // TODO(sang): Find a better way to delivering error messages in this case.
              RAY_LOG(ERROR) << "Failed to delete runtime env"
                             << ", error message: " << reply.error_message();
              RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
              callback(false);
            }

          } else {
            RAY_LOG(ERROR)
                << "Failed to delete runtime env reference"
                << ", status = " << status
                << ", maybe there are some network problems, will fail the request.";
            RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
            callback(false);
          }
        });
  }

 private:
  std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client_;
};
}  // namespace

std::shared_ptr<RuntimeEnvManagerClient> RuntimeEnvManagerClient::Create(
    std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client) {
  return std::make_shared<ThinWrapperRuntimeEnvManagerClient>(runtime_env_agent_client);
}

}  // namespace raylet
}  // namespace ray