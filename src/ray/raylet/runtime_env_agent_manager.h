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

#include <csignal>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/raylet/agent_manager.h"
#include "ray/rpc/agent_manager/runtime_env_agent_manager_client.h"
#include "ray/rpc/agent_manager/runtime_env_agent_manager_server.h"
#include "ray/rpc/runtime_env/runtime_env_client.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

using RuntimeEnvAgentClientFactoryFn =
    std::function<std::shared_ptr<rpc::RuntimeEnvAgentClientInterface>(
        const std::string &, int)>;

/// Callback that's called after runtime env is created.
/// \param[in] successful Whether or not the creation was successful.
/// \param[in] serialized_runtime_env_context Serialized context.
/// \param[in] setup_error_message The error message if runtime env creation fails.
/// It must be only set when successful == false.
using GetOrCreateRuntimeEnvCallback =
    std::function<void(bool, const std::string &, const std::string &)>;
using DeleteRuntimeEnvIfPossibleCallback = std::function<void(bool)>;

class RuntimeEnvAgentManager : public AgentManager {
 public:
  explicit RuntimeEnvAgentManager(
      Options options,
      DelayExecutorFn delay_executor,
      RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory,
      bool restart_when_agent_die = false,
      bool start_agent = true /* for test */)
      : AgentManager(options, delay_executor, restart_when_agent_die, start_agent),
        runtime_env_agent_client_factory_(std::move(runtime_env_agent_client_factory)) {}

  void HandleRegisterRuntimeEnvAgent(rpc::RegisterRuntimeEnvAgentRequest request,
                                     rpc::RegisterRuntimeEnvAgentReply *reply,
                                     rpc::SendReplyCallback send_reply_callback);

  /// Request agent to increase the runtime env reference. This API is not idempotent.
  /// \param[in] job_id The job id which the runtime env belongs to.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] serialized_allocated_resource_instances The serialized allocated resource
  /// instances.
  /// \param[in] callback The callback function.
  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             const std::string &serialized_allocated_resource_instances,
                             GetOrCreateRuntimeEnvCallback callback);

  /// Request agent to decrease the runtime env reference. This API is not idempotent.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] callback The callback function.
  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback);

 private:
  void StartAgent();

 private:
  RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory_;
  std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client_;
  /// When the grpc port of agent is invalid, set this flag to indicate that agent client
  /// is disable.
  bool disable_agent_client_ = false;
};

class DefaultRuntimeEnvAgentManagerServiceHandler
    : public rpc::RuntimeEnvAgentManagerServiceHandler {
 public:
  explicit DefaultRuntimeEnvAgentManagerServiceHandler(
      std::shared_ptr<RuntimeEnvAgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterRuntimeEnvAgent(
      rpc::RegisterRuntimeEnvAgentRequest request,
      rpc::RegisterRuntimeEnvAgentReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleRegisterRuntimeEnvAgent(request, reply, send_reply_callback);
  }

 private:
  std::shared_ptr<RuntimeEnvAgentManager> &delegate_;
};

}  // namespace raylet
}  // namespace ray
