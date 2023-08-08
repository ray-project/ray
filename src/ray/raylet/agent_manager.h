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
#include "ray/rpc/agent_manager/agent_manager_client.h"
#include "ray/rpc/agent_manager/agent_manager_server.h"
#include "ray/rpc/runtime_env/runtime_env_client.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

typedef std::function<std::shared_ptr<rpc::RuntimeEnvAgentClientInterface>(
    const std::string &ip_address, int port)>
    RuntimeEnvAgentClientFactoryFn;

/// Callback that's called after runtime env is created.
/// \param[in] successful Whether or not the creation was successful.
/// \param[in] serialized_runtime_env_context Serialized context.
/// \param[in] setup_error_message The error message if runtime env creation fails.
/// It must be only set when successful == false.
typedef std::function<void(bool successful,
                           const std::string &serialized_runtime_env_context,
                           const std::string &setup_error_message)>
    GetOrCreateRuntimeEnvCallback;
typedef std::function<void(bool successful)> DeleteRuntimeEnvIfPossibleCallback;

class AgentManager : public rpc::AgentManagerServiceHandler {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options,
                        DelayExecutorFn delay_executor,
                        RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory,
                        bool start_agent = true /* for test */)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        runtime_env_agent_client_factory_(std::move(runtime_env_agent_client_factory)) {
    if (start_agent) {
      StartAgent();
    }
  }

  void HandleRegisterAgent(rpc::RegisterAgentRequest request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  /// Request agent to increase the runtime env reference. This API is not idempotent.
  /// \param[in] job_id The job id which the runtime env belongs to.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] serialized_allocated_resource_instances The serialized allocated resource
  /// instances.
  /// \param[in] callback The callback function.
  virtual void GetOrCreateRuntimeEnv(
      const JobID &job_id,
      const std::string &serialized_runtime_env,
      const rpc::RuntimeEnvConfig &runtime_env_config,
      const std::string &serialized_allocated_resource_instances,
      GetOrCreateRuntimeEnvCallback callback);

  /// Request agent to decrease the runtime env reference. This API is not idempotent.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] callback The callback function.
  virtual void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                          DeleteRuntimeEnvIfPossibleCallback callback);

 private:
  void StartAgent();

 private:
  Options options_;
  pid_t reported_agent_id_ = 0;
  int reported_agent_port_ = 0;
  /// Whether or not we intend to start the agent.  This is false if we
  /// are missing Ray Dashboard dependencies, for example.
  bool should_start_agent_ = true;
  std::string reported_agent_ip_address_;
  DelayExecutorFn delay_executor_;
  RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory_;
  std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client_;
  /// When the grpc port of agent is invalid, set this flag to indicate that agent client
  /// is disable.
  bool disable_agent_client_ = false;
};

class DefaultAgentManagerServiceHandler : public rpc::AgentManagerServiceHandler {
 public:
  explicit DefaultAgentManagerServiceHandler(std::shared_ptr<AgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterAgent(rpc::RegisterAgentRequest request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleRegisterAgent(request, reply, send_reply_callback);
  }

 private:
  std::shared_ptr<AgentManager> &delegate_;
};

}  // namespace raylet
}  // namespace ray
