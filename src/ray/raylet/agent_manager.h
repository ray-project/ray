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

/// Callback that's callaed after runtime env is created.
/// \param[in] successful Whether or not the creation was successful.
/// \param[in] serialized_runtime_env_context Serialized context.
/// \param[in] setup_error_message The error message if runtime env creation fails.
/// It must be only set when successful == false.
typedef std::function<void(bool successful,
                           const std::string &serialized_runtime_env_context,
                           const std::string &setup_error_message)>
    CreateRuntimeEnvCallback;
typedef std::function<void(bool successful)> DeleteURIsCallback;

class AgentManager : public rpc::AgentManagerServiceHandler {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options, DelayExecutorFn delay_executor,
                        RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory,
                        bool start_agent = true /* for test */)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        runtime_env_agent_client_factory_(std::move(runtime_env_agent_client_factory)) {
    if (start_agent) {
      StartAgent();
    }
  }

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  /// Request agent to create a runtime env.
  /// \param[in] runtime_env The runtime env.
  virtual void CreateRuntimeEnv(
      const JobID &job_id, const std::string &serialized_runtime_env,
      const std::string &serialized_allocated_resource_instances,
      CreateRuntimeEnvCallback callback);

  /// Request agent to delete a list of URIs.
  /// \param[in] URIs The list of URIs to delete.
  virtual void DeleteURIs(const std::vector<std::string> &uris,
                          DeleteURIsCallback callback);

 private:
  void StartAgent();

 private:
  Options options_;
  pid_t agent_pid_ = 0;
  int agent_port_ = 0;
  /// The number of times the agent is restarted.
  std::atomic<uint32_t> agent_restart_count_ = 0;
  /// Whether or not we intend to start the agent.  This is false if we
  /// are missing Ray Dashboard dependencies, for example.
  bool should_start_agent_ = true;
  std::string agent_ip_address_;
  DelayExecutorFn delay_executor_;
  RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory_;
  std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client_;
};

class DefaultAgentManagerServiceHandler : public rpc::AgentManagerServiceHandler {
 public:
  explicit DefaultAgentManagerServiceHandler(std::shared_ptr<AgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
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
