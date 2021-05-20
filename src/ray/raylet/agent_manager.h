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

typedef std::function<std::shared_ptr<rpc::RuntimeEnvAgentClient>(
    const std::string &ip_address, int port)>
    RuntimeEnvAgentClientFactoryFn;

typedef std::function<void(bool done)> CreateRuntimeEnvCallback;
typedef std::function<void(bool done)> DeleteRuntimeEnvCallback;

class AgentManager : public rpc::AgentManagerServiceHandler {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options, DelayExecutorFn delay_executor,
                        RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        runtime_env_agent_client_factory_(std::move(runtime_env_agent_client_factory)) {
    StartAgent();
  }

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  /// Request agent to create runtime env.
  /// \param[in] runtime_env The runtime env.
  void CreateRuntimeEnvOrReuse(const std::string &serialized_runtime_env,
                               CreateRuntimeEnvCallback callback);

  /// Request agent to delete runtime env.
  /// \param[in] runtime_env The runtime env.
  void DeleteRuntimeEnv(const std::string &serialized_runtime_env,
                        DeleteRuntimeEnvCallback callback);

 private:
  void StartAgent();

 private:
  Options options_;
  pid_t agent_pid_ = 0;
  int agent_port_ = 0;
  std::string agent_ip_address_;
  DelayExecutorFn delay_executor_;
  RuntimeEnvAgentClientFactoryFn runtime_env_agent_client_factory_;
  std::shared_ptr<rpc::RuntimeEnvAgentClient> runtime_env_agent_client_;
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
