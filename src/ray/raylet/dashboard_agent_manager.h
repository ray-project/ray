// Copyright 2023 The Ray Authors.
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
#include "ray/rpc/agent_manager/agent_manager_client.h"
#include "ray/rpc/agent_manager/agent_manager_server.h"
#include "ray/rpc/runtime_env/runtime_env_client.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

class DashboardAgentManager : public AgentManager {
 public:
  explicit DashboardAgentManager(Options options,
                                 DelayExecutorFn delay_executor,
                                 bool restart_when_agent_die = true,
                                 bool start_agent = true /* for test */)
      : AgentManager(options, delay_executor, restart_when_agent_die, start_agent) {}

  void HandleRegisterAgent(rpc::RegisterAgentRequest request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback);
};

class DefaultAgentManagerServiceHandler : public rpc::AgentManagerServiceHandler {
 public:
  explicit DefaultAgentManagerServiceHandler(
      std::shared_ptr<DashboardAgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterAgent(rpc::RegisterAgentRequest request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleRegisterAgent(request, reply, send_reply_callback);
  }

 private:
  std::shared_ptr<DashboardAgentManager> &delegate_;
};

}  // namespace raylet
}  // namespace ray
