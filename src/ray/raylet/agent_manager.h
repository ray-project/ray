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

using DelayExecutorFn = std::function<std::shared_ptr<boost::asio::deadline_timer>(
    std::function<void()>, uint32_t)>;

class AgentManager {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options,
                        DelayExecutorFn delay_executor,
                        bool restart_when_agent_die = false,
                        bool start_agent = true /* for test */)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        restart_when_agent_die_(restart_when_agent_die) {
    if (start_agent) {
      StartAgent();
    }
  }

 protected:
  void StartAgent();

  Options options_;
  DelayExecutorFn delay_executor_;
  bool restart_when_agent_die_ = false;
  pid_t reported_agent_id_ = 0;
  int reported_agent_port_ = 0;
  /// Whether or not we intend to start the agent.  This is false if we
  /// are missing Ray Dashboard dependencies, for example.
  bool should_start_agent_ = true;
  std::string reported_agent_ip_address_;
};

}  // namespace raylet
}  // namespace ray
