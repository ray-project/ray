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

#include <atomic>
#include <boost/asio/deadline_timer.hpp>
#include <csignal>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace raylet {

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

// Manages a separate "Agent" process. In constructor (or the `StartAgent` method) it
// starts a process with `agent_commands` plus some additional arguments.
//
// Raylet outlives agents. This means when Raylet exits, the agents must exit; on the
// other hand, if the agent exits, the raylet may exit only if fate_shares = true. This is
// implement by the dtor of AgentManager to kill the agent process.
//
// We typically start these agents:
// - The DashboardAgent: `ray/dashboard/agent.py`
// - The RuntimeEnvAgent: `ray/_private/runtime_env/agent/main.py`
class AgentManager {
 public:
  struct Options {
    const NodeID node_id;
    const std::string agent_name;
    // Commands to start the agent. Note we append extra arguments:
    // --agent-id $AGENT_ID # A random string of int
    std::vector<std::string> agent_commands;
    // If true: the started process fate-shares with the raylet. i.e., when the process
    // fails to start or exits, we SIGTERM the raylet.
    bool fate_shares;
  };

  explicit AgentManager(
      Options options,
      DelayExecutorFn delay_executor,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
      bool start_agent = true /* for test */)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
        fate_shares_(options_.fate_shares) {
    if (options_.agent_name.empty()) {
      RAY_LOG(FATAL) << "AgentManager agent_name must not be empty.";
    }
    if (options_.agent_commands.empty()) {
      RAY_LOG(FATAL) << "AgentManager agent_commands must not be empty.";
    }
    if (start_agent) {
      StartAgent();
    }
  }
  ~AgentManager();

 private:
  void StartAgent();

 private:
  const Options options_;
  Process process_;
  DelayExecutorFn delay_executor_;
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;
  // If true, when the agent dies, raylet kills itself.
  std::atomic<bool> fate_shares_;
  std::unique_ptr<std::thread> monitor_thread_;
};

}  // namespace raylet
}  // namespace ray
