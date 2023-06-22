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

#include <boost/asio/deadline_timer.hpp>
#include <csignal>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

// Manages a separate "Agent" process. In constructor (or the `StartAgnet` method) it
// starts a process with `agent_commands` plus some additional arguments. The started
// Agent is typically `ray/dashboard/agent.py`.
//
// The started process fate-shares with the raylet. i.e. when the process fails to start
// or exits, we SIGTERM the raylet.
class AgentManager {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options,
                        DelayExecutorFn delay_executor,
                        bool start_agent = true /* for test */)
      : options_(std::move(options)), delay_executor_(std::move(delay_executor)) {
    if (start_agent) {
      StartAgent();
    }
  }

 private:
  void StartAgent();

 private:
  Options options_;
  /// Whether or not we intend to start the agent.  This is false if we
  /// are missing Ray Dashboard dependencies, for example.
  bool should_start_agent_ = true;
  DelayExecutorFn delay_executor_;
};

}  // namespace raylet
}  // namespace ray
