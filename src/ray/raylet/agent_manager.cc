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

#include "ray/raylet/agent_manager.h"

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/thread_utils.h"
#include "ray/util/util.h"

namespace ray {
namespace raylet {

void AgentManager::StartAgent() {
  std::vector<const char *> argv;
  argv.reserve(options_.agent_commands.size());
  for (const std::string &arg : options_.agent_commands) {
    argv.push_back(arg.c_str());
  }

  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting agent process with command:";
    for (const auto &arg : argv) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Do this after the debug print for argv.data()
  argv.push_back(nullptr);

  // Set node id to agent.
  ProcessEnvironment env;
  env.insert({"RAY_NODE_ID", options_.node_id.Hex()});
  env.insert({"RAY_RAYLET_PID", std::to_string(getpid())});
  env.insert({"RAY_enable_pipe_based_agent_to_parent_health_check",
              RayConfig::instance().enable_pipe_based_agent_to_parent_health_check()
                  ? "1"
                  : "0"});

  // Launch the process to create the agent.
  std::error_code ec;
  // NOTE: we pipe to stdin so that agent can read stdin to detect when
  // the parent dies. See
  // https://stackoverflow.com/questions/12193581/detect-death-of-parent-process
  process_ =
      Process(argv.data(),
              nullptr,
              ec,
              false,
              env,
              /*pipe_to_stdin*/
              RayConfig::instance().enable_pipe_based_agent_to_parent_health_check());
  if (!process_.IsValid() || ec) {
    // The worker failed to start. This is a fatal error.
    RAY_LOG(FATAL) << "Failed to start agent " << options_.agent_name
                   << " with return value " << ec << ": " << ec.message();
  }

  monitor_thread_ = std::make_unique<std::thread>([this]() mutable {
    SetThreadName("agent.monitor." + options_.agent_name);
    RAY_LOG(INFO) << "Monitor agent process with name " << options_.agent_name;
    int exit_code = process_.Wait();
    RAY_LOG(INFO) << "Agent process with name " << options_.agent_name
                  << " exited, exit code " << exit_code << ".";

    if (fate_shares_.load()) {
      // Agent died out-of-band (not during planned raylet shutdown).
      // The raylet assumes it controls the lifecycle of the agent, so any
      // unplanned exit is treated as an error condition, regardless of exit code.
      RAY_LOG(ERROR)
          << "The raylet exited immediately because agent '" << options_.agent_name
          << "' failed unexpectedly with exit code " << exit_code
          << ". This can happen because:\n"
          << "- The version of `grpcio` doesn't follow Ray's requirement. "
             "Agent can segfault with the incorrect `grpcio` version. "
             "Check the grpcio version `pip freeze | grep grpcio`.\n"
          << "- The agent failed to start because of unexpected error or port "
             "conflict. Read the log `cat "
             "/tmp/ray/session_latest/logs/{dashboard_agent|runtime_env_agent}.log`. "
             "You can find the log file structure here "
             "https://docs.ray.io/en/master/ray-observability/user-guides/"
             "configure-logging.html#logging-directory-structure.\n"
          << "- The agent is killed by the OS (e.g., out of memory).\n"
          << "- The agent is killed by an external process.";

      // Notify GCS of unexpected termination (this triggers the GCS notification
      // part of graceful shutdown but we'll exit immediately after)
      rpc::NodeDeathInfo node_death_info;
      node_death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
      node_death_info.set_reason_message("Agent '" + options_.agent_name +
                                         "' failed and raylet fate-shares with it.");
      shutdown_raylet_gracefully_(node_death_info);

      // Immediately exit ungracefully - the GCS notification is asynchronous
      // and will be sent before the process actually terminates
      QuickExit();
    }
  });
}

AgentManager::~AgentManager() {
  if (monitor_thread_) {
    RAY_LOG(INFO) << "Killing agent " << options_.agent_name << ", pid "
                  << process_.GetId() << ".";
    // Stop fate sharing because we gracefully kill the agent.
    fate_shares_ = false;
    process_.Kill();
    monitor_thread_->join();
  }
}

}  // namespace raylet
}  // namespace ray
