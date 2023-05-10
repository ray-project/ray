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

#include <thread>

#include "ray/common/ray_config.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {
namespace raylet {

void AgentManager::StartAgent() {
  if (options_.agent_commands.empty()) {
    should_start_agent_ = false;
    RAY_LOG(INFO) << "Not starting agent, the agent command is empty.";
    return;
  }

  // Create a non-zero random agent_id to pass to the child process
  // We cannot use pid an id because os.getpid() from the python process is not
  // reliable when using a launcher.
  // See https://github.com/ray-project/ray/issues/24361 and Python issue
  // https://github.com/python/cpython/issues/83086
  int agent_id = 0;
  while (agent_id == 0) {
    agent_id = rand();
  }
  const std::string agent_id_str = std::to_string(agent_id);
  std::vector<const char *> argv;
  for (const std::string &arg : options_.agent_commands) {
    argv.push_back(arg.c_str());
  }
  argv.push_back("--agent-id");
  argv.push_back(agent_id_str.c_str());

  // Disable metrics report if needed.
  if (!RayConfig::instance().enable_metrics_collection()) {
    argv.push_back("--disable-metrics-collection");
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
  argv.push_back(NULL);

  // Set node id to agent.
  ProcessEnvironment env;
  env.insert({"RAY_NODE_ID", options_.node_id.Hex()});
  env.insert({"RAY_RAYLET_PID", std::to_string(getpid())});

  // Launch the process to create the agent.
  std::error_code ec;
  Process child(argv.data(), nullptr, ec, false, env);
  if (!child.IsValid() || ec) {
    // The worker failed to start. This is a fatal error.
    RAY_LOG(FATAL) << "Failed to start agent with return value " << ec << ": "
                   << ec.message();
  }

  std::thread monitor_thread([this, child, agent_id]() mutable {
    SetThreadName("agent.monitor");
    RAY_LOG(INFO) << "Monitor agent process with id " << agent_id << ", register timeout "
                  << RayConfig::instance().agent_register_timeout_ms() << "ms.";
    auto timer = delay_executor_(
        [this, child, agent_id]() mutable {
          if (reported_agent_id_ != agent_id) {
            if (reported_agent_id_ == 0) {
              RAY_LOG(WARNING) << "Agent process expected id " << agent_id
                               << " timed out before registering. ip "
                               << reported_agent_ip_address_ << ", id "
                               << reported_agent_id_;
            } else {
              RAY_LOG(WARNING) << "Agent process expected id " << agent_id
                               << " but got id " << reported_agent_id_
                               << ", this is a fatal error";
            }
            child.Kill();
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();
    RAY_LOG(INFO) << "Agent process with id " << agent_id << " exited, exit code "
                  << exit_code << ". ip " << reported_agent_ip_address_ << ". id "
                  << reported_agent_id_;
    if (restart_when_agent_die_) {
      delay_executor_([this]() { StartAgent(); }, /*ms*/ 10);
      return;
    }
    RAY_LOG(ERROR)
        << "The raylet exited immediately because the Ray agent failed. "
           "The raylet fate shares with the agent. This can happen because the "
           "Ray agent was unexpectedly killed or failed. Agent can fail when\n"
           "- The version of `grpcio` doesn't follow Ray's requirement. "
           "Agent can segfault with the incorrect `grpcio` version. "
           "Check the grpcio version `pip freeze | grep grpcio`.\n"
           "- The agent failed to start because of unexpected error or port conflict. "
           "Read the log `cat /tmp/ray/session_latest/logs/dashboard_agent.log` "
           "or `cat /tmp/ray/session_latest/logs/runtime_env_agent.log`."
           "You can find the log file structure here "
           "https://docs.ray.io/en/master/ray-observability/"
           "ray-logging.html#logging-directory-structure.\n"
           "- The agent is killed by the OS (e.g., out of memory).";
    // Sending a SIGTERM to itself is equivalent to gracefully shutting down raylet.
    RAY_CHECK(std::raise(SIGTERM) == 0) << "There was a failure while sending a "
                                           "sigterm to itself. The process will not "
                                           "gracefully shutdown.";
    // If the process is not terminated within 10 seconds, forcefully kill itself.
    delay_executor_([]() { QuickExit(); }, /*ms*/ 10000);
  });
  monitor_thread.detach();
}

}  // namespace raylet
}  // namespace ray
