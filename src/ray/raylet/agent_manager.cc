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

void AgentManager::HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                                       rpc::RegisterAgentReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  agent_ip_address_ = request.agent_ip_address();
  agent_port_ = request.agent_port();
  agent_pid_ = request.agent_pid();
  // TODO(SongGuyang): We should remove this after we find better port resolution.
  // Note: `agent_port_` should be 0 if the grpc port of agent is in conflict.
  if (agent_port_ != 0) {
    runtime_env_agent_client_ =
        runtime_env_agent_client_factory_(agent_ip_address_, agent_port_);
    RAY_LOG(INFO) << "HandleRegisterAgent, ip: " << agent_ip_address_
                  << ", port: " << agent_port_ << ", pid: " << agent_pid_;
  } else {
    RAY_LOG(WARNING) << "The GRPC port of the Ray agent is invalid (0), ip: "
                     << agent_ip_address_ << ", pid: " << agent_pid_
                     << ". The agent client in the raylet has been disabled.";
    disable_agent_client_ = true;
  }
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::StartAgent() {
  if (options_.agent_commands.empty()) {
    should_start_agent_ = false;
    RAY_LOG(INFO) << "Not starting agent, the agent command is empty.";
    return;
  }

  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting agent process with command:";
    for (const auto &arg : options_.agent_commands) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Launch the process to create the agent.
  std::error_code ec;
  std::vector<const char *> argv;
  for (const std::string &arg : options_.agent_commands) {
    argv.push_back(arg.c_str());
  }
  // Disable metrics report if needed.
  if (!RayConfig::instance().enable_metrics_collection()) {
    argv.push_back("--disable-metrics-collection");
  }
  argv.push_back(NULL);
  // Set node id to agent.
  ProcessEnvironment env;
  env.insert({"RAY_NODE_ID", options_.node_id.Hex()});
  env.insert({"RAY_RAYLET_PID", std::to_string(getpid())});
  Process child(argv.data(), nullptr, ec, false, env);
  if (!child.IsValid() || ec) {
    // The worker failed to start. This is a fatal error.
    RAY_LOG(FATAL) << "Failed to start agent with return value " << ec << ": "
                   << ec.message();
  }

  std::thread monitor_thread([this, child]() mutable {
    SetThreadName("agent.monitor");
    RAY_LOG(INFO) << "Monitor agent process with pid " << child.GetId()
                  << ", register timeout "
                  << RayConfig::instance().agent_register_timeout_ms() << "ms.";
    auto timer = delay_executor_(
        [this, child]() mutable {
          if (agent_pid_ != child.GetId()) {
            RAY_LOG(WARNING) << "Agent process with pid " << child.GetId()
                             << " has not registered. ip " << agent_ip_address_
                             << ", pid " << agent_pid_;
            child.Kill();
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();
    RAY_LOG(WARNING) << "Agent process with pid " << child.GetId()
                     << " exit, return value " << exit_code << ". ip "
                     << agent_ip_address_ << ". pid " << agent_pid_;

    RAY_LOG(ERROR)
        << "The raylet exited immediately because the Ray agent failed. "
           "The raylet fate shares with the agent. This can happen because the "
           "Ray agent was unexpectedly killed or failed. See "
           "`dashboard_agent.log` for the root cause.";
    QuickExit();
  });
  monitor_thread.detach();
}

void AgentManager::GetOrCreateRuntimeEnv(
    const JobID &job_id,
    const std::string &serialized_runtime_env,
    const rpc::RuntimeEnvConfig &runtime_env_config,
    const std::string &serialized_allocated_resource_instances,
    GetOrCreateRuntimeEnvCallback callback) {
  // If the agent cannot be started, fail the request.
  if (!should_start_agent_) {
    std::stringstream str_stream;
    str_stream << "Not all required Ray dependencies for the runtime_env "
                  "feature were found. To install the required dependencies, "
               << "please run `pip install \"ray[default]\"`.";
    const auto &error_message = str_stream.str();
    RAY_LOG(ERROR) << error_message;
    // Execute the callback after the currently executing callback finishes.  Otherwise
    // the task may be erased from the dispatch queue during the queue iteration in
    // ClusterTaskManager::DispatchScheduledTasksToWorkers(), invalidating the iterator
    // and causing a segfault.
    delay_executor_(
        [callback = std::move(callback), error_message] {
          callback(/*successful=*/false,
                   /*serialized_runtime_env_context=*/"",
                   /*setup_error_message*/ error_message);
        },
        0);
    return;
  }

  // `runtime_env_agent_client_` should be `nullptr` when the agent is starting or the
  // agent has failed.
  if (runtime_env_agent_client_ == nullptr) {
    // If the grpc service of agent is invalid, fail the request.
    if (disable_agent_client_) {
      std::stringstream str_stream;
      str_stream
          << "Failed to create runtime environment " << serialized_runtime_env
          << " because the Ray agent couldn't be started due to the port conflict. See "
             "`dashboard_agent.log` for more details. To solve the problem, start Ray "
             "with a hard-coded agent port. `ray start --dashboard-agent-grpc-port "
             "[port]` and make sure the port is not used by other processes.";
      const auto &error_message = str_stream.str();
      RAY_LOG(ERROR) << error_message;
      delay_executor_(
          [callback = std::move(callback), error_message] {
            callback(/*successful=*/false,
                     /*serialized_runtime_env_context=*/"{}",
                     /*setup_error_message*/ error_message);
          },
          0);
      return;
    }

    RAY_LOG_EVERY_MS(INFO, 3 * 10 * 1000)
        << "Runtime env agent is not registered yet. Will retry "
           "GetOrCreateRuntimeEnv later: "
        << serialized_runtime_env;
    delay_executor_(
        [this,
         job_id,
         serialized_runtime_env,
         runtime_env_config,
         serialized_allocated_resource_instances,
         callback = std::move(callback)] {
          GetOrCreateRuntimeEnv(job_id,
                                serialized_runtime_env,
                                runtime_env_config,
                                serialized_allocated_resource_instances,
                                callback);
        },
        RayConfig::instance().agent_manager_retry_interval_ms());
    return;
  }
  rpc::GetOrCreateRuntimeEnvRequest request;
  request.set_job_id(job_id.Hex());
  request.set_serialized_runtime_env(serialized_runtime_env);
  request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
  request.set_serialized_allocated_resource_instances(
      serialized_allocated_resource_instances);
  runtime_env_agent_client_->GetOrCreateRuntimeEnv(
      request,
      [serialized_runtime_env,
       runtime_env_config,
       serialized_allocated_resource_instances,
       callback = std::move(callback)](const Status &status,
                                       const rpc::GetOrCreateRuntimeEnvReply &reply) {
        if (status.ok()) {
          if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
            callback(true,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ "");
          } else {
            RAY_LOG(INFO) << "Failed to create runtime env: " << serialized_runtime_env
                          << ", error message: " << reply.error_message();
            callback(false,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ reply.error_message());
          }

        } else {
          RAY_LOG(INFO)
              << "Failed to create runtime env: " << serialized_runtime_env
              << ", status = " << status
              << ", maybe there are some network problems, will fail the request.";
          callback(false, "", "Failed to request agent.");
        }
      });
}

void AgentManager::DeleteRuntimeEnvIfPossible(
    const std::string &serialized_runtime_env,
    DeleteRuntimeEnvIfPossibleCallback callback) {
  if (disable_agent_client_) {
    RAY_LOG(ERROR)
        << "Failed to delete runtime environment URI because the Ray agent couldn't be "
           "started due to the port conflict. See `dashboard_agent.log` for more "
           "details. To solve the problem, start Ray with a hard-coded agent port. `ray "
           "start --dashboard-agent-grpc-port [port]` and make sure the port is not used "
           "by other processes.";
    delay_executor_([callback = std::move(callback)] { callback(false); }, 0);
    return;
  }
  // `runtime_env_agent_client_` should be `nullptr` when the agent is starting or the
  // agent has failed.
  if (runtime_env_agent_client_ == nullptr) {
    RAY_LOG(INFO) << "Runtime env agent is not registered yet. Will retry "
                     "DeleteRuntimeEnvIfPossible later.";
    delay_executor_(

        [this, serialized_runtime_env, callback = std::move(callback)] {
          DeleteRuntimeEnvIfPossible(serialized_runtime_env, callback);
        },
        RayConfig::instance().agent_manager_retry_interval_ms());
    return;
  }
  rpc::DeleteRuntimeEnvIfPossibleRequest request;
  request.set_serialized_runtime_env(serialized_runtime_env);
  request.set_source_process("raylet");
  runtime_env_agent_client_->DeleteRuntimeEnvIfPossible(
      request,
      [serialized_runtime_env, callback = std::move(callback)](
          Status status, const rpc::DeleteRuntimeEnvIfPossibleReply &reply) {
        if (status.ok()) {
          if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
            callback(true);
          } else {
            // TODO(sang): Find a better way to delivering error messages in this case.
            RAY_LOG(ERROR) << "Failed to delete runtime env for "
                           << serialized_runtime_env
                           << ", error message: " << reply.error_message();
            callback(false);
          }

        } else {
          RAY_LOG(ERROR)
              << "Failed to delete runtime env reference for " << serialized_runtime_env
              << ", status = " << status
              << ", maybe there are some network problems, will fail the request.";
          callback(false);
        }
      });
}

}  // namespace raylet
}  // namespace ray
