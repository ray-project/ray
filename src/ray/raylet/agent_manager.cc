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

namespace ray {
namespace raylet {

void AgentManager::HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                                       rpc::RegisterAgentReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  agent_ip_address_ = request.agent_ip_address();
  agent_port_ = request.agent_port();
  agent_pid_ = request.agent_pid();
  runtime_env_agent_client_ =
      runtime_env_agent_client_factory_(agent_ip_address_, agent_port_);
  RAY_LOG(INFO) << "HandleRegisterAgent, ip: " << agent_ip_address_
                << ", port: " << agent_port_ << ", pid: " << agent_pid_;
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::StartAgent() {
  if (options_.agent_commands.empty()) {
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
    RAY_UNUSED(delay_executor_([this] { StartAgent(); },
                               RayConfig::instance().agent_restart_interval_ms()));
    return;
  }

  std::thread monitor_thread([this, child]() mutable {
    SetThreadName("agent.monitor");
    RAY_LOG(INFO) << "Monitor agent process with pid " << child.GetId()
                  << ", register timeout "
                  << RayConfig::instance().agent_register_timeout_ms() << "ms.";
    auto timer = delay_executor_(
        [this, child]() mutable {
          if (agent_pid_ != child.GetId()) {
            RAY_EVENT(ERROR, EL_RAY_AGENT_NOT_REGISTERED)
                    .WithField("ip", agent_ip_address_)
                    .WithField("pid", agent_pid_)
                << "Agent process with pid " << child.GetId()
                << " has not registered, restart it.";
            child.Kill();
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();

    RAY_EVENT(ERROR, EL_RAY_AGENT_EXIT)
            .WithField("ip", agent_ip_address_)
            .WithField("pid", agent_pid_)
        << "Agent process with pid " << child.GetId() << " exit, return value "
        << exit_code;
    RAY_UNUSED(delay_executor_([this] { StartAgent(); },
                               RayConfig::instance().agent_restart_interval_ms()));
  });
  monitor_thread.detach();
}

void AgentManager::CreateRuntimeEnv(const JobID &job_id,
                                    const std::string &serialized_runtime_env,
                                    CreateRuntimeEnvCallback callback) {
  if (runtime_env_agent_client_ == nullptr) {
    RAY_LOG(INFO)
        << "Runtime env agent is not registered yet. Will retry CreateRuntimeEnv later: "
        << serialized_runtime_env;
    delay_executor_(
        [this, job_id, serialized_runtime_env, callback] {
          CreateRuntimeEnv(job_id, serialized_runtime_env, callback);
        },
        RayConfig::instance().agent_manager_retry_interval_ms());
    return;
  }
  rpc::CreateRuntimeEnvRequest request;
  request.set_job_id(job_id.Hex());
  request.set_serialized_runtime_env(serialized_runtime_env);
  runtime_env_agent_client_->CreateRuntimeEnv(
      request, [this, job_id, serialized_runtime_env, callback](
                   Status status, const rpc::CreateRuntimeEnvReply &reply) {
        if (status.ok()) {
          if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
            callback(true, reply.serialized_runtime_env_context());
          } else {
            RAY_LOG(ERROR) << "Failed to create runtime env: " << serialized_runtime_env
                           << ", error message: " << reply.error_message();
            callback(false, reply.serialized_runtime_env_context());
          }

        } else {
          RAY_LOG(ERROR)
              << "Failed to create the runtime env: " << serialized_runtime_env
              << ", status = " << status
              << ", maybe there are some network problems, will retry it later.";
          delay_executor_(
              [this, job_id, serialized_runtime_env, callback] {
                CreateRuntimeEnv(job_id, serialized_runtime_env, callback);
              },
              RayConfig::instance().agent_manager_retry_interval_ms());
        }
      });
}

void AgentManager::DeleteURIs(const std::vector<std::string> &uris,
                              DeleteURIsCallback callback) {
  if (runtime_env_agent_client_ == nullptr) {
    RAY_LOG(INFO)
        << "Runtime env agent is not registered yet. Will retry DeleteURIs later.";
    delay_executor_([this, uris, callback] { DeleteURIs(uris, callback); },
                    RayConfig::instance().agent_manager_retry_interval_ms());
    return;
  }
  rpc::DeleteURIsRequest request;
  for (const auto &uri : uris) {
    request.add_uris(uri);
  }
  runtime_env_agent_client_->DeleteURIs(request, [this, uris, callback](
                                                     Status status,
                                                     const rpc::DeleteURIsReply &reply) {
    if (status.ok()) {
      if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
        callback(true);
      } else {
        RAY_LOG(ERROR) << "Failed to delete URIs"
                       << ", error message: " << reply.error_message();
        callback(false);
      }

    } else {
      RAY_LOG(ERROR) << "Failed to delete URIs"
                     << ", status = " << status
                     << ", maybe there are some network problems, will retry it later.";
      delay_executor_([this, uris, callback] { DeleteURIs(uris, callback); },
                      RayConfig::instance().agent_manager_retry_interval_ms());
    }
  });
}

}  // namespace raylet
}  // namespace ray
