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
  // Reset the restart count after registration is done.
  agent_restart_count_ = 0;
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
  argv.push_back(NULL);
  // Set node id to agent.
  ProcessEnvironment env;
  env.insert({"RAY_NODE_ID", options_.node_id.Hex()});
  env.insert({"RAY_RAYLET_PID", std::to_string(getpid())});
  // Report the restart count to the agent so that we can decide whether or not
  // report the error message to drivers.
  env.insert({"RESTART_COUNT", std::to_string(agent_restart_count_)});
  env.insert({"MAX_RESTART_COUNT",
              std::to_string(RayConfig::instance().agent_max_restart_count())});
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
                             << " has not registered, restart it. ip "
                             << agent_ip_address_ << ". pid " << agent_pid_;
            child.Kill();
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();
    RAY_LOG(WARNING) << "Agent process with pid " << child.GetId()
                     << " exit, return value " << exit_code << ". ip "
                     << agent_ip_address_ << ". pid " << agent_pid_;
    if (agent_restart_count_ < RayConfig::instance().agent_max_restart_count()) {
      RAY_UNUSED(delay_executor_(
          [this] {
            agent_restart_count_++;
            StartAgent();
          },
          // Retrying with exponential backoff
          RayConfig::instance().agent_restart_interval_ms() *
              std::pow(2, (agent_restart_count_ + 1))));
    } else {
      RAY_LOG(WARNING) << "Agent has failed "
                       << RayConfig::instance().agent_max_restart_count()
                       << " times in a row without registering the agent. This is highly "
                          "likely there's a bug in the dashboard agent. Please check out "
                          "the dashboard_agent.log file.";
      RAY_EVENT(WARNING, EL_RAY_AGENT_EXIT)
              .WithField("ip", agent_ip_address_)
              .WithField("pid", agent_pid_)
          << "Agent failed to be restarted "
          << RayConfig::instance().agent_max_restart_count()
          << " times. Agent won't be restarted.";
    }
  });
  monitor_thread.detach();
}

void AgentManager::CreateRuntimeEnv(
    const JobID &job_id, const std::string &serialized_runtime_env,
    const std::string &serialized_allocated_resource_instances,
    CreateRuntimeEnvCallback callback) {
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
          callback(/*successful=*/false, /*serialized_runtime_env_context=*/"",
                   /*setup_error_message*/ error_message);
        },
        0);
    return;
  }

  if (runtime_env_agent_client_ == nullptr) {
    // If the agent cannot be restarted anymore, fail the request.
    if (agent_restart_count_ >= RayConfig::instance().agent_max_restart_count()) {
      std::stringstream str_stream;
      str_stream << "Runtime environment " << serialized_runtime_env
                 << " cannot be created on this node because the agent is dead.";
      const auto &error_message = str_stream.str();
      RAY_LOG(WARNING) << error_message;
      delay_executor_(
          [callback = std::move(callback),
           serialized_runtime_env = std::move(serialized_runtime_env), error_message] {
            callback(/*successful=*/false,
                     /*serialized_runtime_env_context=*/serialized_runtime_env,
                     /*setup_error_message*/ error_message);
          },
          0);
      return;
    }

    RAY_LOG_EVERY_MS(INFO, 3 * 10 * 1000)
        << "Runtime env agent is not registered yet. Will retry CreateRuntimeEnv later: "
        << serialized_runtime_env;
    delay_executor_(
        [this, job_id, serialized_runtime_env, serialized_allocated_resource_instances,
         callback = std::move(callback)] {
          CreateRuntimeEnv(job_id, serialized_runtime_env,
                           serialized_allocated_resource_instances, callback);
        },
        RayConfig::instance().agent_manager_retry_interval_ms());
    return;
  }
  rpc::CreateRuntimeEnvRequest request;
  request.set_job_id(job_id.Hex());
  request.set_serialized_runtime_env(serialized_runtime_env);
  request.set_serialized_allocated_resource_instances(
      serialized_allocated_resource_instances);
  runtime_env_agent_client_->CreateRuntimeEnv(
      request, [this, job_id, serialized_runtime_env,
                serialized_allocated_resource_instances, callback = std::move(callback)](
                   const Status &status, const rpc::CreateRuntimeEnvReply &reply) {
        if (status.ok()) {
          if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
            callback(true, reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ "");
          } else {
            RAY_LOG(INFO) << "Failed to create runtime env: " << serialized_runtime_env
                          << ", error message: " << reply.error_message();
            callback(false, reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ reply.error_message());
          }

        } else {
          // TODO(sang): Invoke a callback if it fails more than X times.
          RAY_LOG(INFO)
              << "Failed to create the runtime env: " << serialized_runtime_env
              << ", status = " << status
              << ", maybe there are some network problems, will retry it later.";
          delay_executor_(
              [this, job_id, serialized_runtime_env,
               serialized_allocated_resource_instances, callback = std::move(callback)] {
                CreateRuntimeEnv(job_id, serialized_runtime_env,
                                 serialized_allocated_resource_instances, callback);
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
        // TODO(sang): Find a better way to delivering error messages in this case.
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
