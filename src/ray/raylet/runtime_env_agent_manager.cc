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

#include "ray/raylet/runtime_env_agent_manager.h"

#include <thread>

#include "ray/common/ray_config.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {
namespace raylet {

void RuntimeEnvAgentManager::HandleRegisterRuntimeEnvAgent(
    rpc::RegisterRuntimeEnvAgentRequest request,
    rpc::RegisterRuntimeEnvAgentReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  reported_agent_ip_address_ = request.agent_ip_address();
  reported_agent_port_ = request.agent_port();
  reported_agent_id_ = request.agent_id();
  // TODO(SongGuyang): We should remove this after we find better port resolution.
  // Note: `agent_port_` should be 0 if the grpc port of agent is in conflict.
  if (reported_agent_port_ != 0) {
    runtime_env_agent_client_ = runtime_env_agent_client_factory_(
        reported_agent_ip_address_, reported_agent_port_);
    RAY_LOG(INFO) << "HandleRegisterAgent, ip: " << reported_agent_ip_address_
                  << ", port: " << reported_agent_port_ << ", id: " << reported_agent_id_;
  } else {
    RAY_LOG(WARNING) << "The GRPC port of the Ray agent is invalid (0), ip: "
                     << reported_agent_ip_address_ << ", id: " << reported_agent_id_
                     << ". The agent client in the raylet has been disabled.";
    disable_agent_client_ = true;
  }
  reply->set_status(rpc::RUNTIME_ENV_AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void RuntimeEnvAgentManager::GetOrCreateRuntimeEnv(
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
          << "Failed to create runtime environment for job " << job_id
          << " because the Ray agent couldn't be started due to the port conflict. See "
             "`dashboard_agent.log` for more details. To solve the problem, start Ray "
             "with a hard-coded agent port. `ray start --dashboard-agent-grpc-port "
             "[port]` and make sure the port is not used by other processes.";
      const auto &error_message = str_stream.str();
      RAY_LOG(ERROR) << error_message;
      RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                     << serialized_runtime_env;
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
           "GetOrCreateRuntimeEnv for job_id "
        << job_id << " later";
    RAY_LOG_EVERY_MS(DEBUG, 3 * 10 * 1000)
        << "Serialized runtime env for job " << job_id << ": " << serialized_runtime_env;
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
       job_id,
       callback = std::move(callback)](const Status &status,
                                       const rpc::GetOrCreateRuntimeEnvReply &reply) {
        if (status.ok()) {
          if (reply.status() == rpc::RUNTIME_ENV_AGENT_RPC_STATUS_OK) {
            callback(true,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ "");
          } else {
            RAY_LOG(INFO) << "Failed to create runtime env for job " << job_id
                          << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                           << serialized_runtime_env;
            callback(false,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ reply.error_message());
          }

        } else {
          RAY_LOG(INFO)
              << "Failed to create runtime env for job " << job_id
              << ", status = " << status
              << ", maybe there are some network problems, will fail the request.";
          RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                         << serialized_runtime_env;
          callback(false, "", "Failed to request agent.");
        }
      });
}

void RuntimeEnvAgentManager::DeleteRuntimeEnvIfPossible(
    const std::string &serialized_runtime_env,
    DeleteRuntimeEnvIfPossibleCallback callback) {
  if (disable_agent_client_) {
    RAY_LOG(ERROR)
        << "Failed to delete runtime environment URI because the Ray agent couldn't be "
           "started due to the port conflict. See `dashboard_agent.log` for more "
           "details. To solve the problem, start Ray with a hard-coded agent port. `ray "
           "start --dashboard-agent-grpc-port [port]` and make sure the port is not used "
           "by other processes.";
    RAY_LOG(DEBUG) << "Serialized runtime env for failed URI deletion: "
                   << serialized_runtime_env;
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
          if (reply.status() == rpc::RUNTIME_ENV_AGENT_RPC_STATUS_OK) {
            callback(true);
          } else {
            // TODO(sang): Find a better way to delivering error messages in this case.
            RAY_LOG(ERROR) << "Failed to delete runtime env"
                           << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
            callback(false);
          }

        } else {
          RAY_LOG(ERROR)
              << "Failed to delete runtime env reference"
              << ", status = " << status
              << ", maybe there are some network problems, will fail the request.";
          RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
          callback(false);
        }
      });
}

}  // namespace raylet
}  // namespace ray
