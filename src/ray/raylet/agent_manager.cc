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

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
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
  RAY_LOG(INFO) << "HandleRegisterAgent, ip: " << agent_ip_address_
                << ", port: " << agent_port_ << ", pid: " << agent_pid_;
  job_client_ = job_client_factory_(agent_ip_address_, agent_port_);
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::OnDriverRegistered(JobID job_id, pid_t driver_pid) {
  RAY_LOG(INFO) << "OnDriverRegistered, job id: " << job_id
                << ", driver pid: " << driver_pid;
  auto it = starting_drivers_.find(job_id);
  if (it != starting_drivers_.end()) {
    it->second->has_registered = true;
  } else if (Process::IsAlive(driver_pid)) {
    auto driver_monitor_info = std::make_shared<DriverMonitorInfo>(
        DriverMonitorInfo{job_id, /* has registered */ true, driver_pid, ""});
    starting_drivers_.emplace(job_id, driver_monitor_info);
  }
}

void AgentManager::InitializeJobEnv(std::shared_ptr<rpc::JobTableData> job_data,
                                    bool start_driver) {
  auto job_id = JobID::FromBinary(job_data->job_id());

  if (options_.agent_commands.empty()) {
    RAY_LOG(INFO) << "Not to initialize the env for job " << job_id
                  << ", the agent command is empty.";
    return;
  }

  if (job_client_ == nullptr) {
    delay_executor_(
        [this, job_data, start_driver] { InitializeJobEnv(job_data, start_driver); },
        RayConfig::instance().agent_retry_interval_ms());
    return;
  }

  RAY_LOG(INFO) << "Start initializing the env for job " << job_id
                << ", start_driver = " << start_driver;

  rpc::InitializeJobEnvRequest request;
  request.mutable_job_data()->CopyFrom(*job_data);
  request.set_start_driver(start_driver);
  job_client_->InitializeJobEnv(request, [this, job_id, job_data, start_driver](
                                             Status status,
                                             const rpc::InitializeJobEnvReply &reply) {
    if (status.ok()) {
      if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
        // Update job resource so that the driver/worker could lease worker from raylet.
        gcs::NodeResourceInfoAccessor::ResourceMap resources;
        auto resource = std::make_shared<rpc::ResourceTableData>();
        resource->set_resource_capacity(kMaxResourceCapacity);
        auto resource_name = JOB_RESOURCE_PREFIX + absl::AsciiStrToUpper(job_id.Hex());
        resources.emplace(std::move(resource_name), std::move(resource));
        RAY_CHECK_OK(gcs_client_->NodeResources().AsyncUpdateResources(
            options_.node_id, resources, nullptr));
        if (start_driver) {
          auto it = starting_drivers_.find(job_id);
          if (it != starting_drivers_.end()) {
            RAY_LOG(INFO) << "Driver " << reply.driver_pid() << " of job " << job_id
                          << " has been registered.";
            RAY_CHECK(it->second->has_registered);
            starting_drivers_.erase(job_id);
          } else {
            RAY_LOG(INFO) << "Monitor driver " << reply.driver_pid() << " of job "
                          << job_id;
            auto driver_monitor_info = std::make_shared<DriverMonitorInfo>(
                DriverMonitorInfo{job_id, /* has registered */ false, reply.driver_pid(),
                                  reply.driver_cmdline()});
            starting_drivers_.emplace(job_id, driver_monitor_info);
            MonitorStartingDriver(driver_monitor_info);
          }
        }
        RAY_LOG(INFO) << "Finished initializing the env for job " << job_id;
      } else {
        std::ostringstream ostr;
        ostr << "Failed to initialize env for job " << job_id
             << ", error message: " << reply.error_message();
        std::string error_msg = ostr.str();
        RAY_LOG(ERROR) << error_msg;
        auto error_data_ptr = gcs::CreateErrorTableData("InitializeJobEnv", error_msg,
                                                        current_time_ms(), job_id);
        RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
        if (start_driver) {
          // Mark job failed only if initailize env for driver failed.
          RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFailed(
              job_id, error_msg, reply.driver_cmdline(), nullptr));
        }
      }
    } else {
      RAY_LOG(ERROR) << "Failed to initialize the env for job " << job_id
                     << " with status = " << status
                     << ", maybe there are some network problems, try initialize "
                        "this job env later.";
      job_client_.reset();
      delay_executor_(
          [this, job_data, start_driver] { InitializeJobEnv(job_data, start_driver); },
          RayConfig::instance().agent_retry_interval_ms());
    }
  });
}

void AgentManager::ClearJobEnv(const JobID &job_id) {
  if (options_.agent_commands.empty()) {
    RAY_LOG(INFO) << "Not to clear the env for job " << job_id
                  << ", the agent command is empty.";
    return;
  }
  if (job_client_ == nullptr) {
    RAY_UNUSED(delay_executor_([this, job_id] { ClearJobEnv(job_id); },
                               RayConfig::instance().agent_retry_interval_ms()));
    return;
  }
  RAY_LOG(INFO) << "Start clearing the env for job, job id = " << job_id;
  rpc::CleanJobEnvRequest request;
  request.set_job_id(job_id.Binary());
  job_client_->CleanJobEnv(
      request, [job_id](Status status, const rpc::CleanJobEnvReply &reply) {
        RAY_LOG(INFO) << "Finished clearing the env for job, job id = " << job_id;
      });
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
            RAY_LOG(WARNING) << "Agent process with pid " << child.GetId()
                             << " has not registered, restart it.";
            child.Kill();
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();

    RAY_LOG(WARNING) << "Agent process with pid " << child.GetId()
                     << " exit, return value " << exit_code;
    RAY_UNUSED(delay_executor_([this] { StartAgent(); },
                               RayConfig::instance().agent_restart_interval_ms()));
  });
  monitor_thread.detach();
}

void AgentManager::MonitorStartingDriver(
    std::shared_ptr<DriverMonitorInfo> driver_monitor_info) {
  if (driver_monitor_info->has_registered) {
    RAY_LOG(INFO) << "Driver " << driver_monitor_info->driver_pid << " of job "
                  << driver_monitor_info->job_id << " has been registered.";
    starting_drivers_.erase(driver_monitor_info->job_id);
  } else if (Process::IsAlive(driver_monitor_info->driver_pid)) {
    delay_executor_(
        [this, driver_monitor_info] { MonitorStartingDriver(driver_monitor_info); },
        RayConfig::instance().agent_monitor_driver_starting_interval_ms());
  } else {
    RAY_LOG(INFO) << "Driver " << driver_monitor_info->driver_pid << " of job "
                  << driver_monitor_info->job_id << " was dead.";
    starting_drivers_.erase(driver_monitor_info->job_id);
    std::ostringstream ostr;
    ostr << "Failed to start driver " << driver_monitor_info->driver_pid << " of job "
         << driver_monitor_info->job_id
         << ", please check the detailed error information in the driver log.";
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFailed(
        driver_monitor_info->job_id, ostr.str(), driver_monitor_info->driver_cmdline,
        nullptr));
  }
}
}  // namespace raylet
}  // namespace ray
