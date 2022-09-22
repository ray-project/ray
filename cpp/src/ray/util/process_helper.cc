// Copyright 2020-2021 The Ray Authors.
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

#include "process_helper.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/ray_config.h"
#include "ray/util/process.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::WorkerType;

void ProcessHelper::StartRayNode(const int port,
                                 const std::string redis_password,
                                 const std::vector<std::string> &head_args) {
  std::vector<std::string> cmdargs({"ray",
                                    "start",
                                    "--head",
                                    "--port",
                                    std::to_string(port),
                                    "--redis-password",
                                    redis_password,
                                    "--node-ip-address",
                                    GetNodeIpAddress()});
  if (!head_args.empty()) {
    cmdargs.insert(cmdargs.end(), head_args.begin(), head_args.end());
  }
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  auto spawn_result = Process::Spawn(cmdargs, true);
  RAY_CHECK(!spawn_result.second);
  spawn_result.first.Wait();
  return;
}

void ProcessHelper::StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  auto spawn_result = Process::Spawn(cmdargs, true);
  RAY_CHECK(!spawn_result.second);
  spawn_result.first.Wait();
  return;
}

std::unique_ptr<ray::gcs::GlobalStateAccessor> ProcessHelper::CreateGlobalStateAccessor(
    const std::string &gcs_address) {
  ray::gcs::GcsClientOptions client_options(gcs_address);
  auto global_state_accessor =
      std::make_unique<ray::gcs::GlobalStateAccessor>(client_options);
  RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
  return global_state_accessor;
}

void ProcessHelper::RayStart(CoreWorkerOptions::TaskExecutionCallback callback) {
  std::string bootstrap_ip = ConfigInternal::Instance().bootstrap_ip;
  int bootstrap_port = ConfigInternal::Instance().bootstrap_port;

  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER &&
      bootstrap_ip.empty()) {
    bootstrap_ip = "127.0.0.1";
    StartRayNode(bootstrap_port,
                 ConfigInternal::Instance().redis_password,
                 ConfigInternal::Instance().head_args);
  }
  if (bootstrap_ip == "127.0.0.1") {
    bootstrap_ip = GetNodeIpAddress();
  }

  std::string bootstrap_address = bootstrap_ip + ":" + std::to_string(bootstrap_port);
  std::string node_ip = ConfigInternal::Instance().node_ip_address;
  if (node_ip.empty()) {
    if (!bootstrap_ip.empty()) {
      node_ip = GetNodeIpAddress(bootstrap_address);
    } else {
      node_ip = GetNodeIpAddress();
    }
  }

  std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor =
      CreateGlobalStateAccessor(bootstrap_address);
  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER) {
    std::string node_to_connect;
    auto status =
        global_state_accessor->GetNodeToConnectForDriver(node_ip, &node_to_connect);
    RAY_CHECK_OK(status);
    ray::rpc::GcsNodeInfo node_info;
    node_info.ParseFromString(node_to_connect);
    ConfigInternal::Instance().raylet_socket_name = node_info.raylet_socket_name();
    ConfigInternal::Instance().plasma_store_socket_name =
        node_info.object_store_socket_name();
    ConfigInternal::Instance().node_manager_port = node_info.node_manager_port();
  }
  RAY_CHECK(!ConfigInternal::Instance().raylet_socket_name.empty());
  RAY_CHECK(!ConfigInternal::Instance().plasma_store_socket_name.empty());
  RAY_CHECK(ConfigInternal::Instance().node_manager_port > 0);

  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER) {
    auto session_dir = *global_state_accessor->GetInternalKV("session", "session_dir");
    ConfigInternal::Instance().UpdateSessionDir(session_dir);
  }

  gcs::GcsClientOptions gcs_options = gcs::GcsClientOptions(bootstrap_address);

  CoreWorkerOptions options;
  options.worker_type = ConfigInternal::Instance().worker_type;
  options.language = Language::CPP;
  options.store_socket = ConfigInternal::Instance().plasma_store_socket_name;
  options.raylet_socket = ConfigInternal::Instance().raylet_socket_name;
  if (options.worker_type == WorkerType::DRIVER) {
    if (!ConfigInternal::Instance().job_id.empty()) {
      options.job_id = JobID::FromHex(ConfigInternal::Instance().job_id);
    } else {
      options.job_id = global_state_accessor->GetNextJobID();
    }
  }
  options.gcs_options = gcs_options;
  options.enable_logging = true;
  options.log_dir = ConfigInternal::Instance().logs_dir;
  options.install_failure_signal_handler = true;
  options.node_ip_address = node_ip;
  options.node_manager_port = ConfigInternal::Instance().node_manager_port;
  options.raylet_ip_address = node_ip;
  options.driver_name = "cpp_worker";
  options.metrics_agent_port = -1;
  options.task_execution_callback = callback;
  options.startup_token = ConfigInternal::Instance().startup_token;
  options.runtime_env_hash = ConfigInternal::Instance().runtime_env_hash;
  rpc::JobConfig job_config;
  job_config.set_default_actor_lifetime(
      ConfigInternal::Instance().default_actor_lifetime);

  for (const auto &path : ConfigInternal::Instance().code_search_path) {
    job_config.add_code_search_path(path);
  }
  job_config.set_ray_namespace(ConfigInternal::Instance().ray_namespace);
  if (ConfigInternal::Instance().runtime_env) {
    job_config.mutable_runtime_env_info()->set_serialized_runtime_env(
        ConfigInternal::Instance().runtime_env->Serialize());
  }
  if (ConfigInternal::Instance().job_config_metadata.size()) {
    auto metadata_ptr = job_config.mutable_metadata();
    for (const auto &it : ConfigInternal::Instance().job_config_metadata) {
      (*metadata_ptr)[it.first] = it.second;
    }
  }
  std::string serialized_job_config;
  RAY_CHECK(job_config.SerializeToString(&serialized_job_config));
  options.serialized_job_config = serialized_job_config;
  CoreWorkerProcess::Initialize(options);
}

void ProcessHelper::RayStop() {
  CoreWorkerProcess::Shutdown();
  if (ConfigInternal::Instance().bootstrap_ip.empty()) {
    StopRayNode();
  }
}

}  // namespace internal
}  // namespace ray
