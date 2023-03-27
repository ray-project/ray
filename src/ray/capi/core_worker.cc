// Copyright 2020-2023 The Ray Authors.
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

#ifndef _WIN32
#include <unistd.h>
#endif

#include <assert.h>
#include <ray/common/status.h>

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/gcs/gcs_client/gcs_client.h"

using namespace ray::core;
using namespace ray::gcs;
using namespace ray;
using namespace std;

static GcsClientOptions client_options;
static CoreWorkerOptions options;

// internal core worker callback function for a further rust callback
Status ExecuteTask(
    const rpc::Address &caller_address,
    ray::TaskType task_type,
    const std::string task_name,
    const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args_buffer,
    const std::vector<rpc::ObjectReference> &arg_refs,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *returns,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_returns,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_retryable_error,
    bool *is_application_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute,
    bool is_reattempt) {
  RAY_LOG(INFO) << "ExecuteTask called";
  // TODO: more stuff
  return ray::Status::OK();
}

// export c functions
extern "C" {

int CoreWorkerProcess_Initialize() {
  try {
    CoreWorkerProcess::Initialize(options);
  } catch (const std::exception &e) {
    std::cerr << "CoreWorkerProcess_Initialize failed: " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

void CoreWorkerProcess_Shutdown() { CoreWorkerProcess::Shutdown(); }

// run task execution loop
void CoreWorkerProcess_RunTaskExecutionLoop() {
  CoreWorkerProcess::RunTaskExecutionLoop();
}

// core worker options related functions
void CoreWorkerProcessOptions_SetWorkerType(WorkerType worker_type) {
  options.worker_type = worker_type;
}

void CoreWorkerProcessOptions_SetLanguage(Language language) {
  options.language = language;
}

void CoreWorkerProcessOptions_SetStoreSocket(const uint8_t *store_socket, uint32_t len) {
  options.store_socket = std::string((const char *)store_socket, len);
}

void CoreWorkerProcessOptions_SetRayletSocket(const uint8_t *raylet_socket,
                                              uint32_t len) {
  string raylet_socket_str = std::string((const char *)raylet_socket, len);
  options.raylet_socket = raylet_socket_str;
}

void CoreWorkerProcessOptions_SetJobID_Int(uint32_t job_id) {
  options.job_id = JobID::FromInt(job_id);
}

void CoreWorkerProcessOptions_SetJobID_Hex(const uint8_t *job_id, uint32_t len) {
  options.job_id = JobID::FromHex(std::string((const char *)job_id, len));
}

void CoreWorkerProcessOptions_SetJobID_Binary(const uint8_t *job_id, uint32_t len) {
  options.job_id = JobID::FromBinary(std::string((const char *)job_id, len));
}

// TODO: set gcs_options

void CoreWorkerProcessOptions_SetEnableLogging(bool enable_logging) {
  options.enable_logging = enable_logging;
}

// set log dir
void CoreWorkerProcessOptions_SetLogDir(const uint8_t *log_dir, uint32_t len) {
  options.log_dir = std::string((const char *)log_dir, len);
}

// set install_failure_signal_handler
void CoreWorkerProcessOptions_SetInstallFailureSignalHandler(
    bool install_failure_signal_handler) {
  options.install_failure_signal_handler = install_failure_signal_handler;
}

// set node_ip_address
void CoreWorkerProcessOptions_SetNodeIpAddress(const uint8_t *node_ip_address,
                                               uint32_t len) {
  options.node_ip_address = std::string((const char *)node_ip_address, len);
}

// set node_manager_port
void CoreWorkerProcessOptions_SetNodeManagerPort(int node_manager_port) {
  options.node_manager_port = node_manager_port;
}

// set raylet_ip_address
void CoreWorkerProcessOptions_SetRayletIpAddress(const uint8_t *raylet_ip_address,
                                                 uint32_t len) {
  options.raylet_ip_address = std::string((const char *)raylet_ip_address, len);
}

// set driver_name
void CoreWorkerProcessOptions_SetDriverName(const uint8_t *driver_name, uint32_t len) {
  options.driver_name = std::string((const char *)driver_name, len);
}

// set metrics_agent_port
void CoreWorkerProcessOptions_SetMetricsAgentPort(uint32_t metrics_agent_port) {
  options.metrics_agent_port = metrics_agent_port;
}

// set startup token
void CoreWorkerProcessOptions_SetStartupToken(uint64_t startup_token) {
  options.startup_token = startup_token;
}

// set runtime_env_hash
void CoreWorkerProcessOptions_SetRuntimeEnvHash(int32_t runtime_env_hash) {
  options.runtime_env_hash = runtime_env_hash;
}

// set serialized_job_config
void CoreWorkerProcessOptions_SetSerializedJobConfig(const uint8_t *buf, size_t size) {
  options.serialized_job_config = std::string((const char *)buf, size);
}

// set the task execution callback
void CoreWorkerProcessOptions_SetTaskExecutionCallback() {
  options.task_execution_callback = ExecuteTask;
}

// TODO: more stuff

void CoreWorkerProcessOptions_UpdateGcsClientOptions(const uint8_t *gcs_address,
                                                     uint32_t len) {
  client_options = GcsClientOptions(std::string((const char *)gcs_address, len));
}
}
