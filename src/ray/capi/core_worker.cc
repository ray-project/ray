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

#include <msgpack.hpp>

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/gcs/gcs_client/gcs_client.h"

using namespace ray::core;
using namespace ray::gcs;
using namespace ray;
using namespace std;

// generated from struct definitions inside task_executor.rs
typedef struct Resource {
  const unsigned char *name;
  size_t name_len;
  double value;
} Resource;

typedef struct NamedRayObject {
  const unsigned char *obj_id;
  size_t obj_id_len;
  const void *ray_object;
  size_t ray_object_len;
} NamedRayObject;

typedef struct TaskExecutionInfo {
  const void *caller_address;
  int task_type;
  const unsigned char *task_name;
  size_t task_name_len;
  const void *ray_function;
  const Resource *required_resources;
  size_t required_resources_len;
  const void *args;
  size_t args_len;
  const unsigned char *debugger_breakpoint;
  size_t debugger_breakpoint_len;
  const unsigned char *serialized_retry_exception_allowlist;
  size_t serialized_retry_exception_allowlist_len;
  const NamedRayObject *returns;
  size_t returns_len;
  const NamedRayObject *dynamic_returns;
  size_t dynamic_returns_len;
  const void *creation_task_exception_pb_bytes;
  bool *is_retryable_error;
  unsigned char *is_application_error;
  size_t *is_application_error_len;
  const void **defined_concurrency_groups;
  size_t defined_concurrency_groups_len;
  const unsigned char *name_of_concurrency_group_to_execute;
  size_t name_of_concurrency_group_to_execute_len;
  bool is_reattempt;
} TaskExecutionInfo;
// end generated from struct definitions inside task_executor.rs

// execute task call back
typedef int ExecuteTaskFunction(TaskExecutionInfo *task_execution_info);

static GcsClientOptions client_options;
static CoreWorkerOptions options;

inline ObjectID ObjectIDFromBuffer(uint8_t *object_id_buf, size_t object_id_len) {
  if (object_id_buf == nullptr || object_id_len == 0) {
    return ObjectID::Nil();
  }
  return ObjectID::FromBinary(std::string((const char *)object_id_buf, object_id_len));
}

inline std::vector<ObjectID> ObjectIDVectorFromBuffer(uint8_t **object_ids_buf,
                                                      size_t *object_ids_len,
                                                      size_t num_objects) {
  if (num_objects == 0) {
    return {};
  }
  if (object_ids_buf == nullptr || object_ids_len == nullptr) {
    return {};
  }
  for (size_t i = 0; i < num_objects; i++) {
    if (object_ids_buf[i] == nullptr || object_ids_len[i] == 0) {
      return {};
    }
  }
  std::vector<ObjectID> object_ids;
  for (size_t i = 0; i < num_objects; i++) {
    ObjectID object_id = ObjectID::FromBinary(
        std::string((const char *)object_ids_buf[i], object_ids_len[i]));
    object_ids.push_back(object_id);
  }
  return object_ids;
}

inline int DeserializeMsgPack(uint8_t *msgbuf,
                              size_t msgbuf_len,
                              uint8_t *buf,
                              size_t *buf_len) {
  if (msgbuf == nullptr || msgbuf_len == 0 || buf == nullptr || buf_len == nullptr) {
    return -1;
  }
  msgpack::sbuffer tmpbuf;
  msgpack::packer<msgpack::sbuffer> packer(&tmpbuf);
  packer.pack_bin(msgbuf_len);
  packer.pack_bin_body((const char *)msgbuf, msgbuf_len);

  // return error if data is not enough or null pointer
  if (*buf_len < tmpbuf.size()) {
    return -1;
  }
  memcpy(buf, tmpbuf.data(), tmpbuf.size());
  *buf_len = tmpbuf.size();
  return 0;
}

// export c functions
extern "C" {

ExecuteTaskFunction *execute_task_function = nullptr;

}  // extern "C"

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
    std::string *is_application_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute,
    bool is_reattempt) {
  RAY_LOG(INFO) << "ExecuteTask called";
  if (execute_task_function == nullptr) {
    return ray::Status::Invalid("execute_task_function is not set");
  }

  // TODO: more stuff here, we need to convert the arguments to a TaskExecutionInfo
  execute_task_function((TaskExecutionInfo *)nullptr);
  return ray::Status::OK();
}

extern "C" {

// -----------------  core worker process related functions -----------------

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

void CoreWorkerProcessOptions_SetStoreSocket(const uint8_t *store_socket, size_t len) {
  options.store_socket = std::string((const char *)store_socket, len);
}

void CoreWorkerProcessOptions_SetRayletSocket(const uint8_t *raylet_socket, size_t len) {
  string raylet_socket_str = std::string((const char *)raylet_socket, len);
  options.raylet_socket = raylet_socket_str;
}

void CoreWorkerProcessOptions_SetJobID_Int(uint32_t job_id) {
  options.job_id = JobID::FromInt(job_id);
}

void CoreWorkerProcessOptions_SetJobID_Hex(const uint8_t *job_id, size_t len) {
  options.job_id = JobID::FromHex(std::string((const char *)job_id, len));
}

void CoreWorkerProcessOptions_SetJobID_Binary(const uint8_t *job_id, size_t len) {
  options.job_id = JobID::FromBinary(std::string((const char *)job_id, len));
}

// TODO: set gcs_options

void CoreWorkerProcessOptions_SetEnableLogging(bool enable_logging) {
  options.enable_logging = enable_logging;
}

// set log dir
void CoreWorkerProcessOptions_SetLogDir(const uint8_t *log_dir, size_t len) {
  options.log_dir = std::string((const char *)log_dir, len);
}

// set install_failure_signal_handler
void CoreWorkerProcessOptions_SetInstallFailureSignalHandler(
    bool install_failure_signal_handler) {
  options.install_failure_signal_handler = install_failure_signal_handler;
}

// set node_ip_address
void CoreWorkerProcessOptions_SetNodeIpAddress(const uint8_t *node_ip_address,
                                               size_t len) {
  options.node_ip_address = std::string((const char *)node_ip_address, len);
}

// set node_manager_port
void CoreWorkerProcessOptions_SetNodeManagerPort(int node_manager_port) {
  options.node_manager_port = node_manager_port;
}

// set raylet_ip_address
void CoreWorkerProcessOptions_SetRayletIpAddress(const uint8_t *raylet_ip_address,
                                                 size_t len) {
  options.raylet_ip_address = std::string((const char *)raylet_ip_address, len);
}

// set driver_name
void CoreWorkerProcessOptions_SetDriverName(const uint8_t *driver_name, size_t len) {
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

// set the gcs client options
void CoreWorkerProcessOptions_SetGcsOptions() { options.gcs_options = client_options; }

// TODO: more stuff

void CoreWorkerProcessOptions_UpdateGcsClientOptions(const uint8_t *gcs_address,
                                                     size_t len) {
  client_options = GcsClientOptions(std::string((const char *)gcs_address, len));
}

// ---------------------- core worker functions ----------------------

// core worker put object function
int CoreWorker_Put(uint8_t *data,
                   size_t len,
                   uint8_t *object_id_buf,
                   size_t object_id_len) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();

  auto object_id = ObjectIDFromBuffer(object_id_buf, object_id_len);
  if (object_id.IsNil()) {
    std::cerr << "CoreWorker_Put failed: object_id is nil" << std::endl;
    return -1;
  }

  std::shared_ptr<LocalMemoryBuffer> data_buffer =
      std::make_shared<LocalMemoryBuffer>(data, len, /*copy_data=*/true);
  auto status = core_worker.Put(
      ::ray::RayObject(data_buffer, nullptr, std::vector<rpc::ObjectReference>()),
      {},
      object_id);
  if (!status.ok()) {
    std::cerr << "CoreWorker_Put failed: " << status.ToString() << std::endl;
    return -1;
  }
  return 0;
}

int CoreWorker_GetMulti(uint8_t **object_ids_buf,
                        size_t *object_ids_len,
                        size_t num_objects,
                        uint8_t **data,
                        size_t *data_len,
                        size_t *num_results,
                        int32_t timeout_ms) {
  if (num_objects == 0 || object_ids_buf == nullptr || object_ids_len == nullptr ||
      data == nullptr || data_len == nullptr || num_results == nullptr) {
    return -1;
  }
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();

  auto object_ids = ObjectIDVectorFromBuffer(object_ids_buf, object_ids_len, num_objects);
  if (object_ids.empty()) {
    return -1;
  }

  std::vector<std::shared_ptr<::ray::RayObject>> results;
  ::ray::Status status = core_worker.Get(object_ids, timeout_ms, &results);
  if (!status.ok()) {
    return -1;
  }
  RAY_CHECK(results.size() == object_ids.size());
  for (size_t i = 0; i < results.size(); i++) {
    const auto &meta = results[i]->GetMetadata();
    const auto &data_buffer = results[i]->GetData();
    std::string meta_str = "";
    if (meta != nullptr) {
      meta_str = std::string((const char *)meta->Data(), meta->Size());
      // TODO: raise exception and check meta_str
    }

    const char *tmp_data = nullptr;
    size_t tmp_data_len = 0;
    if (data_buffer) {
      tmp_data = reinterpret_cast<const char *>(data_buffer->Data());
      tmp_data_len = data_buffer->Size();
    }
    if (meta_str == "RAW") {
      int res;
      res = DeserializeMsgPack((uint8_t *)tmp_data, tmp_data_len, data[i], &data_len[i]);
      if (res != 0) {
        return res;
      }
    } else {
      // return error if data is not enough or null pointer
      if (data[i] == nullptr || data_len[i] < tmp_data_len) {
        return -1;
      }
      memcpy(data[i], tmp_data, tmp_data_len);
      data_len[i] = tmp_data_len;
    }
  }
  *num_results = results.size();
  return 0;
}

// core worker get object function
int CoreWorker_Get(const uint8_t *object_id_buf,
                   size_t object_id_len,
                   uint8_t *data,
                   size_t *len,
                   int32_t timeout_ms) {
  uint8_t *object_ids_buf_list[1] = {(uint8_t *)object_id_buf};
  size_t object_ids_len_list[1] = {object_id_len};
  uint8_t *data_list[1] = {data};
  size_t num_results;
  int res;
  res = CoreWorker_GetMulti(object_ids_buf_list,
                            object_ids_len_list,
                            1,
                            data_list,
                            len,
                            &num_results,
                            timeout_ms);
  return res;
}

int CoreWorker_WaitMulti(uint8_t **object_ids_buf,
                         size_t *object_ids_len,
                         size_t obj_ids_num,
                         size_t num_objects,
                         bool *results,
                         int32_t timeout_ms) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();

  auto object_ids = ObjectIDVectorFromBuffer(object_ids_buf, object_ids_len, obj_ids_num);
  if (object_ids.empty()) {
    return -1;
  }
  std::vector<bool> res_vec;

  ::ray::Status status =
      core_worker.Wait(object_ids, num_objects, timeout_ms, &res_vec, true);
  if (!status.ok()) {
    return -1;
  }

  for (size_t i = 0; i < res_vec.size(); i++) {
    results[i] = res_vec[i];
  }
  return 0;
}

int CoreWorker_AddLocalReference(uint8_t *object_Id_buf, size_t object_id_len) {
  if (object_Id_buf == nullptr || object_id_len == 0) {
    return -1;
  }
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.AddLocalReference(ObjectIDFromBuffer(object_Id_buf, object_id_len));
    return 0;
  }
  return -1;
}

int CoreWorker_RemoveLocalReference(uint8_t *object_Id_buf, size_t object_id_len) {
  if (object_Id_buf == nullptr || object_id_len == 0) {
    return -1;
  }
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.RemoveLocalReference(ObjectIDFromBuffer(object_Id_buf, object_id_len));
    return 0;
  }
  return -1;
}

int CoreWorker_SubmitActorTask(const char *actor_id,
                               size_t actor_id_len,
                               void *ray_function,
                               void *task_args_vec,
                               void *task_options,
                               uint8_t *return_id_buf,
                               size_t *return_id_len) {
  if (actor_id == nullptr || actor_id_len == 0 || ray_function == nullptr ||
      task_args_vec == nullptr || task_options == nullptr) {
    return -1;
  }
  if (return_id_buf == nullptr || return_id_len == nullptr) {
    return -1;
  }
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  auto actor_id_ = ActorID::FromBinary(std::string(actor_id, actor_id_len));
  std::vector<rpc::ObjectReference> return_refs;
  auto status =
      core_worker.SubmitActorTask(actor_id_,
                                  *(RayFunction *)ray_function,
                                  *(std::vector<std::unique_ptr<TaskArg>> *)task_args_vec,
                                  *(TaskOptions *)task_options,
                                  return_refs);
  if (!status.ok()) {
    return -1;
  }
  if (return_refs.size() > 1) {
    // TODO: we do not support multi return values now
    return -1;
  }
  auto return_id = ObjectID::FromBinary(return_refs[0].object_id());
  auto return_id_str = return_id.Binary();
  memcpy(return_id_buf, return_id_str.data(), return_id_str.size());
  *return_id_len = return_id_str.size();
  return 0;
}

int CoreWorker_SubmitTask(void *ray_function,
                          void *task_args_vec,
                          void *task_options,
                          /* int32_t max_retries,
                          bool retry_exceptions,
                          void *sched_strategy,
                          const uint8_t *debugger_breakpoint,
                          size_t debugger_breakpoint_len,
                          const uint8_t *serialized_retry_exception_allowlist,
                          size_t serialized_retry_exception_allowlist_len, */
                          uint8_t *return_id_buf,
                          size_t *return_id_len) {
  if (ray_function == nullptr || task_args_vec == nullptr || task_options == nullptr) {
    return -1;
  }
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();

  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();

  auto return_refs =
      core_worker.SubmitTask(*(RayFunction *)ray_function,
                             *(std::vector<std::unique_ptr<TaskArg>> *)task_args_vec,
                             *(TaskOptions *)task_options,
                             1,
                             false,
                             scheduling_strategy,
                             "");
  if (return_refs.size() > 1) {
    // TODO: we do not support multi return values now
    return -1;
  }
  auto return_id = ObjectID::FromBinary(return_refs[0].object_id());
  auto return_id_str = return_id.Binary();
  memcpy(return_id_buf, return_id_str.data(), return_id_str.size());
  *return_id_len = return_id_str.size();
  return 0;
}

int CoreWorker_GetActor(const char *actor_name,
                        size_t actor_name_len,
                        const char *ray_namespace,
                        size_t ray_namespace_len,
                        uint8_t *actor_id_buf,
                        size_t *actor_id_len) {
  if (actor_name == nullptr || actor_name_len == 0 || ray_namespace == nullptr ||
      ray_namespace_len == 0 || actor_id_buf == nullptr || actor_id_len == nullptr) {
    return -1;
  }
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  const std::string actor_name_str = std::string(actor_name, actor_name_len);
  const std::string ray_namespace_str = std::string(ray_namespace, ray_namespace_len);
  const std::string ns = ray_namespace_str.empty()
                             ? core_worker.GetJobConfig().ray_namespace()
                             : ray_namespace_str;
  auto pair = core_worker.GetNamedActorHandle(actor_name_str, ns);
  if (!pair.second.ok()) {
    return -1;
  }

  auto actor_handle = pair.first;
  auto actor_id = actor_handle->GetActorID();
  auto actor_id_str = actor_id.Binary();
  if (actor_id_str.size() > *actor_id_len) {
    return -1;
  }
  memcpy(actor_id_buf, actor_id_str.data(), actor_id_str.size());
  *actor_id_len = actor_id_str.size();
  return 0;
}

// ---------------------- TaskOptions ----------------------

void *TaskOptions_Create() { return new TaskOptions(); }

void TaskOptions_Destroy(void *task_options) {
  if (task_options == nullptr) {
    return;
  }
  delete (TaskOptions *)task_options;
}

int TaskOptions_SetNumReturns(void *task_options, int num_returns) {
  if (task_options == nullptr) {
    return -1;
  }
  ((TaskOptions *)task_options)->num_returns = num_returns;
  return 0;
}

int TaskOptions_SetName(void *task_options, const char *name, size_t name_len) {
  if (task_options == nullptr || name == nullptr || name_len == 0) {
    return -1;
  }
  ((TaskOptions *)task_options)->name = std::string(name, name_len);
  return 0;
}

int TaskOptions_SetConcurrencyGroupName(void *task_options,
                                        const char *concurrency_group_name,
                                        size_t concurrency_group_name_len) {
  if (task_options == nullptr || concurrency_group_name == nullptr ||
      concurrency_group_name_len == 0) {
    return -1;
  }
  ((TaskOptions *)task_options)->concurrency_group_name =
      std::string(concurrency_group_name, concurrency_group_name_len);
  return 0;
}

int TaskOptions_SetSerializedRuntimeEnvInfo(void *task_options,
                                            const uint8_t *serialized_runtime_env_info,
                                            size_t serialized_runtime_env_info_len) {
  if (task_options == nullptr || serialized_runtime_env_info == nullptr ||
      serialized_runtime_env_info_len == 0) {
    return -1;
  }
  ((TaskOptions *)task_options)->serialized_runtime_env_info = std::string(
      (const char *)serialized_runtime_env_info, serialized_runtime_env_info_len);
  return 0;
}

int TaskOptions_AddResource(void *task_options,
                            const char *resource_name,
                            size_t resource_name_len,
                            double resource_value) {
  if (task_options == nullptr || resource_name == nullptr || resource_name_len == 0) {
    return -1;
  }
  ((TaskOptions *)task_options)
      ->resources[std::string(resource_name, resource_name_len)] = resource_value;
  return 0;
}

// Ray function related functions

void *RayFunction_Create() { return new RayFunction(); }

void RayFunction_Destroy(void *ray_function) {
  if (ray_function == nullptr) {
    return;
  }
  delete (RayFunction *)ray_function;
}

int RayFunction_BuildCpp(void *ray_function,
                         const char *function_name,
                         size_t function_name_len,
                         const char *class_name,
                         size_t class_name_len) {
  if (ray_function == nullptr || function_name == nullptr || function_name_len == 0) {
    return -1;
  }
  auto desc =
      FunctionDescriptorBuilder::BuildCpp(std::string(function_name, function_name_len),
                                          "",
                                          std::string(class_name, class_name_len));
  ((RayFunction *)ray_function)->SetFunctionDescriptor(desc);
  ((RayFunction *)ray_function)->SetLanguage(Language::CPP);
  return 0;
}

int RayFunction_BuildPython(void *ray_function,
                            const char *function_name,
                            size_t function_name_len,
                            const char *class_name,
                            size_t class_name_len,
                            const char *module_name,
                            size_t module_name_len) {
  if (ray_function == nullptr || function_name == nullptr || function_name_len == 0) {
    return -1;
  }
  auto desc = FunctionDescriptorBuilder::BuildPython(
      std::string(module_name, module_name_len),
      std::string(class_name, class_name_len),
      std::string(function_name, function_name_len),
      "");
  ((RayFunction *)ray_function)->SetFunctionDescriptor(desc);
  ((RayFunction *)ray_function)->SetLanguage(Language::PYTHON);
  return 0;
}

int RayFunction_BuildJava(void *ray_function,
                          const char *function_name,
                          size_t function_name_len,
                          const char *class_name,
                          size_t class_name_len) {
  if (ray_function == nullptr || function_name == nullptr || function_name_len == 0) {
    return -1;
  }
  auto desc =
      FunctionDescriptorBuilder::BuildJava(std::string(class_name, class_name_len),
                                           std::string(function_name, function_name_len),
                                           "");
  ((RayFunction *)ray_function)->SetFunctionDescriptor(desc);
  ((RayFunction *)ray_function)->SetLanguage(Language::JAVA);
  return 0;
}

int RayFunction_BuildWasm(void *ray_function,
                          const char *function_name,
                          size_t function_name_len,
                          const char *module_name,
                          size_t module_name_len) {
  if (ray_function == nullptr || function_name == nullptr || function_name_len == 0) {
    return -1;
  }
  auto desc =
      FunctionDescriptorBuilder::BuildWasm(std::string(function_name, function_name_len),
                                           std::string(module_name, module_name_len));
  ((RayFunction *)ray_function)->SetFunctionDescriptor(desc);
  ((RayFunction *)ray_function)->SetLanguage(Language::WASM);
  return 0;
}

// task args related functions

void *TaskArg_Vec_Create() { return new std::vector<std::unique_ptr<TaskArg>>(); }

void TaskArg_Vec_Destroy(void *task_args) {
  if (task_args == nullptr) {
    return;
  }
  delete (std::vector<std::unique_ptr<TaskArg>> *)task_args;
}

// we do not support nested task args for now
int TaskArg_Vec_PushByValue(void *task_args,
                            uint8_t *data,
                            size_t data_len,
                            uint8_t *metadata,
                            size_t metadata_len) {
  if (task_args == nullptr || data == nullptr || data_len == 0) {
    return -1;
  }
  // convert data and meta to LocalMemoryBuffers
  auto data_buffer =
      std::make_shared<LocalMemoryBuffer>(data, data_len, true /* copy data */);
  auto metadata_buffer =
      metadata ? std::make_shared<LocalMemoryBuffer>(metadata, metadata_len) : nullptr;

  auto task_arg = std::make_unique<TaskArgByValue>(std::make_shared<RayObject>(
      data_buffer, metadata_buffer, std::vector<rpc::ObjectReference>()));

  ((std::vector<std::unique_ptr<TaskArg>> *)task_args)->emplace_back(std::move(task_arg));
  return 0;
}

}  // extern "C"
