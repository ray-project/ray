#include "go_worker.h"

#include <stdint.h>

#include <iostream>

#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

using namespace std;

template <typename ID>
inline ID ByteArrayToId(char *bytes) {
  std::string id_str(ID::Size(), 0);
  memcpy(&id_str.front(), bytes, ID::Size());
  return ID::FromBinary(id_str);
}

__attribute__((visibility("default"))) void go_worker_Initialize(
    int workerMode, char *store_socket, char *raylet_socket, char *log_dir,
    char *node_ip_address, int node_manager_port, char *raylet_ip_address,
    char *driver_name, int jobId, char *redis_address, int redis_port,
    char *redis_password, char *serialized_job_config) {
  auto task_execution_callback =
      [](ray::TaskType task_type, const std::string task_name,
         const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
         std::vector<std::shared_ptr<ray::RayObject>> *results,
         std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb) {
        // convert RayFunction
        auto function_descriptor = ray_function.GetFunctionDescriptor();
        auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();

        char *fd_data[1];
        fd_data[0] = const_cast<char *>(typed_descriptor->FunctionName().c_str());
        GoSlice fd_list;
        fd_list.data = fd_data;
        fd_list.len = 1;
        fd_list.cap = 1;

        std::vector<DataBuffer> args_array_list;
        for (auto &it : args) {
          DataBuffer db;
          db.p = it->GetData()->Data();
          db.size = it->GetSize();
          args_array_list.push_back(db);
        }

        GoSlice args_go;
        args_go.data = &args_array_list[0];
        args_go.len = args_array_list.size();
        args_go.cap = args_array_list.size();

        std::vector<std::shared_ptr<ReturnValue>> return_value_list;
        for (size_t i = 0; i < return_ids.size(); i++) {
          return_value_list.push_back(make_shared<ReturnValue>());
        }
        GoSlice return_value_list_go;
        return_value_list_go.data = &return_value_list[0];
        return_value_list_go.cap = return_ids.size();
        return_value_list_go.len = return_ids.size();

        // invoke golang method
        go_worker_execute(task_type, fd_list, args_go, return_value_list_go);
        results->clear();
        for (size_t i = 0; i < return_value_list.size(); i++) {
          auto &result_id = return_ids[i];
          auto &return_value = return_value_list[i];
          std::shared_ptr<ray::Buffer> data_buffer =
              std::make_shared<ray::LocalMemoryBuffer>(
                  reinterpret_cast<uint8_t *>(return_value->data->p),
                  return_value->data->size, false);
          std::shared_ptr<ray::Buffer> meta_buffer =
              std::make_shared<ray::LocalMemoryBuffer>(
                  reinterpret_cast<uint8_t *>(return_value->meta->p),
                  return_value->meta->size, false);
          std::vector<ray::ObjectID> contained_object_ids;
          auto value = std::make_shared<ray::RayObject>(data_buffer, meta_buffer,
                                                        contained_object_ids);
          results->emplace_back(value);
          RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
              result_id, value->GetData()->Size(), value->GetMetadata(),
              contained_object_ids, &value));
          RAY_CHECK_OK(
              ray::CoreWorkerProcess::GetCoreWorker().SealReturnObject(result_id, value));
        }
        return ray::Status::OK();
      };

  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::GOLANG;
  options.store_socket = store_socket;
  options.raylet_socket = raylet_socket;
  if (jobId == 0) {
    options.job_id = ray::JobID::Nil();
  } else {
    options.job_id = ray::JobID::FromInt(jobId);
  }
  options.gcs_options =
      ray::gcs::GcsClientOptions(redis_address, redis_port, redis_password);
  options.enable_logging = true;
  options.log_dir = log_dir;
  // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
  options.install_failure_signal_handler = false;
  options.node_ip_address = node_ip_address;
  options.node_manager_port = node_manager_port;
  options.raylet_ip_address = raylet_ip_address;
  options.driver_name = driver_name;
  options.task_execution_callback = task_execution_callback;
  //  options.on_worker_shutdown = on_worker_shutdown;
  //  options.gc_collect = gc_collect;
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.serialized_job_config = serialized_job_config;
  options.metrics_agent_port = -1;
  ray::CoreWorkerProcess::Initialize(options);

  SayHello((char *)"have_fun friends!");
}

__attribute__((visibility("default"))) void go_worker_Run() {
  ray::CoreWorkerProcess::RunTaskExecutionLoop();
  _Exit(0);
}

__attribute__((visibility("default"))) void *go_worker_CreateGlobalStateAccessor(
    char *redis_address, char *redis_password) {
  ray::gcs::GlobalStateAccessor *gcs_accessor =
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password);
  return gcs_accessor;
}

__attribute__((visibility("default"))) bool go_worker_GlobalStateAccessorConnet(void *p) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  return gcs_accessor->Connect();
}

__attribute__((visibility("default"))) int go_worker_GetNextJobID(void *p) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  auto job_id = gcs_accessor->GetNextJobID();
  return job_id.ToInt();
}

__attribute__((visibility("default"))) char *go_worker_GlobalStateAccessorGetInternalKV(
    void *p, char *key) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  auto value = gcs_accessor->GetInternalKV(key);
  if (value != nullptr) {
    std::string *v = value.release();
    int len = strlen(v->c_str());
    char *result = (char *)malloc(len + 1);
    std::memcpy(result, v->c_str(), len);
    return result;
  }
  return nullptr;
}

__attribute__((visibility("default"))) int go_worker_GetNodeToConnectForDriver(
    void *p, char *node_ip_address, char **result) {
  RAY_LOG(WARNING) << "Get nodeinfo:" << node_ip_address;
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  std::string node_to_connect;
  auto status =
      gcs_accessor->GetNodeToConnectForDriver(node_ip_address, &node_to_connect);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to get node to connect for driver:" << status.message();
    return 0;
  }
  RAY_LOG(WARNING) << "got nodeinfo:" << node_to_connect;
  int result_length = node_to_connect.length();
  RAY_LOG(WARNING) << "got nodeinfo len:" << result_length;
  *result = (char *)malloc(result_length + 1);
  memcpy(*result, node_to_connect.c_str(), result_length);
  return result_length;
}

__attribute__((visibility("default"))) int go_worker_CreateActor(char *type_name,
                                                                 char **result) {
  std::vector<std::string> function_descriptor_list = {type_name};
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::rpc::GOLANG,
                                                 function_descriptor_list);
  ActorID actor_id;
  ray::RayFunction ray_function = ray::RayFunction(ray::rpc::GOLANG, function_descriptor);
  // TODO
  std::string full_name = "";
  std::string ray_namespace = "";
  ray::ActorCreationOptions actor_creation_options{
      0,
      0,  // TODO: Allow setting max_task_retries from Java.
      static_cast<int>(1),
      {},
      {},
      {},
      /*is_detached=*/false,
      full_name,
      ray_namespace,
      /*is_asyncio=*/false};
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreateActor(
      ray_function, {}, actor_creation_options,
      /*extension_data*/ "", &actor_id);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to create actor:" << status.message()
                   << " for:" << type_name;
    return 0;
  }
  int result_length = actor_id.Size();
  *result = (char *)malloc(result_length + 1);
  memcpy(*result, (char *)actor_id.Data(), result_length);
  return result_length;
}

__attribute__((visibility("default"))) int go_worker_SubmitActorTask(void *actor_id,
                                                                     char *method_name,
                                                                     int num_returns,
                                                                     void **object_ids) {
  auto actor_id_obj = ByteArrayToId<ray::ActorID>((char *)actor_id);
  std::vector<std::string> function_descriptor_list = {method_name};
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::rpc::GOLANG,
                                                 function_descriptor_list);
  ray::RayFunction ray_function = ray::RayFunction(ray::rpc::GOLANG, function_descriptor);
  std::string name = "";
  std::unordered_map<std::string, double> resources;
  ray::TaskOptions task_options{name, num_returns, resources};

  std::vector<ObjectID> obj_ids;
  ray::CoreWorkerProcess::GetCoreWorker().SubmitActorTask(actor_id_obj, ray_function, {},
                                                          task_options, &obj_ids);

  int object_id_size = ObjectID::Size();
  for (size_t i = 0; i < obj_ids.size(); i++) {
    void *result = (char *)malloc(object_id_size);
    memcpy(result, (char *)obj_ids[i].Data(), object_id_size);
    RAY_LOG(WARNING) << "return object id:" << result;
    object_ids[i] = result;
  }
  return 0;
}

DataBuffer *RayObjectToDataBuffer(std::shared_ptr<ray::Buffer> buffer) {
  DataBuffer *data_db = new DataBuffer();
  data_db->p = buffer->Data();
  data_db->size = buffer->Size();
  return data_db;
}

__attribute__((visibility("default"))) GoSlice go_worker_Get(void **object_ids,
                                                             int object_ids_size,
                                                             int timeout) {
  std::vector<ray::ObjectID> object_ids_data;
  char **object_id_arr = (char **)object_ids;
  for (int i = 0; i < object_ids_size; i++) {
    RAY_LOG(WARNING) << "try to get objectid:" << static_cast<void *>(object_id_arr[i]);
    auto object_id_obj = ByteArrayToId<ray::ObjectID>(object_id_arr[i]);
    RAY_LOG(WARNING) << "try to get object:" << object_id_obj;
    object_ids_data.emplace_back(object_id_obj);
  }
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status =
      ray::CoreWorkerProcess::GetCoreWorker().Get(object_ids_data, timeout, &results);
  GoSlice return_value_list_go;
  // todo throw error, not exit now
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to get object";
    return return_value_list_go;
  }
  std::vector<std::shared_ptr<ReturnValue>> return_value_list;
  for (size_t i = 0; i < results.size(); i++) {
    std::shared_ptr<ReturnValue> rv = make_shared<ReturnValue>();
    rv->data = RayObjectToDataBuffer(results[i]->GetData());
    rv->meta = RayObjectToDataBuffer(results[i]->GetMetadata());
    return_value_list.emplace_back(rv);
  }
  return_value_list_go.data = &return_value_list[0];
  return_value_list_go.cap = results.size();
  return_value_list_go.len = results.size();
  return return_value_list_go;
}
