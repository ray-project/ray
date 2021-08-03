#include "go_worker.h"

#include <stdint.h>

#include <iostream>

#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

using namespace std;

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
        size_t fd_hash = function_descriptor->Hash();
        auto &fd_vector = executor_function_descriptor_cache[fd_hash];
        jobject ray_function_array_list = nullptr;
        for (auto &pair : fd_vector) {
          if (pair.first == function_descriptor) {
            ray_function_array_list = pair.second;
            break;
          }
        }
        if (!ray_function_array_list) {
          ray_function_array_list =
              NativeRayFunctionDescriptorToJavaStringList(env, function_descriptor);
          fd_vector.emplace_back(function_descriptor, ray_function_array_list);
        }

        // convert args
        // TODO (kfstorm): Avoid copying binary data from Java to C++
        jbooleanArray java_check_results =
            static_cast<jbooleanArray>(env->CallObjectMethod(
                java_task_executor, java_task_executor_parse_function_arguments,
                ray_function_array_list));
        RAY_CHECK_JAVA_EXCEPTION(env);
        jobject args_array_list = ToJavaArgs(env, java_check_results, args);

        // invoke Java method
        jobject java_return_objects =
            env->CallObjectMethod(java_task_executor, java_task_executor_execute,
                                  ray_function_array_list, args_array_list);
        // Check whether the exception is `IntentionalSystemExit`.
        jthrowable throwable = env->ExceptionOccurred();
        if (throwable) {
          ray::Status status_to_return = ray::Status::OK();
          if (env->IsInstanceOf(throwable,
                                java_ray_intentional_system_exit_exception_class)) {
            status_to_return = ray::Status::IntentionalSystemExit();
          } else if (env->IsInstanceOf(throwable, java_ray_actor_exception_class)) {
            creation_task_exception_pb = SerializeActorCreationException(env, throwable);
            status_to_return = ray::Status::CreationTaskError();
          } else {
            RAY_LOG(ERROR) << "Unkown java exception was thrown while executing tasks.";
          }
          env->ExceptionClear();
          return status_to_return;
        }
        RAY_CHECK_JAVA_EXCEPTION(env);

        // Process return objects.
        if (!return_ids.empty()) {
          std::vector<std::shared_ptr<ray::RayObject>> return_objects;
          JavaListToNativeVector<std::shared_ptr<ray::RayObject>>(
              env, java_return_objects, &return_objects,
              [](JNIEnv *env, jobject java_native_ray_object) {
                return JavaNativeRayObjectToNativeRayObject(env, java_native_ray_object);
              });
          results->resize(return_ids.size(), nullptr);
          for (size_t i = 0; i < return_objects.size(); i++) {
            auto &result_id = return_ids[i];
            size_t data_size =
                return_objects[i]->HasData() ? return_objects[i]->GetData()->Size() : 0;
            auto &metadata = return_objects[i]->GetMetadata();
            auto &contained_object_id = return_objects[i]->GetNestedIds();
            auto result_ptr = &(*results)[0];

            RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
                result_id, data_size, metadata, contained_object_id, result_ptr));

            // A nullptr is returned if the object already exists.
            auto result = *result_ptr;
            if (result != nullptr) {
              if (result->HasData()) {
                memcpy(result->GetData()->Data(), return_objects[i]->GetData()->Data(),
                       data_size);
              }
            }

            RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().SealReturnObject(
                result_id, result));
          }
        }

        env->DeleteLocalRef(java_check_results);
        env->DeleteLocalRef(java_return_objects);
        env->DeleteLocalRef(args_array_list);
        return ray::Status::OK();
      };

  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::GOLANG;
  options.store_socket = store_socket;
  options.raylet_socket = raylet_socket;
  options.job_id = ray::JobID::FromInt(jobId);
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
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  std::string node_to_connect;
  auto status =
      gcs_accessor->GetNodeToConnectForDriver(node_ip_address, &node_to_connect);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to get node to connect for driver:" << status.message();
    return 0;
  }
  int result_length = strlen(node_to_connect.c_str());
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
  memcpy(*result, actor_id.Data(), result_length);
  return result_length;
}

__attribute__((visibility("default"))) int go_worker_SubmitActorTask(void *actor_id,
                                                                     char *method_name,
                                                                     char ***return_ids) {
  std::string *sp = static_cast<std::string *>(actor_id);
  auto actor_id_obj = ActorID::FromBinary(*sp);
  std::vector<std::string> function_descriptor_list = {method_name};
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::rpc::GOLANG,
                                                 function_descriptor_list);

  ray::RayFunction ray_function = ray::RayFunction(ray::rpc::GOLANG, function_descriptor);
  std::vector<ObjectID> return_obj_ids;
  ray::CoreWorkerProcess::GetCoreWorker().SubmitActorTask(actor_id_obj, ray_function, {},
                                                          {}, &return_obj_ids);
  return 0;
}
