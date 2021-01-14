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

#include "io_ray_runtime_RayNativeRuntime.h"

#include <jni.h>

#include <sstream>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/core_worker.h"

thread_local JNIEnv *local_env = nullptr;
jobject java_task_executor = nullptr;

/// Store Java instances of function descriptor in the cache to avoid unnessesary JNI
/// operations.
thread_local std::unordered_map<size_t,
                                std::vector<std::pair<ray::FunctionDescriptor, jobject>>>
    executor_function_descriptor_cache;

inline ray::gcs::GcsClientOptions ToGcsClientOptions(JNIEnv *env,
                                                     jobject gcs_client_options) {
  std::string ip = JavaStringToNativeString(
      env, (jstring)env->GetObjectField(gcs_client_options, java_gcs_client_options_ip));
  int port = env->GetIntField(gcs_client_options, java_gcs_client_options_port);
  std::string password = JavaStringToNativeString(
      env,
      (jstring)env->GetObjectField(gcs_client_options, java_gcs_client_options_password));
  return ray::gcs::GcsClientOptions(ip, port, password, /*is_test_client=*/false);
}

jobject ToJavaArgs(JNIEnv *env, jbooleanArray java_check_results,
                   const std::vector<std::shared_ptr<ray::RayObject>> &args) {
  if (java_check_results == nullptr) {
    // If `java_check_results` is null, it means that `checkByteBufferArguments`
    // failed. In this case, just return null here. The args won't be used anyway.
    return nullptr;
  } else {
    jboolean *check_results = env->GetBooleanArrayElements(java_check_results, nullptr);
    size_t i = 0;
    jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
        env, args,
        [check_results, &i](JNIEnv *env,
                            const std::shared_ptr<ray::RayObject> &native_object) {
          if (*(check_results + (i++))) {
            // If the type of this argument is ByteBuffer, we create a
            // DirectByteBuffer here To avoid data copy.
            // TODO: Check native_object->GetMetadata() == "RAW"
            jobject obj = env->NewDirectByteBuffer(native_object->GetData()->Data(),
                                                   native_object->GetData()->Size());
            RAY_CHECK(obj);
            return obj;
          }
          return NativeRayObjectToJavaNativeRayObject(env, native_object);
        });
    env->ReleaseBooleanArrayElements(java_check_results, check_results, JNI_ABORT);
    return args_array_list;
  }
}

JNIEnv *GetJNIEnv() {
  JNIEnv *env = local_env;
  if (!env) {
    // Attach the native thread to JVM.
    auto status =
        jvm->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env), nullptr);
    RAY_CHECK(status == JNI_OK) << "Failed to get JNIEnv. Return code: " << status;
    local_env = env;
  }
  RAY_CHECK(env);
  return env;
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeInitialize(
    JNIEnv *env, jclass, jint workerMode, jstring nodeIpAddress, jint nodeManagerPort,
    jstring driverName, jstring storeSocket, jstring rayletSocket, jbyteArray jobId,
    jobject gcsClientOptions, jint numWorkersPerProcess, jstring logDir,
    jobject rayletConfigParameters, jbyteArray jobConfig) {
  auto raylet_config = JavaMapToNativeMap<std::string, std::string>(
      env, rayletConfigParameters,
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        return JavaStringToNativeString(env, (jstring)java_value);
      });
  RayConfig::instance().initialize(raylet_config);

  auto task_execution_callback =
      [](ray::TaskType task_type, const std::string task_name,
         const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
         std::vector<std::shared_ptr<ray::RayObject>> *results) {
        JNIEnv *env = GetJNIEnv();
        RAY_CHECK(java_task_executor);

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
        if (throwable &&
            env->IsInstanceOf(throwable,
                              java_ray_intentional_system_exit_exception_class)) {
          env->ExceptionClear();
          return ray::Status::IntentionalSystemExit();
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
          std::vector<size_t> data_sizes;
          std::vector<std::shared_ptr<ray::Buffer>> metadatas;
          std::vector<std::vector<ray::ObjectID>> contained_object_ids;
          for (size_t i = 0; i < return_objects.size(); i++) {
            data_sizes.push_back(
                return_objects[i]->HasData() ? return_objects[i]->GetData()->Size() : 0);
            metadatas.push_back(return_objects[i]->GetMetadata());
            contained_object_ids.push_back(return_objects[i]->GetNestedIds());
          }
          RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObjects(
              return_ids, data_sizes, metadatas, contained_object_ids, results));
          for (size_t i = 0; i < data_sizes.size(); i++) {
            auto result = (*results)[i];
            // A nullptr is returned if the object already exists.
            if (result != nullptr) {
              if (result->HasData()) {
                memcpy(result->GetData()->Data(), return_objects[i]->GetData()->Data(),
                       data_sizes[i]);
              }
            }
          }
        }

        env->DeleteLocalRef(java_check_results);
        env->DeleteLocalRef(java_return_objects);
        env->DeleteLocalRef(args_array_list);
        return ray::Status::OK();
      };

  auto gc_collect = []() {
    // A Java worker process usually contains more than one worker.
    // A LocalGC request is likely to be received by multiple workers in a short time.
    // Here we ensure that the 1 second interval of `System.gc()` execution is
    // guaranteed no matter how frequent the requests are received and how many workers
    // the process has.
    static absl::Mutex mutex;
    static int64_t last_gc_time_ms = 0;
    absl::MutexLock lock(&mutex);
    int64_t start = current_time_ms();
    if (last_gc_time_ms + 1000 < start) {
      JNIEnv *env = GetJNIEnv();
      RAY_LOG(INFO) << "Calling System.gc() ...";
      env->CallStaticObjectMethod(java_system_class, java_system_gc);
      last_gc_time_ms = current_time_ms();
      RAY_LOG(INFO) << "GC finished in " << (double)(last_gc_time_ms - start) / 1000
                    << " seconds.";
    }
  };

  auto on_worker_shutdown = [](const ray::WorkerID &worker_id) {
    JNIEnv *env = GetJNIEnv();
    auto worker_id_bytes = IdToJavaByteArray<ray::WorkerID>(env, worker_id);
    if (java_task_executor) {
      env->CallVoidMethod(java_task_executor,
                          java_native_task_executor_on_worker_shutdown, worker_id_bytes);
      RAY_CHECK_JAVA_EXCEPTION(env);
    }
  };

  std::string serialized_job_config =
      (jobConfig == nullptr ? "" : JavaByteArrayToNativeString(env, jobConfig));
  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::JAVA;
  options.store_socket = JavaStringToNativeString(env, storeSocket);
  options.raylet_socket = JavaStringToNativeString(env, rayletSocket);
  options.job_id = JavaByteArrayToId<ray::JobID>(env, jobId);
  options.gcs_options = ToGcsClientOptions(env, gcsClientOptions);
  options.enable_logging = true;
  options.log_dir = JavaStringToNativeString(env, logDir);
  // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
  options.install_failure_signal_handler = false;
  options.node_ip_address = JavaStringToNativeString(env, nodeIpAddress);
  options.node_manager_port = static_cast<int>(nodeManagerPort);
  options.raylet_ip_address = JavaStringToNativeString(env, nodeIpAddress);
  options.driver_name = JavaStringToNativeString(env, driverName);
  options.task_execution_callback = task_execution_callback;
  options.on_worker_shutdown = on_worker_shutdown;
  options.gc_collect = gc_collect;
  options.ref_counting_enabled = true;
  options.num_workers = static_cast<int>(numWorkersPerProcess);
  options.serialized_job_config = serialized_job_config;
  options.metrics_agent_port = -1;

  ray::CoreWorkerProcess::Initialize(options);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(
    JNIEnv *env, jclass o, jobject javaTaskExecutor) {
  java_task_executor = javaTaskExecutor;
  ray::CoreWorkerProcess::RunTaskExecutionLoop();
  java_task_executor = nullptr;
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeShutdown(JNIEnv *env,
                                                                           jclass o) {
  ray::CoreWorkerProcess::Shutdown();
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *env, jclass, jstring resourceName, jdouble capacity, jbyteArray nodeId) {
  const auto node_id = JavaByteArrayToId<NodeID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = ray::CoreWorkerProcess::GetCoreWorker().SetResource(
      native_resource_name, static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeGetActorIdOfNamedActor(JNIEnv *env, jclass,
                                                                  jstring actor_name,
                                                                  jboolean global) {
  const char *native_actor_name = env->GetStringUTFChars(actor_name, JNI_FALSE);
  auto full_name = GetActorFullName(global, native_actor_name);

  const auto actor_handle =
      ray::CoreWorkerProcess::GetCoreWorker().GetNamedActorHandle(full_name).first;
  ray::ActorID actor_id;
  if (actor_handle) {
    actor_id = actor_handle->GetActorID();
  } else {
    actor_id = ray::ActorID::Nil();
  }
  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeKillActor(
    JNIEnv *env, jclass, jbyteArray actorId, jboolean noRestart) {
  auto status = ray::CoreWorkerProcess::GetCoreWorker().KillActor(
      JavaByteArrayToId<ActorID>(env, actorId),
      /*force_kill=*/true, noRestart);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetCoreWorker(
    JNIEnv *env, jclass, jbyteArray workerId) {
  const auto worker_id = JavaByteArrayToId<ray::WorkerID>(env, workerId);
  ray::CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id);
}

#ifdef __cplusplus
}
#endif
