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

#include "ray/common/id.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/core_worker.h"
#include "jni_utils.h"

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

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeInitialize(
    JNIEnv *env, jclass, jint workerMode, jstring nodeIpAddress, jint nodeManagerPort,
    jstring driverName, jstring storeSocket, jstring rayletSocket, jbyteArray jobId,
    jobject gcsClientOptions, jint numWorkersPerProcess, jstring logDir,
    jobject rayletConfigParameters) {
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
      [](ray::TaskType task_type, const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids,
         std::vector<std::shared_ptr<ray::RayObject>> *results) {
        JNIEnv *env = local_env;
        if (!env) {
          // Attach the native thread to JVM.
          auto status =
              jvm->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env), nullptr);
          RAY_CHECK(status == JNI_OK) << "Failed to get JNIEnv. Return code: " << status;
          local_env = env;
        }

        RAY_CHECK(env);
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
        RAY_CHECK_JAVA_EXCEPTION(env);
        if (!return_ids.empty()) {
          std::vector<std::shared_ptr<ray::RayObject>> return_objects;
          JavaListToNativeVector<std::shared_ptr<ray::RayObject>>(
              env, java_return_objects, &return_objects,
              [](JNIEnv *env, jobject java_native_ray_object) {
                return JavaNativeRayObjectToNativeRayObject(env, java_native_ray_object);
              });
          for (auto &obj : return_objects) {
            results->push_back(obj);
          }
        }

        env->DeleteLocalRef(java_check_results);
        env->DeleteLocalRef(java_return_objects);
        env->DeleteLocalRef(args_array_list);
        return ray::Status::OK();
      };

  ray::CoreWorkerOptions options = {
      static_cast<ray::WorkerType>(workerMode),     // worker_type
      ray::Language::JAVA,                          // langauge
      JavaStringToNativeString(env, storeSocket),   // store_socket
      JavaStringToNativeString(env, rayletSocket),  // raylet_socket
      JavaByteArrayToId<ray::JobID>(env, jobId),    // job_id
      ToGcsClientOptions(env, gcsClientOptions),    // gcs_options
      true,                                         // enable_logging
      JavaStringToNativeString(env, logDir),        // log_dir
      // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
      false,                                         // install_failure_signal_handler
      JavaStringToNativeString(env, nodeIpAddress),  // node_ip_address
      static_cast<int>(nodeManagerPort),             // node_manager_port
      JavaStringToNativeString(env, nodeIpAddress),  // raylet_ip_address
      JavaStringToNativeString(env, driverName),     // driver_name
      "",                                            // stdout_file
      "",                                            // stderr_file
      task_execution_callback,                       // task_execution_callback
      nullptr,                                       // check_signals
      nullptr,                                       // gc_collect
      nullptr,                                       // get_lang_stack
      nullptr,                                       // kill_main
      false,                                         // ref_counting_enabled
      false,                                         // is_local_mode
      static_cast<int>(numWorkersPerProcess),        // num_workers
  };

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
  const auto node_id = JavaByteArrayToId<ClientID>(env, nodeId);
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

  const auto *actor_handle =
      ray::CoreWorkerProcess::GetCoreWorker().GetNamedActorHandle(full_name).first;
  ray::ActorID actor_id;
  if (actor_handle) {
    actor_id = actor_handle->GetActorID();
  } else {
    actor_id = ray::ActorID::Nil();
  }
  jbyteArray bytes = env->NewByteArray(actor_id.Size());
  env->SetByteArrayRegion(bytes, 0, actor_id.Size(),
                          reinterpret_cast<const jbyte *>(actor_id.Data()));
  return bytes;
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
