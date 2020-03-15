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

#include "ray/core_worker/lib/java/org_ray_runtime_RayNativeRuntime.h"
#include <jni.h>
#include <sstream>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

thread_local JNIEnv *local_env = nullptr;

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

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeInitCoreWorker(
    JNIEnv *env, jclass, jint workerMode, jstring storeSocket, jstring rayletSocket,
    jstring nodeIpAddress, jint nodeManagerPort, jbyteArray jobId,
    jobject gcsClientOptions) {
  auto native_store_socket = JavaStringToNativeString(env, storeSocket);
  auto native_raylet_socket = JavaStringToNativeString(env, rayletSocket);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, jobId);
  auto gcs_client_options = ToGcsClientOptions(env, gcsClientOptions);
  auto node_ip_address = JavaStringToNativeString(env, nodeIpAddress);

  auto task_execution_callback =
      [](ray::TaskType task_type, const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids,
         std::vector<std::shared_ptr<ray::RayObject>> *results,
         const ray::WorkerID &worker_id) {
        JNIEnv *env = local_env;
        if (!env) {
          // Attach the native thread to JVM.
          auto status =
              jvm->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env), nullptr);
          RAY_CHECK(status == JNI_OK) << "Failed to get JNIEnv. Return code: " << status;
          local_env = env;
        }

        RAY_CHECK(env);

        auto worker_id_bytes = IdToJavaByteArray<ray::WorkerID>(env, worker_id);
        jobject local_java_task_executor = env->CallStaticObjectMethod(
            java_task_executor_class, java_task_executor_get, worker_id_bytes);

        RAY_CHECK(local_java_task_executor);
        // convert RayFunction
        jobject ray_function_array_list = NativeRayFunctionDescriptorToJavaStringList(
            env, ray_function.GetFunctionDescriptor());
        // convert args
        // TODO (kfstorm): Avoid copying binary data from Java to C++
        jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
            env, args, NativeRayObjectToJavaNativeRayObject);

        // invoke Java method
        jobject java_return_objects =
            env->CallObjectMethod(local_java_task_executor, java_task_executor_execute,
                                  ray_function_array_list, args_array_list);
        RAY_CHECK_JAVA_EXCEPTION(env);
        std::vector<std::shared_ptr<ray::RayObject>> return_objects;
        JavaListToNativeVector<std::shared_ptr<ray::RayObject>>(
            env, java_return_objects, &return_objects,
            [](JNIEnv *env, jobject java_native_ray_object) {
              return JavaNativeRayObjectToNativeRayObject(env, java_native_ray_object);
            });
        for (auto &obj : return_objects) {
          results->push_back(obj);
        }

        env->DeleteLocalRef(java_return_objects);
        env->DeleteLocalRef(args_array_list);
        env->DeleteLocalRef(ray_function_array_list);
        return ray::Status::OK();
      };

  try {
    auto core_worker = new ray::CoreWorker(
        static_cast<ray::WorkerType>(workerMode), ::Language::JAVA, native_store_socket,
        native_raylet_socket, job_id, gcs_client_options, /*log_dir=*/"", node_ip_address,
        nodeManagerPort, task_execution_callback);
    return reinterpret_cast<jlong>(core_worker);
  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Failed to construct core worker: " << e.what();
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(oss.str()), 0);
    return 0;  // To make compiler no complain
  }
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer) {
  local_env = env;
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->StartExecutingTasks();
  local_env = nullptr;
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeDestroyCoreWorker(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer) {
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->Disconnect();
  delete core_worker;
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeSetup(
    JNIEnv *env, jclass, jstring logDir, jobject rayletConfigParameters) {
  std::string log_dir = JavaStringToNativeString(env, logDir);
  ray::RayLog::StartRayLog("java_worker", ray::RayLogLevel::INFO, log_dir);
  // TODO (kfstorm): We can't InstallFailureSignalHandler here, because JVM already
  // installed its own signal handler. It's possible to fix this by chaining signal
  // handlers. But it's not easy. See
  // https://docs.oracle.com/javase/9/troubleshoot/handle-signals-and-exceptions.htm.
  auto raylet_config = JavaMapToNativeMap<std::string, std::string>(
      env, rayletConfigParameters,
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        return JavaStringToNativeString(env, (jstring)java_value);
      });
  RayConfig::instance().initialize(raylet_config);
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeShutdownHook(JNIEnv *,
                                                                                jclass) {
  ray::RayLog::ShutDownRayLog();
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jstring resourceName,
    jdouble capacity, jbyteArray nodeId) {
  const auto node_id = JavaByteArrayToId<ClientID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status =
      reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
          ->SetResource(native_resource_name, static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeKillActor(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray actorId,
    jboolean noReconstruction) {
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  auto status = core_worker->KillActor(JavaByteArrayToId<ActorID>(env, actorId),
                                       /*force_kill=*/true, noReconstruction);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
