#include "ray/core_worker/lib/java/org_ray_runtime_RayNativeRuntime.h"
#include <jni.h>
#include <sstream>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

thread_local JNIEnv *local_env = nullptr;
thread_local jobject local_java_task_executor = nullptr;

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

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeInitCoreWorker
 * Signature:
 * (ILjava/lang/String;Ljava/lang/String;[BLorg/ray/runtime/gcs/GcsClientOptions;)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeInitCoreWorker(
    JNIEnv *env, jclass, jint workerMode, jstring storeSocket, jstring rayletSocket,
    jbyteArray jobId, jobject gcsClientOptions) {
  auto native_store_socket = JavaStringToNativeString(env, storeSocket);
  auto native_raylet_socket = JavaStringToNativeString(env, rayletSocket);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, jobId);
  auto gcs_client_options = ToGcsClientOptions(env, gcsClientOptions);

  auto task_execution_callback =
      [](ray::TaskType task_type, const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids,
         std::vector<std::shared_ptr<ray::RayObject>> *results) {
        JNIEnv *env = local_env;
        RAY_CHECK(env);
        RAY_CHECK(local_java_task_executor);
        // convert RayFunction
        jobject ray_function_array_list =
            NativeStringVectorToJavaStringList(env, ray_function.GetFunctionDescriptor());
        // convert args
        // TODO (kfstorm): Avoid copying binary data from Java to C++
        jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
            env, args, NativeRayObjectToJavaNativeRayObject);

        // invoke Java method
        jobject java_return_objects =
            env->CallObjectMethod(local_java_task_executor, java_task_executor_execute,
                                  ray_function_array_list, args_array_list);
        std::vector<std::shared_ptr<ray::RayObject>> return_objects;
        JavaListToNativeVector<std::shared_ptr<ray::RayObject>>(
            env, java_return_objects, &return_objects,
            [](JNIEnv *env, jobject java_native_ray_object) {
              return JavaNativeRayObjectToNativeRayObject(env, java_native_ray_object);
            });
        for (auto &obj : return_objects) {
          results->push_back(obj);
        }
        return ray::Status::OK();
      };

  try {
    auto core_worker = new ray::CoreWorker(
        static_cast<ray::WorkerType>(workerMode), ::Language::JAVA, native_store_socket,
        native_raylet_socket, job_id, gcs_client_options, /*log_dir=*/"",
        /*node_ip_address=*/"", task_execution_callback);
    return reinterpret_cast<jlong>(core_worker);
  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Failed to construct core worker: " << e.what();
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(oss.str()), 0);
    return 0;  // To make compiler no complain
  }
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeRunTaskExecutor
 * Signature: (JLorg/ray/runtime/task/TaskExecutor;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jobject javaTaskExecutor) {
  local_env = env;
  local_java_task_executor = javaTaskExecutor;
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->StartExecutingTasks();
  local_env = nullptr;
  local_java_task_executor = nullptr;
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeDestroyCoreWorker
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeDestroyCoreWorker(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer) {
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->Disconnect();
  delete core_worker;
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeSetup
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeSetup(JNIEnv *env,
                                                                         jclass,
                                                                         jstring logDir) {
  std::string log_dir = JavaStringToNativeString(env, logDir);
  ray::RayLog::StartRayLog("java_worker", ray::RayLogLevel::INFO, log_dir);
  // TODO (kfstorm): If we add InstallFailureSignalHandler here, Java test may crash.
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeShutdownHook
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeShutdownHook(JNIEnv *,
                                                                                jclass) {
  ray::RayLog::ShutDownRayLog();
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jstring resourceName,
    jdouble capacity, jbyteArray nodeId) {
  const auto node_id = JavaByteArrayToId<ClientID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto &raylet_client =
      reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->GetRayletClient();
  auto status = raylet_client.SetResource(native_resource_name,
                                          static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
