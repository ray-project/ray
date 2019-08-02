#include "ray/core_worker/lib/java/org_ray_runtime_WorkerImpl.h"
#include <jni.h>
#include <sstream>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

thread_local JNIEnv *local_env = nullptr;
thread_local jobject local_java_worker = nullptr;

inline ray::gcs::GcsClientOptions ToGcsClientOptions(JNIEnv *env,
                                                     jobject gcs_client_options) {
  std::string ip = JavaStringToNativeString(
      env, (jstring)env->GetObjectField(gcs_client_options,
                                        java_native_gcs_client_options_ip));
  int port = env->GetIntField(gcs_client_options, java_native_gcs_client_options_port);
  std::string password = JavaStringToNativeString(
      env, (jstring)env->GetObjectField(gcs_client_options,
                                        java_native_gcs_client_options_password));
  bool is_test_client = env->GetBooleanField(
      gcs_client_options, java_native_gcs_client_options_is_test_client);
  return ray::gcs::GcsClientOptions(ip, port, password, is_test_client);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerImpl
 * Method:    nativeInit
 * Signature:
 * (ILjava/lang/String;Ljava/lang/String;[BLorg/ray/runtime/nativeTypes/NativeGcsClientOptions;)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_WorkerImpl_nativeInit(
    JNIEnv *env, jclass, jint workerMode, jstring storeSocket, jstring rayletSocket,
    jbyteArray jobId, jobject gcsClientOptions) {
  auto native_store_socket = JavaStringToNativeString(env, storeSocket);
  auto native_raylet_socket = JavaStringToNativeString(env, rayletSocket);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, jobId);
  auto native_gcs_client_options = ToGcsClientOptions(env, gcsClientOptions);

  auto executor_func = [](const ray::RayFunction &ray_function,
                          const std::vector<std::shared_ptr<ray::RayObject>> &args,
                          int num_returns,
                          std::vector<std::shared_ptr<ray::RayObject>> *results) {
    JNIEnv *env = local_env;
    RAY_CHECK(env);
    RAY_CHECK(local_java_worker);
    // convert RayFunction
    jobject ray_function_array_list =
        NativeStringVectorToJavaStringList(env, ray_function.function_descriptor);
    // convert args
    jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
        env, args, NativeRayObjectToJavaNativeRayObject);

    // invoke Java method
    jobject java_return_objects =
        env->CallObjectMethod(local_java_worker, java_worker_impl_execute,
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
        native_raylet_socket, job_id, native_gcs_client_options, executor_func);
    return reinterpret_cast<jlong>(core_worker);
  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Failed to construct core worker: " << e.what();
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(oss.str()), 0);
    return 0;  // To make compiler no complain
  }
}

/*
 * Class:     org_ray_runtime_WorkerImpl
 * Method:    nativeRunCoreWorker
 * Signature: (JLorg/ray/runtime/CoreWorkerProxy;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerImpl_nativeRunCoreWorker(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jobject javaCoreWorker) {
  local_env = env;
  local_java_worker = javaCoreWorker;
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->Execution().Run();
  local_env = nullptr;
  local_java_worker = nullptr;
}

/*
 * Class:     org_ray_runtime_WorkerImpl
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerImpl_nativeDestroy(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer) {
  delete reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
}

#ifdef __cplusplus
}
#endif
