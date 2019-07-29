#include "ray/core_worker/lib/java/org_ray_runtime_Worker.h"
#include <jni.h>
#include <sstream>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

thread_local JNIEnv *local_env = nullptr;
thread_local jobject local_java_worker = nullptr;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_Worker
 * Method:    nativeInit
 * Signature: (ILjava/lang/String;Ljava/lang/String;[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_Worker_nativeInit(JNIEnv *env, jclass,
                                                               jint workerMode,
                                                               jstring storeSocket,
                                                               jstring rayletSocket,
                                                               jbyteArray jobId) {
  auto native_store_socket = JavaStringToNativeString(env, storeSocket);
  auto native_raylet_socket = JavaStringToNativeString(env, rayletSocket);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, jobId);

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
    jobject return_value =
        env->CallObjectMethod(local_java_worker, java_worker_run_task_callback,
                              ray_function_array_list, args_array_list);
    results->push_back(JavaNativeRayObjectToNativeRayObject(env, return_value));
    return ray::Status::OK();
  };

  try {
    auto core_worker = new ray::CoreWorker(static_cast<ray::WorkerType>(workerMode),
                                           ::Language::JAVA, native_store_socket,
                                           native_raylet_socket, job_id, executor_func);
    return reinterpret_cast<jlong>(core_worker);
  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Failed to construct core worker: " << e.what();
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(oss.str()), 0);
    return 0;  // To make compiler no complain
  }
}

/*
 * Class:     org_ray_runtime_Worker
 * Method:    nativeRunCoreWorker
 * Signature: (JLorg/ray/runtime/CoreWorkerProxy;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_Worker_nativeRunCoreWorker(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jobject javaCoreWorker) {
  local_env = env;
  local_java_worker = javaCoreWorker;
  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  core_worker->Execution().Run();
  local_env = nullptr;
  local_java_worker = nullptr;
}

/*
 * Class:     org_ray_runtime_Worker
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_Worker_nativeDestroy(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jobject javaCoreWorker) {
  delete reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
}

#ifdef __cplusplus
}
#endif
