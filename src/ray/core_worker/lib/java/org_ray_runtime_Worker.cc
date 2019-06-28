#include "ray/core_worker/lib/java/org_ray_runtime_Worker.h"
#include <jni.h>
#include <sstream>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_Worker
 * Method:    createCoreWorker
 * Signature: (Ljava/lang/String;Ljava/lang/String;[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_Worker_createCoreWorker(JNIEnv *env, jclass,
                                                                     jint workerMode,
                                                                     jstring storeSocket,
                                                                     jstring rayletSocket,
                                                                     jbyteArray jobId) {
  auto native_store_socket = JavaStringToNativeString(env, storeSocket);
  auto native_raylet_socket = JavaStringToNativeString(env, rayletSocket);
  UniqueIdFromJByteArray<ray::JobID> job_id(env, jobId);
  try {
    auto core_worker = new ray::CoreWorker(static_cast<ray::WorkerType>(workerMode),
                                           ray::WorkerLanguage::JAVA, native_store_socket,
                                           native_raylet_socket, job_id.GetId());
    return reinterpret_cast<jlong>(core_worker);
  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Failed to construct core worker: " << e.what();
    ThrowRayExceptionIfNotOK(env, ray::Status::Invalid(oss.str()));
    return 0;  // To make compiler no complain
  }
}

/*
 * Class:     org_ray_runtime_Worker
 * Method:    runCoreWorker
 * Signature: (JLorg/ray/runtime/CoreWorkerProxy;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_Worker_runCoreWorker(JNIEnv *env, jclass o,
                                                                 jlong nativeCoreWorker,
                                                                 jobject javaCoreWorker) {
  jmethodID run_task_method =
      env->GetMethodID(o, "runTaskCallback", "(Ljava/util/List;Ljava/util/List;[B[BII)V");
  auto executor_func = [env, javaCoreWorker, run_task_method](
                           const ray::RayFunction &ray_function,
                           const std::vector<std::shared_ptr<ray::RayObject>> &args,
                           const ray::TaskInfo &task_info, int num_returns) {
    // convert RayFunction
    jobject ray_function_array_list =
        NativeStringVectorToJavaStringList(env, ray_function.function_descriptor);
    // convert args
    jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
        env, args, ToJavaRayObjectProxy);
    // convert task id
    jbyteArray task_id_byte_array =
        JByteArrayFromUniqueId<ray::TaskID>(env, task_info.task_id).GetJByteArray();
    // convert job id
    jbyteArray job_id_byte_array =
        JByteArrayFromUniqueId<ray::JobID>(env, task_info.job_id).GetJByteArray();

    // invoke Java method
    env->CallVoidMethod(javaCoreWorker, run_task_method, ray_function_array_list,
                        args_array_list, task_id_byte_array, job_id_byte_array,
                        (jint)task_info.task_type, (jint)num_returns);
    return ray::Status::OK();
  };

  auto core_worker = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker);
  auto status = core_worker->Execution().Run(executor_func);
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_Worker
 * Method:    getTaskReturnId
 * Signature: ([BJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_Worker_getTaskReturnId(
    JNIEnv *env, jclass o, jbyteArray taskId, jlong returnIndex) {
  UniqueIdFromJByteArray<ray::TaskID> task_id(env, taskId);
  ray::ObjectID return_id =
      ray::ObjectID::ForTaskReturn(task_id.GetId(), (int64_t)returnIndex);
  return JByteArrayFromUniqueId<ray::ObjectID>(env, return_id).GetJByteArray();
}

#ifdef __cplusplus
}
#endif
