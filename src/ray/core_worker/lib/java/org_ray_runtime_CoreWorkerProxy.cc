#include "ray/core_worker/lib/java/org_ray_runtime_CoreWorkerProxy.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/common/java/jni_helper.h"
#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_CoreWorkerProxy
 * Method:    createCoreWorker
 * Signature: (Ljava/lang/String;Ljava/lang/String;[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_CoreWorkerProxy_createCoreWorker(
    JNIEnv *env, jclass, jstring storeSocket, jstring rayletSocket, jbyteArray driverId) {
  const char *native_store_socket = env->GetStringUTFChars(storeSocket, JNI_FALSE);
  const char *native_raylet_socket = env->GetStringUTFChars(rayletSocket, JNI_FALSE);
  UniqueIdFromJByteArray<ray::DriverID> driver_id(env, driverId);
  auto core_worker =
      new ray::CoreWorker(ray::WorkerType::WORKER, ray::WorkerLanguage::JAVA,
                          std::string(native_store_socket),
                          std::string(native_raylet_socket), driver_id.GetId());
  env->ReleaseStringUTFChars(storeSocket, native_store_socket);
  env->ReleaseStringUTFChars(rayletSocket, native_raylet_socket);
  return reinterpret_cast<jlong>(core_worker);
}

/*
 * Class:     org_ray_runtime_CoreWorkerProxy
 * Method:    runCoreWorker
 * Signature: (JLorg/ray/runtime/CoreWorkerProxy;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_CoreWorkerProxy_runCoreWorker(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jobject javaCoreWorker) {
  jmethodID run_task_method =
      env->GetMethodID(o, "runTaskCallback", "(Ljava/util/List;Ljava/util/List;[BI)V");
  jclass array_list_class = env->FindClass("java/util/ArrayList");
  jmethodID array_list_constructor = env->GetMethodID(array_list_class, "<init>", "()V");
  jmethodID array_list_add =
      env->GetMethodID(array_list_class, "add", "(java/lang/Object)V");
  auto executor_func = [env, javaCoreWorker, run_task_method, array_list_class,
                        array_list_constructor, array_list_add](
                           const ray::RayFunction &ray_function,
                           const std::vector<std::shared_ptr<ray::Buffer>> &args,
                           const TaskID &task_id, int num_returns) {
    // convert RayFunction
    jobject ray_function_array_lsit =
        env->NewObject(array_list_class, array_list_constructor);
    for (auto &function_descriptor_item : ray_function.function_descriptor) {
      env->CallVoidMethod(ray_function_array_lsit, array_list_add,
                          env->NewStringUTF(function_descriptor_item.c_str()));
    }
    // convert args
    jobject args_array_list = env->NewObject(array_list_class, array_list_constructor);
    for (auto &arg : args) {
      jbyteArray arg_byte_array = env->NewByteArray(arg->Size());
      env->SetByteArrayRegion(arg_byte_array, 0, arg->Size(),
                              reinterpret_cast<const jbyte *>(arg->Data()));
      env->CallVoidMethod(args_array_list, array_list_add, arg_byte_array);
    }
    // convert task id
    jbyteArray task_id_byte_array =
        JByteArrayFromUniqueId<ray::TaskID>(env, task_id).GetJByteArray();

    // invoke Java method
    env->CallVoidMethod(javaCoreWorker, run_task_method, ray_function_array_lsit,
                        args_array_list, task_id_byte_array, (jint)num_returns);
    return ray::Status::OK();
  };

  RAY_CHECK_OK((reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker))->Execution().Run(executor_func));
}

#ifdef __cplusplus
}
#endif
