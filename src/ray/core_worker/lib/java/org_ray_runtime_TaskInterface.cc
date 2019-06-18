#include "ray/core_worker/lib/java/org_ray_runtime_TaskInterface.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/task_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_TaskInterface
 * Method:    submitTask
 * Signature:
 * (JLorg/ray/runtime/nativeTypes/NativeRayFunction;Ljava/util/List;Lorg/ray/runtime/nativeTypes/NativeTaskOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_TaskInterface_submitTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorker, jobject rayFunction, jobject taskArgs,
    jobject taskOptions) {
  // convert ray function
  std::vector<std::string> function_descriptor;
  JavaStringListToNativeStringVector(
      env, env->GetObjectField(rayFunction, java_native_ray_function_function_descriptor),
      &function_descriptor);
  ray::RayFunction ray_function{
      static_cast<ray::WorkerLanguage>(
          (int)env->GetIntField(rayFunction, java_native_ray_function_worker_language)),
      function_descriptor};

  // convert args
  std::vector<ray::TaskArg> args;
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  JavaListToNativeVector<ray::TaskArg>(
      env, taskArgs, &args,
      [&byte_arrays_to_be_released, &byte_pointers_to_be_released](JNIEnv *env,
                                                                   jobject arg) {
        auto java_id =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_native_task_arg_id));
        if (java_id) {
          return ray::TaskArg::PassByReference(
              UniqueIdFromJByteArray<ray::ObjectID>(env, java_id).GetId());
        }
        auto java_data =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_native_task_arg_data));
        auto data_size = env->GetArrayLength(java_data);
        jbyte *data = env->GetByteArrayElements(java_data, nullptr);
        byte_arrays_to_be_released.push_back(java_data);
        byte_pointers_to_be_released.push_back(data);
        return ray::TaskArg::PassByValue(std::make_shared<ray::LocalMemoryBuffer>(
            reinterpret_cast<uint8_t *>(data), (size_t)data_size));
      });

  // convert task options
  int num_returns = env->GetIntField(taskOptions, java_native_task_options_num_returns);
  jobject resource_keys =
      env->GetObjectField(taskOptions, java_native_task_options_resource_keys);
  jobject resource_values =
      env->GetObjectField(taskOptions, java_native_task_options_resource_values);
  int resource_size = env->CallIntMethod(resource_keys, java_list_size);
  std::unordered_map<std::string, double> resources;
  for (int i = 0; i < resource_size; i++) {
    std::string key = JavaStringToNativeString(
        env, (jstring)env->CallObjectMethod(resource_keys, java_list_get, (jint)i));
    double value = env->CallDoubleMethod(resource_values, java_list_get, (jint)i);
    resources.emplace(key, value);
  }
  ray::TaskOptions task_options{num_returns, resources};

  std::vector<ObjectID> return_ids;
  auto status = (reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker))
                    ->Tasks()
                    .SubmitTask(ray_function, args, task_options, &return_ids);

  for (size_t i = 0; i < byte_arrays_to_be_released.size(); i++) {
    env->ReleaseByteArrayElements(byte_arrays_to_be_released[i],
                                  byte_pointers_to_be_released[i], JNI_ABORT);
  }

  ThrowRayExceptionIfNotOK(env, status);

  // convert return ids
  return NativeVectorToJavaList<ray::ObjectID>(
      env, return_ids, [](JNIEnv *env, const ObjectID &id) {
        return JByteArrayFromUniqueId<ray::ObjectID>(env, id).GetJByteArray();
      });
}

#ifdef __cplusplus
}
#endif
