#include "ray/core_worker/lib/java/org_ray_runtime_TaskInterface.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/task_interface.h"

inline ray::CoreWorkerTaskInterface &GetTaskInterface(jlong nativeCoreWorker) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker)->Tasks();
}

inline ray::RayFunction ToRayFunction(JNIEnv *env, jobject rayFunction) {
  std::vector<std::string> function_descriptor;
  JavaStringListToNativeStringVector(
      env, env->GetObjectField(rayFunction, java_ray_function_proxy_function_descriptor),
      &function_descriptor);
  ray::RayFunction ray_function{
      static_cast<ray::WorkerLanguage>(
          (int)env->GetIntField(rayFunction, java_ray_function_proxy_worker_language)),
      function_descriptor};
  return ray_function;
}

inline std::vector<ray::TaskArg> ToTaskArgs(
    JNIEnv *env, jobject taskArgs, std::vector<jbyteArray> *byte_arrays_to_be_released,
    std::vector<jbyte *> *byte_pointers_to_be_released) {
  std::vector<ray::TaskArg> args;
  JavaListToNativeVector<ray::TaskArg>(
      env, taskArgs, &args,
      [byte_arrays_to_be_released, byte_pointers_to_be_released](JNIEnv *env,
                                                                 jobject arg) {
        auto java_id =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_task_arg_proxy_id));
        if (java_id) {
          return ray::TaskArg::PassByReference(
              UniqueIdFromJByteArray<ray::ObjectID>(env, java_id).GetId());
        }
        auto java_data =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_task_arg_proxy_data));
        auto data_size = env->GetArrayLength(java_data);
        jbyte *data = env->GetByteArrayElements(java_data, nullptr);
        byte_arrays_to_be_released->push_back(java_data);
        byte_pointers_to_be_released->push_back(data);
        return ray::TaskArg::PassByValue(std::make_shared<ray::LocalMemoryBuffer>(
            reinterpret_cast<uint8_t *>(data), (size_t)data_size));
      });
  return args;
}

inline void ReleaseTaskArgs(JNIEnv *env,
                            const std::vector<jbyteArray> &byte_arrays_to_be_released,
                            const std::vector<jbyte *> &byte_pointers_to_be_released) {
  for (size_t i = 0; i < byte_arrays_to_be_released.size(); i++) {
    env->ReleaseByteArrayElements(byte_arrays_to_be_released[i],
                                  byte_pointers_to_be_released[i], JNI_ABORT);
  }
}

inline std::unordered_map<std::string, double> ToResources(JNIEnv *env,
                                                           jobject java_resources) {
  jobject resource_keys = env->GetObjectField(java_resources, java_resources_proxy_keys);
  jobject resource_values =
      env->GetObjectField(java_resources, java_resources_proxy_values);
  int resource_size = env->CallIntMethod(resource_keys, java_list_size);
  std::unordered_map<std::string, double> resources;
  for (int i = 0; i < resource_size; i++) {
    std::string key = JavaStringToNativeString(
        env, (jstring)env->CallObjectMethod(resource_keys, java_list_get, (jint)i));
    jobject valueObject = env->CallObjectMethod(resource_values, java_list_get, (jint)i);
    double value = env->GetDoubleField(valueObject, java_double_value);
    resources.emplace(key, value);
  }
  return resources;
}

inline ray::TaskOptions ToTaskOptions(JNIEnv *env, jobject taskOptions) {
  int num_returns = env->GetIntField(taskOptions, java_task_options_proxy_num_returns);
  jobject java_resources =
      env->GetObjectField(taskOptions, java_task_options_proxy_resources);
  auto resources = ToResources(env, java_resources);

  ray::TaskOptions task_options{num_returns, resources};
  return task_options;
}

inline ray::ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                        jobject actorCreationOptions) {
  uint64_t max_reconstructions = env->GetLongField(
      actorCreationOptions, java_actor_creation_options_proxy_max_reconstructions);
  jobject java_resources = env->GetObjectField(
      actorCreationOptions, java_actor_creation_options_proxy_resources);
  auto resources = ToResources(env, java_resources);

  ray::ActorCreationOptions action_creation_options{max_reconstructions, resources};
  return action_creation_options;
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_TaskInterface
 * Method:    submitTask
 * Signature:
 * (JLorg/ray/runtime/proxyTypes/RayFunctionProxy;Ljava/util/List;Lorg/ray/runtime/proxyTypes/TaskOptionsProxy;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_TaskInterface_submitTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorker, jobject rayFunction, jobject taskArgs,
    jobject taskOptions) {
  auto ray_function = ToRayFunction(env, rayFunction);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto task_options = ToTaskOptions(env, taskOptions);

  std::vector<ObjectID> return_ids;
  auto status = GetTaskInterface(nativeCoreWorker)
                    .SubmitTask(ray_function, args, task_options, &return_ids);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  ThrowRayExceptionIfNotOK(env, status);

  return NativeUniqueIdVectorToJavaBinaryList(env, return_ids);
}

/*
 * Class:     org_ray_runtime_TaskInterface
 * Method:    createActor
 * Signature:
 * (JLorg/ray/runtime/proxyTypes/RayFunctionProxy;Ljava/util/List;Lorg/ray/runtime/proxyTypes/ActorCreationOptionsProxy;)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_TaskInterface_createActor(
    JNIEnv *env, jclass p, jlong nativeCoreWorker, jobject rayFunction, jobject taskArgs,
    jobject actorCreationOptions) {
  auto ray_function = ToRayFunction(env, rayFunction);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  std::unique_ptr<ray::ActorHandle> actor_handle;
  auto status =
      GetTaskInterface(nativeCoreWorker)
          .CreateActor(ray_function, args, actor_creation_options, &actor_handle);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  ThrowRayExceptionIfNotOK(env, status);
  return reinterpret_cast<jlong>(actor_handle.release());
}

/*
 * Class:     org_ray_runtime_TaskInterface
 * Method:    submitActorTask
 * Signature:
 * (JJLorg/ray/runtime/proxyTypes/RayFunctionProxy;Ljava/util/List;Lorg/ray/runtime/proxyTypes/TaskOptionsProxy;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_TaskInterface_submitActorTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorker, jlong nativeActorHandle,
    jobject rayFunction, jobject taskArgs, jobject taskOptions) {
  auto &actor_handle = *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
  auto ray_function = ToRayFunction(env, rayFunction);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto task_options = ToTaskOptions(env, taskOptions);

  std::vector<ObjectID> return_ids;
  auto status =
      GetTaskInterface(nativeCoreWorker)
          .SubmitActorTask(actor_handle, ray_function, args, task_options, &return_ids);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  ThrowRayExceptionIfNotOK(env, status);

  return NativeUniqueIdVectorToJavaBinaryList(env, return_ids);
}

#ifdef __cplusplus
}
#endif
