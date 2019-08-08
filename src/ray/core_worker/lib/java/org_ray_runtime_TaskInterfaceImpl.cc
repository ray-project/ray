#include "ray/core_worker/lib/java/org_ray_runtime_TaskInterfaceImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/core_worker/task_interface.h"

inline ray::CoreWorkerTaskInterface &GetTaskInterfaceFromPointer(
    jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->Tasks();
}

inline ray::RayFunction ToRayFunction(JNIEnv *env, jobject functionDescriptor) {
  std::vector<std::string> function_descriptor;
  JavaStringListToNativeStringVector(
      env, env->CallObjectMethod(functionDescriptor, java_function_descriptor_to_list),
      &function_descriptor);
  jobject java_language =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_get_language);
  int language = env->CallIntMethod(java_language, java_language_get_number);
  ray::RayFunction ray_function{static_cast<::Language>(language), function_descriptor};
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
            static_cast<jbyteArray>(env->GetObjectField(arg, java_native_task_arg_id));
        if (java_id) {
          return ray::TaskArg::PassByReference(
              JavaByteArrayToId<ray::ObjectID>(env, java_id));
        }
        auto java_data =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_native_task_arg_data));
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
  jobject resource_keys = env->GetObjectField(java_resources, java_native_resources_keys);
  jobject resource_values =
      env->GetObjectField(java_resources, java_native_resources_values);
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
  int num_returns = env->GetIntField(taskOptions, java_native_task_options_num_returns);
  jobject java_resources =
      env->GetObjectField(taskOptions, java_native_task_options_resources);
  auto resources = ToResources(env, java_resources);

  ray::TaskOptions task_options{num_returns, resources};
  return task_options;
}

inline ray::ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                        jobject actorCreationOptions) {
  uint64_t max_reconstructions = env->GetLongField(
      actorCreationOptions, java_native_actor_creation_options_max_reconstructions);
  bool is_direct_call = env->GetBooleanField(
      actorCreationOptions, java_native_actor_creation_options_is_direct_call);
  jobject java_resources = env->GetObjectField(
      actorCreationOptions, java_native_actor_creation_options_resources);
  auto resources = ToResources(env, java_resources);
  jobject java_dynamic_worker_options = env->GetObjectField(
      actorCreationOptions, java_native_actor_creation_options_dynamic_worker_options);
  std::vector<std::string> dynamic_worker_options;
  JavaStringListToNativeStringVector(env, java_dynamic_worker_options,
                                     &dynamic_worker_options);

  ray::ActorCreationOptions action_creation_options{max_reconstructions, is_direct_call,
                                                    resources, dynamic_worker_options};
  return action_creation_options;
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_TaskInterfaceImpl
 * Method:    nativeSubmitTask
 * Signature:
 * (JLorg/ray/runtime/nativeTypes/NativeRayFunction;Ljava/util/List;Lorg/ray/runtime/nativeTypes/NativeTaskOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_TaskInterfaceImpl_nativeSubmitTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject taskArgs, jobject taskOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto task_options = ToTaskOptions(env, taskOptions);

  std::vector<ObjectID> return_ids;
  auto status = GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
                    .SubmitTask(ray_function, args, task_options, &return_ids);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

/*
 * Class:     org_ray_runtime_TaskInterfaceImpl
 * Method:    nativeCreateActor
 * Signature:
 * (JLorg/ray/runtime/nativeTypes/NativeRayFunction;Ljava/util/List;Lorg/ray/runtime/nativeTypes/NativeActorCreationOptions;)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_TaskInterfaceImpl_nativeCreateActor(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject taskArgs, jobject actorCreationOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  std::unique_ptr<ray::ActorHandle> actor_handle;
  auto status =
      GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
          .CreateActor(ray_function, args, actor_creation_options, &actor_handle);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, 0);
  return reinterpret_cast<jlong>(actor_handle.release());
}

/*
 * Class:     org_ray_runtime_TaskInterfaceImpl
 * Method:    nativeSubmitActorTask
 * Signature:
 * (JJLorg/ray/runtime/nativeTypes/NativeRayFunction;Ljava/util/List;Lorg/ray/runtime/nativeTypes/NativeTaskOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_TaskInterfaceImpl_nativeSubmitActorTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jlong nativeActorHandle,
    jobject functionDescriptor, jobject taskArgs, jobject taskOptions) {
  auto &actor_handle = *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
  auto ray_function = ToRayFunction(env, functionDescriptor);
  std::vector<jbyteArray> byte_arrays_to_be_released;
  std::vector<jbyte *> byte_pointers_to_be_released;
  auto args = ToTaskArgs(env, taskArgs, &byte_arrays_to_be_released,
                         &byte_pointers_to_be_released);
  auto task_options = ToTaskOptions(env, taskOptions);

  std::vector<ObjectID> return_ids;
  auto status =
      GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
          .SubmitActorTask(actor_handle, ray_function, args, task_options, &return_ids);
  ReleaseTaskArgs(env, byte_arrays_to_be_released, byte_pointers_to_be_released);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

#ifdef __cplusplus
}
#endif
