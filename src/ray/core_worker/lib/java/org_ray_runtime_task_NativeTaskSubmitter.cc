#include "ray/core_worker/lib/java/org_ray_runtime_task_NativeTaskSubmitter.h"
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

inline std::vector<ray::TaskArg> ToTaskArgs(JNIEnv *env, jobject args) {
  std::vector<ray::TaskArg> task_args;
  JavaListToNativeVector<ray::TaskArg>(
      env, args, &task_args, [](JNIEnv *env, jobject arg) {
        auto java_id = env->GetObjectField(arg, java_function_arg_id);
        if (java_id) {
          auto java_id_bytes = static_cast<jbyteArray>(
              env->CallObjectMethod(java_id, java_base_id_get_bytes));
          return ray::TaskArg::PassByReference(
              JavaByteArrayToId<ray::ObjectID>(env, java_id_bytes));
        }
        auto java_value =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_function_arg_value));
        RAY_CHECK(java_value) << "Both id and value of FunctionArg are null.";
        auto value = JavaNativeRayObjectToNativeRayObject(env, java_value);
        return ray::TaskArg::PassByValue(value);
      });
  return task_args;
}

inline std::unordered_map<std::string, double> ToResources(JNIEnv *env,
                                                           jobject java_resources) {
  std::unordered_map<std::string, double> resources;
  if (java_resources) {
    jobject entry_set = env->CallObjectMethod(java_resources, java_map_entry_set);
    jobject iterator = env->CallObjectMethod(entry_set, java_set_iterator);
    while (env->CallBooleanMethod(iterator, java_iterator_has_next)) {
      jobject map_entry = env->CallObjectMethod(iterator, java_iterator_next);
      std::string key = JavaStringToNativeString(
          env, (jstring)env->CallObjectMethod(map_entry, java_map_entry_get_key));
      double value = env->CallDoubleMethod(
          env->CallObjectMethod(map_entry, java_map_entry_get_value),
          java_double_double_value);
      resources.emplace(key, value);
    }
  }
  return resources;
}

inline ray::TaskOptions ToTaskOptions(JNIEnv *env, jint numReturns, jobject callOptions) {
  std::unordered_map<std::string, double> resources;
  if (callOptions) {
    jobject java_resources =
        env->GetObjectField(callOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
  }

  ray::TaskOptions task_options{numReturns, resources};
  return task_options;
}

inline ray::ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                        jobject actorCreationOptions) {
  uint64_t max_reconstructions = 0;
  bool use_direct_call;
  std::unordered_map<std::string, double> resources;
  std::vector<std::string> dynamic_worker_options;
  if (actorCreationOptions) {
    max_reconstructions = static_cast<uint64_t>(env->GetIntField(
        actorCreationOptions, java_actor_creation_options_max_reconstructions));
    use_direct_call = env->GetBooleanField(actorCreationOptions,
                                          java_actor_creation_options_use_direct_call);
    jobject java_resources =
        env->GetObjectField(actorCreationOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    jstring java_jvm_options = (jstring)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_jvm_options);
    if (java_jvm_options) {
      std::string jvm_options = JavaStringToNativeString(env, java_jvm_options);
      dynamic_worker_options.emplace_back(jvm_options);
    }
  } else {
    use_direct_call =
        env->GetStaticBooleanField(java_actor_creation_options_class,
                                   java_actor_creation_options_default_use_direct_call);
  }

  ray::ActorCreationOptions action_creation_options{
      static_cast<uint64_t>(max_reconstructions), use_direct_call, resources,
      dynamic_worker_options};
  return action_creation_options;
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeSubmitTask
 * Signature:
 * (JLorg/ray/runtime/functionmanager/FunctionDescriptor;Ljava/util/List;ILorg/ray/api/options/CallOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject args, jint numReturns, jobject callOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  auto status = GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
                    .SubmitTask(ray_function, task_args, task_options, &return_ids);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

/*
 * Class:     org_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeCreateActor
 * Signature:
 * (JLorg/ray/runtime/functionmanager/FunctionDescriptor;Ljava/util/List;Lorg/ray/api/options/ActorCreationOptions;)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject args, jobject actorCreationOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  std::unique_ptr<ray::ActorHandle> actor_handle;
  auto status =
      GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
          .CreateActor(ray_function, task_args, actor_creation_options, &actor_handle);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, 0);
  return reinterpret_cast<jlong>(actor_handle.release());
}

/*
 * Class:     org_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeSubmitActorTask
 * Signature:
 * (JJLorg/ray/runtime/functionmanager/FunctionDescriptor;Ljava/util/List;ILorg/ray/api/options/CallOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL
Java_org_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jlong nativeActorHandle,
    jobject functionDescriptor, jobject args, jint numReturns, jobject callOptions) {
  auto &actor_handle = *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  auto status = GetTaskInterfaceFromPointer(nativeCoreWorkerPointer)
                    .SubmitActorTask(actor_handle, ray_function, task_args, task_options,
                                     &return_ids);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

#ifdef __cplusplus
}
#endif
