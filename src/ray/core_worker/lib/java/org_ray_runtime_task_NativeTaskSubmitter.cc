#include "ray/core_worker/lib/java/org_ray_runtime_task_NativeTaskSubmitter.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::CoreWorker &GetCoreWorker(jlong nativeCoreWorkerPointer) {
  return *reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
}

inline ray::RayFunction ToRayFunction(JNIEnv *env, jobject functionDescriptor) {
  std::vector<std::string> function_descriptor_list;
  jobject list =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_to_list);
  RAY_CHECK_JAVA_EXCEPTION(env);
  JavaStringListToNativeStringVector(env, list, &function_descriptor_list);
  jobject java_language =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_get_language);
  RAY_CHECK_JAVA_EXCEPTION(env);
  auto language = static_cast<::Language>(
      env->CallIntMethod(java_language, java_language_get_number));
  RAY_CHECK_JAVA_EXCEPTION(env);
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(language, function_descriptor_list);
  ray::RayFunction ray_function{language, function_descriptor};
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
          RAY_CHECK_JAVA_EXCEPTION(env);
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
    RAY_CHECK_JAVA_EXCEPTION(env);
    jobject iterator = env->CallObjectMethod(entry_set, java_set_iterator);
    RAY_CHECK_JAVA_EXCEPTION(env);
    while (env->CallBooleanMethod(iterator, java_iterator_has_next)) {
      RAY_CHECK_JAVA_EXCEPTION(env);
      jobject map_entry = env->CallObjectMethod(iterator, java_iterator_next);
      RAY_CHECK_JAVA_EXCEPTION(env);
      auto java_key = (jstring)env->CallObjectMethod(map_entry, java_map_entry_get_key);
      RAY_CHECK_JAVA_EXCEPTION(env);
      std::string key = JavaStringToNativeString(env, java_key);
      auto java_value = env->CallObjectMethod(map_entry, java_map_entry_get_value);
      RAY_CHECK_JAVA_EXCEPTION(env);
      double value = env->CallDoubleMethod(java_value, java_double_double_value);
      RAY_CHECK_JAVA_EXCEPTION(env);
      resources.emplace(key, value);
    }
    RAY_CHECK_JAVA_EXCEPTION(env);
  }
  return resources;
}

inline ray::TaskOptions ToTaskOptions(JNIEnv *env, jint numReturns, jobject callOptions) {
  std::unordered_map<std::string, double> resources;
  bool use_direct_call;
  if (callOptions) {
    jobject java_resources =
        env->GetObjectField(callOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    use_direct_call =
        env->GetBooleanField(callOptions, java_base_task_options_use_direct_call);
  } else {
    use_direct_call = env->GetStaticBooleanField(
        java_base_task_options_class, java_base_task_options_default_use_direct_call);
  }

  ray::TaskOptions task_options{numReturns, use_direct_call, resources};
  return task_options;
}

inline ray::ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                        jobject actorCreationOptions) {
  uint64_t max_reconstructions = 0;
  bool use_direct_call;
  std::unordered_map<std::string, double> resources;
  std::vector<std::string> dynamic_worker_options;
  uint64_t max_concurrency = 1;
  if (actorCreationOptions) {
    max_reconstructions = static_cast<uint64_t>(env->GetIntField(
        actorCreationOptions, java_actor_creation_options_max_reconstructions));
    use_direct_call = env->GetBooleanField(actorCreationOptions,
                                           java_base_task_options_use_direct_call);
    jobject java_resources =
        env->GetObjectField(actorCreationOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    jstring java_jvm_options = (jstring)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_jvm_options);
    if (java_jvm_options) {
      std::string jvm_options = JavaStringToNativeString(env, java_jvm_options);
      dynamic_worker_options.emplace_back(jvm_options);
    }
    max_concurrency = static_cast<uint64_t>(env->GetIntField(
        actorCreationOptions, java_actor_creation_options_max_concurrency));
  } else {
    use_direct_call = env->GetStaticBooleanField(
        java_base_task_options_class, java_base_task_options_default_use_direct_call);
  }

  ray::ActorCreationOptions actor_creation_options{
      static_cast<uint64_t>(max_reconstructions),
      use_direct_call,
      static_cast<int>(max_concurrency),
      resources,
      resources,
      dynamic_worker_options,
      /*is_detached=*/false,
      /*is_asyncio=*/false};
  return actor_creation_options;
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL Java_org_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject args, jint numReturns, jobject callOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  // TODO (kfstorm): Allow setting `max_retries` via `CallOptions`.
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
                    .SubmitTask(ray_function, task_args, task_options, &return_ids,
                                /*max_retries=*/0);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jobject functionDescriptor,
    jobject args, jobject actorCreationOptions) {
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  ray::ActorID actor_id;
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
                    .CreateActor(ray_function, task_args, actor_creation_options,
                                 /*extension_data*/ "", &actor_id);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(
    JNIEnv *env, jclass p, jlong nativeCoreWorkerPointer, jbyteArray actorId,
    jobject functionDescriptor, jobject args, jint numReturns, jobject callOptions) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  auto ray_function = ToRayFunction(env, functionDescriptor);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  auto status =
      GetCoreWorker(nativeCoreWorkerPointer)
          .SubmitActorTask(actor_id, ray_function, task_args, task_options, &return_ids);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

#ifdef __cplusplus
}
#endif
