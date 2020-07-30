// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io_ray_runtime_task_NativeTaskSubmitter.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

/// Store C++ instances of ray function in the cache to avoid unnessesary JNI operations.
thread_local std::unordered_map<jint, std::vector<std::pair<jobject, ray::RayFunction>>>
    submitter_function_descriptor_cache;

inline const ray::RayFunction &ToRayFunction(JNIEnv *env, jobject functionDescriptor,
                                             jint hash) {
  auto &fd_vector = submitter_function_descriptor_cache[hash];
  for (auto &pair : fd_vector) {
    if (env->CallBooleanMethod(pair.first, java_object_equals, functionDescriptor)) {
      return pair.second;
    }
  }

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
  fd_vector.emplace_back(env->NewGlobalRef(functionDescriptor),
                         ray::RayFunction(language, function_descriptor));
  return fd_vector.back().second;
}

inline std::vector<std::unique_ptr<ray::TaskArg>> ToTaskArgs(JNIEnv *env, jobject args) {
  std::vector<std::unique_ptr<ray::TaskArg>> task_args;
  JavaListToNativeVector<std::unique_ptr<ray::TaskArg>>(
      env, args, &task_args, [](JNIEnv *env, jobject arg) {
        auto java_id = env->GetObjectField(arg, java_function_arg_id);
        if (java_id) {
          auto java_id_bytes = static_cast<jbyteArray>(
              env->CallObjectMethod(java_id, java_base_id_get_bytes));
          RAY_CHECK_JAVA_EXCEPTION(env);
          auto id = JavaByteArrayToId<ray::ObjectID>(env, java_id_bytes);
          return std::unique_ptr<ray::TaskArg>(new ray::TaskArgByReference(
              id, ray::CoreWorkerProcess::GetCoreWorker().GetOwnerAddress(id)));
        }
        auto java_value =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_function_arg_value));
        RAY_CHECK(java_value) << "Both id and value of FunctionArg are null.";
        auto value = JavaNativeRayObjectToNativeRayObject(env, java_value);
        return std::unique_ptr<ray::TaskArg>(new ray::TaskArgByValue(value));
      });
  return task_args;
}

inline std::unordered_map<std::string, double> ToResources(JNIEnv *env,
                                                           jobject java_resources) {
  std::unordered_map<std::string, double> resources;
  return JavaMapToNativeMap<std::string, double>(
      env, java_resources,
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        double value = env->CallDoubleMethod(java_value, java_double_double_value);
        RAY_CHECK_JAVA_EXCEPTION(env);
        return value;
      });
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
  bool global = false;
  std::string name = "";
  int64_t max_restarts = 0;
  std::unordered_map<std::string, double> resources;
  std::vector<std::string> dynamic_worker_options;
  uint64_t max_concurrency = 1;
  auto placement_options = std::make_pair(ray::PlacementGroupID::Nil(), -1);
  if (actorCreationOptions) {
    global =
        env->GetBooleanField(actorCreationOptions, java_actor_creation_options_global);
    auto java_name = (jstring)env->GetObjectField(actorCreationOptions,
                                                  java_actor_creation_options_name);
    if (java_name) {
      name = JavaStringToNativeString(env, java_name);
    }
    max_restarts =
        env->GetIntField(actorCreationOptions, java_actor_creation_options_max_restarts);
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

    auto group =
        env->GetObjectField(actorCreationOptions, java_actor_creation_options_group);
    if (group) {
      auto placement_group_id = env->GetObjectField(group, java_placement_group_id);
      auto java_id_bytes = static_cast<jbyteArray>(
          env->CallObjectMethod(placement_group_id, java_base_id_get_bytes));
      RAY_CHECK_JAVA_EXCEPTION(env);
      auto id = JavaByteArrayToId<ray::PlacementGroupID>(env, java_id_bytes);
      auto index = env->GetIntField(actorCreationOptions,
                                    java_actor_creation_options_bundle_index);
      placement_options = std::make_pair(id, index);
    }
  }

  auto full_name = GetActorFullName(global, name);
  ray::ActorCreationOptions actor_creation_options{
      max_restarts,
      0,  // TODO: Allow setting max_task_retries from Java.
      static_cast<int>(max_concurrency),
      resources,
      resources,
      dynamic_worker_options,
      /*is_detached=*/false,
      full_name,
      /*is_asyncio=*/false,
      placement_options};
  return actor_creation_options;
}

inline ray::PlacementStrategy ConvertStrategy(jint java_strategy) {
  return 0 == java_strategy ? ray::rpc::PACK : ray::rpc::SPREAD;
}

inline ray::PlacementGroupCreationOptions ToPlacementGroupCreationOptions(
    JNIEnv *env, jobject java_bundles, jint java_strategy) {
  std::vector<std::unordered_map<std::string, double>> bundles;
  JavaListToNativeVector<std::unordered_map<std::string, double>>(
      env, java_bundles, &bundles, [](JNIEnv *env, jobject java_bundle) {
        return JavaMapToNativeMap<std::string, double>(
            env, java_bundle,
            [](JNIEnv *env, jobject java_key) {
              return JavaStringToNativeString(env, (jstring)java_key);
            },
            [](JNIEnv *env, jobject java_value) {
              double value = env->CallDoubleMethod(java_value, java_double_double_value);
              RAY_CHECK_JAVA_EXCEPTION(env);
              return value;
            });
      });
  return ray::PlacementGroupCreationOptions("", ConvertStrategy(java_strategy), bundles);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(
    JNIEnv *env, jclass p, jobject functionDescriptor, jint functionDescriptorHash,
    jobject args, jint numReturns, jobject callOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  // TODO (kfstorm): Allow setting `max_retries` via `CallOptions`.
  ray::CoreWorkerProcess::GetCoreWorker().SubmitTask(ray_function, task_args,
                                                     task_options, &return_ids,
                                                     /*max_retries=*/0);

  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(
    JNIEnv *env, jclass p, jobject functionDescriptor, jint functionDescriptorHash,
    jobject args, jobject actorCreationOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  ActorID actor_id;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreateActor(
      ray_function, task_args, actor_creation_options,
      /*extension_data*/ "", &actor_id);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(
    JNIEnv *env, jclass p, jbyteArray actorId, jobject functionDescriptor,
    jint functionDescriptorHash, jobject args, jint numReturns, jobject callOptions) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  ray::CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      actor_id, ray_function, task_args, task_options, &return_ids);

  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreatePlacementGroup(JNIEnv *env,
                                                                        jclass,
                                                                        jobject bundles,
                                                                        jint strategy) {
  auto options = ToPlacementGroupCreationOptions(env, bundles, strategy);
  ray::PlacementGroupID placement_group_id;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreatePlacementGroup(
      options, &placement_group_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::PlacementGroupID>(env, placement_group_id);
}

#ifdef __cplusplus
}
#endif
