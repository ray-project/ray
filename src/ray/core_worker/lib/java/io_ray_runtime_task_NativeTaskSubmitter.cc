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

/// A helper that computes the hash code of a Java object.
inline jint GetHashCodeOfJavaObject(JNIEnv *env, jobject java_object) {
  const jint hashcode = env->CallIntMethod(java_object, java_object_hash_code);
  RAY_CHECK_JAVA_EXCEPTION(env);
  return hashcode;
}

/// Store C++ instances of ray function in the cache to avoid unnessesary JNI operations.
thread_local absl::flat_hash_map<jint, std::vector<std::pair<jobject, RayFunction>>>
    submitter_function_descriptor_cache;

inline const RayFunction &ToRayFunction(JNIEnv *env,
                                        jobject functionDescriptor,
                                        jint hash) {
  auto &fd_vector = submitter_function_descriptor_cache[hash];
  for (auto &[obj, func] : fd_vector) {
    if (env->CallBooleanMethod(obj, java_object_equals, functionDescriptor)) {
      return func;
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
  FunctionDescriptor function_descriptor =
      FunctionDescriptorBuilder::FromVector(language, function_descriptor_list);
  fd_vector.emplace_back(env->NewGlobalRef(functionDescriptor),
                         RayFunction(language, function_descriptor));
  return fd_vector.back().second;
}

inline std::vector<std::unique_ptr<TaskArg>> ToTaskArgs(JNIEnv *env, jobject args) {
  std::vector<std::unique_ptr<TaskArg>> task_args;
  JavaListToNativeVector<std::unique_ptr<TaskArg>>(
      env, args, &task_args, [](JNIEnv *env, jobject arg) {
        auto java_id = env->GetObjectField(arg, java_function_arg_id);
        if (java_id) {
          auto java_id_bytes = static_cast<jbyteArray>(
              env->CallObjectMethod(java_id, java_base_id_get_bytes));
          RAY_CHECK_JAVA_EXCEPTION(env);
          auto id = JavaByteArrayToId<ObjectID>(env, java_id_bytes);
          auto java_owner_address =
              env->GetObjectField(arg, java_function_arg_owner_address);
          RAY_CHECK(java_owner_address);
          auto owner_address = JavaProtobufObjectToNativeProtobufObject<rpc::Address>(
              env, java_owner_address);
          return std::unique_ptr<TaskArg>(
              new TaskArgByReference(id, owner_address, /*call_site=*/""));
        }
        auto java_value =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_function_arg_value));
        RAY_CHECK(java_value) << "Both id and value of FunctionArg are null.";
        auto value = JavaNativeRayObjectToNativeRayObject(env, java_value);
        return std::unique_ptr<TaskArg>(new TaskArgByValue(value));
      });
  return task_args;
}

inline std::unordered_map<std::string, double> ToResources(JNIEnv *env,
                                                           jobject java_resources) {
  return JavaMapToNativeMap<std::string, double>(
      env,
      java_resources,
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        double value = env->CallDoubleMethod(java_value, java_double_double_value);
        RAY_CHECK_JAVA_EXCEPTION(env);
        return value;
      });
}

inline std::pair<PlacementGroupID, int64_t> ToPlacementGroupOptions(JNIEnv *env,
                                                                    jobject callOptions) {
  auto placement_group_options = std::make_pair(PlacementGroupID::Nil(), -1);
  auto group = env->GetObjectField(callOptions, java_task_creation_options_group);
  if (group) {
    auto placement_group_id = env->GetObjectField(group, java_placement_group_id);
    auto java_id_bytes = static_cast<jbyteArray>(
        env->CallObjectMethod(placement_group_id, java_base_id_get_bytes));
    RAY_CHECK_JAVA_EXCEPTION(env);
    auto id = JavaByteArrayToId<PlacementGroupID>(env, java_id_bytes);
    auto index = env->GetIntField(callOptions, java_task_creation_options_bundle_index);
    placement_group_options = std::make_pair(id, index);
  }
  return placement_group_options;
}

inline TaskOptions ToTaskOptions(JNIEnv *env, jint numReturns, jobject callOptions) {
  std::unordered_map<std::string, double> resources;
  std::string name = "";
  std::string concurrency_group_name = "";
  std::string serialzied_runtime_env_info = "";

  if (callOptions) {
    jobject java_resources =
        env->GetObjectField(callOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    auto java_name = (jstring)env->GetObjectField(callOptions, java_call_options_name);
    if (java_name) {
      name = JavaStringToNativeString(env, java_name);
    }
    auto java_concurrency_group_name = reinterpret_cast<jstring>(
        env->GetObjectField(callOptions, java_call_options_concurrency_group_name));
    RAY_CHECK_JAVA_EXCEPTION(env);
    RAY_CHECK(java_concurrency_group_name != nullptr);
    if (java_concurrency_group_name) {
      concurrency_group_name = JavaStringToNativeString(env, java_concurrency_group_name);
    }

    auto java_serialized_runtime_env_info = reinterpret_cast<jstring>(
        env->GetObjectField(callOptions, java_call_options_serialized_runtime_env_info));
    RAY_CHECK_JAVA_EXCEPTION(env);
    RAY_CHECK(java_serialized_runtime_env_info != nullptr);
    if (java_serialized_runtime_env_info) {
      serialzied_runtime_env_info =
          JavaStringToNativeString(env, java_serialized_runtime_env_info);
    }
  }

  TaskOptions task_options{
      name, numReturns, resources, concurrency_group_name, serialzied_runtime_env_info};
  return task_options;
}

inline ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                   jobject actorCreationOptions) {
  std::string name = "";
  std::optional<bool> is_detached = std::nullopt;
  int64_t max_restarts = 0;
  int64_t max_task_retries = 0;
  std::unordered_map<std::string, double> resources;
  std::vector<std::string> dynamic_worker_options;
  uint64_t max_concurrency = 1;
  auto placement_options = std::make_pair(PlacementGroupID::Nil(), -1);
  std::vector<ConcurrencyGroup> concurrency_groups;
  std::string serialized_runtime_env = "";
  std::string ray_namespace = "";
  int32_t max_pending_calls = -1;
  bool is_async = false;

  if (actorCreationOptions) {
    auto java_name = (jstring)env->GetObjectField(actorCreationOptions,
                                                  java_actor_creation_options_name);
    if (java_name) {
      name = JavaStringToNativeString(env, java_name);
    }
    auto java_actor_lifetime = (jobject)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_lifetime);
    if (java_actor_lifetime != nullptr) {
      int java_actor_lifetime_ordinal_value =
          env->CallIntMethod(java_actor_lifetime, java_actor_lifetime_ordinal);
      is_detached = java_actor_lifetime_ordinal_value == DETACHED_LIFETIME_ORDINAL_VALUE;
    }

    max_restarts =
        env->GetIntField(actorCreationOptions, java_actor_creation_options_max_restarts);
    max_task_retries = env->GetIntField(actorCreationOptions,
                                        java_actor_creation_options_max_task_retries);
    jobject java_resources =
        env->GetObjectField(actorCreationOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    jobject java_jvm_options = env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_jvm_options);
    if (java_jvm_options) {
      JavaStringListToNativeStringVector(env, java_jvm_options, &dynamic_worker_options);
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
      auto id = JavaByteArrayToId<PlacementGroupID>(env, java_id_bytes);
      auto index = env->GetIntField(actorCreationOptions,
                                    java_actor_creation_options_bundle_index);
      placement_options = std::make_pair(id, index);
    }
    // Convert concurrency groups from Java to native.
    jobject java_concurrency_groups_field = env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_concurrency_groups);
    RAY_CHECK(java_concurrency_groups_field != nullptr);
    JavaListToNativeVector<ray::ConcurrencyGroup>(
        env,
        java_concurrency_groups_field,
        &concurrency_groups,
        [](JNIEnv *env, jobject java_concurrency_group_impl) {
          RAY_CHECK(java_concurrency_group_impl != nullptr);
          jobject java_func_descriptors =
              env->CallObjectMethod(java_concurrency_group_impl,
                                    java_concurrency_group_impl_get_function_descriptors);
          RAY_CHECK_JAVA_EXCEPTION(env);
          std::vector<ray::FunctionDescriptor> native_func_descriptors;
          JavaListToNativeVector<ray::FunctionDescriptor>(
              env,
              java_func_descriptors,
              &native_func_descriptors,
              [](JNIEnv *env, jobject java_func_descriptor) {
                RAY_CHECK(java_func_descriptor != nullptr);
                const jint hashcode = GetHashCodeOfJavaObject(env, java_func_descriptor);
                ray::FunctionDescriptor native_func =
                    ToRayFunction(env, java_func_descriptor, hashcode)
                        .GetFunctionDescriptor();
                return native_func;
              });
          // Put func_descriptors into this task group.
          const std::string concurrency_group_name = JavaStringToNativeString(
              env,
              (jstring)env->GetObjectField(java_concurrency_group_impl,
                                           java_concurrency_group_impl_name));
          const uint32_t max_concurrency = env->GetIntField(
              java_concurrency_group_impl, java_concurrency_group_impl_max_concurrency);
          return ray::ConcurrencyGroup{
              concurrency_group_name, max_concurrency, native_func_descriptors};
        });
    auto java_serialized_runtime_env = (jstring)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_serialized_runtime_env);
    if (java_serialized_runtime_env) {
      serialized_runtime_env = JavaStringToNativeString(env, java_serialized_runtime_env);
    }

    auto java_namespace = (jstring)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_namespace);
    if (java_namespace) {
      ray_namespace = JavaStringToNativeString(env, java_namespace);
    }

    max_pending_calls = static_cast<int32_t>(env->GetIntField(
        actorCreationOptions, java_actor_creation_options_max_pending_calls));
    is_async = (bool)env->GetBooleanField(actorCreationOptions,
                                          java_actor_creation_options_is_async);
  }

  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  if (!placement_options.first.IsNil()) {
    auto placement_group_scheduling_strategy =
        scheduling_strategy.mutable_placement_group_scheduling_strategy();
    placement_group_scheduling_strategy->set_placement_group_id(
        placement_options.first.Binary());
    placement_group_scheduling_strategy->set_placement_group_bundle_index(
        placement_options.second);
    placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
  }
  ActorCreationOptions actor_creation_options{max_restarts,
                                              max_task_retries,
                                              static_cast<int>(max_concurrency),
                                              resources,
                                              resources,
                                              dynamic_worker_options,
                                              is_detached,
                                              name,
                                              ray_namespace,
                                              is_async,
                                              /*scheduling_strategy=*/scheduling_strategy,
                                              serialized_runtime_env,
                                              concurrency_groups,
                                              /*execute_out_of_order*/ false,
                                              max_pending_calls};
  return actor_creation_options;
}

inline PlacementStrategy ConvertStrategy(jint java_strategy) {
  switch (java_strategy) {
  case 0:
    return rpc::PACK;
  case 1:
    return rpc::SPREAD;
  case 2:
    return rpc::STRICT_PACK;
  default:
    return rpc::STRICT_SPREAD;
  }
}

inline PlacementGroupCreationOptions ToPlacementGroupCreationOptions(
    JNIEnv *env, jobject placementGroupCreationOptions) {
  // We have make sure the placementGroupCreationOptions is not null in java api.
  std::string name = "";
  jstring java_name = (jstring)env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_name);
  if (java_name) {
    name = JavaStringToNativeString(env, java_name);
  }
  jobject java_obj_strategy = env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_strategy);
  jint java_strategy = env->CallIntMethod(
      java_obj_strategy, java_placement_group_creation_options_strategy_value);
  jobject java_bundles = env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_bundles);
  std::vector<std::unordered_map<std::string, double>> bundles;
  JavaListToNativeVector<std::unordered_map<std::string, double>>(
      env, java_bundles, &bundles, [](JNIEnv *env, jobject java_bundle) {
        return JavaMapToNativeMap<std::string, double>(
            env,
            java_bundle,
            [](JNIEnv *env, jobject java_key) {
              return JavaStringToNativeString(env, (jstring)java_key);
            },
            [](JNIEnv *env, jobject java_value) {
              double value = env->CallDoubleMethod(java_value, java_double_double_value);
              RAY_CHECK_JAVA_EXCEPTION(env);
              return value;
            });
      });
  return PlacementGroupCreationOptions(name,
                                       ConvertStrategy(java_strategy),
                                       bundles,
                                       /*is_detached=*/false,
                                       /*max_cpu_fraction_per_node*/ 1.0);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(JNIEnv *env,
                                                              jclass p,
                                                              jobject functionDescriptor,
                                                              jint functionDescriptorHash,
                                                              jobject args,
                                                              jint numReturns,
                                                              jobject callOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);
  auto placement_group_options = ToPlacementGroupOptions(env, callOptions);

  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  if (!placement_group_options.first.IsNil()) {
    auto placement_group_scheduling_strategy =
        scheduling_strategy.mutable_placement_group_scheduling_strategy();
    placement_group_scheduling_strategy->set_placement_group_id(
        placement_group_options.first.Binary());
    placement_group_scheduling_strategy->set_placement_group_bundle_index(
        placement_group_options.second);
    placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
  }
  // TODO (kfstorm): Allow setting `max_retries` via `CallOptions`.
  auto return_refs =
      CoreWorkerProcess::GetCoreWorker().SubmitTask(ray_function,
                                                    task_args,
                                                    task_options,
                                                    /*max_retries=*/0,
                                                    /*retry_exceptions=*/false,
                                                    /*scheduling_strategy=*/
                                                    scheduling_strategy,
                                                    /*debugger_breakpoint*/ "");
  std::vector<ObjectID> return_ids;
  for (const auto &ref : return_refs) {
    return_ids.push_back(ObjectID::FromBinary(ref.object_id()));
  }

  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(
    JNIEnv *env,
    jclass p,
    jobject functionDescriptor,
    jint functionDescriptorHash,
    jobject args,
    jobject actorCreationOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  ActorID actor_id;
  auto status = CoreWorkerProcess::GetCoreWorker().CreateActor(ray_function,
                                                               task_args,
                                                               actor_creation_options,
                                                               /*extension_data*/ "",
                                                               &actor_id);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ActorID>(env, actor_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(
    JNIEnv *env,
    jclass p,
    jbyteArray actorId,
    jobject functionDescriptor,
    jint functionDescriptorHash,
    jobject args,
    jint numReturns,
    jobject callOptions) {
  auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  RAY_CHECK(callOptions != nullptr);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);
  std::vector<rpc::ObjectReference> return_refs;
  auto status = CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      actor_id, ray_function, task_args, task_options, return_refs);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "The task " << ray_function.GetFunctionDescriptor()->ToString()
       << " could not be submitted to " << actor_id;
    ss << " because more than "
       << CoreWorkerProcess::GetCoreWorker().GetActorHandle(actor_id)->MaxPendingCalls();
    ss << " tasks are queued on the actor. This limit can be adjusted with the "
          "`setMaxPendingCalls` actor option.";
    env->ThrowNew(java_ray_pending_calls_limit_exceeded_exception_class,
                  ss.str().c_str());
    return nullptr;
  }

  std::vector<ObjectID> return_ids;
  for (const auto &ref : return_refs) {
    return_ids.push_back(ObjectID::FromBinary(ref.object_id()));
  }

  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreatePlacementGroup(
    JNIEnv *env, jclass, jobject placementGroupCreationOptions) {
  auto options = ToPlacementGroupCreationOptions(env, placementGroupCreationOptions);
  PlacementGroupID placement_group_id;
  auto status = CoreWorkerProcess::GetCoreWorker().CreatePlacementGroup(
      options, &placement_group_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<PlacementGroupID>(env, placement_group_id);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeRemovePlacementGroup(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes) {
  const auto placement_group_id =
      JavaByteArrayToId<PlacementGroupID>(env, placement_group_id_bytes);
  auto status =
      CoreWorkerProcess::GetCoreWorker().RemovePlacementGroup(placement_group_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeWaitPlacementGroupReady(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes, jint timeout_seconds) {
  const auto placement_group_id =
      JavaByteArrayToId<PlacementGroupID>(env, placement_group_id_bytes);
  auto status = CoreWorkerProcess::GetCoreWorker().WaitPlacementGroupReady(
      placement_group_id, timeout_seconds);
  if (status.IsNotFound()) {
    env->ThrowNew(java_ray_exception_class, status.message().c_str());
  }
  return status.ok();
}

#ifdef __cplusplus
}
#endif
