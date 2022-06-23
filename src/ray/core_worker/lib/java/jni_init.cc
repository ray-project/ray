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

#include "jni_utils.h"

jclass java_boolean_class;
jmethodID java_boolean_init;

jclass java_double_class;
jmethodID java_double_double_value;

jclass java_long_class;
jmethodID java_long_init;

jclass java_object_class;
jmethodID java_object_equals;
jmethodID java_object_hash_code;

jclass java_weak_reference_class;
jmethodID java_weak_reference_init;
jmethodID java_weak_reference_get;

jclass java_list_class;
jmethodID java_list_size;
jmethodID java_list_get;
jmethodID java_list_add;

jclass java_array_list_class;
jmethodID java_array_list_init;
jmethodID java_array_list_init_with_capacity;

jclass java_map_class;
jmethodID java_map_entry_set;
jmethodID java_map_put;

jclass java_hash_map_class;
jmethodID java_hash_map_init;

jclass java_set_class;
jmethodID java_set_iterator;

jclass java_iterator_class;
jmethodID java_iterator_has_next;
jmethodID java_iterator_next;

jclass java_map_entry_class;
jmethodID java_map_entry_get_key;
jmethodID java_map_entry_get_value;

jclass java_system_class;
jmethodID java_system_gc;

jclass java_ray_exception_class;
jclass java_ray_intentional_system_exit_exception_class;
jclass java_ray_timeout_exception_class;

jclass java_ray_pending_calls_limit_exceeded_exception_class;

jclass java_ray_actor_exception_class;
jclass java_ray_exception_serializer_class;
jmethodID java_ray_exception_serializer_to_bytes;

jclass java_jni_exception_util_class;
jmethodID java_jni_exception_util_get_stack_trace;

jclass java_base_id_class;
jmethodID java_base_id_get_bytes;

jclass java_abstract_message_lite_class;
jmethodID java_abstract_message_lite_to_byte_array;

jclass java_function_descriptor_class;
jmethodID java_function_descriptor_get_language;
jmethodID java_function_descriptor_to_list;

jclass java_language_class;
jmethodID java_language_get_number;

jclass java_function_arg_class;
jfieldID java_function_arg_id;
jfieldID java_function_arg_owner_address;
jfieldID java_function_arg_value;

jclass java_base_task_options_class;
jfieldID java_base_task_options_resources;

jclass java_call_options_class;
jfieldID java_call_options_name;
jfieldID java_task_creation_options_group;
jfieldID java_task_creation_options_bundle_index;
jfieldID java_call_options_concurrency_group_name;
jfieldID java_call_options_serialized_runtime_env_info;

jclass java_actor_creation_options_class;
jfieldID java_actor_creation_options_name;
jfieldID java_actor_creation_options_lifetime;
jfieldID java_actor_creation_options_max_restarts;
jfieldID java_actor_creation_options_jvm_options;
jfieldID java_actor_creation_options_max_concurrency;
jfieldID java_actor_creation_options_group;
jfieldID java_actor_creation_options_bundle_index;
jfieldID java_actor_creation_options_concurrency_groups;
jfieldID java_actor_creation_options_serialized_runtime_env;
jfieldID java_actor_creation_options_max_pending_calls;

jclass java_actor_lifetime_class;
int DETACHED_LIFETIME_ORDINAL_VALUE;
jmethodID java_actor_lifetime_ordinal;

jclass java_placement_group_creation_options_class;
jclass java_placement_group_creation_options_strategy_class;
jfieldID java_placement_group_creation_options_name;
jfieldID java_placement_group_creation_options_bundles;
jfieldID java_placement_group_creation_options_strategy;
jmethodID java_placement_group_creation_options_strategy_value;

jclass java_gcs_client_options_class;
jfieldID java_gcs_client_options_ip;
jfieldID java_gcs_client_options_port;
jfieldID java_gcs_client_options_password;

jclass java_native_ray_object_class;
jmethodID java_native_ray_object_init;
jfieldID java_native_ray_object_data;
jfieldID java_native_ray_object_metadata;
jfieldID java_native_ray_object_contained_object_ids;

jclass java_task_executor_class;
jmethodID java_task_executor_parse_function_arguments;
jmethodID java_task_executor_execute;

jclass java_concurrency_group_impl_class;
jmethodID java_concurrency_group_impl_get_function_descriptors;
jfieldID java_concurrency_group_impl_name;
jfieldID java_concurrency_group_impl_max_concurrency;

jclass java_native_task_executor_class;
jmethodID java_native_task_executor_on_worker_shutdown;

jclass java_placement_group_class;
jfieldID java_placement_group_id;

jclass java_object_ref_impl_class;
jmethodID java_object_ref_impl_class_on_memory_store_object_allocated;

jclass java_resource_value_class;
jmethodID java_resource_value_init;

JavaVM *jvm;

inline jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
  if (tempLocalClassRef == nullptr) {
    const std::string shaded_class_prefix = "io/ray/shaded/";
    const auto class_name_str = std::string(class_name);
    const auto this_prefix = class_name_str.substr(0, shaded_class_prefix.size());
    if (this_prefix == shaded_class_prefix) {
      // This is a shaded class, and try to load the original class.
      env->ExceptionClear();
      auto no_shaded_class_name =
          class_name_str.substr(shaded_class_prefix.size(), class_name_str.size());
      tempLocalClassRef = env->FindClass(no_shaded_class_name.c_str());
    }
  }
  RAY_CHECK(tempLocalClassRef) << "Can't load Java class " << class_name;

  jclass ret = (jclass)env->NewGlobalRef(tempLocalClassRef);
  RAY_CHECK(ret) << "Can't load Java class " << class_name;
  env->DeleteLocalRef(tempLocalClassRef);
  return ret;
}

/// Load and cache frequently-used Java classes and methods
jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), CURRENT_JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  jvm = vm;

  java_boolean_class = LoadClass(env, "java/lang/Boolean");
  java_boolean_init = env->GetMethodID(java_boolean_class, "<init>", "(Z)V");

  java_double_class = LoadClass(env, "java/lang/Double");
  java_double_double_value = env->GetMethodID(java_double_class, "doubleValue", "()D");

  java_long_class = LoadClass(env, "java/lang/Long");
  java_long_init = env->GetMethodID(java_long_class, "<init>", "(J)V");

  java_object_class = LoadClass(env, "java/lang/Object");
  java_object_equals =
      env->GetMethodID(java_object_class, "equals", "(Ljava/lang/Object;)Z");
  java_object_hash_code = env->GetMethodID(java_object_class, "hashCode", "()I");

  java_weak_reference_class = LoadClass(env, "java/lang/ref/WeakReference");
  java_weak_reference_init =
      env->GetMethodID(java_weak_reference_class, "<init>", "(Ljava/lang/Object;)V");
  java_weak_reference_get =
      env->GetMethodID(java_weak_reference_class, "get", "()Ljava/lang/Object;");

  java_list_class = LoadClass(env, "java/util/List");
  java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  java_list_get = env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  java_list_add = env->GetMethodID(java_list_class, "add", "(Ljava/lang/Object;)Z");

  java_array_list_class = LoadClass(env, "java/util/ArrayList");
  java_array_list_init = env->GetMethodID(java_array_list_class, "<init>", "()V");
  java_array_list_init_with_capacity =
      env->GetMethodID(java_array_list_class, "<init>", "(I)V");

  java_map_class = LoadClass(env, "java/util/Map");
  java_map_entry_set = env->GetMethodID(java_map_class, "entrySet", "()Ljava/util/Set;");
  java_map_put = env->GetMethodID(
      java_map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

  java_hash_map_class = LoadClass(env, "java/util/HashMap");
  java_hash_map_init = env->GetMethodID(java_hash_map_class, "<init>", "()V");

  java_set_class = LoadClass(env, "java/util/Set");
  java_set_iterator =
      env->GetMethodID(java_set_class, "iterator", "()Ljava/util/Iterator;");

  java_iterator_class = LoadClass(env, "java/util/Iterator");
  java_iterator_has_next = env->GetMethodID(java_iterator_class, "hasNext", "()Z");
  java_iterator_next =
      env->GetMethodID(java_iterator_class, "next", "()Ljava/lang/Object;");

  java_map_entry_class = LoadClass(env, "java/util/Map$Entry");
  java_map_entry_get_key =
      env->GetMethodID(java_map_entry_class, "getKey", "()Ljava/lang/Object;");
  java_map_entry_get_value =
      env->GetMethodID(java_map_entry_class, "getValue", "()Ljava/lang/Object;");

  java_system_class = LoadClass(env, "java/lang/System");
  java_system_gc = env->GetStaticMethodID(java_system_class, "gc", "()V");

  java_ray_exception_class = LoadClass(env, "io/ray/api/exception/RayException");
  java_ray_intentional_system_exit_exception_class =
      LoadClass(env, "io/ray/api/exception/RayIntentionalSystemExitException");

  java_ray_timeout_exception_class =
      LoadClass(env, "io/ray/api/exception/RayTimeoutException");

  java_ray_actor_exception_class =
      LoadClass(env, "io/ray/api/exception/RayActorException");

  java_ray_pending_calls_limit_exceeded_exception_class =
      LoadClass(env, "io/ray/api/exception/PendingCallsLimitExceededException");

  java_ray_exception_serializer_class =
      LoadClass(env, "io/ray/runtime/serializer/RayExceptionSerializer");
  java_ray_exception_serializer_to_bytes =
      env->GetStaticMethodID(java_ray_exception_serializer_class,
                             "toBytes",
                             "(Lio/ray/api/exception/RayException;)[B");

  java_jni_exception_util_class = LoadClass(env, "io/ray/runtime/util/JniExceptionUtil");
  java_jni_exception_util_get_stack_trace = env->GetStaticMethodID(
      java_jni_exception_util_class,
      "getStackTrace",
      "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/Throwable;)Ljava/lang/String;");

  java_base_id_class = LoadClass(env, "io/ray/api/id/BaseId");
  java_base_id_get_bytes = env->GetMethodID(java_base_id_class, "getBytes", "()[B");

  java_abstract_message_lite_class =
      LoadClass(env, "io/ray/shaded/com/google/protobuf/AbstractMessage");

  java_abstract_message_lite_to_byte_array =
      env->GetMethodID(java_abstract_message_lite_class, "toByteArray", "()[B");

  java_function_descriptor_class =
      LoadClass(env, "io/ray/runtime/functionmanager/FunctionDescriptor");
  java_function_descriptor_get_language =
      env->GetMethodID(java_function_descriptor_class,
                       "getLanguage",
                       "()Lio/ray/runtime/generated/Common$Language;");
  java_function_descriptor_to_list =
      env->GetMethodID(java_function_descriptor_class, "toList", "()Ljava/util/List;");

  java_language_class = LoadClass(env, "io/ray/runtime/generated/Common$Language");
  java_language_get_number = env->GetMethodID(java_language_class, "getNumber", "()I");

  java_function_arg_class = LoadClass(env, "io/ray/runtime/task/FunctionArg");
  java_function_arg_id =
      env->GetFieldID(java_function_arg_class, "id", "Lio/ray/api/id/ObjectId;");
  java_function_arg_owner_address =
      env->GetFieldID(java_function_arg_class,
                      "ownerAddress",
                      "Lio/ray/runtime/generated/Common$Address;");
  java_function_arg_value = env->GetFieldID(
      java_function_arg_class, "value", "Lio/ray/runtime/object/NativeRayObject;");

  java_base_task_options_class = LoadClass(env, "io/ray/api/options/BaseTaskOptions");
  java_base_task_options_resources =
      env->GetFieldID(java_base_task_options_class, "resources", "Ljava/util/Map;");

  java_call_options_class = LoadClass(env, "io/ray/api/options/CallOptions");
  java_call_options_name =
      env->GetFieldID(java_call_options_class, "name", "Ljava/lang/String;");
  java_task_creation_options_group = env->GetFieldID(
      java_call_options_class, "group", "Lio/ray/api/placementgroup/PlacementGroup;");
  java_task_creation_options_bundle_index =
      env->GetFieldID(java_call_options_class, "bundleIndex", "I");
  java_call_options_concurrency_group_name = env->GetFieldID(
      java_call_options_class, "concurrencyGroupName", "Ljava/lang/String;");
  java_call_options_serialized_runtime_env_info = env->GetFieldID(
      java_call_options_class, "serializedRuntimeEnvInfo", "Ljava/lang/String;");

  java_placement_group_class =
      LoadClass(env, "io/ray/runtime/placementgroup/PlacementGroupImpl");
  java_placement_group_id = env->GetFieldID(
      java_placement_group_class, "id", "Lio/ray/api/id/PlacementGroupId;");

  java_placement_group_creation_options_class =
      LoadClass(env, "io/ray/api/options/PlacementGroupCreationOptions");
  java_placement_group_creation_options_strategy_class =
      LoadClass(env, "io/ray/api/placementgroup/PlacementStrategy");
  java_placement_group_creation_options_name = env->GetFieldID(
      java_placement_group_creation_options_class, "name", "Ljava/lang/String;");
  java_placement_group_creation_options_bundles = env->GetFieldID(
      java_placement_group_creation_options_class, "bundles", "Ljava/util/List;");
  java_placement_group_creation_options_strategy =
      env->GetFieldID(java_placement_group_creation_options_class,
                      "strategy",
                      "Lio/ray/api/placementgroup/PlacementStrategy;");
  java_placement_group_creation_options_strategy_value = env->GetMethodID(
      java_placement_group_creation_options_strategy_class, "value", "()I");

  java_actor_creation_options_class =
      LoadClass(env, "io/ray/api/options/ActorCreationOptions");
  java_actor_creation_options_name =
      env->GetFieldID(java_actor_creation_options_class, "name", "Ljava/lang/String;");
  java_actor_creation_options_lifetime =
      env->GetFieldID(java_actor_creation_options_class,
                      "lifetime",
                      "Lio/ray/api/options/ActorLifetime;");
  java_actor_creation_options_max_restarts =
      env->GetFieldID(java_actor_creation_options_class, "maxRestarts", "I");
  java_actor_creation_options_jvm_options = env->GetFieldID(
      java_actor_creation_options_class, "jvmOptions", "Ljava/util/List;");
  java_actor_creation_options_max_concurrency =
      env->GetFieldID(java_actor_creation_options_class, "maxConcurrency", "I");
  java_actor_creation_options_group =
      env->GetFieldID(java_actor_creation_options_class,
                      "group",
                      "Lio/ray/api/placementgroup/PlacementGroup;");
  java_actor_creation_options_bundle_index =
      env->GetFieldID(java_actor_creation_options_class, "bundleIndex", "I");
  java_actor_creation_options_concurrency_groups = env->GetFieldID(
      java_actor_creation_options_class, "concurrencyGroups", "Ljava/util/List;");
  java_actor_creation_options_serialized_runtime_env = env->GetFieldID(
      java_actor_creation_options_class, "serializedRuntimeEnv", "Ljava/lang/String;");
  java_actor_creation_options_max_pending_calls =
      env->GetFieldID(java_actor_creation_options_class, "maxPendingCalls", "I");

  java_actor_lifetime_class = LoadClass(env, "io/ray/api/options/ActorLifetime");
  java_actor_lifetime_ordinal =
      env->GetMethodID(java_actor_lifetime_class, "ordinal", "()I");
  jfieldID java_actor_lifetime_detached_field = env->GetStaticFieldID(
      java_actor_lifetime_class, "DETACHED", "Lio/ray/api/options/ActorLifetime;");
  jobject status_detached = env->GetStaticObjectField(java_actor_lifetime_class,
                                                      java_actor_lifetime_detached_field);
  DETACHED_LIFETIME_ORDINAL_VALUE =
      env->CallIntMethod(status_detached, java_actor_lifetime_ordinal);

  java_concurrency_group_impl_class =
      LoadClass(env, "io/ray/runtime/ConcurrencyGroupImpl");
  java_concurrency_group_impl_get_function_descriptors = env->GetMethodID(
      java_concurrency_group_impl_class, "getFunctionDescriptors", "()Ljava/util/List;");
  java_concurrency_group_impl_name =
      env->GetFieldID(java_concurrency_group_impl_class, "name", "Ljava/lang/String;");
  java_concurrency_group_impl_max_concurrency =
      env->GetFieldID(java_concurrency_group_impl_class, "maxConcurrency", "I");

  java_gcs_client_options_class = LoadClass(env, "io/ray/runtime/gcs/GcsClientOptions");
  java_gcs_client_options_ip =
      env->GetFieldID(java_gcs_client_options_class, "ip", "Ljava/lang/String;");
  java_gcs_client_options_port =
      env->GetFieldID(java_gcs_client_options_class, "port", "I");
  java_gcs_client_options_password =
      env->GetFieldID(java_gcs_client_options_class, "password", "Ljava/lang/String;");

  java_native_ray_object_class = LoadClass(env, "io/ray/runtime/object/NativeRayObject");
  java_native_ray_object_init =
      env->GetMethodID(java_native_ray_object_class, "<init>", "([B[B)V");
  java_native_ray_object_data =
      env->GetFieldID(java_native_ray_object_class, "data", "[B");
  java_native_ray_object_metadata =
      env->GetFieldID(java_native_ray_object_class, "metadata", "[B");
  java_native_ray_object_contained_object_ids = env->GetFieldID(
      java_native_ray_object_class, "containedObjectIds", "Ljava/util/List;");

  java_task_executor_class = LoadClass(env, "io/ray/runtime/task/TaskExecutor");
  java_task_executor_parse_function_arguments = env->GetMethodID(
      java_task_executor_class, "checkByteBufferArguments", "(Ljava/util/List;)[Z");
  java_task_executor_execute =
      env->GetMethodID(java_task_executor_class,
                       "execute",
                       "(Ljava/util/List;Ljava/util/List;)Ljava/util/List;");
  java_native_task_executor_class =
      LoadClass(env, "io/ray/runtime/task/NativeTaskExecutor");
  java_native_task_executor_on_worker_shutdown =
      env->GetMethodID(java_native_task_executor_class, "onWorkerShutdown", "([B)V");

  java_object_ref_impl_class = LoadClass(env, "io/ray/runtime/object/ObjectRefImpl");
  java_object_ref_impl_class_on_memory_store_object_allocated = env->GetStaticMethodID(
      java_object_ref_impl_class, "onMemoryStoreObjectAllocated", "([B[B)V");

  java_resource_value_class = LoadClass(env, "io/ray/api/runtimecontext/ResourceValue");
  java_resource_value_init =
      env->GetMethodID(java_resource_value_class, "<init>", "(JD)V");
  return CURRENT_JNI_VERSION;
}

/// Unload java classes
void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  vm->GetEnv(reinterpret_cast<void **>(&env), CURRENT_JNI_VERSION);

  env->DeleteGlobalRef(java_boolean_class);
  env->DeleteGlobalRef(java_double_class);
  env->DeleteGlobalRef(java_object_class);
  env->DeleteGlobalRef(java_weak_reference_class);
  env->DeleteGlobalRef(java_long_class);
  env->DeleteGlobalRef(java_list_class);
  env->DeleteGlobalRef(java_array_list_class);
  env->DeleteGlobalRef(java_map_class);
  env->DeleteGlobalRef(java_hash_map_class);
  env->DeleteGlobalRef(java_set_class);
  env->DeleteGlobalRef(java_iterator_class);
  env->DeleteGlobalRef(java_map_entry_class);
  env->DeleteGlobalRef(java_system_class);
  env->DeleteGlobalRef(java_ray_exception_class);
  env->DeleteGlobalRef(java_ray_intentional_system_exit_exception_class);
  env->DeleteGlobalRef(java_ray_timeout_exception_class);
  env->DeleteGlobalRef(java_ray_actor_exception_class);
  env->DeleteGlobalRef(java_ray_exception_serializer_class);
  env->DeleteGlobalRef(java_jni_exception_util_class);
  env->DeleteGlobalRef(java_base_id_class);
  env->DeleteGlobalRef(java_abstract_message_lite_class);
  env->DeleteGlobalRef(java_function_descriptor_class);
  env->DeleteGlobalRef(java_language_class);
  env->DeleteGlobalRef(java_function_arg_class);
  env->DeleteGlobalRef(java_base_task_options_class);
  env->DeleteGlobalRef(java_actor_creation_options_class);
  env->DeleteGlobalRef(java_actor_lifetime_class);
  env->DeleteGlobalRef(java_placement_group_creation_options_class);
  env->DeleteGlobalRef(java_placement_group_creation_options_strategy_class);
  env->DeleteGlobalRef(java_native_ray_object_class);
  env->DeleteGlobalRef(java_task_executor_class);
  env->DeleteGlobalRef(java_native_task_executor_class);
  env->DeleteGlobalRef(java_concurrency_group_impl_class);
  env->DeleteGlobalRef(java_resource_value_class);
}
