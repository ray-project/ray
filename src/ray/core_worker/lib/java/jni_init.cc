#include "ray/core_worker/lib/java/jni_utils.h"

jclass java_boolean_class;
jmethodID java_boolean_init;

jclass java_double_class;
jfieldID java_double_value;

jclass java_list_class;
jmethodID java_list_size;
jmethodID java_list_get;
jmethodID java_list_add;

jclass java_array_list_class;
jmethodID java_array_list_init;
jmethodID java_array_list_init_with_capacity;

jclass java_ray_exception_class;

jclass java_native_ray_function_class;
jfieldID java_native_ray_function_language;
jfieldID java_native_ray_function_function_descriptor;

jclass java_native_task_arg_class;
jfieldID java_native_task_arg_id;
jfieldID java_native_task_arg_data;

jclass java_native_resources_class;
jfieldID java_native_resources_keys;
jfieldID java_native_resources_values;

jclass java_native_task_options_class;
jfieldID java_native_task_options_num_returns;
jfieldID java_native_task_options_resources;

jclass java_native_actor_creation_options_class;
jfieldID java_native_actor_creation_options_max_reconstructions;
jfieldID java_native_actor_creation_options_is_direct_call;
jfieldID java_native_actor_creation_options_resources;
jfieldID java_native_actor_creation_options_dynamic_worker_options;

jclass java_native_gcs_client_options_class;
jfieldID java_native_gcs_client_options_ip;
jfieldID java_native_gcs_client_options_port;
jfieldID java_native_gcs_client_options_password;
jfieldID java_native_gcs_client_options_is_test_client;

jclass java_native_ray_object_class;
jmethodID java_native_ray_object_init;
jfieldID java_native_ray_object_data;
jfieldID java_native_ray_object_metadata;

jclass java_worker_class;
jmethodID java_worker_impl_execute;

JavaVM *jvm;

inline jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
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
  java_double_value = env->GetFieldID(java_double_class, "value", "D");

  java_list_class = LoadClass(env, "java/util/List");
  java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  java_list_get = env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  java_list_add = env->GetMethodID(java_list_class, "add", "(Ljava/lang/Object;)Z");

  java_array_list_class = LoadClass(env, "java/util/ArrayList");
  java_array_list_init = env->GetMethodID(java_array_list_class, "<init>", "()V");
  java_array_list_init_with_capacity =
      env->GetMethodID(java_array_list_class, "<init>", "(I)V");

  java_ray_exception_class = LoadClass(env, "org/ray/api/exception/RayException");

  java_native_ray_function_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeRayFunction");
  java_native_ray_function_language =
      env->GetFieldID(java_native_ray_function_class, "language", "I");
  java_native_ray_function_function_descriptor = env->GetFieldID(
      java_native_ray_function_class, "functionDescriptor", "Ljava/util/List;");

  java_native_task_arg_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeTaskArg");
  java_native_task_arg_id = env->GetFieldID(java_native_task_arg_class, "id", "[B");
  java_native_task_arg_data = env->GetFieldID(java_native_task_arg_class, "data", "[B");

  java_native_resources_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeResources");
  java_native_resources_keys =
      env->GetFieldID(java_native_resources_class, "keys", "Ljava/util/List;");
  java_native_resources_values =
      env->GetFieldID(java_native_resources_class, "values", "Ljava/util/List;");

  java_native_task_options_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeTaskOptions");
  java_native_task_options_num_returns =
      env->GetFieldID(java_native_task_options_class, "numReturns", "I");
  java_native_task_options_resources =
      env->GetFieldID(java_native_task_options_class, "resources",
                      "Lorg/ray/runtime/nativeTypes/NativeResources;");

  java_native_actor_creation_options_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeActorCreationOptions");
  java_native_actor_creation_options_max_reconstructions = env->GetFieldID(
      java_native_actor_creation_options_class, "maxReconstructions", "J");
  java_native_actor_creation_options_resources =
  java_native_actor_creation_options_is_direct_call = env->GetFieldID(
      java_native_actor_creation_options_class, "isDirectCall", "Z");
  java_native_actor_creation_options_resources =
      env->GetFieldID(java_native_actor_creation_options_class, "resources",
                      "Lorg/ray/runtime/nativeTypes/NativeResources;");
  java_native_actor_creation_options_dynamic_worker_options =
      env->GetFieldID(java_native_actor_creation_options_class, "dynamicWorkerOptions",
                      "Ljava/util/List;");

  java_native_gcs_client_options_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeGcsClientOptions");
  java_native_gcs_client_options_ip = env->GetFieldID(
      java_native_gcs_client_options_class, "ip", "Ljava/lang/String;");
  java_native_gcs_client_options_port = env->GetFieldID(
      java_native_gcs_client_options_class, "port", "I");
  java_native_gcs_client_options_password = env->GetFieldID(
      java_native_gcs_client_options_class, "password", "Ljava/lang/String;");
  java_native_gcs_client_options_is_test_client = env->GetFieldID(
      java_native_gcs_client_options_class, "isTestClient", "Z");

  java_native_ray_object_class =
      LoadClass(env, "org/ray/runtime/nativeTypes/NativeRayObject");
  java_native_ray_object_init =
      env->GetMethodID(java_native_ray_object_class, "<init>", "([B[B)V");
  java_native_ray_object_data =
      env->GetFieldID(java_native_ray_object_class, "data", "[B");
  java_native_ray_object_metadata =
      env->GetFieldID(java_native_ray_object_class, "metadata", "[B");

  java_worker_class = LoadClass(env, "org/ray/runtime/WorkerImpl");
  java_worker_impl_execute =
      env->GetMethodID(java_worker_class, "execute",
                       "(Ljava/util/List;Ljava/util/List;)Ljava/util/List;");

  return CURRENT_JNI_VERSION;
}

/// Unload java classes
void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  vm->GetEnv(reinterpret_cast<void **>(&env), CURRENT_JNI_VERSION);

  env->DeleteGlobalRef(java_boolean_class);
  env->DeleteGlobalRef(java_double_class);
  env->DeleteGlobalRef(java_list_class);
  env->DeleteGlobalRef(java_array_list_class);
  env->DeleteGlobalRef(java_ray_exception_class);
  env->DeleteGlobalRef(java_native_ray_function_class);
  env->DeleteGlobalRef(java_native_task_arg_class);
  env->DeleteGlobalRef(java_native_resources_class);
  env->DeleteGlobalRef(java_native_task_options_class);
  env->DeleteGlobalRef(java_native_actor_creation_options_class);
  env->DeleteGlobalRef(java_native_ray_object_class);
  env->DeleteGlobalRef(java_worker_class);
}
