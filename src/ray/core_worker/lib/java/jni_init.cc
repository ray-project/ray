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

jclass java_ray_function_proxy_class;
jfieldID java_ray_function_proxy_language;
jfieldID java_ray_function_proxy_function_descriptor;

jclass java_task_arg_proxy_class;
jfieldID java_task_arg_proxy_id;
jfieldID java_task_arg_proxy_data;

jclass java_resources_proxy_class;
jfieldID java_resources_proxy_keys;
jfieldID java_resources_proxy_values;

jclass java_task_options_proxy_class;
jfieldID java_task_options_proxy_num_returns;
jfieldID java_task_options_proxy_resources;

jclass java_actor_creation_options_proxy_class;
jfieldID java_actor_creation_options_proxy_max_reconstructions;
jfieldID java_actor_creation_options_proxy_resources;
jfieldID java_actor_creation_options_proxy_dynamic_worker_options;

jclass java_native_ray_object_class;
jmethodID java_native_ray_object_init;
jfieldID java_native_ray_object_data;
jfieldID java_native_ray_object_metadata;

jclass java_worker_class;
jmethodID java_worker_run_task_callback;

JavaVM *jvm;

inline jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
  jclass ret = (jclass)env->NewGlobalRef(tempLocalClassRef);
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

  java_ray_function_proxy_class =
      LoadClass(env, "org/ray/runtime/proxyTypes/RayFunctionProxy");
  java_ray_function_proxy_language =
      env->GetFieldID(java_ray_function_proxy_class, "language", "I");
  java_ray_function_proxy_function_descriptor = env->GetFieldID(
      java_ray_function_proxy_class, "functionDescriptor", "Ljava/util/List;");

  java_task_arg_proxy_class = LoadClass(env, "org/ray/runtime/proxyTypes/TaskArgProxy");
  java_task_arg_proxy_id = env->GetFieldID(java_task_arg_proxy_class, "id", "[B");
  java_task_arg_proxy_data = env->GetFieldID(java_task_arg_proxy_class, "data", "[B");

  java_resources_proxy_class =
      LoadClass(env, "org/ray/runtime/proxyTypes/ResourcesProxy");
  java_resources_proxy_keys =
      env->GetFieldID(java_resources_proxy_class, "keys", "Ljava/util/List;");
  java_resources_proxy_values =
      env->GetFieldID(java_resources_proxy_class, "values", "Ljava/util/List;");

  java_task_options_proxy_class =
      LoadClass(env, "org/ray/runtime/proxyTypes/TaskOptionsProxy");
  java_task_options_proxy_num_returns =
      env->GetFieldID(java_task_options_proxy_class, "numReturns", "I");
  java_task_options_proxy_resources =
      env->GetFieldID(java_task_options_proxy_class, "resources",
                      "Lorg/ray/runtime/proxyTypes/ResourcesProxy;");

  java_actor_creation_options_proxy_class =
      LoadClass(env, "org/ray/runtime/proxyTypes/ActorCreationOptionsProxy");
  java_actor_creation_options_proxy_max_reconstructions =
      env->GetFieldID(java_actor_creation_options_proxy_class, "maxReconstructions", "J");
  java_actor_creation_options_proxy_resources =
      env->GetFieldID(java_actor_creation_options_proxy_class, "resources",
                      "Lorg/ray/runtime/proxyTypes/ResourcesProxy;");
  java_actor_creation_options_proxy_dynamic_worker_options =
      env->GetFieldID(java_actor_creation_options_proxy_class, "dynamicWorkerOptions",
                      "Ljava/util/List;");

  java_native_ray_object_class =
      LoadClass(env, "org/ray/runtime/objectstore/NativeRayObject");
  java_native_ray_object_init =
      env->GetMethodID(java_native_ray_object_class, "<init>", "([B[B)V");
  java_native_ray_object_data =
      env->GetFieldID(java_native_ray_object_class, "data", "[B");
  java_native_ray_object_metadata =
      env->GetFieldID(java_native_ray_object_class, "metadata", "[B");

  java_worker_class = LoadClass(env, "org/ray/runtime/Worker");
  java_worker_run_task_callback = env->GetMethodID(
      java_worker_class, "runTaskCallback",
      "(Ljava/util/List;Ljava/util/List;)Lorg/ray/runtime/objectstore/NativeRayObject;");

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
  env->DeleteGlobalRef(java_ray_function_proxy_class);
  env->DeleteGlobalRef(java_task_arg_proxy_class);
  env->DeleteGlobalRef(java_resources_proxy_class);
  env->DeleteGlobalRef(java_task_options_proxy_class);
  env->DeleteGlobalRef(java_actor_creation_options_proxy_class);
  env->DeleteGlobalRef(java_native_ray_object_class);
  env->DeleteGlobalRef(java_worker_class);
}
