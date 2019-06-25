#include "ray/core_worker/lib/java/jni_helper.h"

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

jclass java_ray_function_proxy_class;
jfieldID java_ray_function_proxy_worker_language;
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

jclass java_ray_object_value_proxy_class;
jmethodID java_ray_object_value_proxy_init;
jfieldID java_ray_object_value_proxy_data;
jfieldID java_ray_object_value_proxy_metadata;

jint JNI_VERSION = JNI_VERSION_1_8;

#define LOAD_CLASS(variable_name, class_name)                     \
  {                                                               \
    jclass tempLocalClassRef;                                     \
    tempLocalClassRef = env->FindClass(class_name);               \
    variable_name = (jclass)env->NewGlobalRef(tempLocalClassRef); \
    env->DeleteLocalRef(tempLocalClassRef);                       \
  }

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  LOAD_CLASS(java_boolean_class, "java/lang/Boolean");
  java_boolean_init = env->GetMethodID(java_boolean_class, "<init>", "(Z)V");

  LOAD_CLASS(java_double_class, "java/lang/Double");
  java_double_value = env->GetFieldID(java_double_class, "value", "D");

  LOAD_CLASS(java_list_class, "java/util/List");
  java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  java_list_get = env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  java_list_add = env->GetMethodID(java_list_class, "add", "(Ljava/lang/Object;)Z");

  LOAD_CLASS(java_array_list_class, "java/util/ArrayList");
  java_array_list_init = env->GetMethodID(java_array_list_class, "<init>", "()V");
  java_array_list_init_with_capacity =
      env->GetMethodID(java_array_list_class, "<init>", "(I)V");

  LOAD_CLASS(java_ray_function_proxy_class,
             "org/ray/runtime/proxyTypes/RayFunctionProxy");
  java_ray_function_proxy_worker_language =
      env->GetFieldID(java_ray_function_proxy_class, "workerLanguage", "I");
  java_ray_function_proxy_function_descriptor = env->GetFieldID(
      java_ray_function_proxy_class, "functionDescriptor", "Ljava/util/List;");

  LOAD_CLASS(java_task_arg_proxy_class, "org/ray/runtime/proxyTypes/TaskArgProxy");
  java_task_arg_proxy_id = env->GetFieldID(java_task_arg_proxy_class, "id", "[B");
  java_task_arg_proxy_data = env->GetFieldID(java_task_arg_proxy_class, "data", "[B");

  LOAD_CLASS(java_resources_proxy_class, "org/ray/runtime/proxyTypes/ResourcesProxy");
  java_resources_proxy_keys =
      env->GetFieldID(java_resources_proxy_class, "keys", "Ljava/util/List;");
  java_resources_proxy_values =
      env->GetFieldID(java_resources_proxy_class, "values", "Ljava/util/List;");

  LOAD_CLASS(java_task_options_proxy_class,
             "org/ray/runtime/proxyTypes/TaskOptionsProxy");
  java_task_options_proxy_num_returns =
      env->GetFieldID(java_task_options_proxy_class, "numReturns", "I");
  java_task_options_proxy_resources =
      env->GetFieldID(java_task_options_proxy_class, "resources",
                      "Lorg/ray/runtime/proxyTypes/ResourcesProxy;");

  LOAD_CLASS(java_actor_creation_options_proxy_class,
             "org/ray/runtime/proxyTypes/ActorCreationOptionsProxy");
  java_actor_creation_options_proxy_max_reconstructions =
      env->GetFieldID(java_actor_creation_options_proxy_class, "maxReconstructions", "J");
  java_actor_creation_options_proxy_resources =
      env->GetFieldID(java_actor_creation_options_proxy_class, "resources",
                      "Lorg/ray/runtime/proxyTypes/ResourcesProxy;");

  LOAD_CLASS(java_ray_object_value_proxy_class,
             "org/ray/runtime/proxyTypes/RayObjectValueProxy");
  java_ray_object_value_proxy_init =
      env->GetMethodID(java_ray_object_value_proxy_class, "<init>", "([B[B)V");
  java_ray_object_value_proxy_data =
      env->GetFieldID(java_ray_object_value_proxy_class, "data", "[B");
  java_ray_object_value_proxy_metadata =
      env->GetFieldID(java_ray_object_value_proxy_class, "metadata", "[B");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION);

  env->DeleteGlobalRef(java_boolean_class);
  env->DeleteGlobalRef(java_double_class);
  env->DeleteGlobalRef(java_list_class);
  env->DeleteGlobalRef(java_array_list_class);
  env->DeleteGlobalRef(java_ray_function_proxy_class);
  env->DeleteGlobalRef(java_task_arg_proxy_class);
  env->DeleteGlobalRef(java_resources_proxy_class);
  env->DeleteGlobalRef(java_task_options_proxy_class);
  env->DeleteGlobalRef(java_actor_creation_options_proxy_class);
  env->DeleteGlobalRef(java_ray_object_value_proxy_class);
}