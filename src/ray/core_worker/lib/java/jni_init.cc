#include "ray/core_worker/lib/java/jni_utils.h"

jclass java_boolean_class;
jmethodID java_boolean_init;

jclass java_double_class;
jmethodID java_double_double_value;

jclass java_list_class;
jmethodID java_list_size;
jmethodID java_list_get;
jmethodID java_list_add;

jclass java_array_list_class;
jmethodID java_array_list_init;
jmethodID java_array_list_init_with_capacity;

jclass java_map_class;
jmethodID java_map_entry_set;

jclass java_set_class;
jmethodID java_set_iterator;

jclass java_iterator_class;
jmethodID java_iterator_has_next;
jmethodID java_iterator_next;

jclass java_map_entry_class;
jmethodID java_map_entry_get_key;
jmethodID java_map_entry_get_value;

jclass java_ray_exception_class;

jclass java_base_id_class;
jmethodID java_base_id_get_bytes;

jclass java_function_descriptor_class;
jmethodID java_function_descriptor_get_language;
jmethodID java_function_descriptor_to_list;

jclass java_language_class;
jmethodID java_language_get_number;

jclass java_function_arg_class;
jfieldID java_function_arg_id;
jfieldID java_function_arg_data;

jclass java_base_task_options_class;
jfieldID java_base_task_options_resources;

jclass java_actor_creation_options_class;
jfieldID java_actor_creation_options_max_reconstructions;
jfieldID java_actor_creation_options_jvm_options;

jclass java_gcs_client_options_class;
jfieldID java_gcs_client_options_ip;
jfieldID java_gcs_client_options_port;
jfieldID java_gcs_client_options_password;

jclass java_native_ray_object_class;
jmethodID java_native_ray_object_init;
jfieldID java_native_ray_object_data;
jfieldID java_native_ray_object_metadata;

jclass java_task_executor_class;
jmethodID java_task_executor_execute;

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
  java_double_double_value = env->GetMethodID(java_double_class, "doubleValue", "()D");

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

  java_ray_exception_class = LoadClass(env, "org/ray/api/exception/RayException");

  java_base_id_class = LoadClass(env, "org/ray/api/id/BaseId");
  java_base_id_get_bytes = env->GetMethodID(java_base_id_class, "getBytes", "()[B");

  java_function_descriptor_class =
      LoadClass(env, "org/ray/runtime/functionmanager/FunctionDescriptor");
  java_function_descriptor_get_language =
      env->GetMethodID(java_function_descriptor_class, "getLanguage",
                       "()Lorg/ray/runtime/generated/Common$Language;");
  java_function_descriptor_to_list =
      env->GetMethodID(java_function_descriptor_class, "toList", "()Ljava/util/List;");

  java_language_class = LoadClass(env, "org/ray/runtime/generated/Common$Language");
  java_language_get_number = env->GetMethodID(java_language_class, "getNumber", "()I");

  java_function_arg_class = LoadClass(env, "org/ray/runtime/task/FunctionArg");
  java_function_arg_id =
      env->GetFieldID(java_function_arg_class, "id", "Lorg/ray/api/id/ObjectId;");
  java_function_arg_data = env->GetFieldID(java_function_arg_class, "data", "[B");

  java_base_task_options_class = LoadClass(env, "org/ray/api/options/BaseTaskOptions");
  java_base_task_options_resources =
      env->GetFieldID(java_base_task_options_class, "resources", "Ljava/util/Map;");

  java_actor_creation_options_class =
      LoadClass(env, "org/ray/api/options/ActorCreationOptions");
  java_actor_creation_options_max_reconstructions =
      env->GetFieldID(java_actor_creation_options_class, "maxReconstructions", "I");
  java_actor_creation_options_jvm_options = env->GetFieldID(
      java_actor_creation_options_class, "jvmOptions", "Ljava/lang/String;");

  java_gcs_client_options_class = LoadClass(env, "org/ray/runtime/gcs/GcsClientOptions");
  java_gcs_client_options_ip =
      env->GetFieldID(java_gcs_client_options_class, "ip", "Ljava/lang/String;");
  java_gcs_client_options_port =
      env->GetFieldID(java_gcs_client_options_class, "port", "I");
  java_gcs_client_options_password =
      env->GetFieldID(java_gcs_client_options_class, "password", "Ljava/lang/String;");

  java_native_ray_object_class = LoadClass(env, "org/ray/runtime/object/NativeRayObject");
  java_native_ray_object_init =
      env->GetMethodID(java_native_ray_object_class, "<init>", "([B[B)V");
  java_native_ray_object_data =
      env->GetFieldID(java_native_ray_object_class, "data", "[B");
  java_native_ray_object_metadata =
      env->GetFieldID(java_native_ray_object_class, "metadata", "[B");

  java_task_executor_class = LoadClass(env, "org/ray/runtime/task/TaskExecutor");
  java_task_executor_execute =
      env->GetMethodID(java_task_executor_class, "execute",
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
  env->DeleteGlobalRef(java_map_class);
  env->DeleteGlobalRef(java_set_class);
  env->DeleteGlobalRef(java_iterator_class);
  env->DeleteGlobalRef(java_map_entry_class);
  env->DeleteGlobalRef(java_ray_exception_class);
  env->DeleteGlobalRef(java_base_id_class);
  env->DeleteGlobalRef(java_function_descriptor_class);
  env->DeleteGlobalRef(java_language_class);
  env->DeleteGlobalRef(java_function_arg_class);
  env->DeleteGlobalRef(java_base_task_options_class);
  env->DeleteGlobalRef(java_actor_creation_options_class);
  env->DeleteGlobalRef(java_native_ray_object_class);
  env->DeleteGlobalRef(java_task_executor_class);
}
