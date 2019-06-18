#include "ray/core_worker/lib/java/jni_helper.h"

jclass java_boolean_class;
jmethodID java_boolean_init;

jclass java_list_class;
jmethodID java_list_size;
jmethodID java_list_get;
jmethodID java_list_add;

jclass java_array_list_class;
jmethodID java_array_list_init;
jmethodID java_array_list_init_with_capacity;

jclass java_native_ray_function_class;
jfieldID java_native_ray_function_worker_language;
jfieldID java_native_ray_function_function_descriptor;

jclass java_native_task_arg_class;
jfieldID java_native_task_arg_id;
jfieldID java_native_task_arg_data;

jclass java_native_task_options_class;
jfieldID java_native_task_options_num_returns;
jfieldID java_native_task_options_resource_keys;
jfieldID java_native_task_options_resource_values;

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

  LOAD_CLASS(java_list_class, "java/util/List");
  java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  java_list_get = env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  java_list_add = env->GetMethodID(java_list_class, "add", "(Ljava/lang/Object;)Z");

  LOAD_CLASS(java_array_list_class, "java/util/ArrayList");
  java_array_list_init = env->GetMethodID(java_array_list_class, "<init>", "()V");
  java_array_list_init_with_capacity =
      env->GetMethodID(java_array_list_class, "<init>", "(I)V");

  LOAD_CLASS(java_native_ray_function_class,
             "org/ray/runtime/nativeTypes/NativeRayFunction");
  java_native_ray_function_worker_language =
      env->GetFieldID(java_native_ray_function_class, "workerLanguage", "I");
  java_native_ray_function_function_descriptor = env->GetFieldID(
      java_native_ray_function_class, "functionDescriptor", "Ljava/util/List;");

  LOAD_CLASS(java_native_task_arg_class, "org/ray/runtime/nativeTypes/NativeTaskArg");
  java_native_task_arg_id = env->GetFieldID(java_native_task_arg_class, "id", "[B");
  java_native_task_arg_data = env->GetFieldID(java_native_task_arg_class, "data", "[B");

  LOAD_CLASS(java_native_task_options_class,
             "org/ray/runtime/nativeTypes/NativeTaskOptions");
  java_native_task_options_num_returns =
      env->GetFieldID(java_native_task_options_class, "numReturns", "I");
  java_native_task_options_resource_keys =
      env->GetFieldID(java_native_task_options_class, "resourceKeys", "Ljava/util/List;");
  java_native_task_options_resource_values = env->GetFieldID(
      java_native_task_options_class, "resourceValues", "Ljava/util/List;");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION);

  env->DeleteGlobalRef(java_boolean_class);
  env->DeleteGlobalRef(java_list_class);
  env->DeleteGlobalRef(java_array_list_class);
  env->DeleteGlobalRef(java_native_ray_function_class);
  env->DeleteGlobalRef(java_native_task_arg_class);
  env->DeleteGlobalRef(java_native_task_options_class);
}