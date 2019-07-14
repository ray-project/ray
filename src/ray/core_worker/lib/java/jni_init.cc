#include "ray/core_worker/lib/java/jni_utils.h"

jclass java_boolean_class;
jmethodID java_boolean_init;

jclass java_list_class;
jmethodID java_list_size;
jmethodID java_list_get;
jmethodID java_list_add;

jclass java_array_list_class;
jmethodID java_array_list_init;
jmethodID java_array_list_init_with_capacity;

jclass java_ray_exception_class;

jclass java_native_ray_object_class;
jmethodID java_native_ray_object_init;
jfieldID java_native_ray_object_data;
jfieldID java_native_ray_object_metadata;

jint JNI_VERSION = JNI_VERSION_1_8;

inline jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
  jclass ret = (jclass)env->NewGlobalRef(tempLocalClassRef);
  env->DeleteLocalRef(tempLocalClassRef);
  return ret;
}

/// Load and cache frequently-used Java classes and methods
jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  java_boolean_class = LoadClass(env, "java/lang/Boolean");
  java_boolean_init = env->GetMethodID(java_boolean_class, "<init>", "(Z)V");

  java_list_class = LoadClass(env, "java/util/List");
  java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  java_list_get = env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  java_list_add = env->GetMethodID(java_list_class, "add", "(Ljava/lang/Object;)Z");

  java_array_list_class = LoadClass(env, "java/util/ArrayList");
  java_array_list_init = env->GetMethodID(java_array_list_class, "<init>", "()V");
  java_array_list_init_with_capacity =
      env->GetMethodID(java_array_list_class, "<init>", "(I)V");

  java_ray_exception_class = LoadClass(env, "org/ray/api/exception/RayException");

  java_native_ray_object_class =
      LoadClass(env, "org/ray/runtime/objectstore/NativeRayObject");
  java_native_ray_object_init =
      env->GetMethodID(java_native_ray_object_class, "<init>", "([B[B)V");
  java_native_ray_object_data =
      env->GetFieldID(java_native_ray_object_class, "data", "[B");
  java_native_ray_object_metadata =
      env->GetFieldID(java_native_ray_object_class, "metadata", "[B");

  return JNI_VERSION;
}

/// Unload java classes
void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION);

  env->DeleteGlobalRef(java_boolean_class);
  env->DeleteGlobalRef(java_list_class);
  env->DeleteGlobalRef(java_array_list_class);
  env->DeleteGlobalRef(java_ray_exception_class);
  env->DeleteGlobalRef(java_native_ray_object_class);
}
