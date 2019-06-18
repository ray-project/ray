#ifndef RAY_COMMON_JAVA_JNI_HELPER_H
#define RAY_COMMON_JAVA_JNI_HELPER_H

#include <jni.h>
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

extern jclass java_boolean_class;
extern jmethodID java_boolean_init;

extern jclass java_list_class;
extern jmethodID java_list_size;
extern jmethodID java_list_get;
extern jmethodID java_list_add;

extern jclass java_array_list_class;
extern jmethodID java_array_list_init;
extern jmethodID java_array_list_init_with_capacity;

extern jclass java_map_class;
extern jmethodID java_map_entry_set;
extern jmethodID java_array_list_init_with_capacity;

extern jclass java_native_ray_function_class;
extern jfieldID java_native_ray_function_worker_language;
extern jfieldID java_native_ray_function_function_descriptor;

extern jclass java_native_task_arg_class;
extern jfieldID java_native_task_arg_id;
extern jfieldID java_native_task_arg_data;

extern jclass java_native_resources_class;
extern jfieldID java_native_resources_keys;
extern jfieldID java_native_resources_values;

extern jclass java_native_task_options_class;
extern jfieldID java_native_task_options_num_returns;
extern jfieldID java_native_task_options_resources;

extern jclass java_native_actor_creation_options_class;
extern jfieldID java_native_actor_creation_options_max_reconstructions;
extern jfieldID java_native_actor_creation_options_resources;

inline bool ThrowRayExceptionIfNotOK(JNIEnv *env, const ray::Status &status) {
  if (!status.ok()) {
    jclass exception_class = env->FindClass("org/ray/api/exception/RayException");
    env->ThrowNew(exception_class, status.message().c_str());
    return true;
  } else {
    return false;
  }
}

template <typename ID>
class UniqueIdFromJByteArray {
 public:
  const ID &GetId() const { return id; }

  UniqueIdFromJByteArray(JNIEnv *env, const jbyteArray &bytes) {
    std::string id_str(ID::Size(), 0);
    env->GetByteArrayRegion(bytes, 0, ID::Size(),
                            reinterpret_cast<jbyte *>(&id_str.front()));
    id = ID::FromBinary(id_str);
  }

 private:
  ID id;
};

template <typename ID>
class JByteArrayFromUniqueId {
 public:
  const jbyteArray &GetJByteArray() const { return jbytearray; }

  JByteArrayFromUniqueId(JNIEnv *env, const ID &id) {
    jbytearray = env->NewByteArray(ID::Size());
    env->SetByteArrayRegion(jbytearray, 0, ID::Size(),
                            reinterpret_cast<const jbyte *>(id.Data()));
  }

 private:
  jbyteArray jbytearray;
};

inline std::string JavaStringToNativeString(JNIEnv *env, jstring jstr) {
  const char *c_str = env->GetStringUTFChars(jstr, nullptr);
  std::string result(c_str);
  env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
  return result;
}

template <typename NativeT>
inline void JavaListToNativeVector(
    JNIEnv *env, jobject java_list, std::vector<NativeT> *native_vector,
    std::function<NativeT(JNIEnv *, jobject)> element_converter) {
  int size = env->CallIntMethod(java_list, java_list_size);
  native_vector->clear();
  for (int i = 0; i < size; i++) {
    native_vector->emplace_back(
        element_converter(env, env->CallObjectMethod(java_list, java_list_get, (jint)i)));
  }
}

inline void JavaStringListToNativeStringVector(JNIEnv *env, jobject java_list,
                                               std::vector<std::string> *native_vector) {
  JavaListToNativeVector<std::string>(
      env, java_list, native_vector, [](JNIEnv *env, jobject jstr) {
        return JavaStringToNativeString(env, static_cast<jstring>(jstr));
      });
}

template <typename NativeT>
inline jobject NativeVectorToJavaList(
    JNIEnv *env, const std::vector<NativeT> &native_vector,
    std::function<jobject(JNIEnv *, const NativeT &)> element_converter) {
  jobject java_list =
      env->NewObject(java_array_list_class, java_array_list_init_with_capacity,
                     (jint)native_vector.size());
  for (const auto &item : native_vector) {
    env->CallVoidMethod(java_list, java_list_add, element_converter(env, item));
  }
  return java_list;
}

inline jobject NativeStringVectorToJavaStringList(
    JNIEnv *env, const std::vector<std::string> &native_vector) {
  return NativeVectorToJavaList<std::string>(
      env, native_vector,
      [](JNIEnv *env, const std::string &str) { return env->NewStringUTF(str.c_str()); });
}

inline jobject NativeBufferVectorToJavaBinaryList(
    JNIEnv *env, const std::vector<std::shared_ptr<ray::Buffer>> &native_vector) {
  return NativeVectorToJavaList<std::shared_ptr<ray::Buffer>>(
      env, native_vector, [](JNIEnv *env, const std::shared_ptr<ray::Buffer> &arg) {
        if (arg) {
          jbyteArray arg_byte_array = env->NewByteArray(arg->Size());
          env->SetByteArrayRegion(arg_byte_array, 0, arg->Size(),
                                  reinterpret_cast<const jbyte *>(arg->Data()));
          return arg_byte_array;
        } else {
          return (jbyteArray) nullptr;
        }
      });
}

template <typename ID>
inline jobject NativeUniqueIdVectorToJavaBinaryList(
    JNIEnv *env, const std::vector<ID> &native_vector) {
  return NativeVectorToJavaList<ID>(env, native_vector, [](JNIEnv *env, const ID &id) {
    return JByteArrayFromUniqueId<ID>(env, id).GetJByteArray();
  });
}

#endif  // RAY_COMMON_JAVA_JNI_HELPER_H
