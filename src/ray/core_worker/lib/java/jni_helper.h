#ifndef RAY_COMMON_JAVA_JNI_HELPER_H
#define RAY_COMMON_JAVA_JNI_HELPER_H

#include <jni.h>
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/store_provider/store_provider.h"

extern jclass java_boolean_class;
extern jmethodID java_boolean_init;

extern jclass java_list_class;
extern jmethodID java_list_size;
extern jmethodID java_list_get;
extern jmethodID java_list_add;

extern jclass java_array_list_class;
extern jmethodID java_array_list_init;
extern jmethodID java_array_list_init_with_capacity;

extern jclass java_ray_object_proxy_class;
extern jmethodID java_ray_object_proxy_init;
extern jfieldID java_ray_object_proxy_data;
extern jfieldID java_ray_object_proxy_metadata;

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

inline jbyteArray NativeBufferToJavaByteArray(JNIEnv *env,
                                              const std::shared_ptr<ray::Buffer> buffer) {
  if (!buffer) {
    return nullptr;
  }
  jbyteArray java_byte_array = env->NewByteArray(buffer->Size());
  if (buffer->Size() > 0) {
    env->SetByteArrayRegion(java_byte_array, 0, buffer->Size(),
                            reinterpret_cast<const jbyte *>(buffer->Data()));
  }
  return java_byte_array;
}

template <typename ReturnT>
inline ReturnT ReadJavaRayObjectProxy(
    JNIEnv *env, const jobject &java_obj,
    std::function<ReturnT(const std::shared_ptr<ray::RayObject> &)> reader) {
  if (!java_obj) {
    return reader(nullptr);
  }
  auto java_data = (jbyteArray)env->GetObjectField(java_obj, java_ray_object_proxy_data);
  auto java_metadata =
      (jbyteArray)env->GetObjectField(java_obj, java_ray_object_proxy_metadata);
  auto data_size = env->GetArrayLength(java_data);
  jbyte *data = data_size > 0 ? env->GetByteArrayElements(java_data, nullptr) : nullptr;
  auto metadata_size = java_metadata ? env->GetArrayLength(java_metadata) : 0;
  jbyte *metadata =
      metadata_size > 0 ? env->GetByteArrayElements(java_metadata, nullptr) : nullptr;
  auto data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data), data_size);
  auto metadata_buffer = java_metadata
                             ? std::make_shared<ray::LocalMemoryBuffer>(
                                   reinterpret_cast<uint8_t *>(metadata), metadata_size)
                             : nullptr;

  auto native_obj = std::make_shared<ray::RayObject>(data_buffer, metadata_buffer);
  auto result = reader(native_obj);

  if (data) {
    env->ReleaseByteArrayElements(java_data, data, JNI_ABORT);
  }
  if (metadata) {
    env->ReleaseByteArrayElements(java_metadata, metadata, JNI_ABORT);
  }

  return result;
}

inline jobject ToJavaRayObjectProxy(JNIEnv *env,
                                    const std::shared_ptr<ray::RayObject> &rayObject) {
  if (!rayObject) {
    return nullptr;
  }
  auto java_data = NativeBufferToJavaByteArray(env, rayObject->GetData());
  auto java_metadata = NativeBufferToJavaByteArray(env, rayObject->GetMetadata());
  auto java_obj = env->NewObject(java_ray_object_proxy_class, java_ray_object_proxy_init,
                                 java_data, java_metadata);
  return java_obj;
}

#endif  // RAY_COMMON_JAVA_JNI_HELPER_H
