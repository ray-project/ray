#ifndef RAY_COMMON_JAVA_JNI_HELPER_H
#define RAY_COMMON_JAVA_JNI_HELPER_H

#include <jni.h>
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/store_provider/store_provider.h"

/// Boolean class
extern jclass java_boolean_class;
/// Constructor of Boolean class
extern jmethodID java_boolean_init;

/// List class
extern jclass java_list_class;
/// size method of List class
extern jmethodID java_list_size;
/// get method of List class
extern jmethodID java_list_get;
/// add method of List class
extern jmethodID java_list_add;

/// ArrayList class
extern jclass java_array_list_class;
/// Constructor of ArrayList class
extern jmethodID java_array_list_init;
/// Constructor of ArrayList class with single parameter capacity
extern jmethodID java_array_list_init_with_capacity;

/// RayException class
extern jclass java_ray_exception_class;

/// NativeRayObject class
extern jclass java_native_ray_object_class;
/// Constructor of NativeRayObject class
extern jmethodID java_native_ray_object_init;
/// data field of NativeRayObject class
extern jfieldID java_native_ray_object_data;
/// metadata field of NativeRayObject class
extern jfieldID java_native_ray_object_metadata;

/// Throws a Java RayException if the status is not OK.
#define THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, ret)               \
  {                                                                          \
    if (!(status).ok()) {                                                    \
      (env)->ThrowNew(java_ray_exception_class, (status).message().c_str()); \
      return (ret);                                                          \
    }                                                                        \
  }

/// Convert a Java byte array to a C++ UniqueID.
template <typename ID>
inline ID JavaByteArrayToId(JNIEnv *env, const jbyteArray &bytes) {
  std::string id_str(ID::Size(), 0);
  env->GetByteArrayRegion(bytes, 0, ID::Size(),
                          reinterpret_cast<jbyte *>(&id_str.front()));
  return ID::FromBinary(id_str);
}

/// Convert C++ UniqueID to a Java byte array.
template <typename ID>
inline jbyteArray IdToJavaByteArray(JNIEnv *env, const ID &id) {
  jbyteArray array = env->NewByteArray(ID::Size());
  env->SetByteArrayRegion(array, 0, ID::Size(),
                          reinterpret_cast<const jbyte *>(id.Data()));
  return array;
}

/// Convert C++ UniqueID to a Java ByteBuffer.
template <typename ID>
inline jobject IdToJavaByteBuffer(JNIEnv *env, const ID &id) {
  return env->NewDirectByteBuffer(
      reinterpret_cast<void *>(const_cast<uint8_t *>(id.Data())), id.Size());
}

/// Convert a Java String to C++ std::string.
inline std::string JavaStringToNativeString(JNIEnv *env, jstring jstr) {
  const char *c_str = env->GetStringUTFChars(jstr, nullptr);
  std::string result(c_str);
  env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
  return result;
}

/// Convert a Java List to C++ std::vector.
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

/// Convert a C++ std::vector to a Java List.
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

/// Convert a C++ ray::Buffer to a Java byte array.
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

/// A helper method to help access a Java NativeRayObject instance and ensure memory
/// safety.
///
/// \param[in] java_obj The Java NativeRayObject object.
/// \param[in] reader The callback function to access a C++ ray::RayObject instance.
/// \return The return value of callback function.
template <typename ReturnT>
inline ReturnT ReadJavaNativeRayObject(
    JNIEnv *env, const jobject &java_obj,
    std::function<ReturnT(const std::shared_ptr<ray::RayObject> &)> reader) {
  if (!java_obj) {
    return reader(nullptr);
  }
  auto java_data = (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_data);
  auto java_metadata =
      (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_metadata);
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

/// Convert a C++ ray::RayObject to a Java NativeRayObject.
inline jobject ToJavaNativeRayObject(JNIEnv *env,
                                     const std::shared_ptr<ray::RayObject> &rayObject) {
  if (!rayObject) {
    return nullptr;
  }
  auto java_data = NativeBufferToJavaByteArray(env, rayObject->GetData());
  auto java_metadata = NativeBufferToJavaByteArray(env, rayObject->GetMetadata());
  auto java_obj = env->NewObject(java_native_ray_object_class,
                                 java_native_ray_object_init, java_data, java_metadata);
  return java_obj;
}

#endif  // RAY_COMMON_JAVA_JNI_HELPER_H
