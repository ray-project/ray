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

/// Double class
extern jclass java_double_class;
/// value field of Double class
extern jfieldID java_double_value;

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

/// NativeRayFunction class
extern jclass java_native_ray_function_class;
/// language field of NativeRayFunction class
extern jfieldID java_native_ray_function_language;
/// function_descriptor field of NativeRayFunction class
extern jfieldID java_native_ray_function_function_descriptor;

/// NativeTaskArg class
extern jclass java_native_task_arg_class;
/// id field of NativeTaskArg class
extern jfieldID java_native_task_arg_id;
/// data field of NativeTaskArg class
extern jfieldID java_native_task_arg_data;

/// NativeResources class
extern jclass java_native_resources_class;
/// keys field of NativeResources class
extern jfieldID java_native_resources_keys;
/// values field of NativeResources class
extern jfieldID java_native_resources_values;

/// NativeTaskOptions class
extern jclass java_native_task_options_class;
/// numReturns field of NativeTaskOptions class
extern jfieldID java_native_task_options_num_returns;
/// resources field of NativeTaskOptions class
extern jfieldID java_native_task_options_resources;

/// ActorCreationOptions class
extern jclass java_native_actor_creation_options_class;
/// maxReconstructions field of ActorCreationOptions class
extern jfieldID java_native_actor_creation_options_max_reconstructions;
/// isDirectCall field of ActorCreationOptions class
extern jfieldID java_native_actor_creation_options_is_direct_call;
/// resources field of ActorCreationOptions class
extern jfieldID java_native_actor_creation_options_resources;
/// dynamicWorkerOptions field of ActorCreationOptions class
extern jfieldID java_native_actor_creation_options_dynamic_worker_options;

/// GcsClientOptions class
extern jclass java_gcs_client_options_class;
/// ip field of GcsClientOptions class
extern jfieldID java_gcs_client_options_ip;
/// port field of GcsClientOptions class
extern jfieldID java_gcs_client_options_port;
/// password field of GcsClientOptions class
extern jfieldID java_gcs_client_options_password;
/// isTestClient field of GcsClientOptions class
extern jfieldID java_gcs_client_options_is_test_client;

/// NativeRayObject class
extern jclass java_native_ray_object_class;
/// Constructor of NativeRayObject class
extern jmethodID java_native_ray_object_init;
/// data field of NativeRayObject class
extern jfieldID java_native_ray_object_data;
/// metadata field of NativeRayObject class
extern jfieldID java_native_ray_object_metadata;

/// WorkerImpl class
extern jclass java_worker_class;
/// execute method of Worker class
extern jmethodID java_worker_impl_execute;

#define CURRENT_JNI_VERSION JNI_VERSION_1_8

extern JavaVM *jvm;

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

/// Convert a Java List<String> to C++ std::vector<std::string>.
inline void JavaStringListToNativeStringVector(JNIEnv *env, jobject java_list,
                                               std::vector<std::string> *native_vector) {
  JavaListToNativeVector<std::string>(
      env, java_list, native_vector, [](JNIEnv *env, jobject jstr) {
        return JavaStringToNativeString(env, static_cast<jstring>(jstr));
      });
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

/// Convert a C++ std::vector<std::string> to a Java List<String>
inline jobject NativeStringVectorToJavaStringList(
    JNIEnv *env, const std::vector<std::string> &native_vector) {
  return NativeVectorToJavaList<std::string>(
      env, native_vector,
      [](JNIEnv *env, const std::string &str) { return env->NewStringUTF(str.c_str()); });
}

template <typename ID>
inline jobject NativeIdVectorToJavaByteArrayList(JNIEnv *env,
                                                 const std::vector<ID> &native_vector) {
  return NativeVectorToJavaList<ID>(env, native_vector, [](JNIEnv *env, const ID &id) {
    return IdToJavaByteArray<ID>(env, id);
  });
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

/// Convert a Java byte[] as a C++ std::shared_ptr<ray::LocalMemoryBuffer>.
/// The deleter of std::shared_ptr will automatically call ReleaseByteArrayElements.
/// NOTE: the returned std::shared_ptr cannot be used across threads.
inline std::shared_ptr<ray::LocalMemoryBuffer> JavaByteArrayToNativeBuffer(
    JNIEnv *env, const jbyteArray &javaByteArray) {
  if (!javaByteArray) {
    return nullptr;
  }
  auto size = env->GetArrayLength(javaByteArray);
  if (size == 0) {
    return std::make_shared<ray::LocalMemoryBuffer>(nullptr, 0);
  }
  jbyte *data = env->GetByteArrayElements(javaByteArray, nullptr);
  return std::shared_ptr<ray::LocalMemoryBuffer>(
      new ray::LocalMemoryBuffer(reinterpret_cast<uint8_t *>(data), size),
      [env, javaByteArray](ray::LocalMemoryBuffer *p) {
        env->ReleaseByteArrayElements(javaByteArray, reinterpret_cast<jbyte *>(p->Data()),
                                      JNI_ABORT);
      });
}

/// Convert a Java NativeRayObject to a C++ ray::RayObject.
/// NOTE: the returned ray::RayObject cannot be used across threads.
inline std::shared_ptr<ray::RayObject> JavaNativeRayObjectToNativeRayObject(
    JNIEnv *env, const jobject &java_obj) {
  if (!java_obj) {
    return nullptr;
  }
  auto java_data = (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_data);
  auto java_metadata =
      (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_metadata);
  auto data_buffer = JavaByteArrayToNativeBuffer(env, java_data);
  auto metadata_buffer = JavaByteArrayToNativeBuffer(env, java_metadata);
  if (!data_buffer) {
    data_buffer = std::make_shared<ray::LocalMemoryBuffer>(nullptr, 0);
  }
  if (!metadata_buffer) {
    metadata_buffer = std::make_shared<ray::LocalMemoryBuffer>(nullptr, 0);
  }
  return std::make_shared<ray::RayObject>(data_buffer, metadata_buffer);
}

/// Convert a C++ ray::RayObject to a Java NativeRayObject.
inline jobject NativeRayObjectToJavaNativeRayObject(
    JNIEnv *env, const std::shared_ptr<ray::RayObject> &rayObject) {
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
