// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <jni.h>
#include <algorithm>

#include "ray/common/buffer.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/core_worker.h"

/// Boolean class
extern jclass java_boolean_class;
/// Constructor of Boolean class
extern jmethodID java_boolean_init;

/// Double class
extern jclass java_double_class;
/// doubleValue method of Double class
extern jmethodID java_double_double_value;

/// Object class
extern jclass java_object_class;
/// equals method of Object class
extern jmethodID java_object_equals;

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

/// Map interface
extern jclass java_map_class;
/// entrySet method of Map interface
extern jmethodID java_map_entry_set;

/// Set interface
extern jclass java_set_class;
/// iterator method of Set interface
extern jmethodID java_set_iterator;

/// Iterator interface
extern jclass java_iterator_class;
/// hasNext method of Iterator interface
extern jmethodID java_iterator_has_next;
/// next method of Iterator interface
extern jmethodID java_iterator_next;

/// Map.Entry interface
extern jclass java_map_entry_class;
/// getKey method of Map.Entry interface
extern jmethodID java_map_entry_get_key;
/// getValue method of Map.Entry interface
extern jmethodID java_map_entry_get_value;

/// RayException class
extern jclass java_ray_exception_class;

/// JniExceptionUtil class
extern jclass java_jni_exception_util_class;
/// getStackTrace method of JniExceptionUtil class
extern jmethodID java_jni_exception_util_get_stack_trace;

/// BaseId class
extern jclass java_base_id_class;
/// getBytes method of BaseId class
extern jmethodID java_base_id_get_bytes;

/// FunctionDescriptor interface
extern jclass java_function_descriptor_class;
/// getLanguage method of FunctionDescriptor interface
extern jmethodID java_function_descriptor_get_language;
/// toList method of FunctionDescriptor interface
extern jmethodID java_function_descriptor_to_list;

/// Language class
extern jclass java_language_class;
/// getNumber of Language class
extern jmethodID java_language_get_number;

/// FunctionArg class
extern jclass java_function_arg_class;
/// id field of FunctionArg class
extern jfieldID java_function_arg_id;
/// value field of FunctionArg class
extern jfieldID java_function_arg_value;

/// BaseTaskOptions class
extern jclass java_base_task_options_class;
/// resources field of BaseTaskOptions class
extern jfieldID java_base_task_options_resources;

/// ActorCreationOptions class
extern jclass java_actor_creation_options_class;
/// global field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_global;
/// name field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_name;
/// maxRestarts field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_restarts;
/// jvmOptions field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_jvm_options;
/// maxConcurrency field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_concurrency;
/// group field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_group;
/// bundleIndex field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_bundle_index;

/// GcsClientOptions class
extern jclass java_gcs_client_options_class;
/// ip field of GcsClientOptions class
extern jfieldID java_gcs_client_options_ip;
/// port field of GcsClientOptions class
extern jfieldID java_gcs_client_options_port;
/// password field of GcsClientOptions class
extern jfieldID java_gcs_client_options_password;

/// NativeRayObject class
extern jclass java_native_ray_object_class;
/// Constructor of NativeRayObject class
extern jmethodID java_native_ray_object_init;
/// data field of NativeRayObject class
extern jfieldID java_native_ray_object_data;
/// metadata field of NativeRayObject class
extern jfieldID java_native_ray_object_metadata;

/// TaskExecutor class
extern jclass java_task_executor_class;
/// checkByteBufferArguments method of TaskExecutor class
extern jmethodID java_task_executor_parse_function_arguments;
/// execute method of TaskExecutor class
extern jmethodID java_task_executor_execute;

/// PlacementGroup class
extern jclass java_placement_group_class;
/// id field of PlacementGroup class
extern jfieldID java_placement_group_id;

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

#define RAY_CHECK_JAVA_EXCEPTION(env)                                                 \
  {                                                                                   \
    jthrowable throwable = env->ExceptionOccurred();                                  \
    if (throwable) {                                                                  \
      jstring java_file_name = env->NewStringUTF(__FILE__);                           \
      jstring java_function = env->NewStringUTF(__func__);                            \
      jobject java_error_message = env->CallStaticObjectMethod(                       \
          java_jni_exception_util_class, java_jni_exception_util_get_stack_trace,     \
          java_file_name, __LINE__, java_function, throwable);                        \
      std::string error_message =                                                     \
          JavaStringToNativeString(env, static_cast<jstring>(java_error_message));    \
      env->DeleteLocalRef(throwable);                                                 \
      env->DeleteLocalRef(java_file_name);                                            \
      env->DeleteLocalRef(java_function);                                             \
      env->DeleteLocalRef(java_error_message);                                        \
      RAY_LOG(FATAL) << "An unexpected exception occurred while executing Java code " \
                        "from JNI ("                                                  \
                     << __FILE__ << ":" << __LINE__ << " " << __func__ << ")."        \
                     << "\n"                                                          \
                     << error_message;                                                \
    }                                                                                 \
  }

/// Represents a byte buffer of Java byte array.
/// The destructor will automatically call ReleaseByteArrayElements.
/// NOTE: Instances of this class cannot be used across threads.
class JavaByteArrayBuffer : public ray::Buffer {
 public:
  JavaByteArrayBuffer(JNIEnv *env, jbyteArray java_byte_array)
      : env_(env), java_byte_array_(java_byte_array) {
    native_bytes_ = env_->GetByteArrayElements(java_byte_array_, nullptr);
  }

  uint8_t *Data() const override { return reinterpret_cast<uint8_t *>(native_bytes_); }

  size_t Size() const override { return env_->GetArrayLength(java_byte_array_); }

  bool OwnsData() const override { return true; }

  bool IsPlasmaBuffer() const override { return false; }

  ~JavaByteArrayBuffer() {
    env_->ReleaseByteArrayElements(java_byte_array_, native_bytes_, JNI_ABORT);
    env_->DeleteLocalRef(java_byte_array_);
  }

 private:
  JNIEnv *env_;
  jbyteArray java_byte_array_;
  jbyte *native_bytes_;
};

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

/// Convert C++ String to a Java ByteArray.
inline jbyteArray NativeStringToJavaByteArray(JNIEnv *env, const std::string &str) {
  jbyteArray array = env->NewByteArray(str.size());
  env->SetByteArrayRegion(array, 0, str.size(),
                          reinterpret_cast<const jbyte *>(str.c_str()));
  return array;
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
  RAY_CHECK_JAVA_EXCEPTION(env);
  native_vector->clear();
  for (int i = 0; i < size; i++) {
    auto element = env->CallObjectMethod(java_list, java_list_get, (jint)i);
    RAY_CHECK_JAVA_EXCEPTION(env);
    native_vector->emplace_back(element_converter(env, element));
    env->DeleteLocalRef(element);
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

/// Convert a Java long array to C++ std::vector<long>.
inline void JavaLongArrayToNativeLongVector(JNIEnv *env, jlongArray long_array,
                                            std::vector<long> *native_vector) {
  jlong *long_array_ptr = env->GetLongArrayElements(long_array, nullptr);
  jsize vec_size = env->GetArrayLength(long_array);
  for (int i = 0; i < vec_size; ++i) {
    native_vector->push_back(static_cast<long>(long_array_ptr[i]));
  }
  env->ReleaseLongArrayElements(long_array, long_array_ptr, 0);
}

/// Convert a Java double array to C++ std::vector<double>.
inline void JavaDoubleArrayToNativeDoubleVector(JNIEnv *env, jdoubleArray double_array,
                                                std::vector<double> *native_vector) {
  jdouble *double_array_ptr = env->GetDoubleArrayElements(double_array, nullptr);
  jsize vec_size = env->GetArrayLength(double_array);
  for (int i = 0; i < vec_size; ++i) {
    native_vector->push_back(static_cast<double>(double_array_ptr[i]));
  }
  env->ReleaseDoubleArrayElements(double_array, double_array_ptr, 0);
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
    auto element = element_converter(env, item);
    env->CallVoidMethod(java_list, java_list_add, element);
    RAY_CHECK_JAVA_EXCEPTION(env);
    env->DeleteLocalRef(element);
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

/// Convert a Java Map<?, ?> to a C++ std::unordered_map<?, ?>
template <typename key_type, typename value_type>
inline std::unordered_map<key_type, value_type> JavaMapToNativeMap(
    JNIEnv *env, jobject java_map,
    const std::function<key_type(JNIEnv *, jobject)> &key_converter,
    const std::function<value_type(JNIEnv *, jobject)> &value_converter) {
  std::unordered_map<key_type, value_type> native_map;
  if (java_map) {
    jobject entry_set = env->CallObjectMethod(java_map, java_map_entry_set);
    RAY_CHECK_JAVA_EXCEPTION(env);
    jobject iterator = env->CallObjectMethod(entry_set, java_set_iterator);
    RAY_CHECK_JAVA_EXCEPTION(env);
    while (env->CallBooleanMethod(iterator, java_iterator_has_next)) {
      RAY_CHECK_JAVA_EXCEPTION(env);
      jobject map_entry = env->CallObjectMethod(iterator, java_iterator_next);
      RAY_CHECK_JAVA_EXCEPTION(env);
      auto java_key = (jstring)env->CallObjectMethod(map_entry, java_map_entry_get_key);
      RAY_CHECK_JAVA_EXCEPTION(env);
      key_type key = key_converter(env, java_key);
      auto java_value = env->CallObjectMethod(map_entry, java_map_entry_get_value);
      value_type value = value_converter(env, java_value);
      native_map.emplace(key, value);
      env->DeleteLocalRef(java_key);
      env->DeleteLocalRef(java_value);
      env->DeleteLocalRef(map_entry);
    }
    RAY_CHECK_JAVA_EXCEPTION(env);
    env->DeleteLocalRef(iterator);
    env->DeleteLocalRef(entry_set);
  }
  return native_map;
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

/// Convert a Java byte[] as a C++ std::shared_ptr<JavaByteArrayBuffer>.
inline std::shared_ptr<JavaByteArrayBuffer> JavaByteArrayToNativeBuffer(
    JNIEnv *env, const jbyteArray &javaByteArray) {
  if (!javaByteArray) {
    return nullptr;
  }
  return std::make_shared<JavaByteArrayBuffer>(env, javaByteArray);
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
  std::shared_ptr<ray::Buffer> data_buffer = JavaByteArrayToNativeBuffer(env, java_data);
  std::shared_ptr<ray::Buffer> metadata_buffer =
      JavaByteArrayToNativeBuffer(env, java_metadata);
  if (data_buffer && data_buffer->Size() == 0) {
    data_buffer = nullptr;
  }
  if (metadata_buffer && metadata_buffer->Size() == 0) {
    metadata_buffer = nullptr;
  }
  // TODO: Support nested IDs for Java.
  return std::make_shared<ray::RayObject>(data_buffer, metadata_buffer,
                                          std::vector<ray::ObjectID>());
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
  env->DeleteLocalRef(java_metadata);
  env->DeleteLocalRef(java_data);
  return java_obj;
}

// TODO(po): Convert C++ ray::FunctionDescriptor to Java FunctionDescriptor
inline jobject NativeRayFunctionDescriptorToJavaStringList(
    JNIEnv *env, const ray::FunctionDescriptor &function_descriptor) {
  if (function_descriptor->Type() ==
      ray::FunctionDescriptorType::kJavaFunctionDescriptor) {
    auto typed_descriptor = function_descriptor->As<ray::JavaFunctionDescriptor>();
    std::vector<std::string> function_descriptor_list = {typed_descriptor->ClassName(),
                                                         typed_descriptor->FunctionName(),
                                                         typed_descriptor->Signature()};
    return NativeStringVectorToJavaStringList(env, function_descriptor_list);
  } else if (function_descriptor->Type() ==
             ray::FunctionDescriptorType::kPythonFunctionDescriptor) {
    auto typed_descriptor = function_descriptor->As<ray::PythonFunctionDescriptor>();
    std::vector<std::string> function_descriptor_list = {
        typed_descriptor->ModuleName(), typed_descriptor->ClassName(),
        typed_descriptor->FunctionName(), typed_descriptor->FunctionHash()};
    return NativeStringVectorToJavaStringList(env, function_descriptor_list);
  }
  RAY_LOG(FATAL) << "Unknown function descriptor type: " << function_descriptor->Type();
  return NativeStringVectorToJavaStringList(env, std::vector<std::string>());
}

// Return an actor fullname with job id prepended if this tis a global actor.
inline std::string GetActorFullName(bool global, std::string name) {
  if (name.empty()) {
    return "";
  }
  return global ? name
                : ::ray::CoreWorkerProcess::GetCoreWorker().GetCurrentJobId().Hex() +
                      "-" + name;
}
