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

using namespace ray;
using namespace ray::core;

/// Boolean class
extern jclass java_boolean_class;
/// Constructor of Boolean class
extern jmethodID java_boolean_init;

/// Double class
extern jclass java_double_class;
/// doubleValue method of Double class
extern jmethodID java_double_double_value;

/// Long class
extern jclass java_long_class;
/// longValue method of Long class
extern jmethodID java_long_init;

/// Object class
extern jclass java_object_class;
/// equals method of Object class
extern jmethodID java_object_equals;
/// hashCode method of Object class
extern jmethodID java_object_hash_code;

/// WeakReference class
extern jclass java_weak_reference_class;
extern jmethodID java_weak_reference_init;
extern jmethodID java_weak_reference_get;

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
/// put method of Map interface
extern jmethodID java_map_put;

/// HashMap class
extern jclass java_hash_map_class;
/// Constructor of HashMap class
extern jmethodID java_hash_map_init;

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

/// System class
extern jclass java_system_class;
/// gc method of System class
extern jmethodID java_system_gc;

/// RayException class
extern jclass java_ray_exception_class;

/// PendingCallsLimitExceededException class
extern jclass java_ray_pending_calls_limit_exceeded_exception_class;

/// RayIntentionalSystemExitException class
extern jclass java_ray_intentional_system_exit_exception_class;

/// RayActorException class
extern jclass java_ray_actor_exception_class;

/// RayExceptionSerializer class
extern jclass java_ray_exception_serializer_class;

/// RayTimeoutException class
extern jclass java_ray_timeout_exception_class;

/// RayExceptionSerializer to bytes
extern jmethodID java_ray_exception_serializer_to_bytes;

/// JniExceptionUtil class
extern jclass java_jni_exception_util_class;
/// getStackTrace method of JniExceptionUtil class
extern jmethodID java_jni_exception_util_get_stack_trace;

/// BaseId class
extern jclass java_base_id_class;
/// getBytes method of BaseId class
extern jmethodID java_base_id_get_bytes;

/// AbstractMessageLite class
extern jclass java_abstract_message_lite_class;
/// toByteArray method of AbstractMessageLite class
extern jmethodID java_abstract_message_lite_to_byte_array;

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
/// ownerAddress field of FunctionArg class
extern jfieldID java_function_arg_owner_address;
/// value field of FunctionArg class
extern jfieldID java_function_arg_value;

/// BaseTaskOptions class
extern jclass java_base_task_options_class;
/// resources field of BaseTaskOptions class
extern jfieldID java_base_task_options_resources;

/// CallOptions class
extern jclass java_call_options_class;
/// name field of CallOptions class
extern jfieldID java_call_options_name;
/// group field of CallOptions class
extern jfieldID java_task_creation_options_group;
/// bundleIndex field of CallOptions class
extern jfieldID java_task_creation_options_bundle_index;
/// concurrencyGroupName field of CallOptions class
extern jfieldID java_call_options_concurrency_group_name;
/// serializedRuntimeEnvInfo field of CallOptions class
extern jfieldID java_call_options_serialized_runtime_env_info;

/// ActorCreationOptions class
extern jclass java_actor_creation_options_class;
/// name field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_name;
/// lifetime field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_lifetime;
/// maxRestarts field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_restarts;
/// maxTaskRetries field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_task_retries;
/// jvmOptions field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_jvm_options;
/// maxConcurrency field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_concurrency;
/// group field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_group;
/// bundleIndex field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_bundle_index;
/// concurrencyGroups field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_concurrency_groups;
/// serializedRuntimeEnv field of ActorCreatrionOptions class
extern jfieldID java_actor_creation_options_serialized_runtime_env;
/// namespace field of ActorCreatrionOptions class
extern jfieldID java_actor_creation_options_namespace;
/// maxPendingCalls field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_max_pending_calls;
/// isAsync field of ActorCreationOptions class
extern jfieldID java_actor_creation_options_is_async;
/// ActorLifetime enum class
extern jclass java_actor_lifetime_class;
/// ordinal method of ActorLifetime class
extern jmethodID java_actor_lifetime_ordinal;
/// ordinal value of Enum DETACHED of ActorLifetime class
extern int DETACHED_LIFETIME_ORDINAL_VALUE;
/// ConcurrencyGroupImpl class
extern jclass java_concurrency_group_impl_class;
/// getFunctionDescriptors method of ConcurrencyGroupImpl class
extern jmethodID java_concurrency_group_impl_get_function_descriptors;
/// name field of ConcurrencyGroupImpl class
extern jfieldID java_concurrency_group_impl_name;
/// maxConcurrency field of ConcurrencyGroupImpl class
extern jfieldID java_concurrency_group_impl_max_concurrency;

/// PlacementGroupCreationOptions class
extern jclass java_placement_group_creation_options_class;
/// PlacementStrategy class
extern jclass java_placement_group_creation_options_strategy_class;
/// name field of PlacementGroupCreationOptions class
extern jfieldID java_placement_group_creation_options_name;
/// bundles field of PlacementGroupCreationOptions class
extern jfieldID java_placement_group_creation_options_bundles;
/// strategy field of PlacementGroupCreationOptions class
extern jfieldID java_placement_group_creation_options_strategy;
/// value method of PlacementStrategy class
extern jmethodID java_placement_group_creation_options_strategy_value;

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
// containedObjectIds field of NativeRayObject class
extern jfieldID java_native_ray_object_contained_object_ids;

/// TaskExecutor class
extern jclass java_task_executor_class;
/// checkByteBufferArguments method of TaskExecutor class
extern jmethodID java_task_executor_parse_function_arguments;
/// execute method of TaskExecutor class
extern jmethodID java_task_executor_execute;

/// NativeTaskExecutor class
extern jclass java_native_task_executor_class;

/// PlacementGroup class
extern jclass java_placement_group_class;
/// id field of PlacementGroup class
extern jfieldID java_placement_group_id;

/// ObjectRefImpl class
extern jclass java_object_ref_impl_class;
/// onJavaObjectAllocated method of ObjectRefImpl class
extern jmethodID java_object_ref_impl_class_on_memory_store_object_allocated;

/// ResourceValue class that is used to convert resource_ids() to java class
extern jclass java_resource_value_class;
/// Construtor of ResourceValue class
extern jmethodID java_resource_value_init;

#define CURRENT_JNI_VERSION JNI_VERSION_1_8

extern JavaVM *jvm;

/// Throws a Java RayException if the status is not OK.
#define THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, ret)                         \
  {                                                                                    \
    if (!(status).ok()) {                                                              \
      if (status.IsTimedOut()) {                                                       \
        (env)->ThrowNew(java_ray_timeout_exception_class, (status).message().c_str()); \
      } else {                                                                         \
        (env)->ThrowNew(java_ray_exception_class, (status).message().c_str());         \
      }                                                                                \
      return (ret);                                                                    \
    }                                                                                  \
  }

#define RAY_CHECK_JAVA_EXCEPTION(env)                                                 \
  {                                                                                   \
    jthrowable throwable = env->ExceptionOccurred();                                  \
    if (throwable) {                                                                  \
      jstring java_file_name = env->NewStringUTF(__FILE__);                           \
      jstring java_function = env->NewStringUTF(__func__);                            \
      jobject java_error_message =                                                    \
          env->CallStaticObjectMethod(java_jni_exception_util_class,                  \
                                      java_jni_exception_util_get_stack_trace,        \
                                      java_file_name,                                 \
                                      __LINE__,                                       \
                                      java_function,                                  \
                                      throwable);                                     \
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

/// Convert a Java String to C++ std::string.
/// If the Java String is null, return an empty C++ std::string.
inline std::string JavaStringToNativeString(JNIEnv *env, jstring jstr) {
  if (!jstr) {
    return std::string();
  }
  const char *c_str = env->GetStringUTFChars(jstr, nullptr);
  std::string result(c_str);
  env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
  return result;
}

/// Represents a byte buffer of Java byte array.
/// The destructor will automatically call ReleaseByteArrayElements.
/// NOTE: Instances of this class cannot be used across threads.
class JavaByteArrayBuffer : public Buffer {
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

/// Convert a Java byte array to a C++ string.
inline std::string JavaByteArrayToNativeString(JNIEnv *env, const jbyteArray &bytes) {
  const auto size = env->GetArrayLength(bytes);
  std::string str(size, 0);
  env->GetByteArrayRegion(bytes, 0, size, reinterpret_cast<jbyte *>(&str.front()));
  return str;
}

/// Convert a Java byte array to a C++ UniqueID.
template <typename ID>
inline ID JavaByteArrayToId(JNIEnv *env, const jbyteArray &bytes) {
  std::string id_str(ID::Size(), 0);
  env->GetByteArrayRegion(
      bytes, 0, ID::Size(), reinterpret_cast<jbyte *>(&id_str.front()));
  auto arr_size = static_cast<size_t>(env->GetArrayLength(bytes));
  RAY_CHECK(arr_size == ID::Size())
      << "ID length should be " << ID::Size() << " instead of " << arr_size;
  return ID::FromBinary(id_str);
}

/// Convert C++ UniqueID to a Java byte array.
template <typename ID>
inline jbyteArray IdToJavaByteArray(JNIEnv *env, const ID &id) {
  jbyteArray array = env->NewByteArray(ID::Size());
  env->SetByteArrayRegion(
      array, 0, ID::Size(), reinterpret_cast<const jbyte *>(id.Data()));
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
  env->SetByteArrayRegion(
      array, 0, str.size(), reinterpret_cast<const jbyte *>(str.c_str()));
  return array;
}

/// Convert a Java List to C++ std::vector.
template <typename NativeT>
inline void JavaListToNativeVector(
    JNIEnv *env,
    jobject java_list,
    std::vector<NativeT> *native_vector,
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
inline void JavaStringListToNativeStringVector(JNIEnv *env,
                                               jobject java_list,
                                               std::vector<std::string> *native_vector) {
  JavaListToNativeVector<std::string>(
      env, java_list, native_vector, [](JNIEnv *env, jobject jstr) {
        return JavaStringToNativeString(env, static_cast<jstring>(jstr));
      });
}

/// Convert a Java long array to C++ std::vector<long>.
inline void JavaLongArrayToNativeLongVector(JNIEnv *env,
                                            jlongArray long_array,
                                            std::vector<long> *native_vector) {
  jlong *long_array_ptr = env->GetLongArrayElements(long_array, nullptr);
  jsize vec_size = env->GetArrayLength(long_array);
  for (int i = 0; i < vec_size; ++i) {
    native_vector->push_back(static_cast<long>(long_array_ptr[i]));
  }
  env->ReleaseLongArrayElements(long_array, long_array_ptr, 0);
}

/// Convert a Java double array to C++ std::vector<double>.
inline void JavaDoubleArrayToNativeDoubleVector(JNIEnv *env,
                                                jdoubleArray double_array,
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
    JNIEnv *env,
    const std::vector<NativeT> &native_vector,
    std::function<jobject(JNIEnv *, const NativeT &)> element_converter) {
  jobject java_list = env->NewObject(java_array_list_class,
                                     java_array_list_init_with_capacity,
                                     (jint)native_vector.size());
  RAY_CHECK_JAVA_EXCEPTION(env);
  for (auto it = native_vector.begin(); it != native_vector.end(); ++it) {
    auto element = element_converter(env, *it);
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
      env, native_vector, [](JNIEnv *env, const std::string &str) {
        return env->NewStringUTF(str.c_str());
      });
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
    JNIEnv *env,
    jobject java_map,
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
      auto java_key = env->CallObjectMethod(map_entry, java_map_entry_get_key);
      RAY_CHECK_JAVA_EXCEPTION(env);
      key_type key = key_converter(env, java_key);
      auto java_value = env->CallObjectMethod(map_entry, java_map_entry_get_value);
      RAY_CHECK_JAVA_EXCEPTION(env);
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

/// Convert a C++ std::unordered_map<?, ?> to a Java Map<?, ?>
template <typename key_type, typename value_type>
inline jobject NativeMapToJavaMap(
    JNIEnv *env,
    const std::unordered_map<key_type, value_type> &native_map,
    const std::function<jobject(JNIEnv *, const key_type &)> &key_converter,
    const std::function<jobject(JNIEnv *, const value_type &)> &value_converter) {
  jobject java_map = env->NewObject(java_hash_map_class, java_hash_map_init);
  RAY_CHECK_JAVA_EXCEPTION(env);
  for (const auto &entry : native_map) {
    jobject java_key = key_converter(env, entry.first);
    jobject java_value = value_converter(env, entry.second);
    env->CallObjectMethod(java_map, java_map_put, java_key, java_value);
    RAY_CHECK_JAVA_EXCEPTION(env);
    env->DeleteLocalRef(java_key);
    env->DeleteLocalRef(java_value);
  }
  return java_map;
}

/// Convert a C++ Buffer to a Java byte array.
inline jbyteArray NativeBufferToJavaByteArray(JNIEnv *env,
                                              const std::shared_ptr<Buffer> buffer) {
  if (!buffer) {
    return nullptr;
  }

  auto buffer_size = buffer->Size();
  jbyteArray java_byte_array = env->NewByteArray(buffer_size);
  if (buffer_size > 0) {
    env->SetByteArrayRegion(java_byte_array,
                            0,
                            buffer->Size(),
                            reinterpret_cast<const jbyte *>(buffer->Data()));
    RAY_CHECK_JAVA_EXCEPTION(env);
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

/// Convert a Java NativeRayObject to a C++ RayObject.
/// NOTE: the returned RayObject cannot be used across threads.
inline std::shared_ptr<RayObject> JavaNativeRayObjectToNativeRayObject(
    JNIEnv *env, const jobject &java_obj) {
  if (!java_obj) {
    return nullptr;
  }
  auto java_data = (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_data);
  auto java_metadata =
      (jbyteArray)env->GetObjectField(java_obj, java_native_ray_object_metadata);
  std::shared_ptr<Buffer> data_buffer = JavaByteArrayToNativeBuffer(env, java_data);
  std::shared_ptr<Buffer> metadata_buffer =
      JavaByteArrayToNativeBuffer(env, java_metadata);
  if (data_buffer && data_buffer->Size() == 0) {
    data_buffer = nullptr;
  }
  if (metadata_buffer && metadata_buffer->Size() == 0) {
    metadata_buffer = nullptr;
  }

  auto java_contained_ids =
      env->GetObjectField(java_obj, java_native_ray_object_contained_object_ids);
  std::vector<ObjectID> contained_object_ids;
  JavaListToNativeVector<ObjectID>(
      env, java_contained_ids, &contained_object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ObjectID>(env, static_cast<jbyteArray>(id));
      });
  env->DeleteLocalRef(java_contained_ids);
  auto contained_object_refs =
      CoreWorkerProcess::GetCoreWorker().GetObjectRefs(contained_object_ids);
  return std::make_shared<RayObject>(data_buffer, metadata_buffer, contained_object_refs);
}

/// Convert a C++ RayObject to a Java NativeRayObject.
inline jobject NativeRayObjectToJavaNativeRayObject(
    JNIEnv *env, const std::shared_ptr<RayObject> rayObject) {
  if (!rayObject) {
    return nullptr;
  }

  std::shared_ptr<ray::Buffer> local_buffer = rayObject->GetData();
  auto java_data = NativeBufferToJavaByteArray(env, local_buffer);
  auto java_metadata = NativeBufferToJavaByteArray(env, rayObject->GetMetadata());
  auto java_obj = env->NewObject(java_native_ray_object_class,
                                 java_native_ray_object_init,
                                 java_data,
                                 java_metadata);
  RAY_CHECK_JAVA_EXCEPTION(env);
  env->DeleteLocalRef(java_metadata);
  env->DeleteLocalRef(java_data);
  return java_obj;
}

// TODO(po): Convert C++ FunctionDescriptor to Java FunctionDescriptor
inline jobject NativeRayFunctionDescriptorToJavaStringList(
    JNIEnv *env, const FunctionDescriptor &function_descriptor) {
  if (function_descriptor->Type() == FunctionDescriptorType::kJavaFunctionDescriptor) {
    auto typed_descriptor = function_descriptor->As<JavaFunctionDescriptor>();
    std::vector<std::string> function_descriptor_list = {typed_descriptor->ClassName(),
                                                         typed_descriptor->FunctionName(),
                                                         typed_descriptor->Signature()};
    return NativeStringVectorToJavaStringList(env, function_descriptor_list);
  } else if (function_descriptor->Type() ==
             FunctionDescriptorType::kPythonFunctionDescriptor) {
    auto typed_descriptor = function_descriptor->As<PythonFunctionDescriptor>();
    std::vector<std::string> function_descriptor_list = {
        typed_descriptor->ModuleName(),
        typed_descriptor->ClassName(),
        typed_descriptor->FunctionName(),
        typed_descriptor->FunctionHash()};
    return NativeStringVectorToJavaStringList(env, function_descriptor_list);
  } else if (function_descriptor->Type() ==
             ray::FunctionDescriptorType::kCppFunctionDescriptor) {
    auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
    std::vector<std::string> function_descriptor_list = {typed_descriptor->FunctionName(),
                                                         typed_descriptor->Caller(),
                                                         typed_descriptor->ClassName()};
    return NativeStringVectorToJavaStringList(env, function_descriptor_list);
  }
  RAY_LOG(FATAL) << "Unknown function descriptor type: " << function_descriptor->Type();
  return NativeStringVectorToJavaStringList(env, std::vector<std::string>());
}

/// Convert a Java protobuf object to a C++ protobuf object
template <typename NativeT>
inline NativeT JavaProtobufObjectToNativeProtobufObject(JNIEnv *env, jobject java_obj) {
  NativeT native_obj;
  if (java_obj) {
    jbyteArray bytes = static_cast<jbyteArray>(
        env->CallObjectMethod(java_obj, java_abstract_message_lite_to_byte_array));
    RAY_CHECK_JAVA_EXCEPTION(env);
    RAY_CHECK(bytes != nullptr);
    auto buffer = JavaByteArrayToNativeBuffer(env, bytes);
    RAY_CHECK(buffer);
    native_obj.ParseFromArray(buffer->Data(), buffer->Size());
    // Destroy the buffer before deleting the local ref of `bytes`. We need to make sure
    // that `bytes` is still available when invoking the destructor of
    // `JavaByteArrayBuffer`.
    buffer.reset();
    env->DeleteLocalRef(bytes);
  }
  return native_obj;
}

inline std::shared_ptr<LocalMemoryBuffer> SerializeActorCreationException(
    JNIEnv *env, jthrowable creation_exception) {
  jbyteArray exception_jbyte_array = static_cast<jbyteArray>(
      env->CallStaticObjectMethod(java_ray_exception_serializer_class,
                                  java_ray_exception_serializer_to_bytes,
                                  creation_exception));
  int len = env->GetArrayLength(exception_jbyte_array);
  auto buf = std::make_shared<LocalMemoryBuffer>(len);
  env->GetByteArrayRegion(
      exception_jbyte_array, 0, len, reinterpret_cast<jbyte *>(buf->Data()));
  return buf;
}

inline jobject CreateJavaWeakRef(JNIEnv *env, jobject java_object) {
  jobject java_weak_ref =
      env->NewObject(java_weak_reference_class, java_weak_reference_init, java_object);
  RAY_CHECK_JAVA_EXCEPTION(env);
  return java_weak_ref;
}
