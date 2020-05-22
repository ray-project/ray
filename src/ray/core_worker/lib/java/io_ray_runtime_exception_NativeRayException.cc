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

#include "ray/core_worker/lib/java/io_ray_runtime_exception_NativeRayException.h"

#include <jni.h>

#include "ray/common/id.h"
#include "ray/common/ray_exception.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_exception_NativeRayException_nativeCreateRayException(
    JNIEnv *env, jclass, jint error_type, jstring error_message, jint language,
    jbyteArray job_id, jbyteArray worker_id, jbyteArray task_id, jbyteArray actor_id,
    jbyteArray object_id, jstring ip, jint pid, jstring proc_title, jstring file,
    jlong lineno, jstring function, jstring traceback, jbyteArray data) {
  return reinterpret_cast<long>(new ray::RayException(
      (ray::rpc::ErrorType)error_type, JavaStringToNativeString(env, error_message),
      (ray::Language)language, JavaByteArrayToId<ray::JobID>(env, job_id),
      JavaByteArrayToId<ray::WorkerID>(env, worker_id),
      JavaByteArrayToId<ray::TaskID>(env, task_id),
      JavaByteArrayToId<ray::ActorID>(env, actor_id),
      JavaByteArrayToId<ray::ObjectID>(env, object_id), JavaStringToNativeString(env, ip),
      (pid_t)pid, JavaStringToNativeString(env, proc_title),
      JavaStringToNativeString(env, file), lineno,
      JavaStringToNativeString(env, function), JavaStringToNativeString(env, traceback),
      JavaByteArrayToNativeString(env, data), std::shared_ptr<ray::RayException>()));
}

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_exception_NativeRayException_nativeDeserialize(
    JNIEnv *env, jclass, jbyteArray serialized) {
  return reinterpret_cast<long>(
      new ray::RayException(JavaByteArrayToNativeString(env, serialized)));
}

JNIEXPORT void JNICALL Java_io_ray_runtime_exception_NativeRayException_nativeDestroy(
    JNIEnv *env, jclass, jlong handle) {
  if (handle) {
    delete reinterpret_cast<ray::RayException *>(handle);
  }
}

JNIEXPORT jint JNICALL Java_io_ray_runtime_exception_NativeRayException_nativeLanguage(
    JNIEnv *env, jclass, jlong handle) {
  auto e = reinterpret_cast<ray::RayException *>(handle);
  return (int)e->Language();
}

JNIEXPORT jstring JNICALL Java_io_ray_runtime_exception_NativeRayException_nativeToString(
    JNIEnv *env, jclass, jlong handle) {
  auto e = reinterpret_cast<ray::RayException *>(handle);
  return env->NewStringUTF(e->ToString().c_str());
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_exception_NativeRayException_nativeSerialize(JNIEnv *env, jclass,
                                                                 jlong handle) {
  auto e = reinterpret_cast<ray::RayException *>(handle);
  return NativeStringToJavaByteArray(env, e->Serialize());
}

JNIEXPORT jbyteArray JNICALL Java_io_ray_runtime_exception_NativeRayException_nativeData(
    JNIEnv *env, jclass, jlong handle) {
  auto e = reinterpret_cast<ray::RayException *>(handle);
  return NativeStringToJavaByteArray(env, e->Data());
}
