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

#include "io_ray_runtime_actor_NativeActorHandle.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL Java_io_ray_runtime_actor_NativeActorHandle_nativeGetLanguage(
    JNIEnv *env, jclass o, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  const auto native_actor_handle =
      ray::CoreWorkerProcess::GetCoreWorker().GetActorHandle(actor_id);
  return native_actor_handle->ActorLanguage();
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_actor_NativeActorHandle_nativeGetActorCreationTaskFunctionDescriptor(
    JNIEnv *env, jclass o, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  const auto native_actor_handle =
      ray::CoreWorkerProcess::GetCoreWorker().GetActorHandle(actor_id);
  auto function_descriptor = native_actor_handle->ActorCreationTaskFunctionDescriptor();
  return NativeRayFunctionDescriptorToJavaStringList(env, function_descriptor);
}

JNIEXPORT jbyteArray JNICALL Java_io_ray_runtime_actor_NativeActorHandle_nativeSerialize(
    JNIEnv *env, jclass o, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  std::string output;
  ObjectID actor_handle_id;
  ray::Status status = ray::CoreWorkerProcess::GetCoreWorker().SerializeActorHandle(
      actor_id, &output, &actor_handle_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeStringToJavaByteArray(env, output);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_actor_NativeActorHandle_nativeDeserialize(JNIEnv *env, jclass o,
                                                              jbyteArray data) {
  auto buffer = JavaByteArrayToNativeBuffer(env, data);
  RAY_CHECK(buffer->Size() > 0);
  auto binary = std::string(reinterpret_cast<char *>(buffer->Data()), buffer->Size());
  auto actor_id =
      ray::CoreWorkerProcess::GetCoreWorker().DeserializeAndRegisterActorHandle(
          binary, /*outer_object_id=*/ObjectID::Nil());

  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
