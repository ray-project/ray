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

#include "ray/core_worker/lib/java/org_ray_runtime_actor_NativeRayActor.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::CoreWorker &GetCoreWorker(jlong nativeCoreWorkerPointer) {
  return *reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeGetLanguage(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  ray::ActorHandle *native_actor_handle = nullptr;
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
                    .GetActorHandle(actor_id, &native_actor_handle);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, false);
  return native_actor_handle->ActorLanguage();
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeGetActorCreationTaskFunctionDescriptor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  ray::ActorHandle *native_actor_handle = nullptr;
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
                    .GetActorHandle(actor_id, &native_actor_handle);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  auto function_descriptor = native_actor_handle->ActorCreationTaskFunctionDescriptor();
  return NativeRayFunctionDescriptorToJavaStringList(env, function_descriptor);
}

JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeSerialize(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  std::string output;
  ObjectID actor_handle_id;
  ray::Status status = GetCoreWorker(nativeCoreWorkerPointer)
                           .SerializeActorHandle(actor_id, &output, &actor_handle_id);
  jbyteArray bytes = env->NewByteArray(output.size());
  env->SetByteArrayRegion(bytes, 0, output.size(),
                          reinterpret_cast<const jbyte *>(output.c_str()));
  return bytes;
}

JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeDeserialize(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray data) {
  auto buffer = JavaByteArrayToNativeBuffer(env, data);
  RAY_CHECK(buffer->Size() > 0);
  auto binary = std::string(reinterpret_cast<char *>(buffer->Data()), buffer->Size());
  auto actor_id =
      GetCoreWorker(nativeCoreWorkerPointer)
          .DeserializeAndRegisterActorHandle(binary, /*outer_object_id=*/ObjectID::Nil());

  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
