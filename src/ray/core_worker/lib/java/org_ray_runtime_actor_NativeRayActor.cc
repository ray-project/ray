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

JNIEXPORT jboolean JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeIsDirectCallActor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  ray::ActorHandle *native_actor_handle = nullptr;
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
                    .GetActorHandle(actor_id, &native_actor_handle);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, false);
  return native_actor_handle->IsDirectCallActor();
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
  return NativeStringVectorToJavaStringList(env, function_descriptor);
}

JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeSerialize(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  std::string output;
  ray::Status status =
      GetCoreWorker(nativeCoreWorkerPointer).SerializeActorHandle(actor_id, &output);
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
      GetCoreWorker(nativeCoreWorkerPointer).DeserializeAndRegisterActorHandle(binary);

  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
