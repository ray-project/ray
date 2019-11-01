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

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetLanguage
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeGetLanguage(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  
  ray::Language language;
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  auto status = GetCoreWorker(nativeCoreWorkerPointer).ActorLanguage(actor_id, &language);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (jint)0);
  return (jint)language;
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeIsDirectCallActor
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeIsDirectCallActor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  bool is_direct_call = false;
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  auto status = GetCoreWorker(nativeCoreWorkerPointer).IsDirectCallActor(actor_id, &is_direct_call);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, false);
  return is_direct_call;
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetActorCreationTaskFunctionDescriptor
 * Signature: (J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeGetActorCreationTaskFunctionDescriptor(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  std::vector<std::string> function_descriptor;
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  auto status = GetCoreWorker(nativeCoreWorkerPointer)
      .ActorCreationTaskFunctionDescriptor(actor_id, &function_descriptor);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeStringVectorToJavaStringList(env, function_descriptor);
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeSerialize
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeSerialize(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  std::string output;
  ray::Status status = GetCoreWorker(nativeCoreWorkerPointer)
      .SerializeActorHandle(actor_id, &output);  
  jbyteArray bytes = env->NewByteArray(output.size());
  env->SetByteArrayRegion(bytes, 0, output.size(),
                          reinterpret_cast<const jbyte *>(output.c_str()));
  return bytes;
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeDeserialize
 * Signature: ([B)J
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeDeserialize(
    JNIEnv *env, jclass o, jlong nativeCoreWorkerPointer, jbyteArray data) {
  auto buffer = JavaByteArrayToNativeBuffer(env, data);
  RAY_CHECK(buffer->Size() > 0);
  auto binary = std::string(reinterpret_cast<char *>(buffer->Data()), buffer->Size());  
  auto actor_id = GetCoreWorker(nativeCoreWorkerPointer)
      .DeserializeAndRegisterActorHandle(binary);

  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
