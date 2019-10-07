#include "ray/core_worker/lib/java/org_ray_runtime_actor_NativeRayActor.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/core_worker/task_interface.h"

inline ray::ActorHandle &GetActorHandle(jlong nativeActorHandle) {
  return *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeFork
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeFork(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return reinterpret_cast<jlong>(GetActorHandle(nativeActorHandle).Fork().release());
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeGetActorId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return IdToJavaByteArray<ray::ActorID>(env,
                                         GetActorHandle(nativeActorHandle).GetActorID());
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetActorHandleId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeGetActorHandleId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return IdToJavaByteArray<ray::ActorHandleID>(
      env, GetActorHandle(nativeActorHandle).GetActorHandleID());
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetLanguage
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeGetLanguage(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return (jint)GetActorHandle(nativeActorHandle).ActorLanguage();
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeIsDirectCallActor
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeIsDirectCallActor(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return GetActorHandle(nativeActorHandle).IsDirectCallActor();
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeGetActorCreationTaskFunctionDescriptor
 * Signature: (J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL
Java_org_ray_runtime_actor_NativeRayActor_nativeGetActorCreationTaskFunctionDescriptor(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return NativeStringVectorToJavaStringList(
      env, GetActorHandle(nativeActorHandle).ActorCreationTaskFunctionDescriptor());
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeSerialize
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeSerialize(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  std::string output;
  GetActorHandle(nativeActorHandle).Serialize(&output);
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
JNIEXPORT jlong JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeDeserialize(
    JNIEnv *env, jclass o, jbyteArray data) {
  auto buffer = JavaByteArrayToNativeBuffer(env, data);
  RAY_CHECK(buffer->Size() > 0);
  auto binary = std::string(reinterpret_cast<char *>(buffer->Data()), buffer->Size());
  return reinterpret_cast<jlong>(new ray::ActorHandle(binary, TaskID::Nil()));
}

/*
 * Class:     org_ray_runtime_actor_NativeRayActor
 * Method:    nativeFree
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_actor_NativeRayActor_nativeFree(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  delete &GetActorHandle(nativeActorHandle);
}

#ifdef __cplusplus
}
#endif
