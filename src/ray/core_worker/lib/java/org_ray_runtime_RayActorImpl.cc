#include "ray/core_worker/lib/java/org_ray_runtime_RayActorImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/task_interface.h"

inline ray::ActorHandle &GetActorHandle(jlong nativeActorHandle) {
  return *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeFork
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayActorImpl_nativeFork(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  auto new_actor_handle = GetActorHandle(nativeActorHandle).Fork();
  return reinterpret_cast<jlong>(new ray::ActorHandle(new_actor_handle));
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeGetActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_nativeGetActorId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return JByteArrayFromUniqueId<ray::ActorID>(env,
                                              GetActorHandle(nativeActorHandle).ActorID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeGetActorHandleId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_nativeGetActorHandleId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return JByteArrayFromUniqueId<ray::ActorHandleID>(
             env, GetActorHandle(nativeActorHandle).ActorHandleID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeGetLanguage
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_RayActorImpl_nativeGetLanguage(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return (jint)GetActorHandle(nativeActorHandle).ActorLanguage();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeGetActorCreationTaskFunctionDescriptor
 * Signature: (J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL
Java_org_ray_runtime_RayActorImpl_nativeGetActorCreationTaskFunctionDescriptor(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return NativeStringVectorToJavaStringList(
      env, GetActorHandle(nativeActorHandle).ActorCreationTaskFunctionDescriptor());
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeSerialize
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_nativeSerialize(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  std::string output;
  GetActorHandle(nativeActorHandle).Serialize(&output);
  jbyteArray bytes = env->NewByteArray(output.size());
  env->SetByteArrayRegion(bytes, 0, output.size(),
                          reinterpret_cast<const jbyte *>(output.c_str()));
  return bytes;
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeDeserialize
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayActorImpl_nativeDeserialize(
    JNIEnv *env, jclass o, jbyteArray data) {
  auto binary = ReadBinary<std::string>(env, data, [](const ray::Buffer &buffer) {
    RAY_CHECK(buffer.Size() > 0);
    return std::string(reinterpret_cast<char *>(buffer.Data()), buffer.Size());
  });
  return reinterpret_cast<jlong>(
      new ray::ActorHandle(ray::ActorHandle::Deserialize(binary)));
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    nativeFree
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayActorImpl_nativeFree(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  delete &GetActorHandle(nativeActorHandle);
}

#ifdef __cplusplus
}
#endif
