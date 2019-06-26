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
 * Method:    fork
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayActorImpl_fork(JNIEnv *env, jclass o,
                                                              jlong nativeActorHandle) {
  auto new_actor_handle = GetActorHandle(nativeActorHandle).Fork();
  // TODO (kfstorm): free ActorHandle
  return reinterpret_cast<jlong>(new_actor_handle.release());
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    getActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_getActorId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return JByteArrayFromUniqueId<ray::ActorID>(env,
                                              GetActorHandle(nativeActorHandle).ActorID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    getActorHandleId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_getActorHandleId(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return JByteArrayFromUniqueId<ray::ActorHandleID>(
             env, GetActorHandle(nativeActorHandle).ActorHandleID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    getLanguage
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_RayActorImpl_getLanguage(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return (jint)GetActorHandle(nativeActorHandle).ActorLanguage();
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    getActorDefinitionDescriptor
 * Signature: (J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_RayActorImpl_getActorDefinitionDescriptor(
    JNIEnv *env, jclass o, jlong nativeActorHandle) {
  return NativeStringVectorToJavaStringList(
      env, GetActorHandle(nativeActorHandle).ActorDefinitionDescriptor());
}

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    serialize
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_RayActorImpl_serialize(
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
 * Method:    deserialize
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayActorImpl_deserialize(JNIEnv *env,
                                                                     jclass o,
                                                                     jbyteArray data) {
  auto binary = ReadBinary<std::string>(env, data, [](const ray::Buffer &buffer) {
    RAY_CHECK(buffer.Size() > 0);
    return std::string(reinterpret_cast<char *>(buffer.Data()), buffer.Size());
  });
  return reinterpret_cast<jlong>(ray::ActorHandle::Deserialize(binary).release());
}

#ifdef __cplusplus
}
#endif
