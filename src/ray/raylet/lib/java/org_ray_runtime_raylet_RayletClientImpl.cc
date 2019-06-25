#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/raylet/raylet_client.h"

inline RayletClient &GetRayletClient(jlong nativeCoreWorker) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker)->Raylet();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativePrepareCheckpoint
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativePrepareCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorker, jbyteArray actorId) {
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  ActorCheckpointID checkpoint_id;
  auto status = GetRayletClient(nativeCoreWorker)
                    .PrepareActorCheckpoint(actor_id.GetId(), checkpoint_id);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorker, jbyteArray actorId,
    jbyteArray checkpointId) {
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  UniqueIdFromJByteArray<ActorCheckpointID> checkpoint_id(env, checkpointId);
  auto status =
      GetRayletClient(nativeCoreWorker)
          .NotifyActorResumedFromCheckpoint(actor_id.GetId(), checkpoint_id.GetId());
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSetResource(
    JNIEnv *env, jclass, jlong nativeCoreWorker, jstring resourceName, jdouble capacity,
    jbyteArray nodeId) {
  UniqueIdFromJByteArray<ClientID> node_id(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = GetRayletClient(nativeCoreWorker)
                    .SetResource(native_resource_name, static_cast<double>(capacity),
                                 node_id.GetId());
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
