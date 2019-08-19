#include "ray/core_worker/lib/java/org_ray_runtime_raylet_NativeRayletClient.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/raylet/raylet_client.h"

inline RayletClient &GetRayletClientFromPointer(jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->GetRayletClient();
}

#ifdef __cplusplus
extern "C" {
#endif

using ray::ClientID;

/*
 * Class:     org_ray_runtime_raylet_NativeRayletClient
 * Method:    nativePrepareCheckpoint
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_NativeRayletClient_nativePrepareCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray actorId) {
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  ActorCheckpointID checkpoint_id;
  auto status = GetRayletClientFromPointer(nativeCoreWorkerPointer)
                    .PrepareActorCheckpoint(actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_NativeRayletClient
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_NativeRayletClient_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray actorId,
    jbyteArray checkpointId) {
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  const auto checkpoint_id = JavaByteArrayToId<ActorCheckpointID>(env, checkpointId);
  auto status = GetRayletClientFromPointer(nativeCoreWorkerPointer)
                    .NotifyActorResumedFromCheckpoint(actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_raylet_NativeRayletClient
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_NativeRayletClient_nativeSetResource(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jstring resourceName,
    jdouble capacity, jbyteArray nodeId) {
  const auto node_id = JavaByteArrayToId<ClientID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status =
      GetRayletClientFromPointer(nativeCoreWorkerPointer)
          .SetResource(native_resource_name, static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
